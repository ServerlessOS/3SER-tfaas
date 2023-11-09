package CacheClient

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/lechou-0/common/util"
	zmq "github.com/pebbe/zmq4"
)

type SocketPool struct {
	ctx          *zmq.Context
	sockets      []*zmq.Socket
	availist     []bool
	listMutex    sync.Mutex
	base_address string
}

func CreateSP(ctx *zmq.Context, num int, action string, address string) *SocketPool {
	socketpool := &SocketPool{
		ctx:          ctx,
		sockets:      make([]*zmq.Socket, num),
		availist:     make([]bool, num),
		listMutex:    sync.Mutex{},
		base_address: address,
	}
	if action == "get" {
		for i := 0; i < num; i++ {
			get, err := ctx.NewSocket(zmq.PUSH)
			if err != nil {
				log.Fatalf("Unexpected error while generating socket: %v", err)
			}
			err = get.Connect(util.Cache_get_bind_proc())
			if err != nil {
				log.Fatalf("Unexpected error while connectting socket: %v", err)
			}
			socketpool.sockets[i] = get
			socketpool.availist[i] = true
		}
	} else if action == "set" {
		for i := 0; i < num; i++ {
			set, err := ctx.NewSocket(zmq.PUSH)
			if err != nil {
				log.Fatalf("Unexpected error while generating socket: %v", err)
			}
			err = set.Connect(util.Cache_set_bind_proc())
			if err != nil {
				log.Fatalf("Unexpected error while connectting socket: %v", err)
			}
			socketpool.sockets[i] = set
			socketpool.availist[i] = true
		}
	} else if action == "gets" {
		for i := 0; i < num; i++ {
			gets, err := ctx.NewSocket(zmq.PUSH)
			if err != nil {
				log.Fatalf("Unexpected error while generating socket: %v", err)
			}
			err = gets.Connect(util.Cache_session_bind_proc())
			if err != nil {
				log.Fatalf("Unexpected error while connectting socket: %v", err)
			}
			socketpool.sockets[i] = gets
			socketpool.availist[i] = true
		}
	} else if action == "abort" {
		for i := 0; i < num; i++ {
			abort, err := ctx.NewSocket(zmq.PUSH)
			if err != nil {
				log.Fatalf("Unexpected error while generating socket: %v", err)
			}
			err = abort.Connect(util.Cache_abort_bind_proc())
			if err != nil {
				log.Fatalf("Unexpected error while connectting socket: %v", err)
			}
			socketpool.sockets[i] = abort
			socketpool.availist[i] = true
		}
	} else if action == "commit" {
		for i := 0; i < num; i++ {
			commit, err := ctx.NewSocket(zmq.PUSH)
			if err != nil {
				log.Fatalf("Unexpected error while generating socket: %v", err)
			}
			err = commit.Connect(util.Cache_commit_bind_proc())
			if err != nil {
				log.Fatalf("Unexpected error while connectting socket: %v", err)
			}
			socketpool.sockets[i] = commit
			socketpool.availist[i] = true
		}
	} else if action == "receiver" {
		for i := 0; i < num; i++ {
			ipcaddress := "ipc://" + address + strconv.Itoa(i) + ".ipc"
			receiver, err := ctx.NewSocket(zmq.PULL)
			if err != nil {
				log.Fatalf("Unexpected error while generating socket: %v", err)
			}
			err = receiver.Bind(ipcaddress)
			if err != nil {
				log.Fatalf("Unexpected error while binding socket: %v", err)
			}
			os.Chmod(address+strconv.Itoa(i)+".ipc", 0777)
			socketpool.sockets[i] = receiver
			socketpool.availist[i] = true
		}
	}
	return socketpool
}

func (sp *SocketPool) GetOut(action string) (*zmq.Socket, int) {
	sp.listMutex.Lock()
	for i, value := range sp.availist {
		if value {
			sp.availist[i] = false
			sp.listMutex.Unlock()
			return sp.sockets[i], i
		}
	}
	index := len(sp.availist)
	if action == "get" {
		get, err := sp.ctx.NewSocket(zmq.PUSH)
		if err != nil {
			log.Fatalf("Unexpected error while generating socket: %v", err)
		}
		err = get.Connect(util.Cache_get_bind_proc())
		if err != nil {
			log.Fatalf("Unexpected error while connectting socket: %v", err)
		}
		sp.sockets = append(sp.sockets, get)
		sp.availist = append(sp.availist, false)
		sp.listMutex.Unlock()
		return get, index

	} else if action == "set" {
		set, err := sp.ctx.NewSocket(zmq.PUSH)
		if err != nil {
			log.Fatalf("Unexpected error while generating socket: %v", err)
		}
		err = set.Connect(util.Cache_set_bind_proc())
		if err != nil {
			log.Fatalf("Unexpected error while connectting socket: %v", err)
		}
		sp.sockets = append(sp.sockets, set)
		sp.availist = append(sp.availist, false)
		sp.listMutex.Unlock()
		return set, index

	} else if action == "gets" {
		gets, err := sp.ctx.NewSocket(zmq.PUSH)
		if err != nil {
			log.Fatalf("Unexpected error while generating socket: %v", err)
		}
		err = gets.Connect(util.Cache_session_bind_proc())
		if err != nil {
			log.Fatalf("Unexpected error while connectting socket: %v", err)
		}
		sp.sockets = append(sp.sockets, gets)
		sp.availist = append(sp.availist, false)
		sp.listMutex.Unlock()
		return gets, index

	} else if action == "abort" {
		abort, err := sp.ctx.NewSocket(zmq.PUSH)
		if err != nil {
			log.Fatalf("Unexpected error while generating socket: %v", err)
		}
		err = abort.Connect(util.Cache_abort_bind_proc())
		if err != nil {
			log.Fatalf("Unexpected error while connectting socket: %v", err)
		}
		sp.sockets = append(sp.sockets, abort)
		sp.availist = append(sp.availist, false)
		sp.listMutex.Unlock()
		return abort, index

	} else if action == "commit" {
		commit, err := sp.ctx.NewSocket(zmq.PUSH)
		if err != nil {
			log.Fatalf("Unexpected error while generating socket: %v", err)
		}
		err = commit.Connect(util.Cache_commit_bind_proc())
		if err != nil {
			log.Fatalf("Unexpected error while connectting socket: %v", err)
		}
		sp.sockets = append(sp.sockets, commit)
		sp.availist = append(sp.availist, false)
		sp.listMutex.Unlock()
		return commit, index

	} else if action == "receiver" {
		ipcaddress := "ipc://" + sp.base_address + strconv.Itoa(index) + ".ipc"
		receiver, err := sp.ctx.NewSocket(zmq.PULL)
		if err != nil {
			log.Fatalf("Unexpected error while generating socket: %v", err)
		}
		err = receiver.Bind(ipcaddress)
		if err != nil {
			log.Fatalf("Unexpected error while binding socket: %v", err)
		}
		os.Chmod(sp.base_address+strconv.Itoa(index)+".ipc", 0777)
		sp.sockets = append(sp.sockets, receiver)
		sp.availist = append(sp.availist, false)
		sp.listMutex.Unlock()
		return receiver, index

	}
	sp.listMutex.Unlock()
	log.Println("GetOut: bug")
	return nil, -1
}

func (sp *SocketPool) PutBack(seqnum int) {
	sp.availist[seqnum] = true
}

func (sp *SocketPool) CloseSocket() {
	for _, soc := range sp.sockets {
		soc.Close()
	}
}

type ConnectionCache struct {
	ctx           *zmq.Context
	getPushers    *SocketPool
	setPushers    *SocketPool
	getsPushers   *SocketPool
	commitPushers *SocketPool
	abortPushers  *SocketPool
	receivers     *SocketPool
	BaseAddress   string
	CacheAddress  string
}

func RandStr(length int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	result := []byte{}
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < length; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}

func CreateCC() *ConnectionCache {
	zctx, err := zmq.NewContext()
	if err != nil {
		log.Fatalf("Unexpected error while creating zmq context: %v", err)
	}
	baddress := "/requests/" + RandStr(10) + "_"
	cc := &ConnectionCache{
		ctx:           zctx,
		getPushers:    CreateSP(zctx, 3, "get", ""),
		setPushers:    CreateSP(zctx, 3, "set", ""),
		getsPushers:   CreateSP(zctx, 3, "gets", ""),
		commitPushers: CreateSP(zctx, 3, "commit", ""),
		abortPushers:  CreateSP(zctx, 3, "abort", ""),
		receivers:     CreateSP(zctx, 5, "receiver", baddress),
		BaseAddress:   baddress,
	}
	register, err := zctx.NewSocket(zmq.PUSH)
	if err != nil {
		log.Fatalf("Unexpected error while generating socket: %v", err)
	}
	err = register.Connect(util.Cache_register_bind_proc())
	if err != nil {
		log.Fatalf("Unexpected error while connectting socket: %v", err)
	}
	for i := range cc.receivers.sockets {
		ipcaddress := "ipc://" + baddress + strconv.Itoa(i) + ".ipc"
		register.Send(ipcaddress, 1)
	}
	register.Close()
	var cacheAddress string
	for _, receiver := range cc.receivers.sockets {
		cacheAddress, err = receiver.Recv(0)
		if err != nil {
			log.Fatalf("Unexpected error while receiving socket: %v", err)
		}
	}
	cc.CacheAddress = cacheAddress
	return cc
}

func (cc *ConnectionCache) NewGetPusher() (*zmq.Socket, int) {
	return cc.getPushers.GetOut("get")
}

func (cc *ConnectionCache) CloseGetPusher(seqnum int) {
	cc.getPushers.PutBack(seqnum)
}

func (cc *ConnectionCache) NewGetSPusher() (*zmq.Socket, int) {
	return cc.getsPushers.GetOut("gets")
}

func (cc *ConnectionCache) CloseGetSPusher(seqnum int) {
	cc.getsPushers.PutBack(seqnum)
}

func (cc *ConnectionCache) NewSetPusher() (*zmq.Socket, int) {
	return cc.setPushers.GetOut("set")
}

func (cc *ConnectionCache) CloseSetPusher(seqnum int) {
	cc.setPushers.PutBack(seqnum)
}

func (cc *ConnectionCache) NewCommitPusher() (*zmq.Socket, int) {
	return cc.commitPushers.GetOut("commit")
}

func (cc *ConnectionCache) CloseCommitPusher(seqnum int) {
	cc.commitPushers.PutBack(seqnum)
}

func (cc *ConnectionCache) NewAbortPusher() (*zmq.Socket, int) {
	return cc.abortPushers.GetOut("abort")
}

func (cc *ConnectionCache) CloseAbortPusher(seqnum int) {
	cc.abortPushers.PutBack(seqnum)
}

func (cc *ConnectionCache) NewReceiver() (*zmq.Socket, int) {
	return cc.receivers.GetOut("receiver")
}

func (cc *ConnectionCache) CloseReceiver(seqnum int) {
	cc.receivers.PutBack(seqnum)
}

func (cc *ConnectionCache) CloseConnection() {
	cc.getPushers.CloseSocket()
	cc.setPushers.CloseSocket()
	cc.commitPushers.CloseSocket()
	cc.abortPushers.CloseSocket()
	cc.receivers.CloseSocket()
	cc.ctx.Term()
}
