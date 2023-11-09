/*
 * @Author: lechoux lechoux@qq.com
 * @Date: 2022-08-17 22:42:23
 * @LastEditors: lechoux lechoux@qq.com
 * @LastEditTime: 2022-11-20 23:50:23
 * @Description:
 */
package main

import (
	"context"
	"math/rand"
	"os"
	"strings"
	"time"

	//"encoding/json"
	"log"
	"strconv"
	"sync"

	pb "github.com/lechou-0/common/proto"
	"github.com/lechou-0/common/util"

	cmap "wsh/middleshim/cmap"

	"github.com/lechou-0/common/storage"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/golang/protobuf/ptypes/empty"
	zmq "github.com/pebbe/zmq4"
	"golang.org/x/sync/singleflight"
)

var addresses []string

type MiddleshimServer struct {
	pb.UnimplementedMiddleshimServer
	Zctx                *zmq.Context
	updateCache         map[string]*SocketPool
	updateCacheLock     sync.RWMutex
	metadata            cmap.MetadataMap
	tmpState            cmap.TmpstateMap
	keyCacheAddress     map[string]*addressList
	keyCacheAddressLock sync.RWMutex
	IpAddress           string
	time                int64
	timeLock            sync.Mutex
	KeyLock             cmap.LockMap
	sf                  singleflight.Group
	etcdClient          *clientv3.Client
	StorageManagers     map[string]storage.StorageManager
}

type addressList struct {
	sync.RWMutex
	addresses []string
}

func NewMiddleshimServer(client *clientv3.Client) *MiddleshimServer {
	rand.Seed(time.Now().UnixNano())
	ipAddress := util.GetHostByEnv()
	log.Println(ipAddress)
	ctx, _ := zmq.NewContext()
	getResp, err := client.Get(context.TODO(), ipAddress)
	if err != nil {
		log.Println(err)
	}
	mtime := int64(0)
	if len(getResp.Kvs) != 0 {
		mtime, _ = strconv.ParseInt(util.Bytes2String(getResp.Kvs[0].Value), 10, 64)
	} else {
		client.Put(context.TODO(), ipAddress, strconv.FormatInt(0, 10))
	}

	server := &MiddleshimServer{
		IpAddress:           ipAddress,
		Zctx:                ctx,
		time:                mtime,
		timeLock:            sync.Mutex{},
		updateCache:         map[string]*SocketPool{},
		updateCacheLock:     sync.RWMutex{},
		metadata:            cmap.NewMetadataMap(),
		tmpState:            cmap.NewTmpstateMap(),
		keyCacheAddress:     map[string]*addressList{},
		keyCacheAddressLock: sync.RWMutex{},
		KeyLock:             cmap.NewLockMap(),
		sf:                  singleflight.Group{},
		StorageManagers:     map[string]storage.StorageManager{},
		etcdClient:          client,
	}
	StorageManager1 := storage.NewDynamoStorageManager("test")
	StorageManager2 := storage.NewDynamoStorageManager(ipAddress)
	// StorageManager1 := storage.NewMongoStorageManager("test")
	// StorageManager2 := storage.NewMongoStorageManager(ipAddress)
	server.StorageManagers["data"] = StorageManager1
	server.StorageManagers["tmpstate"] = StorageManager2

	uri, exists := os.LookupEnv("caches")
	if !exists {
		log.Fatal("caches not found")
	}

	addresses = strings.Split(uri, ",")
	for _, cacheAddress := range addresses {
		server.updateCache[cacheAddress] = CreateSP(server.Zctx, 3, "tcp://"+cacheAddress+":"+util.UpdatePort)
	}
	// exist, err := StorageManager3.TableExists()
	// if err != nil {
	// 	log.Fatalf("Unexpected error while determining existence of table: %v", err)
	// }
	// if !exist {
	// 	err = server.StorageManagers["tmpstate"].CreateTable()
	// 	if err != nil {
	// 		log.Fatalf("Unexpected error while creating table: %v", err)
	// 	}
	// }

	//init WRBuffer from dynomadb

	log.Println("init success")
	return server
}

/**
 * @Author: lechoux lechoux@qq.com
 * @description: Final asynchronous update(update or abort) in 2pc .Lock loss does not occur due to fault-tolerant recovery mechanisms
 * @param {context.Context} ctx
 * @param {*pb.TRecord} in : tid and writeset
 * @return {*}
 */
func (middleServer *MiddleshimServer) Update(ctx context.Context, in *pb.KeyValuePair) (*emptypb.Empty, error) {
	key := in.GetKey()
	tid := in.GetTid()
	ctime := in.GetWTimestamp()
	if ctime == -1 {
		middleServer.tmpState.Remove(key, tid)
		middleServer.KeyLock.UnLock(key, tid, cmap.Committing)
		err := middleServer.StorageManagers["tmpstate"].DeleteState(tid)
		if err != nil {
			log.Println(err)
			return nil, err
		}
	} else {
		serialized, ok := middleServer.tmpState.Get(key)
		if !ok {
			log.Printf("%s State loss", key)
		}
		if serialized.Tid != tid {
			log.Printf("%s Update nil", key)
		}
		go func(bkv []byte) {
			err := middleServer.syncCache(key, bkv)
			if err != nil {
				log.Println(err)
			}
		}(serialized.Value)
		err := middleServer.StorageManagers["data"].Put(key, serialized.Value)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		middleServer.metadata.Set(key, &pb.KeyValuePair{
			Key:        key,
			WTimestamp: ctime,
			RTimestamp: ctime,
		})

		middleServer.KeyLock.UnLock(key, tid, cmap.Committing)
		middleServer.tmpState.Remove(key, tid)
		// 可从临时状态中恢复
		if ctime > middleServer.time {
			err := middleServer.setTime(ctime)
			if err != nil {
				log.Println(err)
				return nil, err
			}
		}

		middleServer.StorageManagers["tmpstate"].DeleteState(tid)
		if err != nil {
			log.Println(err)
			return nil, err
		}
	}
	return &empty.Empty{}, nil
}

func (middleServer *MiddleshimServer) CacheMiss(ctx context.Context, in *pb.KeyRequest) (*emptypb.Empty, error) {
	cacheAddress := in.GetResponseAddress()

	middleServer.updateCacheLock.Lock()
	_, ok := middleServer.updateCache[cacheAddress]
	if !ok {
		middleServer.updateCache[cacheAddress] = CreateSP(middleServer.Zctx, 3, "tcp://"+cacheAddress+":"+util.UpdatePort)
	}
	middleServer.updateCacheLock.Unlock()

	middleServer.keyCacheAddressLock.RLock()
	addresses, ok := middleServer.keyCacheAddress[in.GetKey()]
	middleServer.keyCacheAddressLock.RUnlock()
	if ok {
		addresses.Lock()
		addresses.addresses = append(addresses.addresses, cacheAddress)
		addresses.Unlock()
	} else {
		middleServer.keyCacheAddressLock.Lock()
		middleServer.keyCacheAddress[in.GetKey()] = &addressList{addresses: []string{cacheAddress}}
		middleServer.keyCacheAddressLock.Unlock()
	}

	// jsonByte, err := json.Marshal(middleServer.keyCacheAddress)
	// if err != nil {
	// 	log.Printf("Marshal with error: %+v\n", err)
	// 	return nil, err
	// }
	// middleServer.etcdClient.Put(ctx, middleServer.IpAddress+"-cachelist", util.Bytes2String(jsonByte))
	return &empty.Empty{}, nil
}

func (middleServer *MiddleshimServer) getSC(key string) (*pb.KeyValuePair, error) {

	storageManager := middleServer.StorageManagers["data"]

	vb, err := storageManager.GetSC(key)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	if vb == nil {
		return &pb.KeyValuePair{Key: key}, nil
	}
	kv := &pb.KeyValuePair{}
	err = proto.Unmarshal(vb, kv)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return kv, nil
}

/**
 * @Author: lechoux lechoux@qq.com
 * @description: write lock
 * @param {context.Context} ctx
 * @param {*pb.KeyRequest} in
 * @param {bool} status: true: read  false: write
 * @return {*}
 */
func (middleServer *MiddleshimServer) Lock(ctx context.Context, in *pb.KeyRequest) (*pb.KeyResponse, error) {
	key := in.GetKey()
	tid := in.GetTid()
	keyResponse := &pb.KeyResponse{}
	if middleServer.KeyLock.Lock(key, tid, cmap.Writing) {
		keyResponse.Error = pb.CacheError_NO_ERROR
		if mk, ok := middleServer.metadata.Get(key); ok {
			keyResponse.WTimestamp = mk.GetWTimestamp()
			keyResponse.RTimestamp = mk.GetRTimestamp()
		} else {
			kR, err := middleServer.getSC(key)

			if err == nil {
				kR.Value = nil
				if middleServer.time > kR.RTimestamp {
					kR.RTimestamp = middleServer.time
				}
				keyResponse.WTimestamp = kR.GetWTimestamp()
				keyResponse.RTimestamp = kR.GetRTimestamp()
				middleServer.metadata.Set(key, kR)
			} else {
				log.Println(err)
				return nil, err
			}
		}
	} else {
		keyResponse.Error = pb.CacheError_ABORT
	}

	return keyResponse, nil
}

// func (middleServer *MiddleshimServer) syncCache(key string, bkv []byte) error {
// 	middleServer.keyCacheAddressLock.RLock()
// 	cacheAddresses, ok := middleServer.keyCacheAddress[key]
// 	middleServer.keyCacheAddressLock.RUnlock()
// 	if ok {
// 		cacheAddresses.RLock()
// 		for _, address := range cacheAddresses.addresses {
// 			updateSocketPool := middleServer.updateCache[address]
// 			updateSocket, seqnum := updateSocketPool.GetOut()

// 			_, err := updateSocket.SendBytes(bkv, 1)
// 			if err != nil {
// 				cacheAddresses.RUnlock()
// 				log.Printf("Unexpected error while sending update message: %v", err)
// 				return err
// 			}
// 			updateSocketPool.PutBack(seqnum)
// 		}
// 		cacheAddresses.RUnlock()
// 	}
// 	return nil
// }

func (middleServer *MiddleshimServer) syncCache(key string, bkv []byte) error {
	for _, address := range addresses {
		updateSocketPool := middleServer.updateCache[address]
		updateSocket, seqnum := updateSocketPool.GetOut()

		_, err := updateSocket.SendBytes(bkv, 1)
		if err != nil {
			log.Printf("Unexpected error while sending update message: %v", err)
			return err
		}
		updateSocketPool.PutBack(seqnum)
	}
	return nil
}

/**
 * @Author: lechoux lechoux@qq.com
 * @description:
 * @param {context.Context} ctx
 * @param {*pb.TRecord} in : only readset
 * @return {*}
 */
func (middleServer *MiddleshimServer) Validate1(ctx context.Context, in *pb.KeyValuePair) (*pb.ErrorResponse, error) {

	key := in.GetKey()
	tid := in.GetTid()

	if !middleServer.KeyLock.Lock(key, tid, cmap.Validating) {
		log.Printf("%s v1 abort: %s 互斥锁冲突，加锁失败", tid, key)
		return &pb.ErrorResponse{Error: pb.CacheError_ABORT}, nil
	} else {
		var wt, rt int64
		if mk, ok := middleServer.metadata.Get(key); ok {
			wt = mk.GetWTimestamp()
			rt = mk.GetRTimestamp()
			if in.GetWTimestamp() != wt {
				middleServer.KeyLock.UnLock(key, tid, cmap.Validating)
				log.Printf("%s v1 abort: %s 读到的旧值，无法扩展租约", tid, key)
				return &pb.ErrorResponse{Error: pb.CacheError_ABORT}, nil
			}
			if in.GetRTimestamp() > rt {
				if in.GetRTimestamp() > middleServer.time {
					err := middleServer.setTime(in.GetRTimestamp())
					if err != nil {
						log.Println(err)
						middleServer.KeyLock.UnLock(key, tid, cmap.Validating)
						return nil, err
					}
				}

				// mk的rt可能在其他事务V1阶段被修改，所以要加锁,go func参数为in，因为mk可能被修改
				shard := middleServer.metadata.GetShard(key)
				shard.Lock()
				if mk.GetRTimestamp() < in.GetRTimestamp() {
					mk.RTimestamp = in.GetRTimestamp()
					go func(in *pb.KeyValuePair) {
						bkv, err := proto.Marshal(in)
						if err != nil {
							log.Printf("Unexpected error while Marshaling kv: %v", err)
						}
						err = middleServer.syncCache(key, bkv)
						if err != nil {
							log.Println(err)
						}
					}(in)
				}
				shard.Unlock()
			}
		} else {
			// wt一定最新，rt不一定
			kR, err := middleServer.getSC(key)
			if err == nil {
				kR.RTimestamp = middleServer.time
				wt = kR.GetWTimestamp()
				rt = kR.GetRTimestamp()
				mk := &pb.KeyValuePair{
					Key:        key,
					WTimestamp: wt,
					RTimestamp: rt,
				}
				middleServer.metadata.Set(key, mk)
				go func(in *pb.KeyValuePair) {
					bkv, err := proto.Marshal(in)
					if err != nil {
						log.Printf("Unexpected error while Marshaling kv: %v", err)
					}
					err = middleServer.syncCache(key, bkv)
					if err != nil {
						log.Println(err)
					}
				}(kR)

				if in.GetWTimestamp() != wt {
					middleServer.KeyLock.UnLock(key, tid, cmap.Validating)
					log.Printf("%s v1 abort: %s 读到的旧值，无法扩展租约", tid, key)
					return &pb.ErrorResponse{Error: pb.CacheError_ABORT}, nil
				}
				if in.GetRTimestamp() > rt {
					if in.GetRTimestamp() > middleServer.time {
						err := middleServer.setTime(in.GetRTimestamp())
						if err != nil {
							middleServer.KeyLock.UnLock(key, tid, cmap.Validating)
							log.Println(err)
							return nil, err
						}
					}

					// mk的rt可能在其他事务V1阶段被修改，所以要加锁
					shard := middleServer.metadata.GetShard(key)
					shard.Lock()
					if mk.GetRTimestamp() < in.GetRTimestamp() {
						mk.RTimestamp = rt
						go func(in *pb.KeyValuePair) {
							bkv, err := proto.Marshal(in)
							if err != nil {
								log.Printf("Unexpected error while Marshaling kv: %v", err)
							}
							err = middleServer.syncCache(key, bkv)
							if err != nil {
								log.Println(err)
							}
						}(in)
					}
					shard.Unlock()
				}

			} else {
				middleServer.KeyLock.UnLock(key, tid, cmap.Validating)
				log.Println(err)
				return nil, err
			}
		}
	}
	//读锁会立刻解锁
	middleServer.KeyLock.UnLock(key, tid, cmap.Validating)
	return &pb.ErrorResponse{Error: pb.CacheError_NO_ERROR}, nil
}

/**
 * @Author: lechoux lechoux@qq.com
 * @description:
 * @param {context.Context} ctx
 * @param {*pb.TRecord} in
 * @return {*}
 */
func (middleServer *MiddleshimServer) Validate2(ctx context.Context, in *pb.KeyValuePair) (*pb.ErrorResponse, error) {
	//v2b := time.Now()
	tid := in.GetTid()

	isLock := middleServer.KeyLock.TestLock(in.GetKey(), tid)
	if !isLock {
		log.Printf("%s v2 abort: %s 锁丢失", tid, in.Key)
		return &pb.ErrorResponse{Error: pb.CacheError_ABORT}, nil
	}

	state, err := proto.Marshal(in)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	t1 := time.Now()
	middleServer.tmpState.Set(in.GetKey(), &cmap.State{Value: state, Tid: tid})
	t2 := time.Since(t1).Seconds()
	if t2 > 0.01 {
		log.Println(t2)
	}
	// tid + term
	// _, err = middleServer.etcdClient.Put(context.TODO(), tid+"_0", util.Bytes2String(state))
	// if err != nil {
	// 	log.Println(err)
	// 	return nil, err
	// }
	err = middleServer.StorageManagers["tmpstate"].PutState(tid, state)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	//v2e := time.Since(v2b)
	//log.Printf("v2时间：%f", v2e.Seconds())
	return &pb.ErrorResponse{Error: pb.CacheError_NO_ERROR}, nil
}

func (middleServer *MiddleshimServer) GetSession(ctx context.Context, in *pb.SessionRequest) (*pb.SessionRequest, error) {
	clientSession := in.GetClient()
	session, ok := middleServer.metadata.Get(clientSession)
	if ok {
		return &pb.SessionRequest{TimeFence: session.GetWTimestamp()}, nil
	} else {
		sessionkv := &pb.KeyValuePair{
			Key:        clientSession,
			WTimestamp: middleServer.time,
		}
		sessionkv.RTimestamp = sessionkv.GetWTimestamp()
		middleServer.metadata.Set(clientSession, sessionkv)
		return &pb.SessionRequest{TimeFence: sessionkv.GetWTimestamp()}, nil
	}
}

func (middleServer *MiddleshimServer) SetSession(ctx context.Context, in *pb.SessionRequest) (*emptypb.Empty, error) {
	clientSession := in.GetClient()
	timeFence := in.GetTimeFence()
	sessionkv := &pb.KeyValuePair{
		Key:        clientSession,
		WTimestamp: timeFence,
		RTimestamp: timeFence,
	}
	middleServer.metadata.Set(clientSession, sessionkv)
	return &empty.Empty{}, nil
}

func (middleServer *MiddleshimServer) CloseServer() {
	for _, v := range middleServer.updateCache {
		v.CloseSocket()
	}
	for _, v := range middleServer.StorageManagers {
		v.CloseManager()
	}
	middleServer.etcdClient.Close()
	middleServer.Zctx.Term()
}

type SocketPool struct {
	ctx       *zmq.Context
	sockets   []*zmq.Socket
	availist  []bool
	listMutex sync.Mutex
}

func CreateSP(ctx *zmq.Context, num int, address string) *SocketPool {
	socketpool := &SocketPool{
		ctx:       ctx,
		sockets:   make([]*zmq.Socket, num),
		availist:  make([]bool, num),
		listMutex: sync.Mutex{},
	}
	for i := 0; i < num; i++ {
		soc, err := ctx.NewSocket(zmq.PUSH)
		if err != nil {
			log.Fatalf("Unexpected error while generating socket: %v", err)
		}
		err = soc.Connect(address)
		if err != nil {
			log.Fatalf("Unexpected error while binding socket: %v", err)
		}
		socketpool.sockets[i] = soc
		socketpool.availist[i] = true
	}
	return socketpool
}

func (sp *SocketPool) GetOut() (*zmq.Socket, int) {
	sp.listMutex.Lock()
	for i, value := range sp.availist {
		if value {
			sp.availist[i] = false
			sp.listMutex.Unlock()
			return sp.sockets[i], i
		}
	}
	sp.listMutex.Unlock()

	time.Sleep(time.Duration(rand.Intn(10)) * time.Microsecond)
	return sp.GetOut()
}

func (sp *SocketPool) PutBack(seqnum int) {
	sp.availist[seqnum] = true
}

func (sp *SocketPool) CloseSocket() {
	for _, soc := range sp.sockets {
		soc.Close()
	}
}

func (middleServer *MiddleshimServer) setTime(ctime int64) error {
	middleServer.timeLock.Lock()
	if ctime > middleServer.time {
		middleServer.time = ctime
		middleServer.timeLock.Unlock()
		txn := middleServer.etcdClient.KV.Txn(context.TODO())
		_, err := txn.If(clientv3.Compare(clientv3.Value(middleServer.IpAddress), "<", strconv.FormatInt(ctime, 10))).
			Then(clientv3.OpPut(middleServer.IpAddress, strconv.FormatInt(ctime, 10))).Commit()
		if err != nil {
			return err
		}
	} else {
		middleServer.timeLock.Unlock()
	}
	return nil
}
