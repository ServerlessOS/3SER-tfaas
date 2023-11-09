/*
 * @Author: lechoux lechoux@qq.com
 * @Date: 2022-06-16 21:08:23
 * @LastEditors: lechoux lechoux@qq.com
 * @LastEditTime: 2023-01-30 14:17:17
 * @Description:
 */

package cacheserver

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/lechou-0/common/util"

	cmap "wsh/server/cmap"

	pb "github.com/lechou-0/common/proto"
	"github.com/lechou-0/common/storage"

	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/protobuf/proto"

	uuid "github.com/nu7hatch/gouuid"
	zmq "github.com/pebbe/zmq4"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type buffer struct {
	sync.RWMutex
	kvs map[string]*pb.KeyValuePair
}

type CacheServer struct {
	Id              string
	Zctx            *zmq.Context
	IPAddress       string
	serviceClient   pb.MiddleshimClient
	etcdClient      *clientv3.Client
	ReadCache       cmap.ConcurrentMap
	TBuffer         *tBuffer
	PusherCache     *util.SocketCache
	WriteBuffer     map[string]*buffer
	WriterLock      sync.RWMutex
	ReadBuffer      map[string]*buffer
	ReaderLock      sync.RWMutex
	sf              singleflight.Group
	connectionCache map[string]storage.StorageManager
	abortFlags      map[string]chan bool
	abortFlagsLock  sync.RWMutex
}

/**
 * @Author: lechoux lechoux@qq.com
 * @description: receive requests and start coroutine processing
 * @return {*}
 */
func (s *CacheServer) Run() {

	zctx := s.Zctx

	register_puller, _ := zctx.NewSocket(zmq.PULL)
	defer register_puller.Close()
	err := register_puller.Bind(util.Cache_register_bind_proc())
	if err != nil {
		log.Printf("Unexpected error while binding register: %v", err)
	}
	os.Chmod("/requests/register.ipc", 0777)

	get_puller, _ := zctx.NewSocket(zmq.PULL)
	defer get_puller.Close()
	err = get_puller.Bind(util.Cache_get_bind_proc())
	if err != nil {
		log.Printf("Unexpected error while binding get: %v", err)
	}
	os.Chmod("/requests/get.ipc", 0777)

	set_puller, _ := zctx.NewSocket(zmq.PULL)
	defer set_puller.Close()
	err = set_puller.Bind(util.Cache_set_bind_proc())
	if err != nil {
		log.Printf("Unexpected error while binding set: %v", err)
	}
	os.Chmod("/requests/set.ipc", 0777)

	commit_puller, _ := zctx.NewSocket(zmq.PULL)
	defer commit_puller.Close()
	err = commit_puller.Bind(util.Cache_commit_bind_proc())
	if err != nil {
		log.Printf("Unexpected error while binding commit: %v", err)
	}
	os.Chmod("/requests/commit.ipc", 0777)

	update_puller, _ := zctx.NewSocket(zmq.PULL)
	defer update_puller.Close()
	err = update_puller.Bind("tcp://*:" + util.UpdatePort)
	if err != nil {
		log.Printf("Unexpected error while binding update: %v", err)
	}

	rw_reponser, _ := zctx.NewSocket(zmq.REP)
	defer rw_reponser.Close()
	err = rw_reponser.Bind("tcp://*:" + util.RWPort)
	if err != nil {
		log.Printf("Unexpected error while binding RW: %v", err)
	}

	remotec_puller, _ := zctx.NewSocket(zmq.PULL)
	defer remotec_puller.Close()
	err = remotec_puller.Bind("tcp://*:" + util.RemoteCommitPort)
	if err != nil {
		log.Printf("Unexpected error while binding remotec: %v", err)
	}

	recvrc_puller, _ := zctx.NewSocket(zmq.PULL)
	defer recvrc_puller.Close()
	err = recvrc_puller.Bind("tcp://*:" + util.RecvRCPort)
	if err != nil {
		log.Printf("Unexpected error while binding recvrc: %v", err)
	}

	abort_puller, _ := zctx.NewSocket(zmq.PULL)
	defer abort_puller.Close()
	err = abort_puller.Bind(util.Cache_abort_bind_proc())
	if err != nil {
		log.Printf("Unexpected error while binding abort: %v", err)
	}
	os.Chmod("/requests/abort.ipc", 0777)

	session_puller, _ := zctx.NewSocket(zmq.PULL)
	defer session_puller.Close()
	err = session_puller.Bind(util.Cache_session_bind_proc())
	if err != nil {
		log.Printf("Unexpected error while binding session: %v", err)
	}
	os.Chmod("/requests/session.ipc", 0777)

	remotea_puller, _ := zctx.NewSocket(zmq.PULL)
	defer remotea_puller.Close()
	err = remotea_puller.Bind("tcp://*:" + util.RemoteAbortPort)
	if err != nil {
		log.Printf("Unexpected error while binding remotea: %v", err)
	}

	socketPoller := zmq.NewPoller()

	socketPoller.Add(register_puller, zmq.POLLIN)
	socketPoller.Add(get_puller, zmq.POLLIN)
	socketPoller.Add(set_puller, zmq.POLLIN)
	socketPoller.Add(commit_puller, zmq.POLLIN)
	socketPoller.Add(update_puller, zmq.POLLIN)
	socketPoller.Add(rw_reponser, zmq.POLLIN)
	socketPoller.Add(remotec_puller, zmq.POLLIN)
	socketPoller.Add(recvrc_puller, zmq.POLLIN)
	socketPoller.Add(session_puller, zmq.POLLIN)
	socketPoller.Add(abort_puller, zmq.POLLIN)   // local abort
	socketPoller.Add(remotea_puller, zmq.POLLIN) // remote abort

	for {
		sockets, err := socketPoller.Poll(-1)
		if err != nil {
			log.Printf("Unexpected error while processing poller: %v", err)
			return
		}
		for _, polled := range sockets {
			switch socket := polled.Socket; socket {
			case register_puller:
				msg, err := register_puller.RecvBytes(0)
				if err != nil {
					log.Printf("Unexpected error while receiving register inproc message: %v", err)
					return
				}
				go s.execRegister(msg)
			case get_puller:
				msg, err := get_puller.RecvBytes(0)
				if err != nil {
					log.Printf("Unexpected error while receiving get inproc message: %v", err)
					return
				}
				go s.execR(msg)
			case set_puller:
				msg, err := set_puller.RecvBytes(0)
				if err != nil {
					log.Printf("Unexpected error while receiving set inproc message: %v", err)
					return
				}
				go s.execW(msg)
			case commit_puller:
				msg, err := commit_puller.RecvBytes(0)
				if err != nil {
					log.Printf("Unexpected error while receiving commit inproc message: %v", err)
					return
				}
				go s.execC(msg)
			case update_puller:
				msg, err := update_puller.RecvBytes(0)
				if err != nil {
					log.Printf("Unexpected error while receiving update inproc message: %v", err)
					return
				}
				go s.execUpdate(msg)
			case abort_puller:
				msg, err := abort_puller.RecvBytes(0)
				if err != nil {
					log.Printf("Unexpected error while receiving abort inproc message: %v", err)
					return
				}
				go s.execA(msg)
			case remotea_puller:
				msg, err := remotea_puller.RecvBytes(0)
				if err != nil {
					log.Printf("Unexpected error while receiving remote abort inproc message: %v", err)
					return
				}
				go s.execA(msg)
			case session_puller:
				msg, err := session_puller.RecvBytes(0)
				if err != nil {
					log.Printf("Unexpected error while receiving remote session inproc message: %v", err)
					return
				}
				go s.execS(msg)
			case rw_reponser:
				msg, err := rw_reponser.RecvBytes(0)
				if err != nil {
					log.Printf("Unexpected error while receiving rw inproc message: %v", err)
					return
				}
				rw_reponser.SendBytes(s.readRWB(msg), 1)
			case remotec_puller:
				msg, err := remotec_puller.RecvBytes(0)
				if err != nil {
					log.Printf("Unexpected error while receiving remotec_puller message: %v", err)
					return
				}
				go s.execC(msg)
			case recvrc_puller:
				msg, err := recvrc_puller.RecvBytes(0)
				if err != nil {
					log.Printf("Unexpected error while receiving recvrc_puller inproc message: %v", err)
					return
				}
				go s.execRecvRC(msg)
			}
		}
	}
}

func (s *CacheServer) execW(msg []byte) {

	setRequest := &pb.WriteRequest{}
	err := proto.Unmarshal(msg, setRequest)
	if err != nil {
		log.Printf("Unexpected error while Unmarshaling set message: %v", err)
	}

	key := setRequest.GetKey()
	tid := setRequest.GetTid()
	kv := &pb.KeyValuePair{Key: key, Value: setRequest.GetValue(), Tid: tid, WTimestamp: -1}
	s.WriterLock.RLock()
	writeBuffer, ok := s.WriteBuffer[tid]
	s.WriterLock.RUnlock()
	if !ok {
		writeBuffer = &buffer{
			kvs: map[string]*pb.KeyValuePair{},
		}
		s.WriterLock.Lock()
		s.WriteBuffer[tid] = writeBuffer
		s.WriterLock.Unlock()
	}
	writeBuffer.Lock()
	writeBuffer.kvs[key] = kv
	writeBuffer.Unlock()

	ctx := context.WithValue(context.Background(), ctxKey, key)
	setResponse, err := s.serviceClient.Lock(ctx, &pb.KeyRequest{Key: key, Tid: tid})
	if err != nil {
		log.Printf("Unexpected error while locking: %v", err)
	}

	// send msg by pushercache or new pusher
	endpoint := setRequest.GetResponseAddress()

	msg, err = proto.Marshal(setResponse)
	if err != nil {
		log.Printf("Unexpected error while marshaling set message: %v", err)
	}

	err = s.PusherCache.SendMsg(msg, zmq.PUSH, endpoint)
	if err != nil {
		log.Printf("Unexpected error while sending set message: %v", err)
	}

}

func (s *CacheServer) execR(msg []byte) {

	var resMsg []byte
	var readBuffer *buffer
	getRequest := &pb.KeyRequest{}
	// var cacheMiss bool
	err := proto.Unmarshal(msg, getRequest)
	if err != nil {
		log.Printf("Unexpected error while Unmarshaling get message: %v", err)
		return
	}
	key := getRequest.GetKey()
	tid := getRequest.GetTid()
	s.ReaderLock.RLock()
	readBuffer, ok := s.ReadBuffer[tid]
	s.ReaderLock.RUnlock()
	if !ok {
		readBuffer = &buffer{
			kvs: map[string]*pb.KeyValuePair{},
		}
		s.ReaderLock.Lock()
		s.ReadBuffer[tid] = readBuffer
		s.ReaderLock.Unlock()
	}

	// get value from WriteBuffer, ReadBuffer
	if getRequest.GetWTimestamp() < 0 {
		if getRequest.GetKeyAddress() == s.IPAddress {
			s.WriterLock.RLock()
			writeBuffer, ok := s.WriteBuffer[tid]
			s.WriterLock.RUnlock()
			if ok {
				writeBuffer.RLock()
				kv, ok := writeBuffer.kvs[key]
				writeBuffer.RUnlock()
				if ok && kv.GetWTimestamp() == getRequest.GetWTimestamp() {
					getResponse := &pb.KeyResponse{
						Value: kv.GetValue(),
						Error: pb.CacheError_NO_ERROR,
					}
					resMsg, err = proto.Marshal(getResponse)
					if err != nil {
						log.Printf("Unexpected error while marshaling get message: %v", err)
						return
					}

				} else {
					log.Println("key not in write buffer")
					getResponse := &pb.KeyResponse{
						Error: pb.CacheError_KEY_DNE,
					}
					resMsg, err = proto.Marshal(getResponse)
					if err != nil {
						log.Printf("Unexpected error while marshaling get message: %v", err)
						return
					}
				}
			} else {
				log.Println("key not in write buffer")
				getResponse := &pb.KeyResponse{
					Error: pb.CacheError_KEY_DNE,
				}
				resMsg, err = proto.Marshal(getResponse)
				if err != nil {
					log.Printf("Unexpected error while marshaling get message: %v", err)
					return
				}
			}
		} else {
			keyAddr := "tcp://" + getRequest.GetKeyAddress() + ":" + util.RWPort
			err = s.PusherCache.SendMsg(msg, zmq.REQ, keyAddr)
			if err != nil {
				log.Printf("Unexpected error while sending get message to another cache: %v", err)
				return
			}
			resMsg = s.PusherCache.RecvMsg(keyAddr)
		}
	} else if getRequest.GetWTimestamp() > 0 {
		// 优先从本地读取，没有的话再去判断address是本地还是外部
		readBuffer.RLock()
		kv, ok := readBuffer.kvs[key]
		readBuffer.RUnlock()

		if ok && kv.GetWTimestamp() == getRequest.GetWTimestamp() {
			getResponse := &pb.KeyResponse{
				Value:      kv.GetValue(),
				WTimestamp: kv.GetWTimestamp(),
				RTimestamp: kv.GetRTimestamp(),
				Error:      pb.CacheError_NO_ERROR,
			}
			resMsg, err = proto.Marshal(getResponse)
			if err != nil {
				log.Printf("Unexpected error while marshaling get message: %v", err)
				return
			}
		} else {
			kv, ok := s.ReadCache.Get(key)
			if ok && kv.GetWTimestamp() == getRequest.GetWTimestamp() {
				getResponse := &pb.KeyResponse{
					Value:      kv.GetValue(),
					WTimestamp: kv.GetWTimestamp(),
					RTimestamp: kv.GetRTimestamp(),
					Error:      pb.CacheError_NO_ERROR,
				}
				resMsg, err = proto.Marshal(getResponse)
				if err != nil {
					log.Printf("Unexpected error while marshaling get message: %v", err)
					return
				}
			}
		}
		//本地没有再判断远端是否有
		if len(resMsg) == 0 {
			if getRequest.GetKeyAddress() == s.IPAddress {
				log.Println("key not in read buffer")
				getResponse := &pb.KeyResponse{
					Error: pb.CacheError_KEY_DNE,
				}
				resMsg, err = proto.Marshal(getResponse)
				if err != nil {
					log.Printf("Unexpected error while marshaling get message: %v", err)
					return
				}
			} else {
				keyAddr := "tcp://" + getRequest.GetKeyAddress() + ":" + util.RWPort
				err = s.PusherCache.SendMsg(msg, zmq.REQ, keyAddr)
				if err != nil {
					log.Printf("Unexpected error while sending get message to another cache: %v", err)
					return
				}
				resMsg = s.PusherCache.RecvMsg(keyAddr)
			}
		}
		//buffer中没有，从cache中读
	} else {
		kv, ok := s.ReadCache.Get(key)
		if ok {
			getResponse := &pb.KeyResponse{
				Value:      kv.GetValue(),
				WTimestamp: kv.GetWTimestamp(),
				RTimestamp: kv.GetRTimestamp(),
				Error:      pb.CacheError_NO_ERROR,
			}
			resMsg, err = proto.Marshal(getResponse)
			if err != nil {
				log.Printf("Unexpected error while marshaling get message: %v", err)
				return
			}
		} else {
			val, err, _ := s.sf.Do(key, func() (any, error) {
				vb, err := s.connectionCache["data"].Get(key)

				if err != nil {
					return nil, err
				}
				kv := &pb.KeyValuePair{}
				err = proto.Unmarshal(vb, kv)
				if err != nil {
					return nil, err
				}

				if s.ReadCache.Set(key, kv) {
					return kv, nil
				} else {
					kv, _ = s.ReadCache.Get(key)
					return kv, nil
				}

			})
			if err != nil {
				log.Printf("Unexpected error while getting value from storage: %v", err)
				// client will be stuck
				return
			}
			kv = val.(*pb.KeyValuePair)
			if kv != nil {
				// cacheMiss = true
				getResponse := &pb.KeyResponse{
					Value:      kv.GetValue(),
					WTimestamp: kv.GetWTimestamp(),
					RTimestamp: kv.GetRTimestamp(),
					Error:      pb.CacheError_NO_ERROR,
				}
				resMsg, err = proto.Marshal(getResponse)
				if err != nil {
					log.Printf("Unexpected error while marshaling get message: %v", err)
					return
				}
			}
		}

		if kv != nil {
			readBuffer.RWMutex.Lock()
			readBuffer.kvs[key] = &pb.KeyValuePair{
				Key:        key,
				Value:      kv.GetValue(),
				WTimestamp: kv.GetWTimestamp(),
				RTimestamp: kv.GetRTimestamp(),
			}
			readBuffer.RWMutex.Unlock()
		} else {
			getResponse := &pb.KeyResponse{
				Error: pb.CacheError_KEY_DNE,
			}
			resMsg, err = proto.Marshal(getResponse)
			if err != nil {
				log.Printf("Unexpected error while marshaling get message: %v", err)
				return
			}
		}
	}

	endpoint := getRequest.GetResponseAddress()
	err = s.PusherCache.SendMsg(resMsg, zmq.PUSH, endpoint)
	if err != nil {
		log.Printf("Unexpected error while sending get message: %v", err)
		return
	}
	// if cacheMiss {
	// 	ctx := context.WithValue(context.Background(), ctxKey, key)
	// 	s.serviceClient.CacheMiss(ctx, &pb.KeyRequest{Key: key, ResponseAddress: s.IPAddress})
	// }

}

func (s *CacheServer) readRWB(msg []byte) []byte {
	var getResponse *pb.KeyResponse
	getRequest := &pb.KeyRequest{}
	err := proto.Unmarshal(msg, getRequest)
	if err != nil {
		log.Fatalf("Unexpected error while Unmarshaling get message: %v", err)
	}
	key := getRequest.GetKey()
	tid := getRequest.GetTid()
	if getRequest.GetKeyAddress() != s.IPAddress {
		log.Fatal("sendrw address error")
	}
	// get value from WriteBuffer, ReadBuffer
	if getRequest.GetWTimestamp() < 0 {
		s.WriterLock.RLock()
		writeBuffer, ok := s.WriteBuffer[tid]
		s.WriterLock.RUnlock()
		if ok {
			writeBuffer.RLock()
			kv, ok := writeBuffer.kvs[key]
			writeBuffer.RUnlock()
			if ok && kv.GetWTimestamp() == getRequest.GetWTimestamp() {
				getResponse = &pb.KeyResponse{
					Value: kv.GetValue(),
					Error: pb.CacheError_NO_ERROR,
				}
			} else {
				log.Println("key not in write buffer")
				getResponse = &pb.KeyResponse{
					Error: pb.CacheError_KEY_DNE,
				}
			}
		} else {
			log.Println("key not in write buffer")
			getResponse = &pb.KeyResponse{
				Error: pb.CacheError_KEY_DNE,
			}
		}
	} else if getRequest.GetWTimestamp() > 0 {
		s.ReaderLock.RLock()
		readBuffer, ok := s.ReadBuffer[tid]
		s.ReaderLock.RUnlock()
		if ok {
			readBuffer.RLock()
			kv, ok := readBuffer.kvs[key]
			readBuffer.RUnlock()
			if ok && kv.GetWTimestamp() == getRequest.GetWTimestamp() {
				getResponse = &pb.KeyResponse{
					Value:      kv.GetValue(),
					WTimestamp: kv.GetWTimestamp(),
					RTimestamp: kv.GetRTimestamp(),
					Error:      pb.CacheError_NO_ERROR,
				}
			} else {
				log.Println("key not in read buffer")
				getResponse = &pb.KeyResponse{
					Error: pb.CacheError_KEY_DNE,
				}
			}
		} else {
			log.Println("key not in read buffer")
			getResponse = &pb.KeyResponse{
				Error: pb.CacheError_KEY_DNE,
			}
		}
	} else {
		log.Fatal("sendrw address error")
	}
	msg, err = proto.Marshal(getResponse)
	if err != nil {
		log.Fatalf("Unexpected error while marshaling get message: %v", err)
	}
	return msg
}

func (s *CacheServer) execRegister(msg []byte) {

	// cold start
	err := s.PusherCache.SendMsg(util.String2Bytes(s.IPAddress), zmq.PUSH, util.Bytes2String(msg))
	if err != nil {
		log.Printf("Unexpected error while sending register message: %v", err)
		return
	}

}

func (s *CacheServer) execC(msg []byte) {
	commitRequest := &pb.CommitRequest{}

	err := proto.Unmarshal(msg, commitRequest)
	if err != nil {
		log.Printf("Unexpected error while Unmarshaling commit message: %v", err)
		return
	}
	tid := commitRequest.GetTid()
	endpoint := commitRequest.GetResponseAddress()
	cts := commitRequest.GetCommitTimestamp()

	s.WriterLock.RLock()
	writeBuffer := s.WriteBuffer[tid]
	s.WriterLock.RUnlock()

	s.ReaderLock.RLock()
	readBuffer := s.ReadBuffer[tid]
	s.ReaderLock.RUnlock()

	if commitRequest.Local {

		writeSetKeys := []string{}
		readSetKeys := []string{}
		if len(commitRequest.GetWriteSet()) != 0 {
			writeSetKeys = commitRequest.GetWriteSet()[s.IPAddress].Keys
		}
		if len(commitRequest.GetReadSet()) != 0 {
			readSetKeys = commitRequest.GetReadSet()[s.IPAddress].Keys
		}
		rpccounts := len(writeSetKeys) + len(readSetKeys)
		abort := make(chan bool, rpccounts)
		var isMaster bool
		if strings.HasPrefix(endpoint, "ipc") {
			isMaster = true
		}

		for _, key := range writeSetKeys {
			v := writeBuffer.kvs[key]
			v = &pb.KeyValuePair{
				Key:        v.GetKey(),
				Value:      v.GetValue(),
				Tid:        tid,
				WTimestamp: cts,
				RTimestamp: cts,
			}

			go func(v2 *pb.KeyValuePair) {
				// Validate2
				if len(v2.Value) == 0 {
					log.Println("cacheserver bug")
				}
				ctx := context.WithValue(context.Background(), ctxKey, v2.GetKey())
				if errorResponse2, err := s.serviceClient.Validate2(ctx, v2); err == nil {
					if errorResponse2.Error == pb.CacheError_ABORT {
						abort <- true
					} else {
						abort <- false
					}
				} else {
					log.Printf("Unexpected error while processing Validate2: %v", err)
					abort <- true
				}
			}(v)
		}

		// readbuffer中部分需要验证
		for _, key := range readSetKeys {
			v := readBuffer.kvs[key]
			v = &pb.KeyValuePair{
				Key:        v.GetKey(),
				Tid:        tid,
				WTimestamp: v.GetWTimestamp(),
				RTimestamp: cts,
			}
			go func(v1 *pb.KeyValuePair) {
				// Validate1
				ctx := context.WithValue(context.Background(), ctxKey, v1.GetKey())
				if errorResponse1, err := s.serviceClient.Validate1(ctx, v1); err == nil {
					if errorResponse1.Error == pb.CacheError_ABORT {
						abort <- true
					} else {
						abort <- false
					}
				} else {
					log.Printf("Unexpected error while processing Validate1: %v", err)
					abort <- true
				}
			}(v)
		}

		commitResponse := []byte{1}

		// 其中一个abort后就直接break
		for isAbort := range abort {
			rpccounts--
			if isAbort {
				commitResponse = []byte{}
				break
			}
			if rpccounts == 0 {
				break
			}
		}

		if len(commitResponse) == 1 && isMaster {
			if commitRequest.Session.TimeFence < cts {
				commitRequest.Session.TimeFence = cts
				ctx := context.WithValue(context.Background(), ctxKey, commitRequest.Session.Client)
				s.serviceClient.SetSession(ctx, commitRequest.Session)
			}
			rid := tid[:36]
			if len(writeSetKeys) == 0 {
				s.TBuffer.Lock()
				ts, ok := s.TBuffer.CBuffer[rid]
				if !ok {
					ts = &transactions{outputs: make([][]byte, 0), depss: make([][]string, 0), index: make(map[string]int)}
					s.TBuffer.CBuffer[rid] = ts
				}
				s.TBuffer.Unlock()
				ts.Append(tid, commitRequest.Output, commitRequest.Deps)
			} else {
				s.TBuffer.Lock()
				ts, ok := s.TBuffer.CBuffer[rid]
				if !ok {
					ts = &transactions{outputs: make([][]byte, 0), depss: make([][]string, 0), index: make(map[string]int)}
					s.TBuffer.CBuffer[rid] = ts
				}
				s.TBuffer.Unlock()
				err = ts.BFSRecord(tid, commitRequest.Output, commitRequest.Deps, s.etcdClient)
				if err != nil {
					return
				}
			}
		}

		s.ReaderLock.Lock()
		delete(s.ReadBuffer, tid)
		s.ReaderLock.Unlock()
		if isMaster {
			// Return to user response, commit: []byte{1}, abort: []byte{}
			err = s.PusherCache.SendMsg(commitResponse, zmq.PUSH, endpoint)
			if err != nil {
				log.Printf("Unexpected error while sending commit message: %v", err)
				return
			}
		} else {
			cr := &pb.CommitResponse{
				Tid:       tid,
				IPAddress: s.IPAddress,
			}
			if len(commitResponse) == 1 {
				cr.Error = pb.CacheError_NO_ERROR
			} else {
				cr.Error = pb.CacheError_ABORT
			}
			crb, err := proto.Marshal(cr)
			if err != nil {
				log.Printf("Unexpected error while Marshaling crb: %v", err)
			}

			err = s.PusherCache.SendMsg(crb, zmq.PUSH, endpoint)
			if err != nil {
				log.Printf("Unexpected error while sending commit message to master: %v", err)
				return
			}
		}

		if writeBuffer != nil && (isMaster || (!isMaster && len(commitResponse) == 0)) {
			// 前面的验证操作结束后才能执行删除操作，防止删除操作在存储状态操作之前执行
			for ; rpccounts > 0; rpccounts-- {
				<-abort
			}
			for _, v := range writeBuffer.kvs {
				if len(commitResponse) == 0 {
					v = &pb.KeyValuePair{
						Key:        v.GetKey(),
						Tid:        tid,
						WTimestamp: -1,
					}
				} else {
					v = &pb.KeyValuePair{
						Key:        v.GetKey(),
						Tid:        tid,
						WTimestamp: cts,
						RTimestamp: cts,
					}
				}
				go func(v *pb.KeyValuePair) {
					ctx := context.WithValue(context.Background(), ctxKey, v.GetKey())
					if _, err := s.serviceClient.Update(ctx, v); err != nil {
						log.Printf("Unexpected error while updating wb: %v", err)
					}
				}(v)
			}
		}

		s.WriterLock.Lock()
		delete(s.WriteBuffer, tid)
		s.WriterLock.Unlock()
	} else {
		commitRequests := map[string]*pb.CommitRequest{}
		for k, v := range commitRequest.GetWriteSet() {
			cr := &pb.CommitRequest{
				Tid:             tid,
				CommitTimestamp: cts,
				WriteSet:        map[string]*pb.Keys{k: v},
				Local:           true,
				ResponseAddress: "tcp://" + s.IPAddress + ":" + util.RecvRCPort,
			}
			commitRequests[k] = cr
		}
		for k, v := range commitRequest.GetReadSet() {
			cr, ok := commitRequests[k]
			if ok {
				cr.ReadSet = map[string]*pb.Keys{k: v}
			} else {
				cr = &pb.CommitRequest{
					Tid:             tid,
					CommitTimestamp: cts,
					ReadSet:         map[string]*pb.Keys{k: v},
					Local:           true,
					ResponseAddress: "tcp://" + s.IPAddress + ":" + util.RecvRCPort,
				}
				commitRequests[k] = cr
			}
		}
		for k, cr := range commitRequests {
			crb, err := proto.Marshal(cr)
			if err != nil {
				log.Printf("Unexpected error while Marshaling crb: %v", err)
			}
			err = s.PusherCache.SendMsg(crb, zmq.PUSH, "tcp://"+k+":"+util.RemoteCommitPort)
			if err != nil {
				log.Printf("Unexpected error while sending commit message to other servers: %v", err)
				return
			}
		}

		rpccounts := len(commitRequests)
		abortFlag := make(chan bool, rpccounts)
		s.abortFlagsLock.Lock()
		s.abortFlags[tid] = abortFlag
		s.abortFlagsLock.Unlock()
		commitResponse := []byte{1}
		for isAbort := range abortFlag {
			rpccounts--
			if isAbort {
				commitResponse = []byte{}
				break
			}
			if rpccounts == 0 {
				break
			}
		}
		if len(commitResponse) == 1 {
			if commitRequest.Session.TimeFence < cts {
				commitRequest.Session.TimeFence = cts
				ctx := context.WithValue(context.Background(), ctxKey, commitRequest.Session.Client)
				s.serviceClient.SetSession(ctx, commitRequest.Session)
			}
			rid := tid[:36]
			if len(commitRequest.WriteSet) == 0 {
				s.TBuffer.Lock()
				ts, ok := s.TBuffer.CBuffer[rid]
				if !ok {
					ts = &transactions{outputs: make([][]byte, 0), depss: make([][]string, 0), index: make(map[string]int)}
					s.TBuffer.CBuffer[rid] = ts
				}
				s.TBuffer.Unlock()
				ts.Append(tid, commitRequest.Output, commitRequest.Deps)
			} else {
				s.TBuffer.Lock()
				ts, ok := s.TBuffer.CBuffer[rid]
				if !ok {
					ts = &transactions{outputs: make([][]byte, 0), depss: make([][]string, 0), index: make(map[string]int)}
					s.TBuffer.CBuffer[rid] = ts
				}
				s.TBuffer.Unlock()
				err = ts.BFSRecord(tid, commitRequest.Output, commitRequest.Deps, s.etcdClient)
				if err != nil {
					return
				}
			}
		}

		err = s.PusherCache.SendMsg(commitResponse, zmq.PUSH, endpoint)
		if err != nil {
			log.Printf("Unexpected error while sending commit message: %v", err)
			return
		}
		for ; rpccounts > 0; rpccounts-- {
			<-abortFlag
		}

		// 回滚：对于返回提交的节点来说，状态已经全部持久化完毕，下文操作会删除状态；而返回回滚的节点还在异步的存储然后删除状态，因此对于这些节点存储的状态可能被删除两次
		// 提交：对于所有节点都已经将状态持久化完毕，下文操作会同步状态
		for _, wkeys := range commitRequest.GetWriteSet() {
			for _, wkey := range wkeys.GetKeys() {
				kv := &pb.KeyValuePair{
					Key: wkey,
					Tid: tid,
				}
				if len(commitResponse) == 0 {
					kv.WTimestamp = -1
				} else {
					kv.WTimestamp = cts
					kv.RTimestamp = cts
				}
				go func(kv *pb.KeyValuePair) {
					ctx := context.WithValue(context.Background(), ctxKey, kv.GetKey())
					if _, err := s.serviceClient.Update(ctx, kv); err != nil {
						log.Printf("Unexpected error while updating wb: %v", err)
					}
				}(kv)
			}
		}
	}
}

func (s *CacheServer) execS(msg []byte) {
	sessionRequest := &pb.KeyRequest{}
	err := proto.Unmarshal(msg, sessionRequest)
	if err != nil {
		log.Printf("Unexpected error while processing Unmarshal kv: %v", err)
	}
	session := &pb.SessionRequest{
		Client: sessionRequest.Key,
	}
	ctx := context.WithValue(context.Background(), ctxKey, session.Client)
	session, err = s.serviceClient.GetSession(ctx, session)
	if err != nil {
		log.Printf("Unexpected error while getting session: %v", err)
	}
	resMsg, err := proto.Marshal(session)
	if err != nil {
		log.Printf("Unexpected error while marshaling get message: %v", err)
		return
	}

	endpoint := sessionRequest.GetResponseAddress()
	err = s.PusherCache.SendMsg(resMsg, zmq.PUSH, endpoint)
	if err != nil {
		log.Printf("Unexpected error while sending get message: %v", err)
		return
	}
}

func (s *CacheServer) execUpdate(msg []byte) {

	kv := &pb.KeyValuePair{}
	err := proto.Unmarshal(msg, kv)
	if err != nil {
		log.Printf("Unexpected error while processing Unmarshal kv: %v", err)
	}
	s.ReadCache.Set(kv.GetKey(), kv)
	log.Printf("update %s with write timestamp: %d and read timestamp: %d", kv.GetKey(), kv.GetWTimestamp(), kv.GetRTimestamp())

}

func (s *CacheServer) execRecvRC(msg []byte) {

	commitResponse := &pb.CommitResponse{}
	err := proto.Unmarshal(msg, commitResponse)
	if err != nil {
		log.Printf("Unexpected error while processing Unmarshal kv: %v", err)
	}
	s.abortFlagsLock.RLock()
	abortFlag := s.abortFlags[commitResponse.GetTid()]
	s.abortFlagsLock.RUnlock()
	if commitResponse.GetError() == pb.CacheError_NO_ERROR {
		abortFlag <- false
	} else {
		abortFlag <- true
	}

}

func (s *CacheServer) execA(msg []byte) {

	abortRequest := &pb.CommitRequest{}
	err := proto.Unmarshal(msg, abortRequest)
	if err != nil {
		log.Fatalf("Unexpected error while Unmarshaling transaction: %v", err)
	}
	tid := abortRequest.GetTid()

	if abortRequest.GetLocal() {
		// 如果是0说明是从远程来的，本地发送的writeset和keys不可能为nil
		if len(abortRequest.GetWriteSet()) != 0 {
			keys, ok := abortRequest.GetWriteSet()[s.IPAddress]
			if ok {
				for _, key := range keys.GetKeys() {

					go func(key string) {
						kv := &pb.KeyValuePair{
							Key:        key,
							Tid:        tid,
							WTimestamp: -1,
						}
						ctx := context.WithValue(context.Background(), ctxKey, key)
						s.serviceClient.Update(ctx, kv)
					}(key)
				}
			}
		}

		s.ReaderLock.Lock()
		delete(s.ReadBuffer, tid)
		s.ReaderLock.Unlock()
		s.WriterLock.Lock()
		delete(s.WriteBuffer, tid)
		s.WriterLock.Unlock()

	} else {
		rwSet := abortRequest.GetWriteSet()
		ar := &pb.CommitRequest{
			Tid:   tid,
			Local: true,
		}
		bar, err := proto.Marshal(ar)
		if err != nil {
			log.Fatalf("Unexpected error while Unmarshaling abort: %v", err)
		}
		for addr, keys := range rwSet {
			tcpAddr := "tcp://" + addr + ":" + util.RemoteAbortPort
			s.PusherCache.SendMsg(bar, zmq.PUSH, tcpAddr)
			if len(keys.GetKeys()) > 0 {
				for _, key := range keys.GetKeys() {
					go func(key string) {
						kv := &pb.KeyValuePair{
							Key:        key,
							Tid:        tid,
							WTimestamp: -1,
						}
						ctx := context.WithValue(context.Background(), ctxKey, key)
						s.serviceClient.Update(ctx, kv)
					}(key)
				}
			}
		}
	}
}

func NewServer(etcdClient *clientv3.Client) *CacheServer {
	uid, err := uuid.NewV4()
	if err != nil {
		log.Fatalf("Unexpected error while generating UUID: %v", err)
	}
	ctx, err := zmq.NewContext()
	if err != nil {
		log.Fatal(err)
	}

	bB := newConsistentHashBalancerBuilder()
	rB := NewServiceDiscovery(etcdClient)
	resolver.Register(rB)
	balancer.Register(bB)
	conn, err := grpc.Dial(
		fmt.Sprintf("%s:///%s", rB.Scheme(), ""),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, bB.Name())),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Unexpected error while connecting grpc server: %v", err)
	}
	host := util.GetHostByEnv()
	log.Println(host)

	pushercache := &util.SocketCache{
		Ctx: ctx,
	}
	pushercache.Clear_sockets()

	server := &CacheServer{
		Zctx:            ctx,
		Id:              uid.String(),
		IPAddress:       host,
		serviceClient:   pb.NewMiddleshimClient(conn),
		etcdClient:      etcdClient,
		ReadCache:       cmap.New(),
		TBuffer:         &tBuffer{CBuffer: map[string]*transactions{}},
		PusherCache:     pushercache,
		WriteBuffer:     make(map[string]*buffer),
		WriterLock:      sync.RWMutex{},
		ReadBuffer:      make(map[string]*buffer),
		ReaderLock:      sync.RWMutex{},
		sf:              singleflight.Group{},
		connectionCache: make(map[string]storage.StorageManager),
		abortFlags:      make(map[string]chan bool),
		abortFlagsLock:  sync.RWMutex{},
	}

	server.connectionCache["data"] = storage.NewDynamoStorageManager("test")
	// server.connectionCache["data"] = storage.NewMongoStorageManager("test")
	log.Println("init success")
	return server
}
