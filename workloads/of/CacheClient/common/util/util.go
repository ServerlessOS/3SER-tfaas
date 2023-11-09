/*
 * @Author: lechoux lechoux@qq.com
 * @Date: 2022-06-27 22:45:05
 * @LastEditors: lechoux lechoux@qq.com
 * @LastEditTime: 2022-11-26 00:28:00
 * @Description: common util
 */
package util

import (
	"errors"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
	"unsafe"

	zmq "github.com/pebbe/zmq4"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type SocketCache struct {
	Ctx         *zmq.Context
	pusherCache map[string]*reqSocket
	reqCache    map[string]*reqSocket
	CacheLock   sync.RWMutex
}

type reqSocket struct {
	sync.Mutex
	soc *zmq.Socket
}

type InterData struct {
	MetaData Metadata       `json:"metadata"`
	UserData map[string]any `json:"userdata"`
}

type Metadata struct {
	Tid   string           `json:"tid"`
	RwSet map[string][]any `json:"rwSet"`
}

/**
 * @Author: lechoux lechoux@qq.com
 * @description: clear pusher socketcache. Maybe it can be optimized with LRU
 * @return {*}
 */
const (
	UpdatePort       string = "8891"
	RWPort           string = "8892"
	RemoteCommitPort string = "8893"
	RecvRCPort       string = "8894"
	RemoteAbortPort  string = "8895"
	MiddleshimPort   string = "8900"
	Prefix           string = "/middleshim/"
)

func (socketcache *SocketCache) Clear_sockets() {
	for _, socket := range socketcache.pusherCache {
		socket.soc.Close()
	}
	for _, socket := range socketcache.reqCache {
		socket.soc.Close()
	}
	socketcache.pusherCache = make(map[string]*reqSocket)
	socketcache.reqCache = make(map[string]*reqSocket)
}

func (socketcache *SocketCache) SendMsg(msg []byte, socketType zmq.Type, endpoint string) error {
	if socketType == zmq.REQ {
		socketcache.CacheLock.Lock()
		if reqsocket, ok := socketcache.reqCache[endpoint]; ok {
			socketcache.CacheLock.Unlock()
			reqsocket.Lock()
			reqsocket.soc.SendBytes(msg, 0)
		} else {
			reqSoc, err := socketcache.Ctx.NewSocket(socketType)
			if err != nil {
				return err
			}
			err = reqSoc.Connect(endpoint)
			if err != nil {
				return err
			}
			requester := &reqSocket{
				soc: reqSoc,
			}
			socketcache.reqCache[endpoint] = requester
			socketcache.CacheLock.Unlock()
			return socketcache.SendMsg(msg, zmq.REQ, endpoint)
		}
	} else {
		socketcache.CacheLock.RLock()
		if socket, ok := socketcache.pusherCache[endpoint]; ok {
			socketcache.CacheLock.RUnlock()
			socket.Lock()
			socket.soc.SendBytes(msg, 1)
			socket.Unlock()
		} else {
			socketcache.CacheLock.RUnlock()
			pusher, err := socketcache.Ctx.NewSocket(socketType)
			if err != nil {
				return err
			}
			err = pusher.Connect(endpoint)
			if err != nil {
				return err
			}
			socketcache.CacheLock.Lock()
			socketcache.pusherCache[endpoint] = &reqSocket{soc: pusher}
			socketcache.CacheLock.Unlock()
			return socketcache.SendMsg(msg, zmq.PUSH, endpoint)
		}
	}
	return nil
}

func (socketcache *SocketCache) RecvMsg(endpoint string) []byte {
	if reqsocket, ok := socketcache.reqCache[endpoint]; ok {
		msg, err := reqsocket.soc.RecvBytes(0)
		if err != nil {
			log.Fatalf("Unexpected error in util.RecvMsg: %v", err)
			return nil
		}
		reqsocket.Unlock()
		return msg
	} else {
		log.Println("revc error")
	}
	return nil
}

func GetHost() (string, error) {
	// get local ip
	conn, err := net.Dial("udp", "8.8.8.8:8")
	if err != nil {
		return "", err
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	host, _, err := net.SplitHostPort(localAddr.String())
	if err != nil {
		return "", err
	}
	return host, nil
}

func NewEtcdClient() (*clientv3.Client, error) {
	uri, exists := os.LookupEnv("ETCD_URI")
	if !exists {
		return nil, errors.New("ETCD_URI not found")
	}

	return clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(uri, ","),
		DialTimeout: 5 * time.Second,
	})
}

func GetHostByEnv() string {
	ip := os.Getenv("POD_IP")
	if ip != "" {
		return ip
	}
	return ""
}

func Cache_get_bind_proc() string {
	return "ipc:///requests/get.ipc"
}

func Cache_set_bind_proc() string {
	return "ipc:///requests/set.ipc"
}

func Cache_register_bind_proc() string {
	return "ipc:///requests/register.ipc"
}

func Cache_commit_bind_proc() string {
	return "ipc:///requests/commit.ipc"
}

func Cache_abort_bind_proc() string {
	return "ipc:///requests/abort.ipc"
}

func Cache_session_bind_proc() string {
	return "ipc:///requests/session.ipc"
}

func Bytes2String(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
func String2Bytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}
