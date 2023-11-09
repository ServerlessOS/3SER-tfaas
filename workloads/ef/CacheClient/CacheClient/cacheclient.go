package CacheClient

import (
	"encoding/json"
	"errors"
	"log"
	"strconv"

	pb "github.com/lechou-0/common/proto"
	"github.com/lechou-0/common/util"

	"google.golang.org/protobuf/proto"

	zmq "github.com/pebbe/zmq4"
)

type CacheClient struct {
	Tid            string
	RWset          map[string][]any
	newRWset       map[string][]any
	receiver       *zmq.Socket
	receiverSeqnum int
	IPCAddress     string
	CC             *ConnectionCache
}

func NewClient(in []byte, CC *ConnectionCache) (*CacheClient, map[string]any, error) {
	inter := &util.InterData{}
	err := json.Unmarshal(in, inter)
	if err != nil {
		log.Printf("fail to unmarshal inter: %s", err)
		return nil, nil, err
	}
	cacheClient := Register(inter.MetaData.Tid, inter.MetaData.RwSet, CC)
	return cacheClient, inter.UserData, nil
}

func DeleteClient(c *CacheClient, output map[string]any) []byte {
	inter := &util.InterData{
		MetaData: util.Metadata{
			RwSet: c.newRWset,
		},
		UserData: output,
	}
	c.CloseClient()
	out, _ := json.Marshal(inter)
	return out
}

func (c *CacheClient) CloseClient() {
	c.CC.CloseReceiver(c.receiverSeqnum)
}

func Register(tid string, rwset map[string][]any, CC *ConnectionCache) *CacheClient {

	receiver, seqnum := CC.NewReceiver()
	response_address := "ipc://" + CC.BaseAddress + strconv.Itoa(seqnum) + ".ipc"
	client := &CacheClient{
		Tid:            tid,
		RWset:          rwset,
		newRWset:       map[string][]any{},
		receiver:       receiver,
		receiverSeqnum: seqnum,
		IPCAddress:     response_address,
		CC:             CC,
	}

	return client
}

func (c *CacheClient) Get(k string) []byte {

	getPusher, seqnum := c.CC.NewGetPusher()
	keyRequest := &pb.KeyRequest{}

	rw, ok := c.RWset[k]
	if ok {
		keyRequest.KeyAddress = rw[0].(string)
		keyRequest.WTimestamp = int64(rw[1].(float64))
	} else {
		keyRequest.KeyAddress = c.CC.CacheAddress
	}
	keyRequest.Key = k
	keyRequest.Tid = c.Tid
	keyRequest.ResponseAddress = c.IPCAddress

	req_byte, err := proto.Marshal(keyRequest)

	if err != nil {
		log.Fatal(err)
	}

	_, err = getPusher.SendBytes(req_byte, 1)

	if err != nil {
		log.Fatal(err)
	}
	c.CC.CloseGetPusher(seqnum)

	msg, err := c.receiver.RecvBytes(0)
	if err != nil {
		log.Fatal(err)
	}

	keyResponse := &pb.KeyResponse{}
	err = proto.Unmarshal(msg, keyResponse)
	if err != nil {
		log.Fatal(err)
	}
	if keyResponse.GetError() == pb.CacheError_NO_ERROR {
		if !ok {
			c.RWset[k] = []any{c.CC.CacheAddress, float64(keyResponse.GetWTimestamp()), float64(keyResponse.GetRTimestamp())}
			c.newRWset[k] = c.RWset[k]
		}
		return keyResponse.GetValue()
	} else {
		log.Println("key not found")
	}

	return nil
}

func (c *CacheClient) Set(k string, v []byte) error {
	writeRequest := &pb.WriteRequest{}
	writeRequest.Key = k
	writeRequest.Tid = c.Tid
	writeRequest.Value = v
	writeRequest.ResponseAddress = c.IPCAddress
	req_string, err := proto.Marshal(writeRequest)
	if err != nil {
		return err
	}
	setPusher, seqnum := c.CC.NewSetPusher()
	_, err = setPusher.SendBytes(req_string, 1)
	if err != nil {
		return err
	}
	c.CC.CloseSetPusher(seqnum)

	msg, err := c.receiver.RecvBytes(0)
	if err != nil {
		return err
	}
	setResponse := &pb.KeyResponse{}
	err = proto.Unmarshal(msg, setResponse)
	if err != nil {
		return err
	}

	if setResponse.Error != pb.CacheError_NO_ERROR {
		c.abort()
		return errors.New("abort")
	} else {
		metadata, ok := c.RWset[k]
		// "相同的写入会报错！"
		if ok && metadata[1].(float64) != float64(setResponse.GetWTimestamp()) {
			c.RWset[k] = []any{c.CC.CacheAddress, float64(-1), float64(setResponse.GetRTimestamp() + 1)}
			c.abort()
			log.Printf("Set abort: 读到值%s过旧", k)
			return errors.New("abort")
		}
	}
	// wt = -1 indicates that it is in the write buffer
	c.RWset[k] = []any{c.CC.CacheAddress, float64(-1), float64(setResponse.GetRTimestamp() + 1)}
	c.newRWset[k] = c.RWset[k]
	return nil
}

func (c *CacheClient) GetS(clientLabel string) int64 {
	req := &pb.KeyRequest{
		Key:             clientLabel,
		ResponseAddress: c.IPCAddress,
	}
	reqByte, err := proto.Marshal(req)
	if err != nil {
		log.Fatal(err)
	}
	getsPusher, seqnum := c.CC.NewGetSPusher()
	_, err = getsPusher.SendBytes(reqByte, 1)
	if err != nil {
		log.Fatal(err)
	}
	c.CC.CloseGetSPusher(seqnum)

	msg, err := c.receiver.RecvBytes(0)
	if err != nil {
		log.Fatal(err)
	}
	session := &pb.SessionRequest{}
	err = proto.Unmarshal(msg, session)
	if err != nil {
		log.Fatal(err)
	}
	return session.TimeFence
}

func (c *CacheClient) abort() {
	rwSet := map[string]*pb.Keys{}
	for k, v := range c.RWset {
		address := v[0].(string)
		wts := int64(v[1].(float64))
		if keys, ok := rwSet[address]; ok {
			if wts == -1 {
				keys.Keys = append(keys.Keys, k)
			}
		} else {
			if wts == -1 {
				rwSet[address] = &pb.Keys{
					Keys: []string{k},
				}
			} else {
				rwSet[address] = &pb.Keys{
					Keys: []string{},
				}
			}
		}
	}

	local := true
	if len(rwSet) == 1 {
		for k := range rwSet {
			if k != c.CC.CacheAddress {
				local = false
				break
			}
		}
	} else if len(rwSet) == 0 {
		return
	} else {
		local = false
	}

	abortRequest := &pb.CommitRequest{
		Tid:      c.Tid,
		WriteSet: rwSet,
		Local:    local,
	}

	babortRequest, err := proto.Marshal(abortRequest)
	if err != nil {
		log.Fatal(err)
	}
	abortPusher, seqnum := c.CC.NewAbortPusher()
	_, err = abortPusher.SendBytes(babortRequest, 1)
	if err != nil {
		log.Fatal(err)
	}
	c.CC.CloseAbortPusher(seqnum)
}

// cts is only passed into the last function
func Commit(c *CacheClient, cts int64, clientLable string, iscritical bool, output []byte, deps []string) (bool, int64) {
	if len(c.RWset) == 0 {
		return true, cts
	}
	session := &pb.SessionRequest{
		Client:    clientLable,
		TimeFence: cts,
	}
	readSet := map[string]*pb.Keys{}
	writeSet := map[string]*pb.Keys{}
	// k:key, v:[address,wt,rt], 写入的值wt=-1,rt=rt_now+1
	for k, v := range c.RWset {
		address := v[0].(string)
		wts := int64(v[1].(float64))
		rts := int64(v[2].(float64))
		if wts == -1 {
			if keys, ok := writeSet[address]; ok {
				keys.Keys = append(keys.Keys, k)
			} else {
				writeSet[address] = &pb.Keys{
					Keys: []string{k},
				}
			}
			if rts > cts {
				cts = rts
			}
		} else {
			if wts > cts {
				cts = wts
			}
		}
	}

	for k, v := range c.RWset {
		address := v[0].(string)
		wts := int64(v[1].(float64))
		rts := int64(v[2].(float64))
		if wts == -1 {
			continue
		} else if rts < cts {
			if keys, ok := readSet[address]; ok {
				keys.Keys = append(keys.Keys, k)
			} else {
				readSet[address] = &pb.Keys{
					Keys: []string{k},
				}
			}
		}
	}

	// Optimize read-only transactions
	if len(writeSet)+len(readSet) == 0 {
		return true, cts
	}

	local := true
	if len(writeSet) <= 1 && len(readSet) <= 1 {
		for k := range writeSet {
			if k != c.CC.CacheAddress {
				local = false
			}
		}
		for k := range readSet {
			if k != c.CC.CacheAddress {
				local = false
			}
		}
	} else {
		local = false
	}

	if iscritical {
		deps = []string{}
	}
	commitRequest := &pb.CommitRequest{
		Tid:             c.Tid,
		CommitTimestamp: cts,
		WriteSet:        writeSet,
		ReadSet:         readSet,
		Session:         session,
		ResponseAddress: c.IPCAddress,
		Local:           local,
		Output:          output,
		Deps:            deps,
	}
	req_bytes, err := proto.Marshal(commitRequest)
	if err != nil {
		log.Fatal(err)
	}

	commitPusher, seqnum := c.CC.NewCommitPusher()
	_, err = commitPusher.SendBytes(req_bytes, 1)
	if err != nil {
		log.Fatal(err)
	}
	c.CC.CloseCommitPusher(seqnum)
	msg, err := c.receiver.RecvBytes(0)
	if err != nil {
		log.Fatalf("Unexpected error while receiving commitresponse socket: %v", err)
	}
	// []byte{1}: commit, []byte{}:abort
	if len(msg) == 0 {
		log.Println("abort")
		return false, cts
	}
	return true, cts
}
