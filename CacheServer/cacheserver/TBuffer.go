package cacheserver

import (
	"context"
	"log"
	"sort"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type tBuffer struct {
	CBuffer map[string]*transactions
	sync.Mutex
}

type transactions struct {
	outputs [][]byte
	depss   [][]string
	index   map[string]int
	sync.RWMutex
}

func (t *transactions) Append(tid string, output []byte, deps []string) {
	t.Lock()
	t.outputs = append(t.outputs, output)
	t.depss = append(t.depss, deps)
	t.index[tid] = len(t.outputs) - 1
	t.Unlock()
}

func (t *transactions) BFSRecord(tid string, output []byte, deps []string, etcdClient *clientv3.Client) error {
	ancestors := []*struct {
		seqnum int
		tid    string
		result []byte
	}{}
	t.Lock()

	for i := 0; i < len(deps); i++ {
		d, ok := t.index[deps[i]]
		if ok {
			result := t.outputs[d]
			if result == nil {
				continue
			}
			ancestor := &struct {
				seqnum int
				tid    string
				result []byte
			}{seqnum: d, tid: deps[i], result: result}

			t.outputs[d] = nil

			ancestors = append(ancestors, ancestor)
			deps = append(deps, t.depss[d]...)
		}
	}
	t.Unlock()
	sort.Slice(ancestors, func(i, j int) bool {
		return ancestors[i].seqnum < ancestors[j].seqnum
	})
	for _, ancestor := range ancestors {
		txn := etcdClient.KV.Txn(context.TODO())
		txnR, err := txn.If(clientv3.Compare(clientv3.CreateRevision(tid), "=", 0)).
			Then(clientv3.OpPut(ancestor.tid, string(ancestor.result))).
			Commit()
		if err != nil {
			log.Printf("Unexpected error while sending tid message: %v", err)
			return err
		}
		if !txnR.Succeeded {
			log.Println("etcd error")
		}
	}
	txn := etcdClient.KV.Txn(context.TODO())
	txnR, err := txn.If(clientv3.Compare(clientv3.CreateRevision(tid), "=", 0)).
		Then(clientv3.OpPut(tid, string(output))).
		Commit()
	if err != nil {
		log.Printf("Unexpected error while sending tid message: %v", err)
		return err
	}
	if !txnR.Succeeded {
		log.Println("etcd error")
	}
	return nil
}
