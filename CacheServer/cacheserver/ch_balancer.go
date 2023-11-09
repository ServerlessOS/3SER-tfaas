package cacheserver

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/serialx/hashring"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

func newConsistentHashBalancerBuilder() balancer.Builder {
	return &consistentHashBalancerBuilder{}
}

type consistentHashBalancerBuilder struct{}

type consistentHashBalancer struct {
	// cc points to the balancer.ClientConn who creates the consistentHashBalancer.
	cc balancer.ClientConn

	// state indicates the state of the whole ClientConn from the perspective of the balancer.
	state connectivity.State
	// subConns maps the address string to balancer.SubConn.
	subConns map[string]balancer.SubConn
	// scInfos maps the balancer.SubConn to subConnInfo.
	scInfos sync.Map

	// picker is a balancer.Picker created by the balancer but used by the ClientConn.
	picker balancer.Picker

	// resolverErr is the last error reported by the resolver; cleared on successful resolution.
	resolverErr error
	// connErr is the last connection error; cleared upon leaving TransientFailure
	connErr error
}

// Build creates a consistentHashBalancer, and starts its scManager.
func (builder *consistentHashBalancerBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	b := &consistentHashBalancer{
		cc:       cc,
		subConns: make(map[string]balancer.SubConn),
		scInfos:  sync.Map{},
	}
	return b
}

// Name returns the name of the consistentHashBalancer registering in grpc.
func (builder *consistentHashBalancerBuilder) Name() string {
	return policy
}

// subConnInfo records the state and addr corresponding to the SubConn.
type subConnInfo struct {
	state connectivity.State
	addr  string
}

// UpdateClientConnState 会在连接基础信息发生改变时被调用,例如: resolver 更新地址列表信息
func (b *consistentHashBalancer) UpdateClientConnState(s balancer.ClientConnState) error {

	b.resolverErr = nil
	addrsSet := make(map[string]struct{})
	for _, a := range s.ResolverState.Addresses {
		addr := a.Addr
		addrsSet[addr] = struct{}{}
		if sc, ok := b.subConns[addr]; !ok {
			newSC, err := b.cc.NewSubConn([]resolver.Address{a}, balancer.NewSubConnOptions{HealthCheckEnabled: false})
			if err != nil {
				log.Printf("Consistent Hash Balancer: failed to create new SubConn: %v", err)
				continue
			}
			b.subConns[addr] = newSC
			b.scInfos.Store(newSC, &subConnInfo{
				state: connectivity.Idle,
				addr:  addr,
			})
			newSC.Connect()
		} else {
			b.cc.UpdateAddresses(sc, []resolver.Address{a})
		}
	}
	for a, sc := range b.subConns {
		if _, ok := addrsSet[a]; !ok {
			b.cc.RemoveSubConn(sc)
			delete(b.subConns, a)
		}
	}
	if len(s.ResolverState.Addresses) == 0 {
		b.ResolverError(fmt.Errorf("produced zero addresses"))
		return balancer.ErrBadResolverState
	}

	b.regeneratePicker()
	b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})

	return nil
}

// ResolverError is implemented from balancer.Balancer, copied from baseBalancer.
func (b *consistentHashBalancer) ResolverError(err error) {
	b.resolverErr = err

	b.regeneratePicker()
	if b.state != connectivity.TransientFailure {
		// The picker will not change since the balancer does not currently
		// report an error.
		return
	}
	b.cc.UpdateState(balancer.State{
		ConnectivityState: b.state,
		Picker:            b.picker,
	})
}

// regeneratePicker generates a new picker to replace the old one with new data, and update the state of the balancer.
func (b *consistentHashBalancer) regeneratePicker() {
	availableSCs := make(map[string]balancer.SubConn)
	for addr, sc := range b.subConns {
		// The next line may not be safe, but we have to use subConns without check,
		// for we have not set up connections with any SubConn, all of them are Idle.
		if st, ok := b.scInfos.Load(sc); ok && st.(*subConnInfo).state != connectivity.Shutdown && st.(*subConnInfo).state != connectivity.TransientFailure {
			if st.(*subConnInfo).state == connectivity.Idle {
				sc.Connect()
			}
			availableSCs[addr] = sc
		}
	}
	if len(availableSCs) == 0 {
		b.state = connectivity.TransientFailure
		b.picker = base.NewErrPicker(b.mergeErrors())
	} else {
		b.state = connectivity.Ready
		b.picker = NewConsistentHashPicker(availableSCs)
	}
}

// mergeErrors is copied from baseBalancer.
func (b *consistentHashBalancer) mergeErrors() error {
	if b.connErr == nil {
		return fmt.Errorf("last resolver error: %v", b.resolverErr)
	}
	if b.resolverErr == nil {
		return fmt.Errorf("last connection error: %v", b.connErr)
	}
	return fmt.Errorf("last connection error: %v; last resolver error: %v", b.connErr, b.resolverErr)
}

// 同步更新子连接状态
func (b *consistentHashBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	s := state.ConnectivityState
	v, ok := b.scInfos.Load(sc)
	if !ok {
		return
	}
	info := v.(*subConnInfo)
	oldS := info.state
	if oldS == connectivity.TransientFailure && s == connectivity.Connecting {
		return
	}
	info.state = s
	switch s {
	case connectivity.Shutdown:
		b.scInfos.Delete(sc)
	case connectivity.TransientFailure:
		b.connErr = state.ConnectionError
	}

	if (s == connectivity.TransientFailure || s == connectivity.Shutdown) != (oldS == connectivity.TransientFailure || oldS == connectivity.Shutdown) ||
		b.state == connectivity.TransientFailure {
		log.Println("子连接变为：" + s.String())
		b.regeneratePicker()
	}
	b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})
}

// Close is implemented by balancer.Balancer, copied from baseBalancer.
func (b *consistentHashBalancer) Close() {}

type consistentHashPicker struct {
	subConns   map[string]balancer.SubConn // address string -> balancer.SubConn
	hashRing   *hashring.HashRing
	needReport bool
	reportChan chan<- PickResult
}

type PickResult struct {
	Ctx context.Context
	SC  balancer.SubConn
}

func NewConsistentHashPicker(subConns map[string]balancer.SubConn) *consistentHashPicker {

	addrs := make([]string, 0)
	for addr := range subConns {
		addrs = append(addrs, addr)
	}
	log.Printf("consistent hash picker built with addresses %v\n", addrs)
	return &consistentHashPicker{
		subConns:   subConns,
		hashRing:   hashring.New(addrs),
		needReport: false,
	}
}

func (p *consistentHashPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	var ret balancer.PickResult
	key, ok := info.Ctx.Value(ctxKey).(string)
	if !ok {
		key = info.FullMethodName
	}
	
	if targetAddr, ok := p.hashRing.GetNode(key); ok {
		ret.SubConn = p.subConns[targetAddr]
		if p.needReport {
			p.reportChan <- PickResult{Ctx: info.Ctx, SC: ret.SubConn}
		}
	}
	
	//ret.SubConn = p.subConns["localhost:50000"]
	if ret.SubConn == nil {
		return ret, balancer.ErrNoSubConnAvailable
	}
	return ret, nil
}


