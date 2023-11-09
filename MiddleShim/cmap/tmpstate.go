/*
 * @Author: lechoux lechoux@qq.com
 * @Date: 2022-09-07 14:47:55
 * @LastEditors: lechoux lechoux@qq.com
 * @LastEditTime: 2022-11-21 00:00:53
 * @Description: cache map
 */
package cmap

import (
	"sync"
)

var TMPSTATESHARD_COUNT = 128

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (TMPSTATESHARD_COUNT) map shards.
type TmpstateMap []*TmpstateMapShared

type State struct {
	Value []byte
	Tid   string
}

// A "thread" safe string to anything map.
type TmpstateMapShared struct {
	items        map[string]*State
	sync.RWMutex // Read Write mutex, guards access to internal map.
}

// Creates a new concurrent map.
func NewTmpstateMap() TmpstateMap {
	m := make(TmpstateMap, TMPSTATESHARD_COUNT)
	for i := 0; i < TMPSTATESHARD_COUNT; i++ {
		m[i] = &TmpstateMapShared{items: make(map[string]*State)}
	}
	return m
}

// GetShard returns shard under given key
func (m TmpstateMap) GetShard(key string) *TmpstateMapShared {
	return m[uint(fnv32(key))%uint(TMPSTATESHARD_COUNT)]
}

/**
 * @Author: lechoux lechoux@qq.com
 * @description: Sets the given value under the specified key.
 * @param {string} key
 * @param {[]byte} value
 * @return {bool} true: Write Successful  false: There is already an updated value
 */
func (m TmpstateMap) Set(key string, state *State) bool {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	shard.items[key] = state
	shard.Unlock()
	return true
}

// Sets the given value under the specified key if no value was associated with it.
func (m TmpstateMap) SetIfAbsent(key string, state *State) bool {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	_, ok := shard.items[key]
	if !ok {
		shard.items[key] = state
	}
	shard.Unlock()
	return !ok
}

// Get retrieves an element from map under given key.
func (m TmpstateMap) Get(key string) (*State, bool) {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

// Count returns the number of elements within the map.
func (m TmpstateMap) Count() int {
	count := 0
	for i := 0; i < TMPSTATESHARD_COUNT; i++ {
		shard := m[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

// Looks up an item under specified key
func (m TmpstateMap) Has(key string) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// See if element is within shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Remove removes an element from the map.
func (m TmpstateMap) Remove(key string, tid string) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	s, ok := shard.items[key]
	if ok {
		if s.Tid == tid {
			delete(shard.items, key)
		}
	}
	shard.Unlock()
}

// IsEmpty checks if map is empty.
func (m TmpstateMap) IsEmpty() bool {
	return m.Count() == 0
}

// Keys returns all keys as []string
func (m TmpstateMap) Keys() []string {
	count := m.Count()
	ch := make(chan string, count)
	go func() {
		// Foreach shard.
		wg := sync.WaitGroup{}
		wg.Add(TMPSTATESHARD_COUNT)
		for _, shard := range m {
			go func(shard *TmpstateMapShared) {
				// Foreach key, value pair.
				shard.RLock()
				for key := range shard.items {
					ch <- key
				}
				shard.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	// Generate keys
	keys := make([]string, 0, count)
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}
