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

	pb "github.com/lechou-0/common/proto"
)

var METADATASHARD_COUNT = 128

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (METADATASHARD_COUNT) map shards.
type MetadataMap []*MetadataMapShared

// A "thread" safe string to anything map.
type MetadataMapShared struct {
	items        map[string]*pb.KeyValuePair
	sync.RWMutex // Read Write mutex, guards access to internal map.
}

// Creates a new concurrent map.
func NewMetadataMap() MetadataMap {
	m := make(MetadataMap, METADATASHARD_COUNT)
	for i := 0; i < METADATASHARD_COUNT; i++ {
		m[i] = &MetadataMapShared{items: make(map[string]*pb.KeyValuePair)}
	}
	return m
}

// GetShard returns shard under given key
func (m MetadataMap) GetShard(key string) *MetadataMapShared {
	return m[uint(fnv32(key))%uint(METADATASHARD_COUNT)]
}

func (m MetadataMap) MSet(data []*pb.KeyValuePair) {
	for _, value := range data {
		key := value.GetKey()
		shard := m.GetShard(key)
		shard.Lock()
		val, ok := shard.items[key]
		if ok {
			if value.GetWTimestamp() > val.GetWTimestamp() {
				shard.items[key] = value
			}
			if (value.GetWTimestamp() == val.GetWTimestamp()) && (value.GetRTimestamp() > val.GetRTimestamp()) {
				shard.items[key] = value
			}
		} else {
			shard.items[key] = value
		}
		shard.Unlock()
	}
}

/**
 * @Author: lechoux lechoux@qq.com
 * @description: Sets the given value under the specified key.
 * @param {string} key
 * @param {*pb.KeyValuePair} value
 * @return {bool} true: Write Successful  false: There is already an updated value
 */
func (m MetadataMap) Set(key string, value *pb.KeyValuePair) {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	val, ok := shard.items[key]
	if ok {
		if value.GetWTimestamp() > val.GetWTimestamp() {
			shard.items[key] = value
		}
		if (value.GetWTimestamp() == val.GetWTimestamp()) && (value.GetRTimestamp() > val.GetRTimestamp()) {
			shard.items[key] = value
		}
	} else {
		shard.items[key] = value
	}
	shard.Unlock()
}

// Callback to return new element to be inserted into the map
// It is called while lock is held, therefore it MUST NOT
// try to access other keys in same map, as it can lead to deadlock since
// Go sync.RWLock is not reentrant
type UpsertCb func(exist bool, valueInMap *pb.KeyValuePair, newValue *pb.KeyValuePair) *pb.KeyValuePair

// Insert or Update - updates existing element or inserts a new one using UpsertCb
func (m MetadataMap) Upsert(key string, value *pb.KeyValuePair, cb UpsertCb) (res *pb.KeyValuePair) {
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	res = cb(ok, v, value)
	shard.items[key] = res
	shard.Unlock()
	return res
}

// Sets the given value under the specified key if no value was associated with it.
func (m MetadataMap) SetIfAbsent(key string, value *pb.KeyValuePair) bool {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	_, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
	}
	shard.Unlock()
	return !ok
}

// Get retrieves an element from map under given key.
func (m MetadataMap) Get(key string) (*pb.KeyValuePair, bool) {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

// Count returns the number of elements within the map.
func (m MetadataMap) Count() int {
	count := 0
	for i := 0; i < METADATASHARD_COUNT; i++ {
		shard := m[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

// Looks up an item under specified key
func (m MetadataMap) Has(key string) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// See if element is within shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Remove removes an element from the map.
func (m MetadataMap) Remove(key string) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

// RemoveCb is a callback executed in a map.RemoveCb() call, while Lock is held
// If returns true, the element will be removed from the map
type RemoveCb func(key string, v *pb.KeyValuePair, exists bool) bool

// RemoveCb locks the shard containing the key, retrieves its current value and calls the callback with those params
// If callback returns true and element exists, it will remove it from the map
// Returns the value returned by the callback (even if element was not present in the map)
func (m MetadataMap) RemoveCb(key string, cb RemoveCb) bool {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	remove := cb(key, v, ok)
	if remove && ok {
		delete(shard.items, key)
	}
	shard.Unlock()
	return remove
}

// Pop removes an element from the map and returns it
func (m MetadataMap) Pop(key string) (v *pb.KeyValuePair, exists bool) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, exists
}

// IsEmpty checks if map is empty.
func (m MetadataMap) IsEmpty() bool {
	return m.Count() == 0
}

// Keys returns all keys as []string
func (m MetadataMap) Keys() []string {
	count := m.Count()
	ch := make(chan string, count)
	go func() {
		// Foreach shard.
		wg := sync.WaitGroup{}
		wg.Add(METADATASHARD_COUNT)
		for _, shard := range m {
			go func(shard *MetadataMapShared) {
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
