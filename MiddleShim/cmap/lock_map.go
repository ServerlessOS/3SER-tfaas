/*
 * @Author: lechoux lechoux@qq.com
 * @Date: 2022-09-07 14:47:55
 * @LastEditors: lechoux lechoux@qq.com
 * @LastEditTime: 2022-11-09 19:22:22
 * @Description: cache map
 */
package cmap

import (
	"log"
	"strconv"
	"strings"
	"sync"
)

type LockType int8

const (
	Validating LockType = 0
	Writing    LockType = 1
	Committing LockType = 2
)

var KEYSHARD_COUNT = 128

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (KEYSHARD_COUNT) map shards.
type LockMap []*LockMapShared

//
type Lock struct {
	sync.RWMutex
	Tids  map[string]bool
	Term  int
	Type  LockType
	isRob bool
}

// A "thread" safe string to anything map.
type LockMapShared struct {
	items        map[string]*Lock
	sync.RWMutex // Read Write mutex, guards access to internal map.
}

// Creates a new concurrent map.
func NewLockMap() LockMap {
	m := make(LockMap, KEYSHARD_COUNT)
	for i := 0; i < KEYSHARD_COUNT; i++ {
		m[i] = &LockMapShared{items: make(map[string]*Lock)}
	}
	return m
}

// GetShard returns shard under given key
func (m LockMap) GetShard(key string) *LockMapShared {
	return m[uint(fnv32(key))%uint(KEYSHARD_COUNT)]
}

// tid = 实际的tid+","+term
func (m LockMap) Lock(key string, tid string, lockType LockType) bool {
	// Get map shard.
	tidterm := strings.Split(tid, ",")
	term, _ := strconv.Atoi(tidterm[1])
	shard := m.GetShard(key)
	shard.Lock()
	val, ok := shard.items[key]
	if ok {
		//如果tid不在的话但是read可以加锁，否则是互斥锁返回false
		shard.Unlock()
		val.Lock()
		if len(val.Tids) != 0 {
			if val.Type == Validating {
				if lockType == Validating {
					val.Tids[tidterm[0]] = true
					val.Unlock()
					return true
				} else {
					log.Printf("%s Lock abort: %s 的锁已存在", tid, key)
					// 扩展租约的锁不可被互斥锁抢占，并且不可能出现同一事务既扩展该key租约又写入该key(可以等待)
					val.Unlock()
					return false
				}
			} else {
				// 互斥锁只有一个事务可以获取
				_, ok = val.Tids[tidterm[0]]
				if ok {
					if val.Term <= term {
						val.Term = term
						val.Unlock()
						return true
					} else {
						log.Printf("%s Lock abort: %s 的锁已存在", tid, key)
						val.Unlock()
						return false
					}
				} else if val.Type == Writing {
					// writing 判断当前回滚次数是否过多
					if !val.isRob {
						if term-val.Term >= 100 || (lockType == Validating && term > val.Term) {
							val.Tids = map[string]bool{tidterm[0]: true}
							val.Term = term
							val.Type = lockType
							val.isRob = true
							val.Unlock()
							return true
						}
					}
					log.Printf("%s Lock abort: %s 的锁已存在", tid, key)
					val.Unlock()
					return false
				} else {
					// committing 不可获取锁
					log.Printf("%s Lock abort: %s 的锁已存在", tid, key)
					val.Unlock()
					return false
				}
			}
		} else {
			val.Tids = map[string]bool{tidterm[0]: true}
			val.Term = term
			val.Type = lockType
			val.isRob = false
			val.Unlock()
			return true
		}
	} else {
		tids := map[string]bool{tidterm[0]: true}
		shard.items[key] = &Lock{
			Tids:  tids,
			Term:  term,
			Type:  lockType,
			isRob: false,
		}
		shard.Unlock()
		return true
	}
}

func (m LockMap) UnLock(key string, tid string, lockType LockType) {
	// Get map shard.
	tidterm := strings.Split(tid, ",")
	term, _ := strconv.Atoi(tidterm[1])
	shard := m.GetShard(key)
	shard.RLock()
	val, ok := shard.items[key]
	shard.RUnlock()
	if ok {
		val.Lock()
		if val.Tids[tidterm[0]] {
			if lockType != Validating {
				// 反之，如果持锁term更大，不做操作
				if term >= val.Term {
					delete(val.Tids, tidterm[0])
					val.Term = 0
					val.isRob = false
				}
			} else if lockType == val.Type {
				delete(val.Tids, tidterm[0])
				val.Term = 0
				val.isRob = false
			}
		}
		val.Unlock()
	}
}

func (m LockMap) TestLock(key string, tid string) bool {
	// Get map shard.
	tidterm := strings.Split(tid, ",")
	term, _ := strconv.Atoi(tidterm[1])
	shard := m.GetShard(key)
	shard.RLock()
	val, ok := shard.items[key]
	shard.RUnlock()
	if ok {
		val.RLock()
		if len(val.Tids) == 0 {
			val.RUnlock()
			return false
		}
		_, ok = val.Tids[tidterm[0]]
		if ok {
			if val.Term == term {
				val.Type = Committing
				val.RUnlock()
				return true
			} else {
				log.Println("重复testlock")
			}
		}
		val.RUnlock()
	}

	return false
}

// Count returns the number of elements within the map.
func (m LockMap) Count() int {
	count := 0
	for i := 0; i < KEYSHARD_COUNT; i++ {
		shard := m[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

// Looks up an item under specified key
func (m LockMap) Has(key string) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// See if element is within shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Pop removes an element from the map and returns it
func (m LockMap) Pop(key string) (v *Lock, exists bool) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, exists
}

// IsEmpty checks if map is empty.
func (m LockMap) IsEmpty() bool {
	return m.Count() == 0
}

// Keys returns all keys as []string
func (m LockMap) Keys() []string {
	count := m.Count()
	ch := make(chan string, count)
	go func() {
		// Foreach shard.
		wg := sync.WaitGroup{}
		wg.Add(KEYSHARD_COUNT)
		for _, shard := range m {
			go func(shard *LockMapShared) {
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

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
