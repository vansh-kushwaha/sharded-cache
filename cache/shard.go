package cache

import (
	"hash/fnv"
	"sync"
	"time"
)

type CacheItem struct {
	Value     interface{}
	ExpiredAt time.Time
}

func (item *CacheItem) IsExpired() bool {
	if item.ExpiredAt.IsZero() {
		return false
	}

	return time.Now().After(item.ExpiredAt)
}

type CacheShard struct {
	mu    sync.RWMutex
	items map[string]CacheItem
}

func (c *ShardedCache) GetShard(key string) *CacheShard {
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	hashValue := hasher.Sum32()

	return c.shards[hashValue%c.shardCount]
}

func (s *CacheShard) startJanitor(interval time.Duration, wg *sync.WaitGroup, stopChan <-chan struct{}) {
	defer wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:

			s.mu.Lock()
			for key, item := range s.items {
				if item.IsExpired() {
					delete(s.items, key)
				}
			}
			s.mu.Unlock()
		case <-stopChan:
			return
		}
	}
}

func (s *CacheShard) get(key string) (interface{}, bool) {
	s.mu.RLock()
	val, exists := s.items[key]
	s.mu.RUnlock()

	if !exists {
		return nil, false
	}
	if !val.IsExpired() {

		return val.Value, true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	val, exists = s.items[key]
	if !exists {
		return nil, false
	}

	if val.IsExpired() {
		delete(s.items, key)
		return nil, false
	}

	return val, exists
}
