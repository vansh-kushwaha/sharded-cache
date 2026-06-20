package cache

import (
	"sync"
	"time"
)

const JanitorInterval = 1 * time.Second

type ShardedCache struct {
	shards     []*CacheShard
	shardCount uint32
	stop       chan struct{}
	closeOnce  sync.Once
	wg         sync.WaitGroup
}

func NewShardedCache() *ShardedCache {
	return NewShardedCacheWithCount(16)
}
func NewShardedCacheWithCount(count uint32) *ShardedCache {
	if count == 0 {
		count = 16
	}

	shards := make([]*CacheShard, count)

	cache := &ShardedCache{
		shardCount: count,
		shards:     shards,
		stop:       make(chan struct{}),
	}

	for i := uint32(0); i < count; i++ {
		shards[i] = &CacheShard{
			items: make(map[string]CacheItem),
		}
		cache.wg.Add(1)
		go shards[i].startJanitor(JanitorInterval, &cache.wg, cache.stop)
	}
	return cache
}

func (c *ShardedCache) Set(key string, value interface{}, ttl time.Duration) {
	shard := c.GetShard(key)
	var expiration time.Time
	if ttl > 0 {
		expiration = time.Now().Add(ttl)
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.items[key] = CacheItem{Value: value, ExpiredAt: expiration}
}

func (c *ShardedCache) Get(key string) (interface{}, bool) {
	shard := c.GetShard(key)
	return shard.get(key)

}

func (c *ShardedCache) Close() {
	c.closeOnce.Do(func() {
		close(c.stop)
		c.wg.Wait()
	})
}
