package cache

type ShardedCache struct {
	shards     []*CacheShard
	shardCount uint32
}

func NewShardedCache() *ShardedCache {
	return NewShardedCacheWithCount(16)
}
func NewShardedCacheWithCount(count uint32) *ShardedCache {
	if count == 0 {
		count = 16
	}

	shards := make([]*CacheShard, count)

	for i := 0; i < ShardCount; i++ {
		shards[i] = &CacheShard{
			items: make(map[string]interface{}),
		}
	}
	return &ShardedCache{
		shardCount: count,
		shards:     shards,
	}
}

func (c ShardedCache) Set(key string, value interface{}) {
	shard := c.GetShard(key)
	shard.mu.Lock()
	shard.items[key] = value
	shard.mu.Unlock()
}

func (c ShardedCache) Get(key string) (interface{}, bool) {
	shard := c.GetShard(key)
	shard.mu.RLock()
	val, exists := shard.items[key]
	shard.mu.RUnlock()

	return val, exists
}
