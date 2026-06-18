package cache

type ShardedCache []*CacheShard

func NewShardedCache() ShardedCache {
	c := make(ShardedCache, ShardCount)

	for i := 0; i < ShardCount; i++ {
		c[i] = &CacheShard{
			items: make(map[string]interface{}),
		}
	}
	return c
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
