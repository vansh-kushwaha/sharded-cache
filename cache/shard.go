package cache

import (
	"hash/fnv"
	"sync"
)

const ShardCount = 16

type CacheShard struct {
	mu    sync.RWMutex
	items map[string]interface{}
}

func (c ShardedCache) GetShard(key string) *CacheShard {
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	hashValue := hasher.Sum32()

	return c.shards[hashValue%c.shardCount]
}
