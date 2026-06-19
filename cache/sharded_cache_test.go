package cache

import (
	"fmt"
	"sync"
	"testing"
)

func TestShardedCacheConcurrency(t *testing.T) {
	c := NewShardedCache()
	var wg sync.WaitGroup

	const workerCount = 100
	const iterations = 1000

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := fmt.Sprintf("worker_%d_key_%d", workerId, j)
				value := j
				c.Set(key, value)
			}
		}(i)
	}

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := fmt.Sprintf("worker_%d_key_%d", workerId, j)
				_, _ = c.Get(key)
			}
		}(i)
	}
	wg.Wait()
}
