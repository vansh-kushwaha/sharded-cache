package cache

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestShardedCacheConcurrency(t *testing.T) {
	c := NewShardedCache()
	defer c.Close()
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
				c.Set(key, value, 1*time.Second)
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

func TestCacheEviction(t *testing.T) {
	c := NewShardedCache()
	defer c.Close()
	t.Log("Inserting short-lived keys...")
	c.Set("key_fast", "will_expire_soon", 50*time.Millisecond)
	c.Set("key_immortal", "will_live_forever", 0)

	if val, found := c.Get("key_fast"); !found || val != "will_expire_soon" {
		t.Fatalf("Expected 'key_fast' to exist immediately, go found=%v, val=%v", found, val)
	}

	time.Sleep(1 * time.Second)

	if _, found := c.Get("key_fast"); found {
		t.Error("Lazy eviction failed: 'key_fast' should have been evicted after TTL expired")
	}

	if val, found := c.Get("key_immortal"); !found || val != "will_live_forever" {
		t.Errorf("Expected 'key_immortal' to survive, got found=%v, val=%v", found, val)
	}
}

func TestJanitorGoroutineLeak(t *testing.T) {
	runtime.GC()

	startingGoroutines := runtime.NumGoroutine()

	t.Logf("Starting goroutine count: %d", startingGoroutines)

	// Create and abandon 5 separate ShardedCache instances.
	// Since each cache spawns 16 janitor goroutines, this creates 5 * 16 = 80 threads.
	for range 5 {
		c := NewShardedCache()
		c.Close()
	}

	// Give the runtime a brief moment to settle, then force garbage collection again
	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	endingGoroutines := runtime.NumGoroutine()

	t.Logf("Ending goroutine count: %d", endingGoroutines)

	leakedCount := endingGoroutines - startingGoroutines
	t.Logf("Threads leaked: %d goroutines are still spinning in the background", leakedCount)
}
