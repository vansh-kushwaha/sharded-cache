package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/vansh-kushwaha/sharded-cache/cache"
)

func main() {
	myCache := cache.NewShardedCache()
	defer myCache.Close()
	var wg sync.WaitGroup

	fmt.Println("Launching concurrent simulation tracking..")

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			k := fmt.Sprintf("session_id_%d", id)
			v := fmt.Sprintf("payload_%d", id*100)
			myCache.Set(k, v, 2*time.Second)
		}(i)
	}

	wg.Wait()
	fmt.Println("Cache populated smoothly!")

	if val, found := myCache.Get("session_id_25"); found {
		fmt.Printf("Verified Session 25 Data : %v\n", val)
	}
}
