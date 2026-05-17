package pqtime

import (
	"math/rand"
	"runtime"
	"sync"
	"testing"
)

func BenchmarkLocationCache(b *testing.B) {
	Reset()
	for i := 0; i < b.N; i++ {
		globalLocationCache.getLocation(rand.Intn(10000))
	}
}

func BenchmarkLocationCacheMultiThread(b *testing.B) {
	oldProcs := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(oldProcs)
	runtime.GOMAXPROCS(runtime.NumCPU())
	Reset()

	f := func(wg *sync.WaitGroup, loops int) {
		defer wg.Done()
		for range loops {
			globalLocationCache.getLocation(rand.Intn(10000))
		}
	}

	wg := &sync.WaitGroup{}
	b.ResetTimer()
	for range 10 {
		wg.Add(1)
		go f(wg, b.N/10)
	}
	wg.Wait()
}
