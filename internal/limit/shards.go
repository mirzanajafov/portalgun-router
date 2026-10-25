package limit

import (
	"hash/maphash"
	"sync"
)

var seed = maphash.MakeSeed()

const (
	shardCount = 32
	minSweep   = 1024
)

type shards[V any] [shardCount]shard[V]

type shard[V any] struct {
	mu      sync.Mutex
	m       map[string]*V
	sweepAt int
}

func (s *shards[V]) of(key string) *shard[V] { return &s[maphash.String(seed, key)%shardCount] }

func (s *shards[V]) size() int {
	n := 0
	for i := range s {
		sh := &s[i]
		sh.mu.Lock()
		n += len(sh.m)
		sh.mu.Unlock()
	}
	return n
}

func (sh *shard[V]) add(key string, v *V, stale func(*V) bool) {
	if sh.m == nil {
		sh.m = map[string]*V{}
	}
	if len(sh.m) >= max(sh.sweepAt, minSweep) {
		for k, old := range sh.m {
			if stale(old) {
				delete(sh.m, k)
			}
		}
		sh.sweepAt = max(minSweep, 2*len(sh.m))
	}
	sh.m[key] = v
}
