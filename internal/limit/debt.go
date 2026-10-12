package limit

import (
	"context"
	"errors"
	"hash/maphash"
	"net"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const forgetDebt = time.Minute

func TimedOut(err error) bool {
	var ne net.Error
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout())
}

func unsent(err error) bool {
	var reply redis.Error
	return err != nil && !TimedOut(err) && !errors.Is(err, context.Canceled) && !errors.As(err, &reply)
}

type owed struct {
	n  float64
	at int64
}

type ledgerShard struct {
	mu      sync.Mutex
	m       map[string]owed
	sweepAt int
}

type ledger struct {
	shards [shardCount]ledgerShard
}

func newLedger() *ledger {
	l := &ledger{}
	for i := range l.shards {
		l.shards[i].m = map[string]owed{}
		l.shards[i].sweepAt = minSweep
	}
	return l
}

func (l *ledger) shard(key string) *ledgerShard {
	return &l.shards[maphash.String(seed, key)%shardCount]
}

func (l *ledger) add(key string, n float64, now int64) {
	if n <= 0 {
		return
	}
	sh := l.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	o, ok := sh.m[key]
	if !ok && len(sh.m) >= sh.sweepAt {
		for k, old := range sh.m {
			if now-old.at >= int64(forgetDebt) {
				delete(sh.m, k)
			}
		}
		sh.sweepAt = max(minSweep, 2*len(sh.m))
	}
	sh.m[key] = owed{n: o.n + n, at: now}
}

func (l *ledger) take(key string) float64 {
	sh := l.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	o, ok := sh.m[key]
	if !ok {
		return 0
	}
	delete(sh.m, key)
	return o.n
}

func (l *ledger) size() int {
	n := 0
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		n += len(sh.m)
		sh.mu.Unlock()
	}
	return n
}
