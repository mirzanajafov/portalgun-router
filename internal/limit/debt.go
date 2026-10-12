package limit

import (
	"context"
	"errors"
	"hash/maphash"
	"math/rand/v2"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	forgetDebt = time.Minute
	debtTTL    = 10 * time.Minute
)

func TimedOut(err error) bool {
	var ne net.Error
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout())
}

type debt struct {
	epoch string
	total float64
	paid  float64
	at    int64
}

func (d *debt) add(n float64, now int64) {
	if n <= 0 {
		return
	}
	if d.epoch == "" {
		d.epoch = strconv.FormatUint(rand.Uint64(), 36)
	}
	d.total += n
	d.at = now
}

func (d *debt) unpaid() (string, float64) {
	if d.total > d.paid {
		return d.epoch, d.total
	}
	return "", 0
}

func (d *debt) settle(epoch string, total float64) {
	if epoch == "" || epoch != d.epoch {
		return
	}
	d.paid = max(d.paid, total)
	if d.paid >= d.total {
		d.epoch, d.total, d.paid = "", 0, 0
	}
}

func (d *debt) clear() bool { return d.epoch == "" }

func debtKeys(key, epoch string) []string {
	if epoch == "" {
		return []string{key}
	}
	return []string{key, key + ":debt:" + epoch}
}

type ledgerShard struct {
	mu      sync.Mutex
	m       map[string]*debt
	sweepAt int
}

type ledger struct {
	shards [shardCount]ledgerShard
}

func newLedger() *ledger {
	l := &ledger{}
	for i := range l.shards {
		l.shards[i].m = map[string]*debt{}
		l.shards[i].sweepAt = minSweep
	}
	return l
}

func (l *ledger) shard(key string) *ledgerShard {
	return &l.shards[maphash.String(seed, key)%shardCount]
}

func (l *ledger) add(key string, n float64, now int64) {
	sh := l.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	d, ok := sh.m[key]
	if !ok {
		if len(sh.m) >= sh.sweepAt {
			for k, old := range sh.m {
				if now-old.at >= int64(forgetDebt) {
					delete(sh.m, k)
				}
			}
			sh.sweepAt = max(minSweep, 2*len(sh.m))
		}
		d = &debt{}
		sh.m[key] = d
	}
	d.add(n, now)
}

func (l *ledger) unpaid(key string) (string, float64) {
	sh := l.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if d, ok := sh.m[key]; ok {
		return d.unpaid()
	}
	return "", 0
}

func (l *ledger) settle(key, epoch string, total float64) {
	if epoch == "" {
		return
	}
	sh := l.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if d, ok := sh.m[key]; ok {
		d.settle(epoch, total)
		if d.clear() {
			delete(sh.m, key)
		}
	}
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
