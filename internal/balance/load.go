package balance

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const penaltyNanos = float64(time.Second)

type Load struct {
	inflight    atomic.Int64
	maxInflight int64
	decay       float64

	mu    sync.Mutex
	cost  float64
	stamp int64
}

func NewLoad(maxInflight int64, decay time.Duration) *Load {
	return &Load{maxInflight: maxInflight, decay: float64(decay)}
}

func (l *Load) Inflight() int64 { return l.inflight.Load() }

func (l *Load) MaxInflight() int64 { return l.maxInflight }

func (l *Load) Available() bool { return l.inflight.Load() < l.maxInflight }

func (l *Load) Acquire() bool {
	for {
		v := l.inflight.Load()
		if v >= l.maxInflight {
			return false
		}
		if l.inflight.CompareAndSwap(v, v+1) {
			return true
		}
	}
}

func (l *Load) Release() { l.inflight.Add(-1) }

func (l *Load) Observe(rtt time.Duration, now int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	sample := float64(rtt)
	if sample > l.cost {
		l.cost = sample
	} else {
		w := math.Exp(-float64(now-l.stamp) / l.decay)
		l.cost = l.cost*w + sample*(1-w)
	}
	l.stamp = now
}

func (l *Load) Cost(now int64) float64 {
	pending := l.inflight.Load()
	l.mu.Lock()
	if elapsed := now - l.stamp; elapsed > 0 {
		l.cost *= math.Exp(-float64(elapsed) / l.decay)
		l.stamp = now
	}
	cost := l.cost
	l.mu.Unlock()
	if cost == 0 && pending != 0 {
		return penaltyNanos + float64(pending)
	}
	return cost * float64(pending+1)
}

func (l *Load) Latency() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	return time.Duration(l.cost)
}
