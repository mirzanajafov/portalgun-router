package proxy

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/limit"
)

type decisionLog struct {
	mu      sync.RWMutex
	buf     []limit.Logged
	next    atomic.Int64
	dropped atomic.Int64
}

func newDecisionLog(capacity int) *decisionLog {
	if capacity <= 0 {
		return nil
	}
	return &decisionLog{buf: make([]limit.Logged, capacity)}
}

func (l *decisionLog) add(key string, allowed bool) {
	if l == nil {
		return
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	i := l.next.Add(1) - 1
	if i >= int64(len(l.buf)) {
		l.dropped.Add(1)
		return
	}
	l.buf[i] = limit.Logged{UnixNano: time.Now().UnixNano(), Key: key, Allowed: allowed}
}

type Decisions struct {
	Recording bool           `json:"recording"`
	Dropped   int64          `json:"dropped"`
	Decisions []limit.Logged `json:"decisions"`
}

func (l *decisionLog) drain() Decisions {
	if l == nil {
		return Decisions{Decisions: []limit.Logged{}}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	n := min(l.next.Load(), int64(len(l.buf)))
	out := Decisions{Recording: true, Dropped: l.dropped.Swap(0), Decisions: make([]limit.Logged, n)}
	copy(out.Decisions, l.buf[:n])
	clear(l.buf[:n])
	l.next.Store(0)
	return out
}

func (g *Gateway) DrainDecisions() Decisions { return g.decisions.drain() }
