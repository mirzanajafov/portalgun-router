package proxy

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
)

type decisionLog struct {
	mu      sync.RWMutex
	buf     []gatewayapi.Decision
	next    atomic.Int64
	dropped atomic.Int64
}

func newDecisionLog(capacity int) *decisionLog {
	if capacity <= 0 {
		return nil
	}
	return &decisionLog{buf: make([]gatewayapi.Decision, capacity)}
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
	l.buf[i] = gatewayapi.Decision{UnixNano: time.Now().UnixNano(), Key: key, Allowed: allowed}
}

func (l *decisionLog) drain() gatewayapi.Decisions {
	if l == nil {
		return gatewayapi.Decisions{Decisions: []gatewayapi.Decision{}}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	n := min(l.next.Load(), int64(len(l.buf)))
	out := gatewayapi.Decisions{Recording: true, Dropped: l.dropped.Swap(0), Decisions: make([]gatewayapi.Decision, n)}
	copy(out.Decisions, l.buf[:n])
	clear(l.buf[:n])
	l.next.Store(0)
	return out
}

func (g *Gateway) DrainDecisions() gatewayapi.Decisions { return g.decisions.drain() }
