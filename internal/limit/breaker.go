package limit

import (
	"context"
	"sync"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/stats"
	"github.com/redis/go-redis/v9"
)

const (
	windowSlots  = 10
	slotLen      = 10 * time.Millisecond
	tripFailures = 5
	tripShare    = 0.5
	openFor      = 100 * time.Millisecond
)

func ClientOptions(addr string) *redis.Options {
	return &redis.Options{
		Addr:                  addr,
		MaxRetries:            -1,
		DialTimeout:           time.Second,
		ReadTimeout:           time.Second,
		WriteTimeout:          time.Second,
		PoolSize:              64,
		MinIdleConns:          16,
		ContextTimeoutEnabled: true,
	}
}

type breakerState int

const (
	closed breakerState = iota
	tripped
	probing
)

type callCount struct {
	calls    int
	failures int
}

type breaker struct {
	now       Clock
	mu        sync.Mutex
	state     breakerState
	openUntil int64
	window    stats.Slots[callCount]
}

func newBreaker(now Clock) *breaker {
	return &breaker{now: now, window: stats.NewSlots[callCount](windowSlots, slotLen)}
}

func (b *breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case closed:
		return true
	case tripped:
		if b.now() < b.openUntil {
			return false
		}
		b.state = probing
		return true
	}
	return false
}

func (b *breaker) record(ctx context.Context, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	gone := err != nil && ctx.Err() != nil
	switch b.state {
	case probing:
		switch {
		case err == nil:
			b.state = closed
			b.window.Reset()
		case gone:
			b.state, b.openUntil = tripped, now
		default:
			b.state, b.openUntil = tripped, now+int64(openFor)
		}
	case closed:
		if gone {
			return
		}
		calls, failures := b.count(now, err != nil)
		if failures >= tripFailures && float64(failures) >= tripShare*float64(calls) {
			b.state, b.openUntil = tripped, now+int64(openFor)
		}
	}
}

func (b *breaker) count(now int64, failed bool) (calls, failures int) {
	slot := b.window.At(now)
	slot.calls++
	if failed {
		slot.failures++
	}
	for c := range b.window.Recent(now) {
		calls += c.calls
		failures += c.failures
	}
	return calls, failures
}
