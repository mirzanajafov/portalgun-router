package limit

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	tripAfter = 3
	openFor   = time.Second
)

func ClientOptions(addr string) *redis.Options {
	return &redis.Options{
		Addr:                  addr,
		MaxRetries:            -1,
		DialTimeout:           time.Second,
		ReadTimeout:           time.Second,
		WriteTimeout:          time.Second,
		PoolSize:              64,
		ContextTimeoutEnabled: true,
	}
}

type breaker struct {
	now       Clock
	failures  atomic.Int32
	openUntil atomic.Int64
}

func (b *breaker) open() bool {
	return b.now() < b.openUntil.Load()
}

func (b *breaker) record(ctx context.Context, err error) {
	if err == nil {
		b.failures.Store(0)
		return
	}
	if ctx.Err() != nil {
		return
	}
	if b.failures.Add(1) >= tripAfter {
		b.failures.Store(0)
		b.openUntil.Store(b.now() + int64(openFor))
	}
}
