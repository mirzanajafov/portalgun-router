package limit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Redis interface {
	redis.Scripter
}

type exact struct {
	rdb       Redis
	prefix    string
	interval  float64
	tolerance float64
	timeout   time.Duration
	fallback  *Buckets
	now       Clock
	guard     breaker
	debts     *ledger
}

func NewRedisExact(rdb Redis, prefix string, rule Rule, instances int, timeout time.Duration, now Clock) (Limiter, error) {
	if err := validate(rule); err != nil {
		return nil, err
	}
	interval := usPerToken(rule.Rate)
	return &exact{
		rdb:       rdb,
		prefix:    prefix,
		interval:  interval,
		tolerance: interval * rule.Burst,
		timeout:   timeout,
		fallback:  NewBuckets(split(rule, instances)),
		now:       now,
		guard:     breaker{now: now},
		debts:     newLedger(),
	}, nil
}

func (e *exact) Allow(ctx context.Context, key string) Decision {
	if !e.guard.allow() {
		return e.degraded(key)
	}
	epoch, owed := e.debts.unpaid(key)
	rctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	res, err := gcraScript.Run(rctx, e.rdb, debtKeys(e.prefix+key, epoch), e.interval, e.tolerance, owed, debtTTL.Milliseconds()).Int64Slice()
	e.guard.record(ctx, err)
	if err == nil {
		e.debts.settle(key, epoch, owed)
	}
	if err != nil || len(res) != 2 {
		return e.degraded(key)
	}
	return Decision{Allowed: res[0] == 1, RetryAfter: time.Duration(res[1]) * time.Microsecond}
}

func (e *exact) degraded(key string) Decision {
	now := e.now()
	ok, after := e.fallback.Take(key, now)
	if ok {
		e.debts.add(key, 1, now)
	}
	return Decision{Allowed: ok, RetryAfter: after, Degraded: true}
}

func (e *exact) Close() {}
