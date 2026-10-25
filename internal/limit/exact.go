package limit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type exact struct {
	rdb       redis.Scripter
	prefix    string
	interval  float64
	tolerance float64
	timeout   time.Duration
	fallback  *Buckets
	now       Clock
	guard     *breaker
	debts     *ledger
}

func NewRedisExact(o Options) (Limiter, error) {
	if err := validate(o.Rule); err != nil {
		return nil, err
	}
	interval := usPerToken(o.Rule.Rate)
	return &exact{
		rdb:       o.Redis,
		prefix:    o.Prefix,
		interval:  interval,
		tolerance: interval * o.Rule.Burst,
		timeout:   o.RedisTimeout,
		fallback:  NewBuckets(split(o.Rule, o.Instances)),
		now:       o.Now,
		guard:     newBreaker(o.Now),
		debts:     &ledger{},
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
