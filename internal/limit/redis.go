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
	}, nil
}

func (e *exact) Allow(ctx context.Context, key string) Decision {
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	res, err := gcraScript.Run(ctx, e.rdb, []string{e.prefix + key}, e.interval, e.tolerance).Int64Slice()
	if err != nil || len(res) != 2 {
		ok, after := e.fallback.Take(key, e.now())
		return Decision{Allowed: ok, RetryAfter: after, Degraded: true}
	}
	return Decision{Allowed: res[0] == 1, RetryAfter: time.Duration(res[1]) * time.Microsecond}
}

func (e *exact) Close() {}
