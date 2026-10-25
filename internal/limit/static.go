package limit

import "context"

type static struct {
	buckets *Buckets
	now     Clock
}

func NewStatic(rule Rule, instances int, now Clock) Limiter {
	return &static{buckets: NewBuckets(split(rule, instances)), now: now}
}

func (s *static) Allow(_ context.Context, key string) Decision {
	ok, after := s.buckets.Take(key, s.now())
	return Decision{Allowed: ok, RetryAfter: after}
}

func (s *static) Close() {}
