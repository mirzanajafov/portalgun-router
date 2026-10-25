package limit

import "context"

type static struct {
	buckets *Buckets
	now     Clock
}

func NewStatic(o Options) Limiter {
	return &static{buckets: NewBuckets(split(o.Rule, o.Instances)), now: o.Now}
}

func (s *static) Allow(_ context.Context, key string) Decision {
	ok, after := s.buckets.Take(key, s.now())
	return Decision{Allowed: ok, RetryAfter: after}
}

func (s *static) Close() {}
