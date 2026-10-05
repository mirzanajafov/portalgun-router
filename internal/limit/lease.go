package limit

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type leaseKey struct {
	mu         sync.Mutex
	tokens     float64
	expires    int64
	emptyUntil int64
	fetching   chan struct{}
	second     int64
	thisSecond float64
	lastSecond float64
}

func (k *leaseKey) count(now int64) {
	sec := now / int64(time.Second)
	switch sec {
	case k.second:
	case k.second + 1:
		k.lastSecond, k.thisSecond = k.thisSecond, 0
	default:
		k.lastSecond, k.thisSecond = 0, 0
	}
	k.second = sec
	k.thisSecond++
}

func (k *leaseKey) take(now int64) bool {
	if k.tokens >= 1 && now < k.expires {
		k.tokens--
		return true
	}
	return false
}

type lease struct {
	rdb      Redis
	prefix   string
	rule     Rule
	target   time.Duration
	ttl      time.Duration
	timeout  time.Duration
	fallback *Buckets
	now      Clock

	mu      sync.Mutex
	keys    map[string]*leaseKey
	fetches atomic.Int64
	sweeps  int
}

func NewLease(rdb Redis, prefix string, rule Rule, instances int, target, ttl, timeout time.Duration, now Clock) (Limiter, error) {
	if err := validate(rule); err != nil {
		return nil, err
	}
	return &lease{
		rdb:      rdb,
		prefix:   prefix,
		rule:     rule,
		target:   target,
		ttl:      ttl,
		timeout:  timeout,
		fallback: NewBuckets(split(rule, instances)),
		now:      now,
		keys:     map[string]*leaseKey{},
	}, nil
}

func (l *lease) key(k string, now int64) *leaseKey {
	l.mu.Lock()
	defer l.mu.Unlock()
	st, ok := l.keys[k]
	if !ok {
		l.sweeps++
		if l.sweeps&0x3ff == 0 {
			for name, old := range l.keys {
				old.mu.Lock()
				stale := old.fetching == nil && now >= old.expires && now >= old.emptyUntil
				old.mu.Unlock()
				if stale {
					delete(l.keys, name)
				}
			}
		}
		st = &leaseKey{}
		l.keys[k] = st
	}
	return st
}

func (l *lease) Allow(ctx context.Context, key string) Decision {
	now := l.now()
	st := l.key(key, now)
	st.mu.Lock()
	st.count(now)
	for {
		if st.take(now) {
			st.mu.Unlock()
			return Decision{Allowed: true}
		}
		if now < st.emptyUntil {
			after := time.Duration(st.emptyUntil - now)
			st.mu.Unlock()
			return Decision{RetryAfter: after}
		}
		if wait := st.fetching; wait != nil {
			st.mu.Unlock()
			select {
			case <-wait:
			case <-ctx.Done():
				return Decision{RetryAfter: time.Duration(1e9 / l.rule.Rate)}
			}
			st.mu.Lock()
			now = l.now()
			continue
		}
		want := math.Ceil(max(st.thisSecond, st.lastSecond) * l.target.Seconds())
		want = min(max(want, 1), l.rule.Burst)
		done := make(chan struct{})
		st.fetching = done
		st.mu.Unlock()

		grant, err := l.fetch(ctx, key, want)

		st.mu.Lock()
		st.fetching = nil
		close(done)
		now = l.now()
		if err != nil {
			st.mu.Unlock()
			ok, after := l.fallback.Take(key, now)
			return Decision{Allowed: ok, RetryAfter: after, Degraded: true}
		}
		st.tokens = grant
		st.expires = now + int64(l.ttl)
		if grant < 1 {
			st.emptyUntil = now + int64(1e9/l.rule.Rate)
		}
	}
}

func (l *lease) fetch(ctx context.Context, key string, want float64) (float64, error) {
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	l.fetches.Add(1)
	n, err := leaseScript.Run(ctx, l.rdb, []string{l.prefix + key}, l.rule.Rate/1e6, l.rule.Burst, ttlMillis(l.rule), want).Int64()
	return float64(n), err
}

func (l *lease) Fetches() int64 { return l.fetches.Load() }

func (l *lease) Close() {}
