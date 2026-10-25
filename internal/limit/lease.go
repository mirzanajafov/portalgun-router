package limit

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
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
	debt       debt
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
	rdb      redis.Scripter
	prefix   string
	rule     Rule
	target   time.Duration
	ttl      time.Duration
	timeout  time.Duration
	fallback *Buckets
	now      Clock
	guard    *breaker

	mu     sync.Mutex
	keys   map[string]*leaseKey
	sweeps int
}

func NewLease(o Options) (Limiter, error) {
	if err := validate(o.Rule); err != nil {
		return nil, err
	}
	return &lease{
		rdb:      o.Redis,
		prefix:   o.Prefix,
		rule:     o.Rule,
		target:   o.LeaseTarget,
		ttl:      o.LeaseTTL,
		timeout:  o.RedisTimeout,
		fallback: NewBuckets(split(o.Rule, o.Instances)),
		now:      o.Now,
		guard:    newBreaker(o.Now),
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
				stale := old.fetching == nil && now >= old.expires && now >= old.emptyUntil && (old.debt.clear() || now-old.debt.at >= int64(forgetDebt))
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
		if !l.guard.allow() {
			d := l.degraded(st, key, now)
			st.mu.Unlock()
			return d
		}
		want := math.Ceil(max(st.thisSecond, st.lastSecond) * l.target.Seconds())
		want = min(max(want, 1), l.rule.Burst)
		epoch, owed := st.debt.unpaid()
		done := make(chan struct{})
		st.fetching = done
		st.mu.Unlock()

		grant, err := l.fetch(ctx, key, want, epoch, owed)

		st.mu.Lock()
		st.fetching = nil
		close(done)
		now = l.now()
		if err != nil {
			d := l.degraded(st, key, now)
			st.mu.Unlock()
			return d
		}
		st.debt.settle(epoch, owed)
		st.tokens = grant
		st.expires = now + int64(l.ttl)
		if grant < want {
			st.emptyUntil = now + int64((want-grant)/l.rule.Rate*1e9)
		}
	}
}

func (l *lease) degraded(st *leaseKey, key string, now int64) Decision {
	ok, after := l.fallback.Take(key, now)
	if ok {
		st.debt.add(1, now)
	}
	return Decision{Allowed: ok, RetryAfter: after, Degraded: true}
}

func (l *lease) fetch(ctx context.Context, key string, want float64, epoch string, owed float64) (float64, error) {
	rctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	n, err := leaseScript.Run(rctx, l.rdb, debtKeys(l.prefix+key, epoch), l.rule.Rate/1e6, l.rule.Burst, ttlMillis(l.rule), want, owed, debtTTL.Milliseconds()).Int64()
	l.guard.record(ctx, err)
	return float64(n), err
}

func (l *lease) Close() {}
