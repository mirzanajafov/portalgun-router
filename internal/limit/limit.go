package limit

import (
	"context"
	"fmt"
	"hash/maphash"
	"math"
	"sync"
	"time"
)

type Rule struct {
	Rate  float64
	Burst float64
}

type Decision struct {
	Allowed    bool
	RetryAfter time.Duration
	Degraded   bool
}

type Limiter interface {
	Allow(ctx context.Context, key string) Decision
	Close()
}

type Clock func() int64

var seed = maphash.MakeSeed()

const shardCount = 32

type bucket struct {
	tokens float64
	last   int64
}

type shard struct {
	mu      sync.Mutex
	m       map[string]*bucket
	sweepAt int
}

const minSweep = 1024

type Buckets struct {
	rule   Rule
	shards [shardCount]shard
}

func NewBuckets(rule Rule) *Buckets {
	b := &Buckets{rule: rule}
	for i := range b.shards {
		b.shards[i].m = map[string]*bucket{}
		b.shards[i].sweepAt = minSweep
	}
	return b
}

func (b *Buckets) Take(key string, now int64) (bool, time.Duration) {
	sh := &b.shards[maphash.String(seed, key)%shardCount]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	bk, ok := sh.m[key]
	if !ok {
		if len(sh.m) >= sh.sweepAt {
			b.sweep(sh, now)
			sh.sweepAt = max(minSweep, 2*len(sh.m))
		}
		bk = &bucket{tokens: b.rule.Burst, last: now}
		sh.m[key] = bk
	}
	bk.tokens = min(b.rule.Burst, bk.tokens+float64(now-bk.last)/1e9*b.rule.Rate)
	bk.last = now
	if bk.tokens >= 1 {
		bk.tokens--
		return true, 0
	}
	return false, time.Duration((1 - bk.tokens) / b.rule.Rate * 1e9)
}

func (b *Buckets) sweep(sh *shard, now int64) {
	for k, bk := range sh.m {
		if bk.tokens+float64(now-bk.last)/1e9*b.rule.Rate >= b.rule.Burst {
			delete(sh.m, k)
		}
	}
}

func (b *Buckets) Len() int {
	n := 0
	for i := range b.shards {
		sh := &b.shards[i]
		sh.mu.Lock()
		n += len(sh.m)
		sh.mu.Unlock()
	}
	return n
}

type static struct {
	buckets *Buckets
	now     Clock
}

func NewStatic(rule Rule, instances int, now Clock) Limiter {
	return &static{buckets: NewBuckets(split(rule, instances)), now: now}
}

func split(rule Rule, instances int) Rule {
	n := float64(max(instances, 1))
	return Rule{Rate: rule.Rate / n, Burst: max(1, rule.Burst/n)}
}

func (s *static) Allow(_ context.Context, key string) Decision {
	ok, after := s.buckets.Take(key, s.now())
	return Decision{Allowed: ok, RetryAfter: after}
}

func (s *static) Close() {}

func usPerToken(rate float64) float64 { return 1e6 / rate }

func ttlMillis(rule Rule) int64 {
	return int64(math.Ceil(rule.Burst/rule.Rate*1000)) + 1000
}

func validate(rule Rule) error {
	if !(rule.Rate > 0) || !(rule.Burst >= 1) || math.IsInf(rule.Rate, 0) || math.IsInf(rule.Burst, 0) {
		return fmt.Errorf("rate must be positive and burst at least 1, got %v/%v", rule.Rate, rule.Burst)
	}
	return nil
}
