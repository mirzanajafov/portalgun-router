package limit

import (
	"time"
)

type bucket struct {
	tokens float64
	last   int64
}

func (bk *bucket) refilled(now int64, rate float64) float64 {
	return bk.tokens + float64(now-bk.last)/1e9*rate
}

type Buckets struct {
	rule    Rule
	buckets shards[bucket]
}

func NewBuckets(rule Rule) *Buckets { return &Buckets{rule: rule} }

func (b *Buckets) Take(key string, now int64) (bool, time.Duration) {
	sh := b.buckets.of(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	bk, ok := sh.m[key]
	if !ok {
		bk = &bucket{tokens: b.rule.Burst, last: now}
		sh.add(key, bk, func(old *bucket) bool { return old.refilled(now, b.rule.Rate) >= b.rule.Burst })
	}
	bk.tokens = min(b.rule.Burst, bk.refilled(now, b.rule.Rate))
	bk.last = now
	if bk.tokens >= 1 {
		bk.tokens--
		return true, 0
	}
	return false, time.Duration((1 - bk.tokens) / b.rule.Rate * 1e9)
}
