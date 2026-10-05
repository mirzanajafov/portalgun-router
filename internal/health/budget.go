package health

import (
	"sync"
	"time"
)

const budgetWindow = 10

type bucket struct {
	sec      int64
	requests float64
	retries  float64
}

type Budget struct {
	ratio     float64
	minPerSec float64

	mu      sync.Mutex
	buckets [budgetWindow]bucket
}

func NewBudget(ratio, minPerSec float64) *Budget {
	return &Budget{ratio: ratio, minPerSec: minPerSec}
}

func (b *Budget) at(sec int64) *bucket {
	bk := &b.buckets[sec%budgetWindow]
	if bk.sec != sec {
		*bk = bucket{sec: sec}
	}
	return bk
}

func (b *Budget) Request(now int64) {
	sec := now / int64(time.Second)
	b.mu.Lock()
	b.at(sec).requests++
	b.mu.Unlock()
}

func (b *Budget) TryRetry(now int64) bool {
	sec := now / int64(time.Second)
	b.mu.Lock()
	defer b.mu.Unlock()
	var requests, retries float64
	for _, bk := range b.buckets {
		if sec-bk.sec < budgetWindow && bk.sec <= sec {
			requests += bk.requests
			retries += bk.retries
		}
	}
	if retries+1 > max(b.ratio*requests, b.minPerSec*budgetWindow) {
		return false
	}
	b.at(sec).retries++
	return true
}
