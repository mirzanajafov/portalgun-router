package health

import (
	"sync"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/stats"
)

const budgetWindow = 10

type budgetCount struct {
	requests float64
	retries  float64
}

type Budget struct {
	ratio     float64
	minPerSec float64

	mu     sync.Mutex
	counts stats.Slots[budgetCount]
}

func NewBudget(ratio, minPerSec float64) *Budget {
	return &Budget{ratio: ratio, minPerSec: minPerSec, counts: stats.NewSlots[budgetCount](budgetWindow, time.Second)}
}

func (b *Budget) Request(now int64) {
	b.mu.Lock()
	b.counts.At(now).requests++
	b.mu.Unlock()
}

func (b *Budget) TryRetry(now int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	var requests, retries float64
	for c := range b.counts.Recent(now) {
		requests += c.requests
		retries += c.retries
	}
	if retries+1 > max(b.ratio*requests, b.minPerSec*budgetWindow) {
		return false
	}
	b.counts.At(now).retries++
	return true
}
