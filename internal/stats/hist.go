package stats

import (
	"math"
	"sync/atomic"
	"time"
)

const (
	Buckets   = 64
	perOctave = 3
	floor     = 50 * time.Microsecond
)

type Counts [Buckets]int64

type Histogram struct {
	counts [Buckets]atomic.Int64
}

func bucketOf(d time.Duration) int {
	if d <= floor {
		return 0
	}
	i := int(math.Log2(float64(d)/float64(floor))*perOctave) + 1
	return min(i, Buckets-1)
}

func UpperBound(i int) time.Duration {
	if i <= 0 {
		return floor
	}
	return time.Duration(float64(floor) * math.Exp2(float64(i)/perOctave))
}

func (h *Histogram) Observe(d time.Duration) {
	h.counts[bucketOf(d)].Add(1)
}

func (h *Histogram) Drain() Counts {
	var c Counts
	for i := range h.counts {
		c[i] = h.counts[i].Swap(0)
	}
	return c
}

func (c *Counts) Add(o Counts) {
	for i := range c {
		c[i] += o[i]
	}
}

func (c *Counts) Sub(o Counts) {
	for i := range c {
		c[i] -= o[i]
	}
}

func (c Counts) Total() int64 {
	var n int64
	for _, v := range c {
		n += v
	}
	return n
}

func (c Counts) Quantile(q float64) time.Duration {
	total := c.Total()
	if total == 0 {
		return 0
	}
	rank := int64(math.Ceil(q * float64(total)))
	var seen int64
	for i, v := range c {
		seen += v
		if seen >= rank {
			return UpperBound(i)
		}
	}
	return UpperBound(Buckets - 1)
}

type Rolling struct {
	slots []Counts
	next  int
	sum   Counts
}

func NewRolling(seconds int) *Rolling {
	return &Rolling{slots: make([]Counts, seconds)}
}

func (r *Rolling) Push(c Counts) {
	r.sum.Sub(r.slots[r.next])
	r.slots[r.next] = c
	r.sum.Add(c)
	r.next = (r.next + 1) % len(r.slots)
}

func (r *Rolling) Sum() Counts { return r.sum }
