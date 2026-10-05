package stats

import (
	"testing"
	"time"
)

func TestBucketsCoverTheRange(t *testing.T) {
	if bucketOf(10*time.Microsecond) != 0 || bucketOf(time.Hour) != Buckets-1 {
		t.Fatal("ends of the range")
	}
	for _, d := range []time.Duration{60 * time.Microsecond, time.Millisecond, 37 * time.Millisecond, 2 * time.Second, 50 * time.Second} {
		i := bucketOf(d)
		if UpperBound(i) < d || UpperBound(i-1) > d {
			t.Errorf("%v landed in bucket %d (%v, %v]", d, i, UpperBound(i-1), UpperBound(i))
		}
	}
	if UpperBound(Buckets-2) < time.Minute {
		t.Fatalf("top bucket starts at %v", UpperBound(Buckets-2))
	}
}

func TestQuantileIsWithinOneBucket(t *testing.T) {
	var h Histogram
	for i := range 1000 {
		h.Observe(time.Duration(i+1) * 100 * time.Microsecond)
	}
	c := h.Drain()
	p95 := c.Quantile(0.95)
	if p95 < 95*time.Millisecond || p95 > 95*time.Millisecond*126/100 {
		t.Fatalf("p95 %v, want 95ms within one bucket", p95)
	}
	if h.Drain().Total() != 0 {
		t.Fatal("drain did not reset")
	}
	if (Counts{}).Quantile(0.5) != 0 {
		t.Fatal("empty histogram has a quantile")
	}
}

func TestRollingForgetsOldSeconds(t *testing.T) {
	r := NewRolling(3)
	var one Counts
	one[5] = 10
	r.Push(one)
	r.Push(Counts{})
	r.Push(Counts{})
	if r.Sum().Total() != 10 {
		t.Fatalf("sum %d inside the window", r.Sum().Total())
	}
	r.Push(Counts{})
	if r.Sum().Total() != 0 {
		t.Fatalf("sum %d after the window passed", r.Sum().Total())
	}
}
