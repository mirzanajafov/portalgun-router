package balance

import (
	"sync"
	"testing"
	"time"
)

const decay = 10 * time.Second

func balancers() map[string]Balancer[*item] {
	return map[string]Balancer[*item]{"p2c-ewma": P2C[*item]{}, "round-robin": &RoundRobin[*item]{}, "least-outstanding": LeastOutstanding[*item]{}}
}

type item struct {
	name string
	load *Load
}

func (i *item) Load() *Load { return i.load }

func items(names ...string) []*item {
	out := make([]*item, len(names))
	for i, n := range names {
		out[i] = &item{name: n, load: NewLoad(100, decay)}
	}
	return out
}

func TestPeakEWMAJumpsUpAndDecaysDown(t *testing.T) {
	l := NewLoad(10, decay)
	l.Observe(time.Millisecond, 0)
	l.Observe(100*time.Millisecond, int64(time.Millisecond))
	if got := l.Latency(); got != 100*time.Millisecond {
		t.Fatalf("after a slow sample: %v, want an immediate jump to 100ms", got)
	}
	now := int64(time.Millisecond)
	for range 100 {
		now += int64(100 * time.Millisecond)
		l.Observe(time.Millisecond, now)
	}
	if got := l.Latency(); got < 30*time.Millisecond || got > 45*time.Millisecond {
		t.Fatalf("after 10s of fast samples: %v, want about 1ms + 99ms/e", got)
	}
	for range 500 {
		now += int64(100 * time.Millisecond)
		l.Observe(time.Millisecond, now)
	}
	if got := l.Latency(); got > 2*time.Millisecond {
		t.Fatalf("after 60s of fast samples: %v, want close to 1ms", got)
	}
}

func TestCostDecaysWithoutSamples(t *testing.T) {
	l := NewLoad(10, decay)
	l.Observe(500*time.Millisecond, 0)
	early := l.Cost(int64(time.Second))
	late := l.Cost(int64(60 * time.Second))
	if !(late < early/100) {
		t.Fatalf("cost after 1s %.0f, after 60s %.0f; want the idle cost to decay", early, late)
	}
}

func TestShorterDecayForgetsFaster(t *testing.T) {
	short, long := NewLoad(10, time.Second), NewLoad(10, decay)
	short.Observe(100*time.Millisecond, 0)
	long.Observe(100*time.Millisecond, 0)
	at := int64(5 * time.Second)
	s, l := short.Cost(at), long.Cost(at)
	if s > float64(time.Millisecond) || l < float64(50*time.Millisecond) {
		t.Fatalf("after 5s idle: 1s decay %.2fms, default decay %.2fms", s/1e6, l/1e6)
	}
}

func TestCostPenalizesPendingWithoutSamples(t *testing.T) {
	fresh, busy := NewLoad(10, decay), NewLoad(10, decay)
	busy.Acquire()
	if fresh.Cost(0) != 0 || busy.Cost(0) < penaltyNanos {
		t.Fatalf("fresh %.0f busy %.0f", fresh.Cost(0), busy.Cost(0))
	}
}

func TestAcquireNeverExceedsTheCap(t *testing.T) {
	l := NewLoad(10, decay)
	var wg sync.WaitGroup
	var mu sync.Mutex
	peak := int64(0)
	for range 64 {
		wg.Go(func() {
			for range 1000 {
				if l.Acquire() {
					mu.Lock()
					peak = max(peak, l.Inflight())
					mu.Unlock()
					l.Release()
				}
			}
		})
	}
	wg.Wait()
	if peak > 10 || l.Inflight() != 0 {
		t.Fatalf("peak %d, left %d", peak, l.Inflight())
	}
}

func TestP2CPrefersTheCheaperTarget(t *testing.T) {
	its := items("fast", "slow")
	its[0].load.Observe(time.Millisecond, 0)
	its[1].load.Observe(50*time.Millisecond, 0)
	var b P2C[*item]
	for range 100 {
		got, ok := b.Pick(its, 0, nil)
		if !ok || got.name != "fast" {
			t.Fatalf("picked %v", got)
		}
	}
}

func TestP2CSpreadsOverEqualTargets(t *testing.T) {
	its := items("a", "b", "c")
	var b P2C[*item]
	counts := map[string]int{}
	for range 3000 {
		got, _ := b.Pick(its, 0, nil)
		counts[got.name]++
	}
	for _, it := range its {
		if counts[it.name] < 700 {
			t.Fatalf("counts %v", counts)
		}
	}
}

func TestBalancersSkipCappedTargets(t *testing.T) {
	for name, b := range balancers() {
		its := items("a", "b")
		its[0].load = NewLoad(1, decay)
		its[0].load.Acquire()
		for range 50 {
			got, ok := b.Pick(its, 0, nil)
			if !ok || got.name != "b" {
				t.Fatalf("%s picked %v", name, got)
			}
		}
		its[1].load = NewLoad(1, decay)
		its[1].load.Acquire()
		if _, ok := b.Pick(its, 0, nil); ok {
			t.Fatalf("%s picked from a fully capped pool", name)
		}
	}
}

func TestRoundRobinIsEven(t *testing.T) {
	its := items("a", "b", "c")
	b := &RoundRobin[*item]{}
	counts := map[string]int{}
	for range 300 {
		got, _ := b.Pick(its, 0, nil)
		counts[got.name]++
	}
	if counts["a"] != 100 || counts["b"] != 100 || counts["c"] != 100 {
		t.Fatalf("counts %v", counts)
	}
}

func TestLeastOutstandingPicksTheIdlest(t *testing.T) {
	its := items("a", "b", "c")
	for range 3 {
		its[0].load.Acquire()
	}
	its[2].load.Acquire()
	got, _ := LeastOutstanding[*item]{}.Pick(its, 0, nil)
	if got.name != "b" {
		t.Fatalf("picked %s", got.name)
	}
}

func TestFilterExcludesTargets(t *testing.T) {
	notA := func(it *item, _ int64) bool { return it.name != "a" }
	for name, b := range balancers() {
		its := items("a", "b", "c")
		for range 60 {
			got, ok := b.Pick(its, 0, notA)
			if !ok || got.name == "a" {
				t.Fatalf("%s picked %v", name, got)
			}
		}
		if _, ok := b.Pick(its, 0, func(*item, int64) bool { return false }); ok {
			t.Fatalf("%s picked through a filter that rejects everything", name)
		}
	}
}
