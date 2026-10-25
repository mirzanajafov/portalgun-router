package limit

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"
)

type coverage struct{ cases, hits, slow atomic.Int64 }

func (c *coverage) saw(hit bool) {
	c.cases.Add(1)
	if hit {
		c.hits.Add(1)
	}
}

func (c *coverage) degraded(t *rapid.T, n int) {
	c.slow.Add(1)
	t.Skipf("%d decisions fell back to the local split, so this case says nothing about the shared bucket", n)
}

func (c *coverage) atLeast(t *testing.T, share float64, what string) {
	t.Helper()
	if slow, n := c.slow.Load(), c.cases.Load()+c.slow.Load(); slow*10 > n {
		t.Fatalf("%d of %d cases had a Redis call slower than its timeout", slow, n)
	}
	if n, hits := c.cases.Load(), c.hits.Load(); float64(hits) < share*float64(n) {
		t.Fatalf("only %d of %d cases %s, so the property was barely exercised", hits, n, what)
	}
}

func overWindow(admitted []int64, rate, burst, slack float64) (int, int, bool) {
	for i := range admitted {
		for j := i; j < len(admitted); j++ {
			allowed := burst + rate*float64(admitted[j]-admitted[i])/1e9 + slack
			if float64(j-i+1) > allowed {
				return i, j, true
			}
		}
	}
	return 0, 0, false
}

func TestBucketNeverAdmitsMoreThanRateTimesWindowPlusBurst(t *testing.T) {
	var cov coverage
	rapid.Check(t, func(t *rapid.T) {
		rate := rapid.Float64Range(0.5, 5000).Draw(t, "rate")
		burst := float64(rapid.IntRange(1, 50).Draw(t, "burst"))
		gaps := rapid.SliceOfN(rapid.Int64Range(0, int64(2e9/rate)), 20, 400).Draw(t, "gaps")
		var clk fakeClock
		l := NewStatic(Options{Rule: Rule{Rate: rate, Burst: burst}, Instances: 1, Now: clk.now})
		var admitted []int64
		for _, g := range gaps {
			clk.advance(time.Duration(g))
			if l.Allow(context.Background(), "k").Allowed {
				admitted = append(admitted, clk.now())
			}
		}
		cov.saw(len(admitted) < len(gaps))
		if i, j, over := overWindow(admitted, rate, burst, 1e-6); over {
			t.Fatalf("admitted %d between %v and %v, more than %.0f + %.2f/s over the window", j-i+1,
				time.Duration(admitted[i]), time.Duration(admitted[j]), burst, rate)
		}
	})
	cov.atLeast(t, 0.3, "refused anything")
}

func TestBucketAdmitsEverythingThatArrivesWithinTheRate(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		rate := rapid.Float64Range(0.5, 5000).Draw(t, "rate")
		burst := float64(rapid.IntRange(1, 50).Draw(t, "burst"))
		interval := int64(1e9/rate) + 2
		gaps := rapid.SliceOfN(rapid.Int64Range(interval, 3*interval), 20, 400).Draw(t, "gaps")
		var clk fakeClock
		l := NewStatic(Options{Rule: Rule{Rate: rate, Burst: burst}, Instances: 1, Now: clk.now})
		for i, g := range gaps {
			clk.advance(time.Duration(g))
			if d := l.Allow(context.Background(), "k"); !d.Allowed {
				t.Fatalf("request %d refused %v after the previous one, at %.2f/s", i+1, time.Duration(g), rate)
			}
		}
	})
}

var gapChoices = []time.Duration{0, 0, 0, 200 * time.Microsecond, time.Millisecond, 3 * time.Millisecond}

func TestRedisExactNeverAdmitsMoreThanRateTimesWindowPlusBurst(t *testing.T) {
	rdb := testRedis(t)
	base := prefix(t)
	var cases atomic.Int64
	var cov coverage
	rapid.Check(t, func(rt *rapid.T) {
		rate := rapid.Float64Range(50, 2000).Draw(rt, "rate")
		burst := float64(rapid.IntRange(1, 20).Draw(rt, "burst"))
		gaps := rapid.SliceOfN(rapid.SampledFrom(gapChoices), 10, 50).Draw(rt, "gaps")
		l, err := NewRedisExact(Options{Redis: rdb, Prefix: fmt.Sprintf("%s%d:", base, cases.Add(1)), Rule: Rule{Rate: rate, Burst: burst}, Instances: 1, RedisTimeout: time.Second, Now: wall})
		if err != nil {
			rt.Fatal(err)
		}
		var starts, ends []int64
		degraded := 0
		for _, g := range gaps {
			time.Sleep(g)
			start := wall()
			d := l.Allow(context.Background(), "k")
			if d.Degraded {
				degraded++
			}
			if d.Allowed {
				starts, ends = append(starts, start), append(ends, wall())
			}
		}
		if degraded > 0 {
			cov.degraded(rt, degraded)
		}
		cov.saw(len(starts) < len(gaps))
		slack := 0.1 + rate*2e-6
		for i := range starts {
			for j := i; j < len(starts); j++ {
				if float64(j-i+1) > burst+rate*float64(ends[j]-starts[i])/1e9+slack {
					rt.Fatalf("admitted %d within %v at %.0f/s and a burst of %.0f", j-i+1, time.Duration(ends[j]-starts[i]), rate, burst)
				}
			}
		}
	})
	cov.atLeast(t, 0.3, "refused anything")
}

func TestLeaseNeverAdmitsAheadOfTheSharedBucket(t *testing.T) {
	rdb := testRedis(t)
	base := prefix(t)
	var cases atomic.Int64
	var cov coverage
	rapid.Check(t, func(rt *rapid.T) {
		rate := rapid.Float64Range(100, 2000).Draw(rt, "rate")
		burst := float64(rapid.IntRange(1, 40).Draw(rt, "burst"))
		instances := rapid.IntRange(1, 4).Draw(rt, "instances")
		target := rapid.SampledFrom([]time.Duration{5 * time.Millisecond, 20 * time.Millisecond, 100 * time.Millisecond}).Draw(rt, "target")
		schedules := make([][]time.Duration, instances)
		for i := range schedules {
			schedules[i] = rapid.SliceOfN(rapid.SampledFrom(gapChoices), 10, 40).Draw(rt, fmt.Sprint("gaps", i))
		}
		p := fmt.Sprintf("%s%d:", base, cases.Add(1))
		rule := Rule{Rate: rate, Burst: burst}
		var mu sync.Mutex
		var admitted []int64
		var degraded int
		var wg sync.WaitGroup
		began := wall()
		for _, gaps := range schedules {
			l, err := NewLease(Options{Redis: rdb, Prefix: p, Rule: rule, Instances: instances, LeaseTarget: target, LeaseTTL: time.Second, RedisTimeout: time.Second, Now: wall})
			if err != nil {
				rt.Fatal(err)
			}
			wg.Go(func() {
				for _, g := range gaps {
					time.Sleep(g)
					d := l.Allow(context.Background(), "k")
					end := wall()
					mu.Lock()
					if d.Degraded {
						degraded++
					}
					if d.Allowed {
						admitted = append(admitted, end)
					}
					mu.Unlock()
				}
			})
		}
		wg.Wait()
		sent := 0
		for _, gaps := range schedules {
			sent += len(gaps)
		}
		if degraded > 0 {
			cov.degraded(rt, degraded)
		}
		cov.saw(len(admitted) < sent)
		slices.Sort(admitted)
		for k, at := range admitted {
			if ceiling := burst + rate*float64(at-began)/1e9 + 0.01; float64(k+1) > ceiling {
				rt.Fatalf("%d admitted %v after the start, the shared bucket only ever held %.2f", k+1, time.Duration(at-began), ceiling)
			}
		}
	})
	cov.atLeast(t, 0.3, "refused anything")
}
