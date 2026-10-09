package limit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type fakeClock struct{ ns atomic.Int64 }

func (c *fakeClock) now() int64              { return c.ns.Load() }
func (c *fakeClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

func TestBucketsBurstThenRate(t *testing.T) {
	var clk fakeClock
	b := NewBuckets(Rule{Rate: 10, Burst: 5})
	for i := range 5 {
		if ok, _ := b.Take("k", clk.now()); !ok {
			t.Fatalf("request %d of the burst refused", i+1)
		}
	}
	ok, after := b.Take("k", clk.now())
	if ok || after != 100*time.Millisecond {
		t.Fatalf("sixth request: allowed %v, retry after %v", ok, after)
	}
	clk.advance(100 * time.Millisecond)
	if ok, _ := b.Take("k", clk.now()); !ok {
		t.Fatal("no token after one refill interval")
	}
	if ok, _ := b.Take("other", clk.now()); !ok {
		t.Fatal("keys share a bucket")
	}
}

func TestBucketsForgetFullBuckets(t *testing.T) {
	var clk fakeClock
	b := NewBuckets(Rule{Rate: 1000, Burst: 10})
	for i := range 100000 {
		b.Take(fmt.Sprint("ip-", i), clk.now())
		clk.advance(time.Millisecond)
	}
	if n := b.Len(); n > 20000 {
		t.Fatalf("%d buckets kept for 100k one-off keys", n)
	}
}

func TestStaticSplitDividesTheRule(t *testing.T) {
	var clk fakeClock
	l := NewStatic(Rule{Rate: 100, Burst: 10}, 2, clk.now)
	allowed := 0
	for range 20 {
		if l.Allow(context.Background(), "k").Allowed {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("allowed %d of a burst of 10 split over 2 instances", allowed)
	}
}

func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("PORTAL_TEST_REDIS")
	if addr == "" {
		t.Skip("PORTAL_TEST_REDIS not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis at %s: %v", addr, err)
	}
	return rdb
}

func prefix(t *testing.T) string {
	return fmt.Sprintf("test:%s:%d:", t.Name(), time.Now().UnixNano())
}

func wall() int64 { return time.Now().UnixNano() }

func TestRedisExactIsSharedAndExact(t *testing.T) {
	rdb := testRedis(t)
	p := prefix(t)
	rule := Rule{Rate: 10, Burst: 20}
	a, _ := NewRedisExact(rdb, p, rule, 2, time.Second, wall)
	b, _ := NewRedisExact(rdb, p, rule, 2, time.Second, wall)
	allowed := 0
	for i := range 60 {
		l := a
		if i%2 == 1 {
			l = b
		}
		d := l.Allow(context.Background(), "k")
		if d.Degraded {
			t.Fatal("degraded with Redis up")
		}
		if d.Allowed {
			allowed++
		}
	}
	if allowed < 20 || allowed > 21 {
		t.Fatalf("two instances allowed %d of a shared burst of 20", allowed)
	}
	d := a.Allow(context.Background(), "k")
	if d.Allowed || d.RetryAfter <= 0 || d.RetryAfter > 100*time.Millisecond {
		t.Fatalf("after the burst: %+v", d)
	}
	time.Sleep(110 * time.Millisecond)
	if !a.Allow(context.Background(), "k").Allowed {
		t.Fatal("no token after one refill interval")
	}
}

func TestRedisExactFallsBackWhenRedisIsGone(t *testing.T) {
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 50 * time.Millisecond, MaxRetries: -1})
	defer dead.Close()
	l, _ := NewRedisExact(dead, "x:", Rule{Rate: 10, Burst: 4}, 2, 20*time.Millisecond, wall)
	allowed := 0
	for range 10 {
		d := l.Allow(context.Background(), "k")
		if !d.Degraded {
			t.Fatal("decision not marked degraded")
		}
		if d.Allowed {
			allowed++
		}
	}
	if allowed != 2 {
		t.Fatalf("fallback allowed %d, want half of the burst", allowed)
	}
}

func silentRedis(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var accepted atomic.Int64
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			go func() {
				_, _ = io.Copy(io.Discard, c)
				c.Close()
			}()
		}
	}()
	return ln.Addr().String(), &accepted
}

func TestRedisLimitersGiveUpOnARedisThatNeverAnswers(t *testing.T) {
	addr, accepted := silentRedis(t)
	rdb := redis.NewClient(ClientOptions(addr))
	t.Cleanup(func() { rdb.Close() })
	rule := Rule{Rate: 10, Burst: 4}
	e, _ := NewRedisExact(rdb, "x:", rule, 2, 5*time.Millisecond, wall)
	l, _ := NewLease(rdb, "y:", rule, 2, 20*time.Millisecond, time.Second, 5*time.Millisecond, wall)
	for name, lim := range map[string]Limiter{"redis-exact": e, "lease": l} {
		before := accepted.Load()
		began := time.Now()
		for range 50 {
			if d := lim.Allow(context.Background(), "k"); !d.Degraded {
				t.Fatalf("%s: decision %+v not degraded", name, d)
			}
		}
		if took := time.Since(began); took > 500*time.Millisecond {
			t.Fatalf("%s: 50 decisions took %v; the timeout has to bound the reply, not only the wait for a connection", name, took)
		}
		if n := accepted.Load() - before; n > 2*tripAfter {
			t.Fatalf("%s: %d connections for 50 decisions; a timed-out connection is closed, so without the breaker every request dials", name, n)
		}
	}
}

func TestBreakerOpensOnFailuresInARowOnly(t *testing.T) {
	var clk fakeClock
	b := breaker{now: clk.now}
	ctx := context.Background()
	fail := errors.New("i/o timeout")
	for _, err := range []error{fail, fail, nil, fail, fail} {
		b.record(ctx, err)
	}
	if b.open() {
		t.Fatal("open after a success broke the run of failures")
	}
	gone, cancel := context.WithCancel(ctx)
	cancel()
	b.record(gone, fail)
	if b.open() {
		t.Fatal("a request its client gave up on counted against Redis")
	}
	b.record(ctx, fail)
	if !b.open() {
		t.Fatalf("still closed after %d failures in a row", tripAfter)
	}
	clk.advance(openFor)
	if b.open() {
		t.Fatal("still open after the cool-down")
	}
}

func hammer(t *testing.T, limiters []Limiter, key string, perInstance int, gap time.Duration) int {
	t.Helper()
	var wg sync.WaitGroup
	var allowed atomic.Int64
	for _, l := range limiters {
		wg.Go(func() {
			for range perInstance {
				if l.Allow(context.Background(), key).Allowed {
					allowed.Add(1)
				}
				time.Sleep(gap)
			}
		})
	}
	wg.Wait()
	return int(allowed.Load())
}

func TestLeaseNeverOverAdmits(t *testing.T) {
	rdb := testRedis(t)
	p := prefix(t)
	rule := Rule{Rate: 200, Burst: 20}
	var ls []Limiter
	for range 3 {
		l, _ := NewLease(rdb, p, rule, 3, 20*time.Millisecond, time.Second, time.Second, wall)
		ls = append(ls, l)
	}
	began := time.Now()
	allowed := hammer(t, ls, "k", 400, time.Millisecond)
	ceiling := rule.Burst + rule.Rate*time.Since(began).Seconds()
	if float64(allowed) > ceiling {
		t.Fatalf("admitted %d, more than the %0.f the bucket could hold", allowed, ceiling)
	}
	if float64(allowed) < 0.5*ceiling {
		t.Fatalf("admitted %d of a possible %.0f", allowed, ceiling)
	}
}

func TestLeaseFetchesInBatches(t *testing.T) {
	rdb := testRedis(t)
	l, _ := NewLease(rdb, prefix(t), Rule{Rate: 10000, Burst: 10000}, 1, 50*time.Millisecond, time.Second, time.Second, wall)
	allowed := hammer(t, []Limiter{l}, "k", 2000, 0)
	if f := l.(*lease).Fetches(); allowed != 2000 || f > 250 {
		t.Fatalf("%d Redis calls for %d admitted requests out of 2000", f, allowed)
	}
}

func TestLeaseKeepsBatchingWhenTheBucketRunsDry(t *testing.T) {
	rdb := testRedis(t)
	rule := Rule{Rate: 200, Burst: 20}
	l, _ := NewLease(rdb, prefix(t), rule, 1, 20*time.Millisecond, time.Second, time.Second, wall)
	began := time.Now()
	allowed := hammer(t, []Limiter{l}, "k", 1000, 2*time.Millisecond)
	exact := rule.Burst + rule.Rate*time.Since(began).Seconds()
	fetches := l.(*lease).Fetches()
	if float64(allowed) > exact || float64(allowed) < 0.8*exact {
		t.Fatalf("admitted %d against an exact %.0f", allowed, exact)
	}
	if float64(fetches) > 0.5*float64(allowed) {
		t.Fatalf("%d Redis calls for %d admitted requests: over the limit every grant is short, and asking again a token later is a call per request", fetches, allowed)
	}
}

func TestLocalSyncStaysCloseToTheLimit(t *testing.T) {
	rdb := testRedis(t)
	p := prefix(t)
	rule := Rule{Rate: 300, Burst: 30}
	var ls []Limiter
	for i := range 2 {
		l, err := NewLocalSync(rdb, p, rule, 2, fmt.Sprint("gw-", i), 20*time.Millisecond, time.Second, wall)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(l.Close)
		ls = append(ls, l)
	}
	began := time.Now()
	allowed := hammer(t, ls, "k", 1500, time.Millisecond)
	ceiling := rule.Burst + rule.Rate*time.Since(began).Seconds()
	if float64(allowed) > 1.2*ceiling || float64(allowed) < 0.6*ceiling {
		t.Fatalf("admitted %d against an exact %.0f", allowed, ceiling)
	}
	if ls[0].(*localSync).Syncs() == 0 {
		t.Fatal("never synced")
	}
}

func TestLocalSyncFollowsSkewedDemandOnAFreshRedis(t *testing.T) {
	rdb := testRedis(t)
	if err := rdb.ScriptFlush(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	p := prefix(t)
	rule := Rule{Rate: 200, Burst: 20}
	var ls []Limiter
	for i := range 2 {
		l, err := NewLocalSync(rdb, p, rule, 2, fmt.Sprint("gw-", i), 20*time.Millisecond, time.Second, wall)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(l.Close)
		ls = append(ls, l)
	}
	var allowed, degraded int
	began := time.Now()
	for i := 0; time.Since(began) < 2*time.Second; i++ {
		l := ls[0]
		if i%10 == 9 {
			l = ls[1]
		}
		d := l.Allow(context.Background(), "k")
		if d.Allowed {
			allowed++
		}
		if d.Degraded {
			degraded++
		}
		time.Sleep(2500 * time.Microsecond)
	}
	exact := rule.Burst + rule.Rate*time.Since(began).Seconds()
	if degraded > 0 {
		t.Fatalf("%d decisions fell back with Redis up", degraded)
	}
	if float64(allowed) < 0.85*exact || float64(allowed) > 1.15*exact {
		t.Fatalf("admitted %d against an exact %.0f; a static split would admit about two thirds at 90/10", allowed, exact)
	}
	if ls[0].(*localSync).Syncs() == 0 || ls[1].(*localSync).Syncs() == 0 {
		t.Fatal("an instance never synced")
	}
}

func TestLocalSyncFallsBackAndCharges(t *testing.T) {
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 20 * time.Millisecond, MaxRetries: -1})
	defer dead.Close()
	l, _ := NewLocalSync(dead, "x:", Rule{Rate: 100, Burst: 10}, 2, "gw", 10*time.Millisecond, 10*time.Millisecond, wall)
	defer l.Close()
	l.Allow(context.Background(), "k")
	deadline := time.Now().Add(2 * time.Second)
	for !l.(*localSync).degraded.Load() {
		if time.Now().After(deadline) {
			t.Fatal("never noticed that Redis is gone")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if d := l.Allow(context.Background(), "k"); !d.Degraded {
		t.Fatalf("decision %+v not degraded", d)
	}
}
