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
	if n := b.buckets.size(); n > 20000 {
		t.Fatalf("%d buckets kept for 100k one-off keys", n)
	}
}

func TestStaticSplitDividesTheRule(t *testing.T) {
	var clk fakeClock
	l := NewStatic(Options{Rule: Rule{Rate: 100, Burst: 10}, Instances: 2, Now: clk.now})
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

func (s *shards[V]) size() int {
	n := 0
	for i := range s {
		sh := &s[i]
		sh.mu.Lock()
		n += len(sh.m)
		sh.mu.Unlock()
	}
	return n
}

type countedScripts struct {
	*redis.Client
	calls   atomic.Int64
	answers atomic.Int64
}

func (c *countedScripts) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	c.calls.Add(1)
	return c.Client.EvalSha(ctx, sha1, keys, args...)
}

func (c *countedScripts) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	c.calls.Add(1)
	return c.Client.Eval(ctx, script, keys, args...)
}

func (c *countedScripts) Pipeline() redis.Pipeliner {
	return countedPipeline{Pipeliner: c.Client.Pipeline(), answers: &c.answers}
}

type countedPipeline struct {
	redis.Pipeliner
	answers *atomic.Int64
}

func (p countedPipeline) Exec(ctx context.Context) ([]redis.Cmder, error) {
	cmds, err := p.Pipeliner.Exec(ctx)
	for _, cmd := range cmds {
		if cmd.Err() == nil {
			p.answers.Add(1)
		}
	}
	return cmds, err
}

func prefix(t *testing.T) string {
	return fmt.Sprintf("test:%s:%d:", t.Name(), time.Now().UnixNano())
}

func wall() int64 { return time.Now().UnixNano() }

func TestRedisExactIsSharedAndExact(t *testing.T) {
	rdb := testRedis(t)
	p := prefix(t)
	rule := Rule{Rate: 10, Burst: 20}
	a, _ := NewRedisExact(Options{Redis: rdb, Prefix: p, Rule: rule, Instances: 2, RedisTimeout: time.Second, Now: wall})
	b, _ := NewRedisExact(Options{Redis: rdb, Prefix: p, Rule: rule, Instances: 2, RedisTimeout: time.Second, Now: wall})
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
	l, _ := NewRedisExact(Options{Redis: dead, Prefix: "x:", Rule: Rule{Rate: 10, Burst: 4}, Instances: 2, RedisTimeout: 20 * time.Millisecond, Now: wall})
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
	opts := ClientOptions(addr)
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { rdb.Close() })
	deadline := time.Now().Add(2 * time.Second)
	for accepted.Load() < int64(opts.MinIdleConns) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := accepted.Load(); n != int64(opts.MinIdleConns) {
		t.Fatalf("%d connections opened ahead of the first call, want %d", n, opts.MinIdleConns)
	}
	rule := Rule{Rate: 10, Burst: 4}
	e, _ := NewRedisExact(Options{Redis: rdb, Prefix: "x:", Rule: rule, Instances: 2, RedisTimeout: 5 * time.Millisecond, Now: wall})
	l, _ := NewLease(Options{Redis: rdb, Prefix: "y:", Rule: rule, Instances: 2, LeaseTarget: 20 * time.Millisecond, LeaseTTL: time.Second, RedisTimeout: 5 * time.Millisecond, Now: wall})
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
		if n := accepted.Load() - before; n > tripFailures+2 {
			t.Fatalf("%s: %d new connections for 50 decisions; a timed-out connection is closed and replaced, so without the breaker every request costs a dial", name, n)
		}
	}
}

var errStall = errors.New("i/o timeout")

func TestBreakerIgnoresAStallThatFailsAFewCallsAmongMany(t *testing.T) {
	var clk fakeClock
	b := newBreaker(clk.now)
	for i := range 200 {
		var err error
		if i >= 100 && i < 100+tripFailures+2 {
			err = errStall
		}
		if !b.allow() {
			t.Fatalf("call %d refused: one stall that failed %d calls in a row opened the breaker", i, tripFailures+2)
		}
		b.record(context.Background(), err)
		clk.advance(time.Millisecond)
	}
}

func TestBreakerOpensWhenMostCallsFailAndProbesOneAtATime(t *testing.T) {
	var clk fakeClock
	b := newBreaker(clk.now)
	ctx := context.Background()
	for range tripFailures - 1 {
		b.allow()
		b.record(ctx, errStall)
	}
	if !b.allow() {
		t.Fatalf("open after %d failures", tripFailures-1)
	}
	b.record(ctx, errStall)
	if b.allow() {
		t.Fatalf("still closed after %d failures and no success", tripFailures)
	}
	clk.advance(openFor)
	if !b.allow() {
		t.Fatal("no probe after the cool-down")
	}
	if b.allow() {
		t.Fatal("a second probe while the first is out")
	}
	b.record(ctx, errStall)
	if b.allow() {
		t.Fatal("closed after a failed probe")
	}
	clk.advance(openFor)
	if !b.allow() {
		t.Fatal("no probe after the second cool-down")
	}
	b.record(ctx, nil)
	for range 2 {
		if !b.allow() {
			t.Fatal("still open after a probe got through")
		}
	}
}

func TestBreakerSkipsCallsTheClientGaveUpOn(t *testing.T) {
	var clk fakeClock
	b := newBreaker(clk.now)
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	for range 2 * tripFailures {
		b.allow()
		b.record(gone, context.Canceled)
	}
	if !b.allow() {
		t.Fatal("requests their client gave up on counted against Redis")
	}
	for range tripFailures {
		b.record(context.Background(), errStall)
	}
	clk.advance(openFor)
	if !b.allow() {
		t.Fatal("no probe")
	}
	b.record(gone, context.Canceled)
	if !b.allow() {
		t.Fatal("a probe its client gave up on kept the breaker open for another cool-down")
	}
}

func TestBreakerForgetsFailuresOutsideItsWindow(t *testing.T) {
	var clk fakeClock
	b := newBreaker(clk.now)
	for range tripFailures - 1 {
		b.allow()
		b.record(context.Background(), errStall)
	}
	clk.advance(windowSlots*slotLen + slotLen)
	b.allow()
	b.record(context.Background(), errStall)
	if !b.allow() {
		t.Fatal("failures from more than a window ago still counted")
	}
}

func TestLedgerSendsTheRunningTotalUntilRedisConfirmsIt(t *testing.T) {
	l := &ledger{}
	l.add("k", 2, 0)
	l.add("k", 1, 0)
	epoch, total := l.unpaid("k")
	if epoch == "" || total != 3 {
		t.Fatalf("unpaid %q %v, want 3", epoch, total)
	}
	l.add("k", 4, 0)
	l.settle("k", "someone-else", 7)
	l.settle("k", epoch, 3)
	if again, total := l.unpaid("k"); again != epoch || total != 7 {
		t.Fatalf("unpaid %q %v: the total keeps growing under one epoch until all of it is confirmed", again, total)
	}
	l.settle("k", epoch, 7)
	if l.debts.size() != 0 {
		t.Fatal("a key with everything confirmed is kept")
	}
	l.add("k", 1, 0)
	if next, total := l.unpaid("k"); next == epoch || total != 1 {
		t.Fatalf("unpaid %q %v: a paid-up key starts a new epoch from zero", next, total)
	}
}

func TestLedgerForgetsOldDebts(t *testing.T) {
	l := &ledger{}
	var now int64
	for i := range 300000 {
		l.add(fmt.Sprint("ip-", i), 1, now)
		now += int64(time.Millisecond)
	}
	if n := l.debts.size(); n > 150000 {
		t.Fatalf("%d debts kept from 300k one-off keys over five minutes", n)
	}
}

func redisNow(t *testing.T, rdb *redis.Client) float64 {
	t.Helper()
	at, err := rdb.Time(context.Background()).Result()
	if err != nil {
		t.Fatal(err)
	}
	return float64(at.UnixMicro())
}

func TestADebtSentTwiceIsChargedOnce(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	p := prefix(t)
	rule := Rule{Rate: 100, Burst: 10}
	interval := usPerToken(rule.Rate)
	gcra := func(epoch string, total float64) {
		if err := gcraScript.Run(ctx, rdb, debtKeys(p+"exact", epoch), interval, interval*rule.Burst, total, debtTTL.Milliseconds()).Err(); err != nil {
			t.Fatal(err)
		}
	}
	gcra("", 0)
	gcra("e1", 30)
	gcra("e1", 50)
	gcra("e1", 50)
	gcra("e1", 30)
	tat, err := rdb.Get(ctx, p+"exact").Float64()
	if err != nil {
		t.Fatal(err)
	}
	if ahead := tat - redisNow(t, rdb); ahead < 400_000 || ahead > 600_000 {
		t.Fatalf("theoretical arrival %.0f ms ahead, want about 510: a debt that reached 50 at 10 ms each, charged once however often and in whatever order its totals arrive", ahead/1000)
	}

	lease := func(epoch string, total float64) {
		if err := leaseScript.Run(ctx, rdb, debtKeys(p+"lease", epoch), rule.Rate/1e6, rule.Burst, ttlMillis(rule), 1, total, debtTTL.Milliseconds()).Err(); err != nil {
			t.Fatal(err)
		}
	}
	lease("", 0)
	lease("e1", 30)
	lease("e1", 50)
	lease("e1", 50)
	lease("e1", 30)
	level, err := rdb.HGet(ctx, p+"lease", "level").Float64()
	if err != nil {
		t.Fatal(err)
	}
	if level < -45 || level > -35 {
		t.Fatalf("bucket at %.1f, want about -41: a debt of 50 against 9 tokens, charged once", level)
	}
}

type gate struct {
	ln     net.Listener
	target string
	shut   atomic.Bool
	mu     sync.Mutex
	conns  map[net.Conn]bool
}

func gated(t *testing.T) (*gate, *redis.Client) {
	t.Helper()
	testRedis(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := &gate{ln: ln, target: os.Getenv("PORTAL_TEST_REDIS"), conns: map[net.Conn]bool{}}
	go g.serve()
	rdb := redis.NewClient(ClientOptions(ln.Addr().String()))
	t.Cleanup(func() {
		rdb.Close()
		ln.Close()
		g.cut()
	})
	return g, rdb
}

func (g *gate) serve() {
	for {
		c, err := g.ln.Accept()
		if err != nil {
			return
		}
		if g.shut.Load() {
			c.Close()
			continue
		}
		up, err := net.Dial("tcp", g.target)
		if err != nil {
			c.Close()
			continue
		}
		g.mu.Lock()
		g.conns[c], g.conns[up] = true, true
		g.mu.Unlock()
		go func() {
			_, _ = io.Copy(up, c)
			up.Close()
			c.Close()
		}()
		go func() {
			_, _ = io.Copy(c, up)
			c.Close()
			up.Close()
		}()
	}
}

func (g *gate) cut() {
	g.shut.Store(true)
	g.mu.Lock()
	defer g.mu.Unlock()
	for c := range g.conns {
		c.Close()
	}
	clear(g.conns)
}

func (g *gate) restore() { g.shut.Store(false) }

func drive(limiters []Limiter, key string, d, gap time.Duration) float64 {
	var wg sync.WaitGroup
	var allowed atomic.Int64
	end := time.Now().Add(d)
	for _, l := range limiters {
		wg.Go(func() {
			for time.Now().Before(end) {
				if l.Allow(context.Background(), key).Allowed {
					allowed.Add(1)
				}
				time.Sleep(gap)
			}
		})
	}
	wg.Wait()
	return float64(allowed.Load())
}

func pair(t *testing.T, build func(rdb *redis.Client, prefix string) Limiter) (alone, connected Limiter, g *gate) {
	g, cut := gated(t)
	p := prefix(t)
	alone = build(cut, p)
	connected = build(testRedis(t), p)
	t.Cleanup(alone.Close)
	t.Cleanup(connected.Close)
	return alone, connected, g
}

var chargeRule = Rule{Rate: 200, Burst: 20}

func checkCharged(t *testing.T, alone, connected Limiter, g *gate) {
	t.Helper()
	both := []Limiter{alone, connected}
	began := time.Now()
	g.cut()
	got := drive(both, "k", time.Second, 2*time.Millisecond)
	g.restore()
	got += drive(both, "k", 1500*time.Millisecond, 2*time.Millisecond)
	exact := chargeRule.Burst + chargeRule.Rate*time.Since(began).Seconds()
	if got > 1.08*exact || got < 0.8*exact {
		t.Fatalf("admitted %.0f against an exact %.0f; one gateway cut off from Redis admits its static share while the other spends the whole bucket, and what it admitted alone has to be charged back once Redis answers again", got, exact)
	}
}

func checkForgiven(t *testing.T, l Limiter, g *gate) {
	t.Helper()
	one := []Limiter{l}
	drive(one, "k", 300*time.Millisecond, 2*time.Millisecond)
	g.cut()
	drive(one, "k", 2*time.Second, 2*time.Millisecond)
	g.restore()
	got := drive(one, "k", time.Second, 2*time.Millisecond)
	if got < 0.7*chargeRule.Rate {
		t.Fatalf("admitted %.0f in the second after a 2 s outage, against a rate of %.0f; nobody spent from the shared bucket while it was gone, so there is nothing to pay back", got, chargeRule.Rate)
	}
}

func TestRedisExactChargesWhatAGatewayAdmittedAlone(t *testing.T) {
	alone, connected, g := pair(t, func(rdb *redis.Client, p string) Limiter {
		l, _ := NewRedisExact(Options{Redis: rdb, Prefix: p, Rule: chargeRule, Instances: 2, RedisTimeout: 50 * time.Millisecond, Now: wall})
		return l
	})
	checkCharged(t, alone, connected, g)
}

func TestLeaseChargesWhatAGatewayAdmittedAlone(t *testing.T) {
	alone, connected, g := pair(t, func(rdb *redis.Client, p string) Limiter {
		l, _ := NewLease(Options{Redis: rdb, Prefix: p, Rule: chargeRule, Instances: 2, LeaseTarget: 20 * time.Millisecond, LeaseTTL: time.Second, RedisTimeout: 50 * time.Millisecond, Now: wall})
		return l
	})
	checkCharged(t, alone, connected, g)
}

func TestRedisExactForgetsWhatNobodyElseSpent(t *testing.T) {
	g, cut := gated(t)
	l, _ := NewRedisExact(Options{Redis: cut, Prefix: prefix(t), Rule: chargeRule, Instances: 1, RedisTimeout: 50 * time.Millisecond, Now: wall})
	checkForgiven(t, l, g)
}

func TestLeaseForgetsWhatNobodyElseSpent(t *testing.T) {
	g, cut := gated(t)
	l, _ := NewLease(Options{Redis: cut, Prefix: prefix(t), Rule: chargeRule, Instances: 1, LeaseTarget: 20 * time.Millisecond, LeaseTTL: time.Second, RedisTimeout: 50 * time.Millisecond, Now: wall})
	checkForgiven(t, l, g)
}

func TestLocalSyncForgetsWhatNobodyElseSpent(t *testing.T) {
	g, cut := gated(t)
	l, err := NewLocalSync(Options{Redis: cut, Prefix: prefix(t), Rule: chargeRule, Instances: 1, InstanceName: "gw", SyncInterval: 20 * time.Millisecond, RedisTimeout: 50 * time.Millisecond, Now: wall})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Close)
	checkForgiven(t, l, g)
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
		l, _ := NewLease(Options{Redis: rdb, Prefix: p, Rule: rule, Instances: 3, LeaseTarget: 20 * time.Millisecond, LeaseTTL: time.Second, RedisTimeout: time.Second, Now: wall})
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
	rdb := &countedScripts{Client: testRedis(t)}
	l, _ := NewLease(Options{Redis: rdb, Prefix: prefix(t), Rule: Rule{Rate: 10000, Burst: 10000}, Instances: 1, LeaseTarget: 50 * time.Millisecond, LeaseTTL: time.Second, RedisTimeout: time.Second, Now: wall})
	allowed := hammer(t, []Limiter{l}, "k", 2000, 0)
	if f := rdb.calls.Load(); allowed != 2000 || f > 250 {
		t.Fatalf("%d Redis calls for %d admitted requests out of 2000", f, allowed)
	}
}

func TestLeaseKeepsBatchingWhenTheBucketRunsDry(t *testing.T) {
	rdb := &countedScripts{Client: testRedis(t)}
	rule := Rule{Rate: 200, Burst: 20}
	l, _ := NewLease(Options{Redis: rdb, Prefix: prefix(t), Rule: rule, Instances: 1, LeaseTarget: 20 * time.Millisecond, LeaseTTL: time.Second, RedisTimeout: time.Second, Now: wall})
	began := time.Now()
	allowed := hammer(t, []Limiter{l}, "k", 1000, 2*time.Millisecond)
	exact := rule.Burst + rule.Rate*time.Since(began).Seconds()
	fetches := rdb.calls.Load()
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
	var clients []*countedScripts
	for i := range 2 {
		client := &countedScripts{Client: rdb}
		clients = append(clients, client)
		l, err := NewLocalSync(Options{Redis: client, Prefix: p, Rule: rule, Instances: 2, InstanceName: fmt.Sprint("gw-", i), SyncInterval: 20 * time.Millisecond, RedisTimeout: time.Second, Now: wall})
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
	if clients[0].answers.Load() == 0 {
		t.Fatal("never synced")
	}
}

func TestLocalSyncKeepsTheRateWhenAnIntervalRefillsMoreThanTheBurst(t *testing.T) {
	rdb := testRedis(t)
	p := prefix(t)
	rule := Rule{Rate: 200, Burst: 10}
	var ls []Limiter
	for i := range 2 {
		l, err := NewLocalSync(Options{Redis: rdb, Prefix: p, Rule: rule, Instances: 2, InstanceName: fmt.Sprint("gw-", i), SyncInterval: 200 * time.Millisecond, RedisTimeout: time.Second, Now: wall})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(l.Close)
		ls = append(ls, l)
	}
	began := time.Now()
	allowed := hammer(t, ls, "k", 3000, time.Millisecond)
	exact := rule.Burst + rule.Rate*time.Since(began).Seconds()
	if float64(allowed) < 0.85*exact || float64(allowed) > 1.15*exact {
		t.Fatalf("admitted %d against an exact %.0f; capping the refill at the burst before charging what was spent loses tokens whenever rate x interval is above the burst", allowed, exact)
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
	var clients []*countedScripts
	for i := range 2 {
		client := &countedScripts{Client: rdb}
		clients = append(clients, client)
		l, err := NewLocalSync(Options{Redis: client, Prefix: p, Rule: rule, Instances: 2, InstanceName: fmt.Sprint("gw-", i), SyncInterval: 20 * time.Millisecond, RedisTimeout: time.Second, Now: wall})
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
	if clients[0].answers.Load() == 0 || clients[1].answers.Load() == 0 {
		t.Fatal("an instance never synced")
	}
}

func TestLocalSyncFallsBackAndCharges(t *testing.T) {
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 20 * time.Millisecond, MaxRetries: -1})
	defer dead.Close()
	l, _ := NewLocalSync(Options{Redis: dead, Prefix: "x:", Rule: Rule{Rate: 100, Burst: 10}, Instances: 2, InstanceName: "gw", SyncInterval: 10 * time.Millisecond, RedisTimeout: 10 * time.Millisecond, Now: wall})
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
