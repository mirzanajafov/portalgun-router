package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/dimension"
	"github.com/mirzanajafov/portalgun-router/internal/health"
)

func deadAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

type tally struct {
	status   map[int]int
	attempts map[string]int
	served   map[string]int
}

func send(t *testing.T, client *http.Client, method, url string, n int) tally {
	t.Helper()
	out := tally{status: map[int]int{}, attempts: map[string]int{}, served: map[string]int{}}
	for range n {
		req, _ := http.NewRequest(method, url, strings.NewReader("ping"))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		out.status[resp.StatusCode]++
		out.attempts[resp.Header.Get(AttemptsHeader)]++
		out.served[resp.Header.Get(DimensionHeader)]++
	}
	return out
}

func resilient(t *testing.T, balancer config.Balancer, mutate func(*config.Pool, *config.Route), dims ...config.Dimension) *config.Config {
	c := config.Config{
		Routes: []config.Route{{Name: "all", PathPrefix: "/", Pool: "main"}},
		Pools: []config.Pool{{Name: "main", Balancer: balancer, Dimensions: dims,
			HealthCheck: config.HealthCheck{Disabled: true}}},
	}
	if mutate != nil {
		mutate(&c.Pools[0], &c.Routes[0])
	}
	return parse(t, c)
}

func TestRetriesMoveToAnotherDimension(t *testing.T) {
	cfg := resilient(t, config.BalancerRoundRobin, func(p *config.Pool, _ *config.Route) { p.Outlier.Disabled = true },
		config.Dimension{ID: "dead", Address: deadAddr(t)},
		config.Dimension{ID: "b", Address: dim(t, "b", nil)},
	)
	_, srv := serve(t, cfg)
	got := send(t, srv.Client(), http.MethodGet, srv.URL+"/", 40)
	if got.status[200] != 40 || got.served["b"] != 40 {
		t.Fatalf("status %v served %v", got.status, got.served)
	}
	if got.attempts["2"] < 20 {
		t.Fatalf("attempts %v, want the refused tries retried on the other dimension", got.attempts)
	}
}

func TestPostIsRetriedOnlyWhenItNeverLeft(t *testing.T) {
	failing := dim(t, "failing", func(b *dimension.Behavior) { b.ErrorRate = 1 })
	cfg := resilient(t, config.BalancerRoundRobin, func(p *config.Pool, _ *config.Route) { p.Outlier.Disabled = true },
		config.Dimension{ID: "failing", Address: failing},
		config.Dimension{ID: "b", Address: dim(t, "b", nil)},
	)
	_, srv := serve(t, cfg)
	post := send(t, srv.Client(), http.MethodPost, srv.URL+"/", 20)
	if post.status[503] != 10 || post.status[200] != 10 {
		t.Fatalf("POST status %v, want the 503s passed through unretried", post.status)
	}
	getRes := send(t, srv.Client(), http.MethodGet, srv.URL+"/", 20)
	if getRes.status[200] != 20 {
		t.Fatalf("GET status %v, want every 503 retried", getRes.status)
	}

	dead := resilient(t, config.BalancerRoundRobin, func(p *config.Pool, _ *config.Route) { p.Outlier.Disabled = true },
		config.Dimension{ID: "dead", Address: deadAddr(t)},
		config.Dimension{ID: "c", Address: dim(t, "c", nil)},
	)
	_, srv2 := serve(t, dead)
	if got := send(t, srv2.Client(), http.MethodPost, srv2.URL+"/", 20); got.status[200] != 20 {
		t.Fatalf("POST to a refused dimension: %v, want every request retried", got.status)
	}
}

func TestOutlierEjectionStopsTheRetries(t *testing.T) {
	failing := dim(t, "failing", func(b *dimension.Behavior) { b.ErrorRate = 1 })
	cfg := resilient(t, config.BalancerRoundRobin, nil,
		config.Dimension{ID: "failing", Address: failing},
		config.Dimension{ID: "b", Address: dim(t, "b", nil)},
	)
	g, srv := serve(t, cfg)
	send(t, srv.Client(), http.MethodGet, srv.URL+"/", 20)
	if st := g.State(); st.Upstreams[1].Outlier != health.Ejected || st.Upstreams[1].EjectedMs <= 0 {
		t.Fatalf("state %+v", st.Upstreams)
	}
	after := send(t, srv.Client(), http.MethodGet, srv.URL+"/", 30)
	if after.attempts["1"] != 30 || after.served["b"] != 30 {
		t.Fatalf("after ejection: attempts %v served %v", after.attempts, after.served)
	}
}

func TestTransitionsAreLoggedOnTheWallClock(t *testing.T) {
	failing := dim(t, "failing", func(b *dimension.Behavior) { b.ErrorRate = 1 })
	cfg := resilient(t, config.BalancerRoundRobin, nil,
		config.Dimension{ID: "failing", Address: failing},
		config.Dimension{ID: "b", Address: dim(t, "b", nil)},
	)
	g, srv := serve(t, cfg)
	before := time.Now().UnixNano()
	send(t, srv.Client(), http.MethodGet, srv.URL+"/", 20)
	after := time.Now().UnixNano()
	events := g.Events(before)
	if len(events) != 1 {
		t.Fatalf("events %+v, want one ejection", events)
	}
	e := events[0]
	if e.Dimension != "failing" || e.State != "ejected" || e.Reason == "" || e.UnixNano < before || e.UnixNano > after {
		t.Fatalf("event %+v outside [%d, %d]", e, before, after)
	}
	if late := g.Events(after + 1); len(late) != 0 {
		t.Fatalf("events after the run: %+v", late)
	}
}

func TestEventLogKeepsTheNewest(t *testing.T) {
	g, _ := serve(t, resilient(t, config.BalancerRoundRobin, nil, config.Dimension{ID: "a", Address: deadAddr(t)}))
	for i := range eventLog + 10 {
		g.logEvent("a", int64(i), "ejected", "")
	}
	all := g.Events(0)
	if len(all) != eventLog || all[0].UnixNano != g.epoch.UnixNano()+10 {
		t.Fatalf("kept %d events starting at %d", len(all), all[0].UnixNano-g.epoch.UnixNano())
	}
}

func TestRetryBudgetCapsRetries(t *testing.T) {
	cfg := resilient(t, config.BalancerRoundRobin, func(p *config.Pool, r *config.Route) {
		p.Outlier.Disabled = true
		r.Retries = config.Retries{Max: new(2), BudgetRatio: 0.0001, MinPerSecond: 0.3}
	},
		config.Dimension{ID: "dead", Address: deadAddr(t)},
		config.Dimension{ID: "b", Address: dim(t, "b", nil)},
	)
	g, srv := serve(t, cfg)
	got := send(t, srv.Client(), http.MethodGet, srv.URL+"/", 40)
	if got.attempts["2"] != 3 || got.status[502] != 18 || got.status[200] != 22 {
		t.Fatalf("attempts %v status %v, want 3 retries (0.3/s over 10s) and the next 18 refusals surfaced", got.attempts, got.status)
	}
	body := scrape(t, g)
	if !strings.Contains(body, "portal_retry_budget_exhausted_total{route=\"all\"} 18\n") {
		t.Fatalf("metrics:\n%s", grep(body, "portal_retr"))
	}
}

func TestPanicRoutingWhenMostOfThePoolLooksDown(t *testing.T) {
	sick := func(b *dimension.Behavior) { b.Healthy = false }
	build := func(threshold float64) *config.Config {
		return resilient(t, "", func(p *config.Pool, _ *config.Route) {
			p.HealthCheck = config.HealthCheck{Interval: config.Duration(100 * time.Millisecond), Timeout: config.Duration(50 * time.Millisecond), UnhealthyAfter: 1, HealthyAfter: 1}
			p.PanicThreshold = new(threshold)
		},
			config.Dimension{ID: "a", Address: dim(t, "a", sick)},
			config.Dimension{ID: "b", Address: dim(t, "b", sick)},
		)
	}
	g, srv := serve(t, build(0.5))
	waitUnhealthy(t, g, 2)
	if got := send(t, srv.Client(), http.MethodGet, srv.URL+"/", 10); got.status[200] != 10 {
		t.Fatalf("panic routing: %v", got.status)
	}
	if !strings.Contains(scrape(t, g), "portal_panic_routing_total{route=\"all\"} 10\n") {
		t.Fatal("panic routing not counted")
	}

	strict, srv2 := serve(t, build(0))
	waitUnhealthy(t, strict, 2)
	if got := send(t, srv2.Client(), http.MethodGet, srv2.URL+"/", 5); got.status[503] != 5 {
		t.Fatalf("without panic routing: %v, want 503", got.status)
	}
}

func TestHealthCheckTakesADimensionOut(t *testing.T) {
	cfg := resilient(t, "", func(p *config.Pool, _ *config.Route) {
		p.HealthCheck = config.HealthCheck{Interval: config.Duration(100 * time.Millisecond), Timeout: config.Duration(50 * time.Millisecond), UnhealthyAfter: 1, HealthyAfter: 1}
	},
		config.Dimension{ID: "a", Address: dim(t, "a", func(b *dimension.Behavior) { b.Healthy = false })},
		config.Dimension{ID: "b", Address: dim(t, "b", nil)},
		config.Dimension{ID: "c", Address: dim(t, "c", nil)},
	)
	g, srv := serve(t, cfg)
	waitUnhealthy(t, g, 1)
	got := send(t, srv.Client(), http.MethodGet, srv.URL+"/", 60)
	if got.served["a"] != 0 || got.status[200] != 60 {
		t.Fatalf("served %v status %v", got.served, got.status)
	}
}

func waitUnhealthy(t *testing.T, g *Gateway, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		down := 0
		for _, u := range g.State().Upstreams {
			if !u.Healthy {
				down++
			}
		}
		if down >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("fewer than %d dimensions marked unhealthy: %+v", n, g.State().Upstreams)
}

func TestSlowStartAdmitsAGrowingShare(t *testing.T) {
	u := &upstream{slowStart: int64(10 * time.Second)}
	u.readyAt.Store(0)
	share := func(now int64) float64 {
		n := 0
		for range 10000 {
			if u.warm(now) {
				n++
			}
		}
		return float64(n) / 10000
	}
	if s := share(0); s < 0.08 || s > 0.12 {
		t.Fatalf("share right after recovery %.3f, want the 10%% floor", s)
	}
	if s := share(int64(5 * time.Second)); s < 0.46 || s > 0.54 {
		t.Fatalf("share halfway %.3f, want about 50%%", s)
	}
	if share(int64(10*time.Second)) != 1 {
		t.Fatal("still ramping after the window")
	}
}

func scrape(t *testing.T, g *Gateway) string {
	t.Helper()
	srv := httptest.NewServer(AdminHandler(g, ""))
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func grep(s, substr string) string {
	var out []string
	for line := range strings.Lines(s) {
		if strings.Contains(line, substr) {
			out = append(out, strings.TrimSpace(line))
		}
	}
	return strings.Join(out, "\n")
}

func TestAnIdleDimensionIsNotReportedEjectedAfterItsEjectionEnds(t *testing.T) {
	cfg := resilient(t, config.BalancerRoundRobin, func(p *config.Pool, _ *config.Route) {
		p.Outlier = config.Outlier{Consecutive: 1, BaseEjection: config.Duration(50 * time.Millisecond), MaxEjection: config.Duration(50 * time.Millisecond)}
	},
		config.Dimension{ID: "a", Address: dim(t, "a", nil)},
		config.Dimension{ID: "b", Address: dim(t, "b", func(b *dimension.Behavior) { b.ErrorRate = 1 })},
	)
	g, srv := manual(t, cfg)
	send(t, srv.Client(), http.MethodGet, srv.URL+"/", 4)
	outlier := func(snap Snapshot) health.State {
		for _, d := range snap.Dimensions {
			if d.ID == "b" {
				return d.Outlier
			}
		}
		return health.Disabled
	}
	if got := outlier(g.Tick(1)); got != health.Ejected {
		t.Fatalf("b is %q after failing, want ejected", got)
	}
	time.Sleep(80 * time.Millisecond)
	snap := g.Tick(2)
	if got := outlier(snap); got != health.HalfOpen {
		t.Fatalf("b is %q with no traffic after its ejection ended, want half-open", got)
	}
	for _, u := range g.State().Upstreams {
		if u.ID == "b" && (u.Outlier != health.HalfOpen || u.EjectedMs != 0) {
			t.Fatalf("state reports %+v", u)
		}
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func TestOnlyAnEjectionLogsHowLongItLasts(t *testing.T) {
	cfg := resilient(t, config.BalancerRoundRobin, func(p *config.Pool, _ *config.Route) {
		p.Outlier = config.Outlier{Consecutive: 1, BaseEjection: config.Duration(50 * time.Millisecond), MaxEjection: config.Duration(50 * time.Millisecond)}
	},
		config.Dimension{ID: "a", Address: dim(t, "a", nil)},
		config.Dimension{ID: "b", Address: dim(t, "b", func(b *dimension.Behavior) { b.ErrorRate = 1 })},
	)
	var logs lockedBuffer
	g, err := New(cfg, Options{ManualTick: true, Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.Close)
	srv := httptest.NewServer(g)
	t.Cleanup(srv.Close)
	send(t, srv.Client(), http.MethodGet, srv.URL+"/", 4)
	time.Sleep(80 * time.Millisecond)
	send(t, srv.Client(), http.MethodGet, srv.URL+"/", 4)
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(logs.String(), `"to":"half-open"`) {
		if time.Now().After(deadline) {
			t.Fatalf("b never left its ejection:\n%s", logs.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	ejections := 0
	for line := range strings.Lines(logs.String()) {
		var e struct {
			Msg        string  `json:"msg"`
			To         string  `json:"to"`
			EjectedFor *string `json:"ejected_for"`
		}
		if json.Unmarshal([]byte(line), &e) != nil || e.Msg != "dimension state" {
			continue
		}
		switch {
		case e.To == "ejected" && (e.EjectedFor == nil || *e.EjectedFor != "50ms"):
			t.Fatalf("an ejection logged %v, want 50ms: %s", e.EjectedFor, line)
		case e.To != "ejected" && e.EjectedFor != nil:
			t.Fatalf("a move to %s logged ejected_for %s: %s", e.To, *e.EjectedFor, line)
		}
		if e.To == "ejected" {
			ejections++
		}
	}
	if ejections == 0 {
		t.Fatal("no ejection was logged")
	}
}
