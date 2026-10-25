package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/dimension"
	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
)

func dim(t *testing.T, id string, mutate func(*dimension.Behavior)) string {
	t.Helper()
	b := dimension.DefaultBehavior()
	if mutate != nil {
		mutate(&b)
	}
	s, err := dimension.New(id, "v1", b)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

func parse(t testing.TB, c config.Config) *config.Config {
	t.Helper()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := config.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func onePool(t *testing.T, balancer config.Balancer, dims ...config.Dimension) *config.Config {
	return parse(t, config.Config{
		Routes: []config.Route{{Name: "all", PathPrefix: "/", Pool: "main"}},
		Pools:  []config.Pool{{Name: "main", Balancer: balancer, Dimensions: dims}},
	})
}

func newManualGateway(cfg *config.Config, opts Options, transport func(address string) http.RoundTripper) (*Gateway, error) {
	g, err := newGateway(cfg, opts, transport)
	if err != nil {
		return nil, err
	}
	close(g.tickDone)
	return g, nil
}

func serve(t *testing.T, cfg *config.Config) (*Gateway, *httptest.Server) {
	t.Helper()
	g, err := New(cfg, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.Close)
	srv := httptest.NewServer(g)
	t.Cleanup(srv.Close)
	return g, srv
}

func get(t *testing.T, client *http.Client, url, host string) (int, http.Header) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if host != "" {
		req.Host = host
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, resp.Header
}

func TestEachConfiguredBalancerGetsItsOwnAlgorithm(t *testing.T) {
	seen := map[reflect.Type]config.Balancer{}
	for _, kind := range config.Balancers {
		algorithm := reflect.TypeOf(newBalancer(kind))
		if other, ok := seen[algorithm]; ok {
			t.Fatalf("%s and %s both run %v", other, kind, algorithm)
		}
		seen[algorithm] = kind
	}
}
func TestRoutesByHostThenLongestPrefix(t *testing.T) {
	a, b, c := dim(t, "a", nil), dim(t, "b", nil), dim(t, "c", nil)
	cfg := parse(t, config.Config{
		Routes: []config.Route{
			{Name: "root", PathPrefix: "/", Pool: "pb"},
			{Name: "api", PathPrefix: "/api", Pool: "pa"},
			{Name: "special", Host: "Portal.Test", PathPrefix: "/", Pool: "pc"},
		},
		Pools: []config.Pool{
			{Name: "pa", Dimensions: []config.Dimension{{ID: "a", Address: a}}},
			{Name: "pb", Dimensions: []config.Dimension{{ID: "b", Address: b}}},
			{Name: "pc", Dimensions: []config.Dimension{{ID: "c", Address: c}}},
		},
	})
	_, srv := serve(t, cfg)
	for _, tc := range []struct{ path, host, want string }{
		{"/api/users", "", "a"},
		{"/apiary", "", "a"},
		{"/other", "", "b"},
		{"/api/users", "portal.test:8080", "c"},
	} {
		status, h := get(t, srv.Client(), srv.URL+tc.path, tc.host)
		if status != http.StatusOK || h.Get(dimension.IDHeader) != tc.want || h.Get(gatewayapi.DimensionHeader) != tc.want {
			t.Errorf("%s %s: status %d dimension %q/%q, want %s", tc.host, tc.path, status, h.Get(dimension.IDHeader), h.Get(gatewayapi.DimensionHeader), tc.want)
		}
	}
}

func TestUnroutedRequestsGet404(t *testing.T) {
	cfg := parse(t, config.Config{
		Routes: []config.Route{{Name: "api", PathPrefix: "/api", Pool: "p"}},
		Pools:  []config.Pool{{Name: "p", Dimensions: []config.Dimension{{ID: "a", Address: dim(t, "a", nil)}}}},
	})
	_, srv := serve(t, cfg)
	if status, _ := get(t, srv.Client(), srv.URL+"/elsewhere", ""); status != http.StatusNotFound {
		t.Fatalf("got %d", status)
	}
}

func TestP2CMovesTrafficOffASlowDimension(t *testing.T) {
	fast := dim(t, "fast", nil)
	slow := dim(t, "slow", func(b *dimension.Behavior) { b.DelayMs = 30 })
	_, srv := serve(t, onePool(t, config.BalancerP2C, config.Dimension{ID: "fast", Address: fast}, config.Dimension{ID: "slow", Address: slow}))
	counts := map[string]int{}
	for range 200 {
		_, h := get(t, srv.Client(), srv.URL+"/", "")
		counts[h.Get(gatewayapi.DimensionHeader)]++
	}
	if counts["fast"] < 180 {
		t.Fatalf("counts %v, want almost everything on the fast dimension", counts)
	}
}

func TestRoundRobinSpreadsEvenly(t *testing.T) {
	_, srv := serve(t, onePool(t, config.BalancerRoundRobin,
		config.Dimension{ID: "a", Address: dim(t, "a", nil)},
		config.Dimension{ID: "b", Address: dim(t, "b", nil)},
	))
	counts := map[string]int{}
	for range 100 {
		_, h := get(t, srv.Client(), srv.URL+"/", "")
		counts[h.Get(gatewayapi.DimensionHeader)]++
	}
	if counts["a"] != 50 || counts["b"] != 50 {
		t.Fatalf("counts %v", counts)
	}
}

func TestFullPoolAnswers503(t *testing.T) {
	hang := dim(t, "a", func(b *dimension.Behavior) { b.Hang = true })
	g, srv := serve(t, onePool(t, "", config.Dimension{ID: "a", Address: hang, MaxInflight: 2}))
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/", nil)
			if resp, err := srv.Client().Do(req); err == nil {
				resp.Body.Close()
			}
		})
	}
	deadline := time.Now().Add(2 * time.Second)
	for g.State().Upstreams[0].Inflight < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if status, _ := get(t, srv.Client(), srv.URL+"/", ""); status != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503 with both slots taken", status)
	}
	cancel()
	wg.Wait()
	waitIdle(t, g)
}

type holding struct {
	release chan struct{}
	once    sync.Once
	now     atomic.Int64
	most    atomic.Int64
	seen    atomic.Int64
}

func (h *holding) let() { h.once.Do(func() { close(h.release) }) }

func raiseTo(v *atomic.Int64, n int64) {
	for m := v.Load(); n > m; m = v.Load() {
		if v.CompareAndSwap(m, n) {
			return
		}
	}
}

func holdingDim(t *testing.T) (*holding, string) {
	t.Helper()
	h := &holding{release: make(chan struct{})}
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		h.seen.Add(1)
		n := h.now.Add(1)
		defer h.now.Add(-1)
		raiseTo(&h.most, n)
		if r.URL.Path == "/hold" {
			select {
			case <-h.release:
			case <-r.Context().Done():
			}
		}
		if d, err := time.ParseDuration(r.URL.Query().Get("sleep")); err == nil {
			time.Sleep(d)
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(h.let)
	return h, srv.Listener.Addr().String()
}

func capped(t *testing.T, limit int, addr string) *config.Config {
	return parse(t, config.Config{
		MaxInflight: new(limit),
		Routes:      []config.Route{{Name: "all", PathPrefix: "/", Pool: "main"}},
		Pools:       []config.Pool{{Name: "main", Dimensions: []config.Dimension{{ID: "a", Address: addr}}}},
	})
}

func waitHeld(t *testing.T, g *Gateway, h *holding, n int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for g.State().Inflight < n || h.now.Load() < n {
		if time.Now().After(deadline) {
			t.Fatalf("the gateway holds %d requests and the dimension %d, want %d", g.State().Inflight, h.now.Load(), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestGatewayShedsPastItsCap(t *testing.T) {
	h, addr := holdingDim(t)
	g, srv := serve(t, capped(t, 2, addr))
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			if resp, err := srv.Client().Get(srv.URL + "/hold"); err == nil {
				resp.Body.Close()
			}
		})
	}
	waitHeld(t, g, h, 2)

	seen := h.seen.Load()
	began := time.Now()
	status, header := get(t, srv.Client(), srv.URL+"/", "")
	if status != http.StatusServiceUnavailable || header.Get("Retry-After") != "1" {
		t.Fatalf("got %d with Retry-After %q, want 503 and 1", status, header.Get("Retry-After"))
	}
	if elapsed := time.Since(began); elapsed > 100*time.Millisecond {
		t.Fatalf("shedding took %v", elapsed)
	}
	if h.seen.Load() != seen {
		t.Fatal("a shed request reached the dimension")
	}
	if got := gathered(t, g.Metrics(), "portal_shed_total"); got != 1 {
		t.Fatalf("portal_shed_total is %v, want 1", got)
	}

	h.let()
	wg.Wait()
	waitIdle(t, g)
	if g.State().Inflight != 0 {
		t.Fatalf("the gateway still holds %d requests", g.State().Inflight)
	}
	if status, _ := get(t, srv.Client(), srv.URL+"/", ""); status != http.StatusOK {
		t.Fatalf("got %d after the held requests finished, want 200", status)
	}
}

func TestGatewayCapHoldsUnderConcurrency(t *testing.T) {
	h, addr := holdingDim(t)
	g, srv := serve(t, capped(t, 8, addr))
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64}}
	t.Cleanup(client.CloseIdleConnections)
	var wg sync.WaitGroup
	var ok, shed, other atomic.Int64
	for range 64 {
		wg.Go(func() {
			for range 20 {
				resp, err := client.Get(srv.URL + "/?sleep=2ms")
				if err != nil {
					t.Error(err)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				switch resp.StatusCode {
				case http.StatusOK:
					ok.Add(1)
				case http.StatusServiceUnavailable:
					shed.Add(1)
				default:
					other.Add(1)
				}
			}
		})
	}
	wg.Wait()
	if most := h.most.Load(); most > 8 {
		t.Fatalf("the dimension saw %d requests at once through a cap of 8", most)
	}
	if ok.Load() == 0 || shed.Load() == 0 || other.Load() != 0 || ok.Load()+shed.Load() != 64*20 {
		t.Fatalf("ok %d, shed %d, other %d", ok.Load(), shed.Load(), other.Load())
	}
	if got := gathered(t, g.Metrics(), "portal_shed_total"); got != float64(shed.Load()) {
		t.Fatalf("portal_shed_total is %v, clients saw %d", got, shed.Load())
	}
	if g.State().Inflight != 0 {
		t.Fatalf("the gateway still holds %d requests", g.State().Inflight)
	}
}

func TestGatewayCapFollowsTheConfig(t *testing.T) {
	h, addr := holdingDim(t)
	g, srv := serve(t, capped(t, 1, addr))
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := srv.Client().Get(srv.URL + "/hold"); err == nil {
			resp.Body.Close()
		}
	}()
	waitHeld(t, g, h, 1)
	if status, _ := get(t, srv.Client(), srv.URL+"/", ""); status != http.StatusServiceUnavailable {
		t.Fatalf("got %d at a cap of 1, want 503", status)
	}
	if err := g.Apply(capped(t, 0, addr)); err != nil {
		t.Fatal(err)
	}
	if status, _ := get(t, srv.Client(), srv.URL+"/", ""); status != http.StatusOK {
		t.Fatalf("got %d with the cap off, want 200", status)
	}
	if err := g.Apply(capped(t, 2, addr)); err != nil {
		t.Fatal(err)
	}
	if st := g.State(); st.MaxInflight != 2 || st.Inflight != 1 {
		t.Fatalf("state %+v, want a cap of 2 with the held request still counted", st)
	}
	h.let()
	<-done
	waitIdle(t, g)
}

func waitIdle(t *testing.T, g *Gateway) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		idle := true
		for _, u := range g.State().Upstreams {
			idle = idle && u.Inflight == 0
		}
		if idle {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("in-flight counts never returned to zero: %+v", g.State().Upstreams)
}

func TestPerTryTimeoutAnswers504(t *testing.T) {
	slow := dim(t, "a", func(b *dimension.Behavior) { b.DelayMs = 300 })
	cfg := parse(t, config.Config{
		Routes: []config.Route{{Name: "all", PathPrefix: "/", Pool: "p", PerTryTimeout: config.Duration(50 * time.Millisecond), Timeout: config.Duration(time.Second)}},
		Pools:  []config.Pool{{Name: "p", Dimensions: []config.Dimension{{ID: "a", Address: slow}}}},
	})
	g, srv := serve(t, cfg)
	began := time.Now()
	status, _ := get(t, srv.Client(), srv.URL+"/", "")
	if status != http.StatusGatewayTimeout {
		t.Fatalf("got %d, want 504", status)
	}
	if elapsed := time.Since(began); elapsed > 250*time.Millisecond {
		t.Fatalf("took %v, want the per-try timeout to cut it short", elapsed)
	}
	waitIdle(t, g)
}

func TestDeadDimensionAnswers502(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	g, srv := serve(t, onePool(t, "", config.Dimension{ID: "a", Address: addr}))
	if status, _ := get(t, srv.Client(), srv.URL+"/", ""); status != http.StatusBadGateway {
		t.Fatalf("got %d, want 502", status)
	}
	waitIdle(t, g)
}

func TestForwardsBodiesHeadersAndLargeResponses(t *testing.T) {
	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Got-Bytes", strconv.Itoa(len(body)))
		w.Header().Set("X-Got-Forwarded", r.Header.Get("X-Forwarded-For"))
		_, _ = w.Write(bytes.Repeat([]byte("x"), 1<<20))
	}))
	t.Cleanup(echo.Close)
	g, srv := serve(t, onePool(t, "", config.Dimension{ID: "echo", Address: echo.Listener.Addr().String()}))

	resp, err := srv.Client().Post(srv.URL+"/upload", "application/octet-stream", bytes.NewReader(make([]byte, 100<<10)))
	if err != nil {
		t.Fatal(err)
	}
	n, _ := io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.Header.Get("X-Got-Bytes") != strconv.Itoa(100<<10) || n != 1<<20 {
		t.Fatalf("upstream got %s bytes, client got %d", resp.Header.Get("X-Got-Bytes"), n)
	}
	if resp.Header.Get("X-Got-Forwarded") == "" {
		t.Fatal("no X-Forwarded-For reached the upstream")
	}
	waitIdle(t, g)
}

func TestApplyUnderLoad(t *testing.T) {
	a, b := dim(t, "a", nil), dim(t, "b", nil)
	first := onePool(t, "", config.Dimension{ID: "a", Address: a})
	second := onePool(t, config.BalancerRoundRobin, config.Dimension{ID: "a", Address: a}, config.Dimension{ID: "b", Address: b})
	g, srv := serve(t, first)

	var wg sync.WaitGroup
	var mu sync.Mutex
	bad := map[int]int{}
	for range 8 {
		wg.Go(func() {
			for range 150 {
				resp, err := srv.Client().Get(srv.URL + "/")
				if err != nil {
					t.Error(err)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					mu.Lock()
					bad[resp.StatusCode]++
					mu.Unlock()
				}
			}
		})
	}
	for i := range 60 {
		next := first
		if i%2 == 0 {
			next = second
		}
		if err := g.Apply(next); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	wg.Wait()
	if len(bad) > 0 {
		t.Fatalf("non-200 answers during config swaps: %v", bad)
	}
	if g.Version() != 61 {
		t.Fatalf("version %d, want 61", g.Version())
	}
	waitIdle(t, g)
}

func TestApplyRefusesALimiterItCannotRunAndKeepsTheOldConfig(t *testing.T) {
	addr := dim(t, "a", nil)
	g, srv := serve(t, onePool(t, "", config.Dimension{ID: "a", Address: addr}))
	needsRedis := parse(t, config.Config{
		Routes: []config.Route{{Name: "all", PathPrefix: "/", Pool: "main", RateLimit: &config.RateLimit{Design: config.LimitRedisExact, Rate: 10, Burst: 10}}},
		Pools:  []config.Pool{{Name: "main", Dimensions: []config.Dimension{{ID: "a", Address: addr}}}},
	})
	if err := g.Apply(needsRedis); err == nil {
		t.Fatal("a Redis limiter applied without Redis")
	}
	if g.Version() != 1 {
		t.Fatalf("version moved to %d", g.Version())
	}
	if status, _ := get(t, srv.Client(), srv.URL+"/", ""); status != http.StatusOK {
		t.Fatalf("the old config stopped serving: %d", status)
	}
}

func TestAdminEndpoints(t *testing.T) {
	addr := dim(t, "a", nil)
	g, srv := serve(t, onePool(t, "", config.Dimension{ID: "a", Address: addr}))
	admin := httptest.NewServer(AdminHandler(g, "s3cret"))
	t.Cleanup(admin.Close)
	get(t, srv.Client(), srv.URL+"/", "")

	resp, err := http.Get(admin.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{`portal_requests_total{class="2xx",dimension="a",route="all",version="v1"} 1`, "portal_dimension_inflight", "portal_request_duration_seconds_bucket"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics lack %q", want)
		}
	}

	next := fmt.Sprintf(`{"routes":[{"name":"all","pathPrefix":"/","pool":"p"}],"pools":[{"name":"p","dimensions":[{"id":"a","address":%q,"maxInflight":7}]}]}`, addr)
	for _, tc := range []struct {
		auth string
		body string
		want int
	}{
		{"", next, http.StatusUnauthorized},
		{"Bearer wrong", next, http.StatusUnauthorized},
		{"Bearer s3cret", `{"routes":[]}`, http.StatusBadRequest},
		{"Bearer s3cret", next, http.StatusOK},
	} {
		req, _ := http.NewRequest(http.MethodPut, admin.URL+"/config", strings.NewReader(tc.body))
		if tc.auth != "" {
			req.Header.Set("Authorization", tc.auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("auth %q: got %d, want %d", tc.auth, resp.StatusCode, tc.want)
		}
	}
	if st := g.State(); st.Version != 2 || st.Upstreams[0].MaxInflight != 7 {
		t.Fatalf("state after the update: %+v", st)
	}
}
