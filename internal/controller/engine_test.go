package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/distrib"
	"github.com/mirzanajafov/portalgun-router/internal/proxy"
	"github.com/mirzanajafov/portalgun-router/internal/store"
)

type memStore struct {
	mu       sync.Mutex
	configs  []store.ConfigVersion
	rollouts []store.Rollout
	steps    map[string]map[int]store.Step
	outcomes map[string]string
}

func newMemStore() *memStore {
	return &memStore{steps: map[string]map[int]store.Step{}, outcomes: map[string]string{}}
}

func (m *memStore) LatestConfig(context.Context) (*store.ConfigVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.configs) == 0 {
		return nil, nil
	}
	c := m.configs[len(m.configs)-1]
	return &c, nil
}

func (m *memStore) SaveConfig(_ context.Context, author, reason string, body json.RawMessage) (store.ConfigVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := store.ConfigVersion{Version: int64(len(m.configs) + 1), Author: author, Reason: reason, Body: body}
	m.configs = append(m.configs, c)
	return c, nil
}

func (m *memStore) ConfigHistory(context.Context, int) ([]store.ConfigVersion, error) {
	return nil, nil
}

func (m *memStore) Rollouts(context.Context, int) ([]store.Rollout, error) { return m.rollouts, nil }

func (m *memStore) StartRollout(_ context.Context, r store.Rollout) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rollouts = append(m.rollouts, r)
	return nil
}

func (m *memStore) SaveStep(_ context.Context, id string, st store.Step) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.steps[id] == nil {
		m.steps[id] = map[int]store.Step{}
	}
	m.steps[id][st.Step] = st
	return nil
}

func (m *memStore) FinishRollout(_ context.Context, id, outcome, _ string, _ time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, done := m.outcomes[id]; done {
		return errors.New("already finished")
	}
	m.outcomes[id] = outcome
	return nil
}

const baseConfig = `{"routes":[{"name":"api","pathPrefix":"/","pool":"p","versions":{"stable":"v1"}}],
"pools":[{"name":"p","dimensions":[{"id":"s1","address":"s1:8080","version":"v1"},{"id":"c1","address":"c1:8080","version":"v2"}]}]}`

type rig struct {
	store  *memStore
	cfgs   *ConfigManager
	engine *Engine
	now    time.Time
	sec    int64
}

func newRig(t *testing.T) *rig {
	t.Helper()
	r := &rig{store: newMemStore(), now: time.Unix(1_000_000, 0), sec: 1_000_000}
	r.cfgs = NewConfigManager(r.store, func(context.Context, distrib.Envelope) error { return nil }, nil)
	if err := r.cfgs.Load(context.Background(), []byte(baseConfig)); err != nil {
		t.Fatal(err)
	}
	opts := EngineOptions{Steps: []float64{0.1, 0.5}, MaxStep: time.Hour, MinStep: 0, DeadSeconds: 10, Alpha: 0.01, Beta: 0.05, Rho: 2}
	r.engine = NewEngine(opts, r.cfgs, r.store, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	r.engine.now = func() time.Time { return r.now }
	return r
}

type bin struct {
	stable, canary, stableErr, canaryErr, stableSlow, canarySlow int64
}

func (r *rig) feed(b bin, opts ...func(*Merged)) {
	r.now = r.now.Add(time.Second)
	r.sec++
	_, version := r.cfgs.Raw()
	st := r.engine.Status()
	m := Merged{
		Second:         r.sec,
		ConfigVersions: map[string]int64{"gw-1": version},
		Routes: map[string]proxy.RouteSnapshot{"api": {
			Rollout: st.Rollout.ID,
			Versions: [2]proxy.VersionSnapshot{
				{Attempts: b.stable, Errors: b.stableErr, Slow: b.stableSlow},
				{Attempts: b.canary, Errors: b.canaryErr, Slow: b.canarySlow},
			},
		}},
		Dimensions: []proxy.DimensionSnapshot{{ID: "c1", Version: "v2", Healthy: true, Outlier: "closed"}},
	}
	for _, o := range opts {
		o(&m)
	}
	r.engine.Observe(context.Background(), m)
}

func (r *rig) versions(t *testing.T) config.Versions {
	t.Helper()
	cfg, _ := r.cfgs.Current()
	route, _, err := findRoute(cfg, "api")
	if err != nil {
		t.Fatal(err)
	}
	return route.Versions
}

func TestABadCanaryIsRolledBack(t *testing.T) {
	r := newRig(t)
	ro, err := r.engine.Start(context.Background(), "api", "v2", "admin")
	if err != nil {
		t.Fatal(err)
	}
	if v := r.versions(t); v.Canary != "v2" || v.Weight != 0.1 || v.Rollout != ro.ID {
		t.Fatalf("versions after start %+v", v)
	}
	for range 10 {
		r.feed(bin{stable: 900, canary: 100, stableErr: 2, canaryErr: 40, stableSlow: 45, canarySlow: 5})
	}
	if r.engine.Status().Active {
		t.Fatal("still running after ten seconds of a canary failing 40% of requests")
	}
	if v := r.versions(t); v.Canary != "" || v.Weight != 0 || v.Stable != "v1" {
		t.Fatalf("versions after rollback %+v", v)
	}
	if r.store.outcomes[ro.ID] != "rolled_back" || r.store.steps[ro.ID][0].Decision != "rollback" {
		t.Fatalf("outcome %q, step %+v", r.store.outcomes[ro.ID], r.store.steps[ro.ID][0])
	}
}

func TestAGoodCanaryWalksEveryStepAndBecomesStable(t *testing.T) {
	r := newRig(t)
	ro, err := r.engine.Start(context.Background(), "api", "v2", "admin")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 600 && r.engine.Status().Active; i++ {
		w := r.versions(t).Weight
		canary := int64(1000 * w)
		r.feed(bin{stable: 1000 - canary, canary: canary, stableErr: 10 - canary/100, canaryErr: canary / 100, stableSlow: 50 - canary/20, canarySlow: canary / 20})
	}
	if r.engine.Status().Active {
		t.Fatalf("still running: %+v", r.engine.Status())
	}
	if r.store.outcomes[ro.ID] != "promoted" || len(r.store.steps[ro.ID]) != 2 {
		t.Fatalf("outcome %q with %d steps", r.store.outcomes[ro.ID], len(r.store.steps[ro.ID]))
	}
	if v := r.versions(t); v.Stable != "v2" || v.Canary != "" || v.Weight != 0 {
		t.Fatalf("versions after promotion %+v", v)
	}
}

func TestSecondsFromAnOlderConfigDoNotCount(t *testing.T) {
	r := newRig(t)
	if _, err := r.engine.Start(context.Background(), "api", "v2", "admin"); err != nil {
		t.Fatal(err)
	}
	stale := func(m *Merged) { m.ConfigVersions["gw-2"] = 1 }
	for range 30 {
		r.feed(bin{stable: 900, canary: 100, canaryErr: 50}, stale)
	}
	if st := r.engine.Status(); !st.Active || st.DataSec != 0 {
		t.Fatalf("decided on seconds where one instance still ran the old weight: %+v", st)
	}
}

func TestTheBrakeRollsBackAtOnce(t *testing.T) {
	r := newRig(t)
	ro, _ := r.engine.Start(context.Background(), "api", "v2", "admin")
	r.feed(bin{stable: 900, canary: 100}, func(m *Merged) { rs := m.Routes["api"]; rs.Brake = true; m.Routes["api"] = rs })
	if r.engine.Status().Active || r.store.outcomes[ro.ID] != "rolled_back" {
		t.Fatalf("outcome %q", r.store.outcomes[ro.ID])
	}
}

func withStable(healthy bool) func(*Merged) {
	return func(m *Merged) {
		m.Dimensions = append(m.Dimensions, proxy.DimensionSnapshot{ID: "s1", Version: "v1", Healthy: healthy, Outlier: "closed"})
	}
}

func TestADeadCanaryIsRolledBackOnlyOnceItStaysDead(t *testing.T) {
	r := newRig(t)
	ro, _ := r.engine.Start(context.Background(), "api", "v2", "admin")
	dead := func(m *Merged) { m.Dimensions[0].Outlier = "ejected" }
	for range 9 {
		r.feed(bin{}, dead, withStable(true))
	}
	r.feed(bin{}, withStable(true))
	for range 9 {
		r.feed(bin{}, dead, withStable(true))
	}
	if !r.engine.Status().Active {
		t.Fatalf("rolled back after two 9 s spells of a dead canary with a healthy second between them: %q", r.store.outcomes[ro.ID])
	}
	r.feed(bin{}, dead, withStable(true))
	if r.engine.Status().Active || r.store.outcomes[ro.ID] != "rolled_back" {
		t.Fatalf("still %q after 10 s of a dead canary", r.store.outcomes[ro.ID])
	}
	if reason := r.store.steps[ro.ID][0].Reason; !strings.Contains(reason, "for 10 s") {
		t.Fatalf("reason %q", reason)
	}
}

func TestAnOutageOfBothVersionsIsNotTheCanarysFault(t *testing.T) {
	r := newRig(t)
	ro, _ := r.engine.Start(context.Background(), "api", "v2", "admin")
	down := func(m *Merged) { m.Dimensions[0].Healthy = false }
	for range 60 {
		r.feed(bin{}, down, withStable(false))
	}
	if !r.engine.Status().Active {
		t.Fatalf("rolled back while both versions were down: %q", r.store.outcomes[ro.ID])
	}
}

func TestStartChecksItsInputs(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	if _, err := r.engine.Start(ctx, "nope", "v2", "admin"); err == nil {
		t.Fatal("unknown route accepted")
	}
	if _, err := r.engine.Start(ctx, "api", "v1", "admin"); err == nil {
		t.Fatal("the stable version accepted as a canary")
	}
	if _, err := r.engine.Start(ctx, "api", "v9", "admin"); err == nil {
		t.Fatal("a version no dimension runs accepted")
	}
	if _, err := r.engine.Start(ctx, "api", "v2", "admin"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.engine.Start(ctx, "api", "v2", "admin"); !errors.Is(err, ErrBusy) {
		t.Fatalf("second start: %v", err)
	}
	if err := r.engine.Abort(ctx, "changed my mind"); err != nil {
		t.Fatal(err)
	}
	if err := r.engine.Abort(ctx, "again"); err == nil {
		t.Fatal("aborted a rollout that isn't running")
	}
}

func TestAPIRequiresTheTokenToChangeThings(t *testing.T) {
	r := newRig(t)
	api := NewAPI(r.cfgs, r.engine, r.store, NewHub(), "s3cret")
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	post := func(auth, body string) int {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/rollouts", strings.NewReader(body))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	for _, tc := range []struct {
		auth, body string
		want       int
	}{
		{"", `{"route":"api","canary":"v2"}`, 401},
		{"Bearer nope", `{"route":"api","canary":"v2"}`, 401},
		{"Bearer s3cret", `{"route":"api","canary":"v9"}`, 400},
		{"Bearer s3cret", `{"route":"api","canary":"v2"}`, 201},
		{"Bearer s3cret", `{"route":"api","canary":"v2"}`, 409},
	} {
		if got := post(tc.auth, tc.body); got != tc.want {
			t.Errorf("auth %q body %s: %d, want %d", tc.auth, tc.body, got, tc.want)
		}
	}
	check := func(auth string) int {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/admin", nil)
		req.Header.Set("Authorization", auth)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if check("Bearer s3cret") != 200 || check("Bearer guess") != 401 {
		t.Fatal("the admin check endpoint does not check")
	}
	resp, err := http.Get(srv.URL + "/api/state")
	if err != nil {
		t.Fatal(err)
	}
	var st State
	_ = json.NewDecoder(resp.Body).Decode(&st)
	resp.Body.Close()
	if !st.Rollout.Active || st.ConfigVersion != 2 {
		t.Fatalf("state %+v", st)
	}
}

func TestVisitorsCanStartAFewRolloutsAnHourButNotAbortThem(t *testing.T) {
	r := newRig(t)
	api := NewAPI(r.cfgs, r.engine, r.store, NewHub(), "s3cret")
	counter := &fakeCounter{hits: map[string]int64{}}
	api.LetVisitorsRollOut(counter, RolloutLimits{PerHour: 2}, behindLoopback(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	post := func(path, auth, body string) int {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(body))
		req.Header.Set("X-Forwarded-For", "9.9.9.9")
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	start := `{"route":"api","canary":"v2"}`
	abort := func() {
		t.Helper()
		if got := post("/api/rollouts/abort", "", `{}`); got != 401 {
			t.Fatalf("visitor abort: %d", got)
		}
		if got := post("/api/rollouts/abort", "Bearer s3cret", `{}`); got != 200 {
			t.Fatalf("admin abort: %d", got)
		}
		r.now = r.now.Add(time.Minute)
	}
	if got := post("/api/rollouts", "", start); got != 201 {
		t.Fatalf("visitor start: %d", got)
	}
	if got := post("/api/rollouts", "", start); got != 409 {
		t.Fatalf("visitor start while one runs: %d", got)
	}
	abort()
	if got := post("/api/rollouts", "", start); got != 201 {
		t.Fatalf("second visitor start: %d", got)
	}
	abort()
	if got := post("/api/rollouts", "", start); got != 429 {
		t.Fatalf("third visitor start in the hour: %d", got)
	}
	if got := post("/api/rollouts", "Bearer s3cret", start); got != 201 {
		t.Fatalf("admin start after the visitor limit: %d", got)
	}
	if n := counter.hits["rollout:ip:9.9.9.9"]; n != 3 {
		t.Fatalf("counted %d visitor starts, want 3: the one refused because a rollout was running must not count", n)
	}
	var authors []string
	for _, c := range r.store.configs {
		if strings.Contains(c.Reason, "starts") {
			authors = append(authors, c.Author)
		}
	}
	if strings.Join(authors, ",") != "visitor,visitor,admin" {
		t.Fatalf("rollouts started by %v", authors)
	}
}

func TestVisitorsCannotStartARolloutWhenItCannotBeCounted(t *testing.T) {
	r := newRig(t)
	logs := &logSink{}
	api := NewAPI(r.cfgs, r.engine, r.store, NewHub(), "s3cret")
	api.LetVisitorsRollOut(downCounter(), DefaultRolloutLimits(), behindLoopback(), slog.New(slog.NewTextHandler(logs, nil)))
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	post := func(auth string) *http.Response {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/rollouts", strings.NewReader(`{"route":"api","canary":"v2"}`))
		req.Header.Set("X-Forwarded-For", "9.9.9.9")
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}
	if resp := post(""); resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "60" {
		t.Fatalf("visitor with the counter down: %d, Retry-After %q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	if r.engine.Status().Active {
		t.Fatal("a rollout started without counting the visitor")
	}
	if !strings.Contains(logs.String(), "connection refused") || !strings.Contains(logs.String(), "limit=rollout") || strings.Contains(logs.String(), "9.9.9.9") {
		t.Fatalf("the log should carry the counter's error and the limit, and not the visitor: %q", logs.String())
	}
	if resp := post("Bearer s3cret"); resp.StatusCode != http.StatusCreated {
		t.Fatalf("admin with the counter down: %d", resp.StatusCode)
	}
}

type barrierCounter struct{ arrived sync.WaitGroup }

func (b *barrierCounter) Hit(context.Context, string, time.Duration) (int64, error) {
	b.arrived.Done()
	b.arrived.Wait()
	return 0, errors.New("dial tcp redis:6379: i/o timeout")
}

func TestABurstOfVisitorsDuringAnOutageIsLoggedOnce(t *testing.T) {
	r := newRig(t)
	logs := &logSink{}
	counter := &barrierCounter{}
	counter.arrived.Add(5)
	api := NewAPI(r.cfgs, r.engine, r.store, NewHub(), "s3cret")
	api.LetVisitorsRollOut(counter, DefaultRolloutLimits(), behindLoopback(), slog.New(slog.NewTextHandler(logs, nil)))
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			resp, err := http.Post(srv.URL+"/api/rollouts", "application/json", strings.NewReader(`{"route":"api","canary":"v2"}`))
			if err != nil {
				t.Error(err)
				return
			}
			resp.Body.Close()
		})
	}
	wg.Wait()
	if n := strings.Count(logs.String(), "i/o timeout"); n != 1 {
		t.Fatalf("five visitors failing together were logged %d times, want once: %q", n, logs.String())
	}
}
