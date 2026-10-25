package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/distrib"
	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
	"github.com/mirzanajafov/portalgun-router/internal/health"
	"github.com/mirzanajafov/portalgun-router/internal/store"
)

type memStore struct {
	mu       sync.Mutex
	configs  []store.ConfigVersion
	rollouts []store.Rollout
	steps    map[string]map[int]store.Step
	outcomes map[string]store.Outcome
	finished map[string]time.Time

	refuseWrites, refuseReads, loseEndReply, loseStartReply atomic.Bool

	gate *gate
}

var errStoreDown = errors.New("dial tcp postgres:5432: connect: connection refused")

type gate struct {
	reached chan struct{}
	open    chan struct{}
	once    sync.Once
}

func newGate() *gate { return &gate{reached: make(chan struct{}, 1), open: make(chan struct{})} }

func (g *gate) pass() {
	if g == nil {
		return
	}
	select {
	case g.reached <- struct{}{}:
	default:
	}
	<-g.open
}

func (g *gate) release() { g.once.Do(func() { close(g.open) }) }

func newMemStore() *memStore {
	return &memStore{steps: map[string]map[int]store.Step{}, outcomes: map[string]store.Outcome{}, finished: map[string]time.Time{}}
}

func (m *memStore) LatestConfig(context.Context) (store.ConfigVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.configs) == 0 {
		return store.ConfigVersion{}, store.ErrNoConfig
	}
	return m.configs[len(m.configs)-1], nil
}

func (m *memStore) write(ctx context.Context, apply func() (store.ConfigVersion, error)) (store.ConfigVersion, error) {
	if err := ctx.Err(); err != nil {
		return store.ConfigVersion{}, err
	}
	m.gate.pass()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.refuseWrites.Load() {
		return store.ConfigVersion{}, errStoreDown
	}
	return apply()
}

func (m *memStore) saveConfig(c store.ConfigChange) store.ConfigVersion {
	saved := store.ConfigVersion{Version: int64(len(m.configs) + 1), Author: c.Author, Reason: c.Reason, Body: c.Body}
	m.configs = append(m.configs, saved)
	return saved
}

func (m *memStore) saveStep(id string, st store.Step) {
	if m.steps[id] == nil {
		m.steps[id] = map[int]store.Step{}
	}
	m.steps[id][st.Step] = st
}

func (m *memStore) started(id string) bool {
	return slices.ContainsFunc(m.rollouts, func(r store.Rollout) bool { return r.ID == id })
}

func (m *memStore) SaveConfig(ctx context.Context, c store.ConfigChange) (store.ConfigVersion, error) {
	return m.write(ctx, func() (store.ConfigVersion, error) { return m.saveConfig(c), nil })
}

func (m *memStore) ConfigHistory(context.Context, int) ([]store.ConfigVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.refuseReads.Load() {
		return nil, errStoreDown
	}
	return nil, nil
}

func (m *memStore) Rollouts(context.Context, int) ([]store.Rollout, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.refuseReads.Load() {
		return nil, errStoreDown
	}
	return m.rollouts, nil
}

func (m *memStore) StartRollout(ctx context.Context, r store.Rollout, c store.ConfigChange) (store.ConfigVersion, error) {
	saved, err := m.write(ctx, func() (store.ConfigVersion, error) {
		if m.started(r.ID) {
			return store.ConfigVersion{}, errors.New("duplicate key value violates unique constraint \"rollouts_pkey\"")
		}
		for _, open := range m.rollouts {
			if _, done := m.outcomes[open.ID]; !done {
				m.outcomes[open.ID], m.finished[open.ID] = store.OutcomeAborted, r.StartedAt
			}
		}
		m.rollouts = append(m.rollouts, r)
		return m.saveConfig(c), nil
	})
	if err == nil && m.loseStartReply.CompareAndSwap(true, false) {
		return store.ConfigVersion{}, errStoreDown
	}
	return saved, err
}

func (m *memStore) SaveStep(ctx context.Context, id string, st store.Step) error {
	_, err := m.write(ctx, func() (store.ConfigVersion, error) {
		m.saveStep(id, st)
		return store.ConfigVersion{}, nil
	})
	return err
}

func (m *memStore) EndRollout(ctx context.Context, end store.RolloutEnd, c *store.ConfigChange) (store.ConfigVersion, error) {
	saved, err := m.write(ctx, func() (store.ConfigVersion, error) {
		if !m.started(end.ID) {
			return store.ConfigVersion{}, errors.New("insert or update on table \"rollout_steps\" violates foreign key constraint")
		}
		m.outcomes[end.ID], m.finished[end.ID] = end.Outcome, end.At
		m.saveStep(end.ID, end.Last)
		if c == nil {
			return store.ConfigVersion{}, nil
		}
		return m.saveConfig(*c), nil
	})
	if err == nil && m.loseEndReply.CompareAndSwap(true, false) {
		return store.ConfigVersion{}, errStoreDown
	}
	return saved, err
}

func (m *memStore) AbandonActive(ctx context.Context, _ string, at time.Time) ([]string, error) {
	var ids []string
	_, err := m.write(ctx, func() (store.ConfigVersion, error) {
		for _, r := range m.rollouts {
			if _, done := m.outcomes[r.ID]; !done {
				m.outcomes[r.ID], m.finished[r.ID] = store.OutcomeAborted, at
				ids = append(ids, r.ID)
			}
		}
		return store.ConfigVersion{}, nil
	})
	return ids, err
}

var quiet = slog.New(slog.DiscardHandler)

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
	r.cfgs = NewConfigManager(r.store, func(context.Context, distrib.Envelope) error { return nil }, nil, quiet)
	if err := r.cfgs.Load(context.Background(), []byte(baseConfig)); err != nil {
		t.Fatal(err)
	}
	opts := EngineOptions{Steps: []float64{0.1, 0.5}, MaxStep: time.Hour, MinStep: 0, DeadSeconds: 10, Alpha: 0.01, Beta: 0.05, Rho: 2}
	r.engine = NewEngine(opts, r.cfgs, r.store, quiet, nil)
	r.engine.now = func() time.Time { return r.now }
	return r
}

func (r *rig) apiOptions() APIOptions {
	return APIOptions{Configs: r.cfgs, Engine: r.engine, History: r.store, Hub: NewHub(quiet), Token: adminToken, ProxyToken: proxyToken, Logger: quiet}
}

func (r *rig) visitorAPI(perHour int64, counter Counter, logger *slog.Logger) *API {
	o := r.apiOptions()
	o.VisitorRolloutsPerHour, o.Counter, o.Logger = perHour, counter, logger
	return NewAPI(o)
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
		Routes: map[string]gatewayapi.RouteSnapshot{"api": {
			Rollout: st.Rollout.ID,
			Versions: [2]gatewayapi.VersionSnapshot{
				{Attempts: b.stable, Errors: b.stableErr, Slow: b.stableSlow},
				{Attempts: b.canary, Errors: b.canaryErr, Slow: b.canarySlow},
			},
		}},
		Dimensions: []gatewayapi.DimensionSnapshot{{ID: "c1", Version: "v2", Healthy: true, Outlier: health.Closed}},
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
	ro, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin)
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
	if r.store.outcomes[ro.ID] != store.OutcomeRolledBack || r.store.steps[ro.ID][0].Decision != store.DecisionRollback {
		t.Fatalf("outcome %q, step %+v", r.store.outcomes[ro.ID], r.store.steps[ro.ID][0])
	}
}

func TestAGoodCanaryWalksEveryStepAndBecomesStable(t *testing.T) {
	r := newRig(t)
	ro, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 600 && r.engine.Status().Active; i++ {
		r.feed(goodBin(r.versions(t).Weight))
	}
	if r.engine.Status().Active {
		t.Fatalf("still running: %+v", r.engine.Status())
	}
	if r.store.outcomes[ro.ID] != store.OutcomePromoted || len(r.store.steps[ro.ID]) != 2 {
		t.Fatalf("outcome %q with %d steps", r.store.outcomes[ro.ID], len(r.store.steps[ro.ID]))
	}
	if v := r.versions(t); v.Stable != "v2" || v.Canary != "" || v.Weight != 0 {
		t.Fatalf("versions after promotion %+v", v)
	}
}

func TestSecondsFromAnOlderConfigDoNotCount(t *testing.T) {
	r := newRig(t)
	if _, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin); err != nil {
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
	ro, _ := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin)
	r.feed(bin{stable: 900, canary: 100}, func(m *Merged) { rs := m.Routes["api"]; rs.Brake = true; m.Routes["api"] = rs })
	if r.engine.Status().Active || r.store.outcomes[ro.ID] != store.OutcomeRolledBack {
		t.Fatalf("outcome %q", r.store.outcomes[ro.ID])
	}
}

func withStable(healthy bool) func(*Merged) {
	return func(m *Merged) {
		m.Dimensions = append(m.Dimensions, gatewayapi.DimensionSnapshot{ID: "s1", Version: "v1", Healthy: healthy, Outlier: health.Closed})
	}
}

func TestADeadCanaryIsRolledBackOnlyOnceItStaysDead(t *testing.T) {
	r := newRig(t)
	ro, _ := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin)
	dead := func(m *Merged) { m.Dimensions[0].Outlier = health.Ejected }
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
	if r.engine.Status().Active || r.store.outcomes[ro.ID] != store.OutcomeRolledBack {
		t.Fatalf("still %q after 10 s of a dead canary", r.store.outcomes[ro.ID])
	}
	if reason := r.store.steps[ro.ID][0].Reason; !strings.Contains(reason, "for 10 s") {
		t.Fatalf("reason %q", reason)
	}
}

func TestAnOutageOfBothVersionsIsNotTheCanarysFault(t *testing.T) {
	r := newRig(t)
	ro, _ := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin)
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
	if _, err := r.engine.Start(ctx, "nope", "v2", store.ActorAdmin); err == nil {
		t.Fatal("unknown route accepted")
	}
	if _, err := r.engine.Start(ctx, "api", "v1", store.ActorAdmin); err == nil {
		t.Fatal("the stable version accepted as a canary")
	}
	if _, err := r.engine.Start(ctx, "api", "v9", store.ActorAdmin); err == nil {
		t.Fatal("a version no dimension runs accepted")
	}
	if _, err := r.engine.Start(ctx, "api", "v2", store.ActorAdmin); err != nil {
		t.Fatal(err)
	}
	if _, err := r.engine.Start(ctx, "api", "v2", store.ActorAdmin); !errors.Is(err, ErrBusy) {
		t.Fatalf("second start: %v", err)
	}
	if err := r.engine.Abort(ctx, "changed my mind"); err != nil {
		t.Fatal(err)
	}
	if err := r.engine.Abort(ctx, "again"); err == nil {
		t.Fatal("aborted a rollout that isn't running")
	}
}

func TestARestartClosesTheRolloutItInterruptedAndTakesTheCanaryOff(t *testing.T) {
	cases := []struct {
		name    string
		before  func(t *testing.T, r *rig)
		written bool
	}{
		{"nothing was running", func(*testing.T, *rig) {}, false},
		{"a rollout was running", func(t *testing.T, r *rig) {
			if _, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin); err != nil {
				t.Fatal(err)
			}
		}, true},
		{"a canary was left in the config without a rollout", func(t *testing.T, r *rig) {
			_, err := r.cfgs.Update(context.Background(), store.ActorAdmin, "by hand", func(c *config.Config) error {
				c.Routes[0].Versions = config.Versions{Stable: "v1", Canary: "v2", Weight: 0.1}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newRig(t)
			c.before(t, r)
			before := len(r.store.configs)
			restarted := NewEngine(r.engine.opts, r.cfgs, r.store, quiet, nil)
			if err := restarted.Recover(context.Background()); err != nil {
				t.Fatal(err)
			}
			if v := r.versions(t); v.Canary != "" || v.Weight != 0 {
				t.Fatalf("versions after the restart: %+v", v)
			}
			for _, ro := range r.store.rollouts {
				if r.store.outcomes[ro.ID] != store.OutcomeAborted {
					t.Fatalf("rollout %s left as %q", ro.ID, r.store.outcomes[ro.ID])
				}
			}
			written := len(r.store.configs) > before
			if written != c.written || (written && r.store.configs[len(r.store.configs)-1].Author != store.ActorController) {
				t.Fatalf("configs %v, want a new one: %v", r.store.configs[before:], c.written)
			}
		})
	}
}

func TestAPIRequiresTheTokenToChangeThings(t *testing.T) {
	r := newRig(t)
	api := NewAPI(r.apiOptions())
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	for _, tc := range []struct {
		auth, body string
		want       int
	}{
		{"", `{"route":"api","canary":"v2"}`, 401},
		{"Bearer nope", `{"route":"api","canary":"v2"}`, 401},
		{"Bearer " + adminToken, `{"route":"api","canary":"v9"}`, 400},
		{"Bearer " + adminToken, `{"route":"api","canary":"v2"}`, 201},
		{"Bearer " + adminToken, `{"route":"api","canary":"v2"}`, 409},
	} {
		header := http.Header{}
		if tc.auth != "" {
			header.Set("Authorization", tc.auth)
		}
		if got := send(t, http.MethodPost, srv.URL+"/api/rollouts", tc.body, header).StatusCode; got != tc.want {
			t.Errorf("auth %q body %s: %d, want %d", tc.auth, tc.body, got, tc.want)
		}
	}
	check := func(auth string) int {
		return send(t, http.MethodGet, srv.URL+"/api/admin", "", http.Header{"Authorization": {auth}}).StatusCode
	}
	if check("Bearer "+adminToken) != 200 || check("Bearer guess") != 401 {
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
	counter := &fakeCounter{hits: map[string]int64{}}
	api := r.visitorAPI(2, counter, quiet)
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	visitor, admin := viaProxy("9.9.9.9"), asAdmin("9.9.9.9")
	start := `{"route":"api","canary":"v2"}`
	abort := func() {
		t.Helper()
		if got := send(t, http.MethodPost, srv.URL+"/api/rollouts/abort", `{}`, visitor).StatusCode; got != 401 {
			t.Fatalf("visitor abort: %d", got)
		}
		if got := send(t, http.MethodPost, srv.URL+"/api/rollouts/abort", `{}`, admin).StatusCode; got != 200 {
			t.Fatalf("admin abort: %d", got)
		}
		r.now = r.now.Add(time.Minute)
	}
	if got := send(t, http.MethodPost, srv.URL+"/api/rollouts", start, visitor).StatusCode; got != 201 {
		t.Fatalf("visitor start: %d", got)
	}
	if got := send(t, http.MethodPost, srv.URL+"/api/rollouts", start, visitor).StatusCode; got != 409 {
		t.Fatalf("visitor start while one runs: %d", got)
	}
	abort()
	if got := send(t, http.MethodPost, srv.URL+"/api/rollouts", start, visitor).StatusCode; got != 201 {
		t.Fatalf("second visitor start: %d", got)
	}
	abort()
	if got := send(t, http.MethodPost, srv.URL+"/api/rollouts", start, visitor).StatusCode; got != 429 {
		t.Fatalf("third visitor start in the hour: %d", got)
	}
	if got := send(t, http.MethodPost, srv.URL+"/api/rollouts", start, admin).StatusCode; got != 201 {
		t.Fatalf("admin start after the visitor limit: %d", got)
	}
	if n := counter.hits["rollout:ip:9.9.9.9"]; n != 3 {
		t.Fatalf("counted %d visitor starts, want 3: the one refused because a rollout was running must not count", n)
	}
	var authors []store.Actor
	for _, c := range r.store.configs {
		if strings.Contains(c.Reason, "starts") {
			authors = append(authors, c.Author)
		}
	}
	if !slices.Equal(authors, []store.Actor{store.ActorVisitor, store.ActorVisitor, store.ActorAdmin}) {
		t.Fatalf("rollouts started by %v", authors)
	}
}

func TestVisitorsCannotStartARolloutWhenItCannotBeCounted(t *testing.T) {
	r := newRig(t)
	logs := &logSink{}
	api := r.visitorAPI(3, downCounter(), slog.New(slog.NewTextHandler(logs, nil)))
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	url, start := srv.URL+"/api/rollouts", `{"route":"api","canary":"v2"}`
	if resp := send(t, http.MethodPost, url, start, viaProxy("9.9.9.9")); resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "60" {
		t.Fatalf("visitor with the counter down: %d, Retry-After %q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	if r.engine.Status().Active {
		t.Fatal("a rollout started without counting the visitor")
	}
	if !strings.Contains(logs.String(), "connection refused") || !strings.Contains(logs.String(), "limit=rollout") || strings.Contains(logs.String(), "9.9.9.9") {
		t.Fatalf("the log should carry the counter's error and the limit, and not the visitor: %q", logs.String())
	}
	if resp := send(t, http.MethodPost, url, start, asAdmin("9.9.9.9")); resp.StatusCode != http.StatusCreated {
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
	api := r.visitorAPI(3, counter, slog.New(slog.NewTextHandler(logs, nil)))
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

func TestARollbackTheStoreRefusesIsTriedAgainUntilItIsSaved(t *testing.T) {
	r := newRig(t)
	ro, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin)
	if err != nil {
		t.Fatal(err)
	}
	r.store.refuseWrites.Store(true)
	r.failCanary()
	if st := r.engine.Status(); !st.Active || st.Pending != store.OutcomeRolledBack || r.versions(t).Canary != "v2" {
		t.Fatalf("a rollback that couldn't be saved left the rollout %+v and the config %+v", st, r.versions(t))
	}
	r.store.refuseWrites.Store(false)
	r.engine.retry(context.Background())
	if v := r.versions(t); r.engine.Status().Active || v.Canary != "" || v.Stable != "v1" || r.store.outcomes[ro.ID] != store.OutcomeRolledBack {
		t.Fatalf("after the store came back: active %v, versions %+v, outcome %q", r.engine.Status().Active, v, r.store.outcomes[ro.ID])
	}
}

func TestAStartTheStoreRefusesLeavesNothingBehind(t *testing.T) {
	r := newRig(t)
	r.store.refuseWrites.Store(true)
	if _, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin); !errors.Is(err, ErrRolloutFailed) {
		t.Fatalf("start during the outage: %v", err)
	}
	if r.engine.Status().Active || r.versions(t).Canary != "" || len(r.store.rollouts) != 0 || len(r.store.configs) != 1 {
		t.Fatalf("active %v, versions %+v, rollouts %+v, %d configs", r.engine.Status().Active, r.versions(t), r.store.rollouts, len(r.store.configs))
	}
}

func TestAStoreFailureIsA503AndOnlyTheAdminSeesWhy(t *testing.T) {
	r := newRig(t)
	api := r.visitorAPI(10, &fakeCounter{hits: map[string]int64{}}, quiet)
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	admin := asAdmin("9.9.9.9")
	call := func(method, path, body string, header http.Header) (int, string) {
		t.Helper()
		req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header = header
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(b)
	}
	r.store.refuseReads.Store(true)
	r.store.refuseWrites.Store(true)
	for _, c := range []struct {
		name, method, path, body string
		header                   http.Header
		named                    bool
	}{
		{"a visitor's rollout", http.MethodPost, "/api/rollouts", `{"route":"api","canary":"v2"}`, viaProxy("9.9.9.9"), false},
		{"an admin's rollout", http.MethodPost, "/api/rollouts", `{"route":"api","canary":"v2"}`, admin, true},
		{"the config history", http.MethodGet, "/api/config/history", "", http.Header{}, false},
		{"the rollout history", http.MethodGet, "/api/rollouts", "", http.Header{}, false},
		{"the history for the admin", http.MethodGet, "/api/rollouts", "", admin, true},
		{"an admin's config", http.MethodPut, "/api/config", baseConfig, admin, true},
	} {
		status, body := call(c.method, c.path, c.body, c.header)
		if named := strings.Contains(body, "postgres:5432"); status != http.StatusServiceUnavailable || named != c.named {
			t.Errorf("%s: %d %s", c.name, status, body)
		}
	}

	r.store.refuseWrites.Store(false)
	if status, body := call(http.MethodPost, "/api/rollouts", `{"route":"api","canary":"v2"}`, admin); status != http.StatusCreated {
		t.Fatalf("start with the store back: %d %s", status, body)
	}
	r.store.refuseWrites.Store(true)
	if status, body := call(http.MethodPost, "/api/rollouts/abort", `{}`, admin); status != http.StatusServiceUnavailable || !r.engine.Status().Active {
		t.Fatalf("an abort the store refused: %d %s, active %v", status, body, r.engine.Status().Active)
	}
}

func TestAConfigThatIsSavedButNotPublishedStaysSaved(t *testing.T) {
	var redisDown atomic.Bool
	var logs logSink
	publish := func(context.Context, distrib.Envelope) error {
		if redisDown.Load() {
			return errors.New("redis: connection refused")
		}
		return nil
	}
	cfgs := NewConfigManager(newMemStore(), publish, nil, slog.New(slog.NewTextHandler(&logs, nil)))
	if err := cfgs.Load(context.Background(), []byte(baseConfig)); err != nil {
		t.Fatal(err)
	}
	redisDown.Store(true)
	v, err := cfgs.Replace(context.Background(), store.ActorAdmin, "edited", []byte(baseConfig))
	if _, current := cfgs.Raw(); err != nil || v != 2 || current != 2 || !strings.Contains(logs.String(), "next republish") {
		t.Fatalf("version %d, current %d, err %v, logs %s", v, current, err, logs.String())
	}
}

func goodBin(weight float64) bin {
	canary := int64(1000 * weight)
	return bin{stable: 1000 - canary, canary: canary, stableErr: 10 - canary/100, canaryErr: canary / 100, stableSlow: 50 - canary/20, canarySlow: canary / 20}
}

func (r *rig) pendingPromotion(t *testing.T) store.Rollout {
	t.Helper()
	ro, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 600 && r.engine.Status().Step.Step == 0; i++ {
		r.feed(goodBin(r.versions(t).Weight))
	}
	r.store.refuseWrites.Store(true)
	for i := 0; i < 600 && r.engine.Status().Pending == ""; i++ {
		r.feed(goodBin(r.versions(t).Weight))
	}
	if st := r.engine.Status(); st.Pending != store.OutcomePromoted {
		t.Fatalf("no promotion pending: %+v", st)
	}
	return ro
}

func TestAPendingPromotionBecomesARollbackWhenTheCanaryBreaksMeanwhile(t *testing.T) {
	braked := func(m *Merged) { rs := m.Routes["api"]; rs.Brake = true; m.Routes["api"] = rs }
	dead := func(m *Merged) { m.Dimensions[0].Outlier = health.Ejected }
	for name, breaks := range map[string]func(*rig){
		"the brake": func(r *rig) { r.feed(bin{stable: 500, canary: 500, canaryErr: 500}, braked) },
		"a dead canary": func(r *rig) {
			for range 10 {
				r.feed(bin{}, dead, withStable(true))
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			ro := r.pendingPromotion(t)
			breaks(r)
			if st := r.engine.Status(); st.Pending != store.OutcomeRolledBack {
				t.Fatalf("%s on a pending promotion left it %q", name, st.Pending)
			}
			r.store.refuseWrites.Store(false)
			r.engine.retry(context.Background())
			last := r.store.steps[ro.ID][1]
			if v := r.versions(t); v.Stable != "v1" || v.Canary != "" || r.store.outcomes[ro.ID] != store.OutcomeRolledBack || last.Decision != store.DecisionRollback {
				t.Fatalf("versions %+v, outcome %q, last step %+v", v, r.store.outcomes[ro.ID], last)
			}
		})
	}
}

func TestAnAbortTakesBackAPromotionThatIsNotSavedYet(t *testing.T) {
	r := newRig(t)
	ro := r.pendingPromotion(t)
	if err := r.engine.Abort(context.Background(), "changed my mind"); !errors.Is(err, ErrEndNotSaved) || r.engine.Status().Pending != store.OutcomeAborted {
		t.Fatalf("an abort during the outage: %v, pending %q", err, r.engine.Status().Pending)
	}
	r.store.refuseWrites.Store(false)
	r.engine.retry(context.Background())
	if v := r.versions(t); v.Stable != "v1" || v.Canary != "" || r.store.outcomes[ro.ID] != store.OutcomeAborted {
		t.Fatalf("versions %+v, outcome %q", v, r.store.outcomes[ro.ID])
	}
}

func TestAnEndThatCannotBeSavedIsLoggedOnceAMinute(t *testing.T) {
	r := newRig(t)
	var logs logSink
	r.engine.logger = slog.New(slog.NewTextHandler(&logs, nil))
	if _, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin); err != nil {
		t.Fatal(err)
	}
	r.store.refuseWrites.Store(true)
	r.failCanary()
	for range 13 {
		r.now = r.now.Add(settleEvery)
		r.engine.retry(context.Background())
	}
	if n := strings.Count(logs.String(), "end could not be saved"); n != 2 {
		t.Fatalf("%d lines over about 65 s of failed attempts, want the first and one a minute later:\n%s", n, logs.String())
	}
}

func TestReasonsAreCheckedAtTheDoor(t *testing.T) {
	r := newRig(t)
	if _, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewAPI(r.apiOptions()).Handler())
	t.Cleanup(srv.Close)
	for _, reason := range []string{"stop\u0000", strings.Repeat("x", maxReason+1)} {
		body, err := json.Marshal(map[string]string{"reason": reason})
		if err != nil {
			t.Fatal(err)
		}
		if got := send(t, http.MethodPost, srv.URL+"/api/rollouts/abort", string(body), asAdmin("9.9.9.9")).StatusCode; got != http.StatusBadRequest {
			t.Errorf("abort reason %.20q: %d", reason, got)
		}
	}
	if !r.engine.Status().Active {
		t.Fatal("a refused reason aborted the rollout")
	}
	_, before := r.cfgs.Raw()
	for _, reason := range []string{"edited \xff\xfe", strings.Repeat("x", maxReason+1)} {
		header := asAdmin("9.9.9.9")
		header.Set("X-Reason", reason)
		if got := send(t, http.MethodPut, srv.URL+"/api/config", baseConfig, header).StatusCode; got != http.StatusBadRequest {
			t.Errorf("config reason %.20q: %d", reason, got)
		}
	}
	if _, after := r.cfgs.Raw(); after != before {
		t.Fatalf("a refused reason saved config %d", after)
	}
}

func TestAConfigIsSavedEvenIfItsCallerLeaves(t *testing.T) {
	r := newRig(t)
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	if v, err := r.cfgs.Replace(gone, store.ActorAdmin, "the caller left", []byte(baseConfig)); err != nil || v != 2 {
		t.Fatalf("version %d, %v", v, err)
	}
}

func TestAnAbortKeepsARollbackThatIsAlreadyDecided(t *testing.T) {
	r := newRig(t)
	ro, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin)
	if err != nil {
		t.Fatal(err)
	}
	r.store.refuseWrites.Store(true)
	r.failCanary()
	decided := r.engine.Status().Step.Reason
	if err := r.engine.Abort(context.Background(), "aborted from the dashboard"); err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("an abort during the outage came back as %v, want the store's cause", err)
	}
	if st := r.engine.Status(); st.Pending != store.OutcomeRolledBack || st.Step.Reason != decided {
		t.Fatalf("the abort replaced the decided rollback: pending %q, step reason %q, was %q", st.Pending, st.Step.Reason, decided)
	}
	r.store.refuseWrites.Store(false)
	r.engine.retry(context.Background())
	if r.store.outcomes[ro.ID] != store.OutcomeRolledBack {
		t.Fatalf("outcome %q", r.store.outcomes[ro.ID])
	}
}

func TestClosingTheHubEndsEveryStream(t *testing.T) {
	r := newRig(t)
	o := r.apiOptions()
	hub := o.Hub
	srv := httptest.NewServer(NewAPI(o).Handler())
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL + "/api/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	first := make([]byte, 64)
	if _, err := resp.Body.Read(first); err != nil || !strings.HasPrefix(string(first), "event: state") {
		t.Fatalf("first frame %q, %v", first, err)
	}
	hub.Close()
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, resp.Body)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the stream ended with %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the stream kept going after the hub closed")
	}
}

func TestTheDashboardsAreAnsweredWhileARolloutWaitsOnPostgres(t *testing.T) {
	r := newRig(t)
	api := NewAPI(r.apiOptions())
	g := newGate()
	r.store.gate = g
	t.Cleanup(g.release)
	started := make(chan error, 1)
	go func() {
		_, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin)
		started <- err
	}()
	<-g.reached
	answered := make(chan State, 1)
	go func() { answered <- api.State() }()
	select {
	case st := <-answered:
		if st.Rollout.Active {
			t.Fatal("a rollout whose start isn't saved yet shows as running")
		}
	case <-time.After(time.Second):
		t.Fatal("the state waited for the rollout's write")
	}
	g.release()
	if err := <-started; err != nil || !r.engine.Status().Active {
		t.Fatalf("start: %v, then %+v", err, r.engine.Status())
	}
}

func TestEverySecondTheDashboardsGetATickWithChaosOnlyWhenChaosIsOn(t *testing.T) {
	for _, withChaos := range []bool{false, true} {
		r := newRig(t)
		o := r.apiOptions()
		want := []string{"latest", "rollout", "viewers"}
		if withChaos {
			o.Chaos, _, _ = newChaos(DefaultChaosLimits())
			want = append(want, "chaos")
		}
		api := NewAPI(o)
		frames, done := o.Hub.Subscribe()
		api.Observe(context.Background(), Merged{Second: 42})
		done()
		kind, data, _ := strings.Cut(string(<-frames), "\ndata: ")
		var tick map[string]json.RawMessage
		if err := json.Unmarshal([]byte(data), &tick); err != nil || kind != "event: tick" {
			t.Fatalf("%s %q: %v", kind, data, err)
		}
		if got := slices.Sorted(maps.Keys(tick)); !slices.Equal(got, slices.Sorted(slices.Values(want))) {
			t.Fatalf("with chaos %v the tick has %v", withChaos, got)
		}
		if latest := api.State().Latest; latest == nil || latest.Second != 42 {
			t.Fatalf("the state's latest second is %+v", latest)
		}
	}
}

func (r *rig) failCanary() {
	for range 15 {
		r.feed(bin{stable: 900, canary: 100, stableErr: 2, canaryErr: 40})
	}
}

func TestTheBrakeLeavesAPendingRollbackAsItWasDecided(t *testing.T) {
	r := newRig(t)
	if _, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin); err != nil {
		t.Fatal(err)
	}
	r.store.refuseWrites.Store(true)
	r.failCanary()
	decided := r.engine.Status().Step.Reason
	r.feed(bin{stable: 500, canary: 500, canaryErr: 500}, func(m *Merged) { rs := m.Routes["api"]; rs.Brake = true; m.Routes["api"] = rs })
	if st := r.engine.Status(); st.Pending != store.OutcomeRolledBack || st.Step.Reason != decided {
		t.Fatalf("the brake rewrote a pending rollback: %q, %q, was %q", st.Pending, st.Step.Reason, decided)
	}
}

func TestARolloutWhoseEndWasSavedButNotAnsweredStillEnds(t *testing.T) {
	r := newRig(t)
	ro, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin)
	if err != nil {
		t.Fatal(err)
	}
	r.store.loseEndReply.Store(true)
	r.failCanary()
	if !r.engine.Status().Active {
		t.Fatal("the rollout ended although the store's answer was lost")
	}
	r.engine.retry(context.Background())
	if v := r.versions(t); r.engine.Status().Active || v.Canary != "" || v.Stable != "v1" || r.store.outcomes[ro.ID] != store.OutcomeRolledBack {
		t.Fatalf("after saving the end again: active %v, versions %+v, outcome %q", r.engine.Status().Active, v, r.store.outcomes[ro.ID])
	}
}

func TestTheEndKeepsTheTimeItWasDecided(t *testing.T) {
	r := newRig(t)
	ro, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin)
	if err != nil {
		t.Fatal(err)
	}
	r.store.refuseWrites.Store(true)
	r.failCanary()
	decided := *r.engine.Status().Step.EndedAt
	r.now = r.now.Add(10 * time.Minute)
	r.store.refuseWrites.Store(false)
	r.engine.retry(context.Background())
	if at := r.store.finished[ro.ID]; !at.Equal(decided) {
		t.Fatalf("finished at %v, decided at %v", at, decided)
	}
}

func TestAStartWhoseAnswerWasLostIsNotRun(t *testing.T) {
	r := newRig(t)
	r.store.loseStartReply.Store(true)
	if _, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin); !errors.Is(err, ErrRolloutFailed) {
		t.Fatalf("start: %v", err)
	}
	if r.engine.Status().Active || r.versions(t).Canary != "" {
		t.Fatalf("a start the store never confirmed: active %v, versions %+v", r.engine.Status().Active, r.versions(t))
	}
	unconfirmed := r.store.rollouts[0].ID
	r.now = r.now.Add(time.Second)
	ro, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin)
	if err != nil {
		t.Fatalf("the next start: %v", err)
	}
	if r.store.outcomes[unconfirmed] != store.OutcomeAborted || r.store.outcomes[ro.ID] != "" {
		t.Fatalf("the next start should close the unconfirmed one and run: outcomes %v", r.store.outcomes)
	}
}

func TestAStartAndAnAbortFinishEvenIfTheirCallerLeaves(t *testing.T) {
	r := newRig(t)
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	ro, err := r.engine.Start(gone, "api", "v2", store.ActorAdmin)
	if err != nil || r.versions(t).Canary != "v2" {
		t.Fatalf("a start whose caller left: %v, versions %+v", err, r.versions(t))
	}
	if err := r.engine.Abort(gone, "the caller left"); err != nil || r.engine.Status().Active || r.store.outcomes[ro.ID] != store.OutcomeAborted {
		t.Fatalf("an abort whose caller left: %v, outcome %q", err, r.store.outcomes[ro.ID])
	}
}

func TestSecondsFromAnOlderConfigCannotTurnAPendingPromotion(t *testing.T) {
	r := newRig(t)
	r.pendingPromotion(t)
	stale := func(m *Merged) {
		m.ConfigVersions["gw-2"] = 1
		rs := m.Routes["api"]
		rs.Brake = true
		m.Routes["api"] = rs
	}
	r.feed(bin{stable: 500, canary: 500, canaryErr: 500}, stale)
	if st := r.engine.Status(); st.Pending != store.OutcomePromoted {
		t.Fatalf("a braked second from an older config turned the promotion into %q", st.Pending)
	}
}

func TestTheLastAttemptRunsOnShutdown(t *testing.T) {
	r := newRig(t)
	ro, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin)
	if err != nil {
		t.Fatal(err)
	}
	r.store.refuseWrites.Store(true)
	r.failCanary()
	r.store.refuseWrites.Store(false)
	stopped, cancel := context.WithCancel(context.Background())
	cancel()
	r.engine.Run(stopped)
	if r.engine.Status().Active || r.store.outcomes[ro.ID] != store.OutcomeRolledBack {
		t.Fatalf("after shutdown: active %v, outcome %q", r.engine.Status().Active, r.store.outcomes[ro.ID])
	}
}

func TestARefusedConfigEndsTheRolloutInsteadOfRetrying(t *testing.T) {
	r := newRig(t)
	ro, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin)
	if err != nil {
		t.Fatal(err)
	}
	renamed := strings.Replace(baseConfig, `"name":"api"`, `"name":"web"`, 1)
	if _, err := r.cfgs.Replace(context.Background(), store.ActorAdmin, "the route was renamed", []byte(renamed)); err != nil {
		t.Fatal(err)
	}
	r.failCanary()
	if r.engine.Status().Active || r.store.outcomes[ro.ID] != store.OutcomeRolledBack {
		t.Fatalf("a rollback the config refused: active %v, outcome %q", r.engine.Status().Active, r.store.outcomes[ro.ID])
	}
}

func TestAStepIsTimedBySecondsTheGatewaysRanIt(t *testing.T) {
	r := newRig(t)
	r.engine.opts.MaxStep = 10 * time.Second
	if _, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin); err != nil {
		t.Fatal(err)
	}
	stale := func(m *Merged) { m.ConfigVersions["gw-2"] = 1 }
	for range 30 {
		r.feed(goodBin(r.versions(t).Weight), stale)
	}
	r.feed(goodBin(r.versions(t).Weight))
	if st := r.engine.Status(); !st.Active || st.Step.Step != 0 || st.DataSec != 1 {
		t.Fatalf("a step decided on its first second after the gateways caught up: %+v", st)
	}
}
