package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
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

	refuseConfigs, refuseRollouts, refuseReads, refuseFinishes atomic.Bool
}

var errStoreDown = errors.New("dial tcp postgres:5432: connect: connection refused")

func newMemStore() *memStore {
	return &memStore{steps: map[string]map[int]store.Step{}, outcomes: map[string]store.Outcome{}}
}

func (m *memStore) LatestConfig(context.Context) (store.ConfigVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.configs) == 0 {
		return store.ConfigVersion{}, store.ErrNoConfig
	}
	return m.configs[len(m.configs)-1], nil
}

func (m *memStore) SaveConfig(_ context.Context, author store.Actor, reason string, body json.RawMessage) (store.ConfigVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.refuseConfigs.Load() {
		return store.ConfigVersion{}, errStoreDown
	}
	c := store.ConfigVersion{Version: int64(len(m.configs) + 1), Author: author, Reason: reason, Body: body}
	m.configs = append(m.configs, c)
	return c, nil
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

func (m *memStore) StartRollout(_ context.Context, r store.Rollout) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.refuseRollouts.Load() {
		return errStoreDown
	}
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

func (m *memStore) FinishRollout(_ context.Context, id string, outcome store.Outcome, _ string, _ time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.refuseFinishes.Load() {
		return errStoreDown
	}
	if _, done := m.outcomes[id]; done {
		return store.ErrRolloutNotActive
	}
	m.outcomes[id] = outcome
	return nil
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

func TestAPIRequiresTheTokenToChangeThings(t *testing.T) {
	r := newRig(t)
	api := NewAPI(r.cfgs, r.engine, r.store, NewHub(quiet), adminToken, quiet)
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
	api := NewAPI(r.cfgs, r.engine, r.store, NewHub(quiet), adminToken, quiet)
	counter := &fakeCounter{hits: map[string]int64{}}
	api.LetVisitorsRollOut(counter, RolloutLimits{PerHour: 2}, proxyToken, quiet)
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
	api := NewAPI(r.cfgs, r.engine, r.store, NewHub(quiet), adminToken, quiet)
	api.LetVisitorsRollOut(downCounter(), DefaultRolloutLimits(), proxyToken, slog.New(slog.NewTextHandler(logs, nil)))
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
	api := NewAPI(r.cfgs, r.engine, r.store, NewHub(quiet), adminToken, quiet)
	api.LetVisitorsRollOut(counter, DefaultRolloutLimits(), proxyToken, slog.New(slog.NewTextHandler(logs, nil)))
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
	r.store.refuseConfigs.Store(true)
	for range 15 {
		r.feed(bin{stable: 900, canary: 100, stableErr: 2, canaryErr: 40})
	}
	if st := r.engine.Status(); !st.Active || st.Pending != store.OutcomeRolledBack || r.versions(t).Canary != "v2" {
		t.Fatalf("a rollback that couldn't be saved left the rollout %+v and the config %+v", st, r.versions(t))
	}
	r.store.refuseConfigs.Store(false)
	r.feed(bin{})
	if v := r.versions(t); r.engine.Status().Active || v.Canary != "" || v.Stable != "v1" || r.store.outcomes[ro.ID] != store.OutcomeRolledBack {
		t.Fatalf("after the store came back: active %v, versions %+v, outcome %q", r.engine.Status().Active, v, r.store.outcomes[ro.ID])
	}
}

func TestAStartTheStoreRefusesLeavesNoCanaryBehind(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	r.store.refuseRollouts.Store(true)
	if _, err := r.engine.Start(ctx, "api", "v2", store.ActorAdmin); !errors.Is(err, ErrRolloutFailed) || r.versions(t).Canary != "" {
		t.Fatalf("got %v with versions %+v", err, r.versions(t))
	}
	r.store.refuseRollouts.Store(false)
	r.store.refuseConfigs.Store(true)
	if _, err := r.engine.Start(ctx, "api", "v2", store.ActorAdmin); !errors.Is(err, ErrRolloutFailed) || r.versions(t).Canary != "" {
		t.Fatalf("got %v with versions %+v", err, r.versions(t))
	}
	if r.engine.Status().Active || len(r.store.rollouts) != 1 || r.store.outcomes[r.store.rollouts[0].ID] != store.OutcomeAborted {
		t.Fatalf("a rollout whose config wasn't saved: active %v, rollouts %+v, outcomes %v", r.engine.Status().Active, r.store.rollouts, r.store.outcomes)
	}
}

func TestAStoreFailureIsA503AndOnlyTheAdminSeesWhy(t *testing.T) {
	r := newRig(t)
	api := NewAPI(r.cfgs, r.engine, r.store, NewHub(quiet), adminToken, quiet)
	api.LetVisitorsRollOut(&fakeCounter{hits: map[string]int64{}}, RolloutLimits{PerHour: 10}, proxyToken, quiet)
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	admin := http.Header{"Authorization": {"Bearer " + adminToken}}
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
	r.store.refuseRollouts.Store(true)
	r.store.refuseReads.Store(true)
	r.store.refuseConfigs.Store(true)
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

	r.store.refuseRollouts.Store(false)
	r.store.refuseConfigs.Store(false)
	if status, body := call(http.MethodPost, "/api/rollouts", `{"route":"api","canary":"v2"}`, admin); status != http.StatusCreated {
		t.Fatalf("start with the store back: %d %s", status, body)
	}
	r.store.refuseConfigs.Store(true)
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

func TestAPendingPromotionBecomesARollbackWhenTheCanaryBreaksMeanwhile(t *testing.T) {
	r := newRig(t)
	ro, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 600 && r.engine.Status().Step.Step == 0; i++ {
		r.feed(goodBin(r.versions(t).Weight))
	}
	r.store.refuseConfigs.Store(true)
	for i := 0; i < 600 && r.engine.Status().Pending == ""; i++ {
		r.feed(goodBin(r.versions(t).Weight))
	}
	if st := r.engine.Status(); st.Pending != store.OutcomePromoted {
		t.Fatalf("no promotion pending: %+v", st)
	}
	r.feed(bin{stable: 500, canary: 500, canaryErr: 500}, func(m *Merged) { rs := m.Routes["api"]; rs.Brake = true; m.Routes["api"] = rs })
	if st := r.engine.Status(); st.Pending != store.OutcomeRolledBack {
		t.Fatalf("the brake fired on a pending promotion and left it %q", st.Pending)
	}
	r.store.refuseConfigs.Store(false)
	r.feed(bin{})
	if v := r.versions(t); v.Stable != "v1" || v.Canary != "" || r.store.outcomes[ro.ID] != store.OutcomeRolledBack {
		t.Fatalf("versions %+v, outcome %q", v, r.store.outcomes[ro.ID])
	}
}

func TestAnAbortKeepsARollbackThatIsAlreadyDecided(t *testing.T) {
	r := newRig(t)
	ro, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin)
	if err != nil {
		t.Fatal(err)
	}
	r.store.refuseConfigs.Store(true)
	for range 15 {
		r.feed(bin{stable: 900, canary: 100, stableErr: 2, canaryErr: 40})
	}
	decided := r.engine.Status().Step.Reason
	if err := r.engine.Abort(context.Background(), "aborted from the dashboard"); err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("an abort during the outage came back as %v, want the store's cause", err)
	}
	if st := r.engine.Status(); st.Pending != store.OutcomeRolledBack || st.Step.Reason != decided {
		t.Fatalf("the abort replaced the decided rollback: pending %q, step reason %q, was %q", st.Pending, st.Step.Reason, decided)
	}
	r.store.refuseConfigs.Store(false)
	r.feed(bin{})
	if r.store.outcomes[ro.ID] != store.OutcomeRolledBack {
		t.Fatalf("outcome %q", r.store.outcomes[ro.ID])
	}
}

func TestTheOutcomeIsRecordedOnceTheStoreTakesIt(t *testing.T) {
	r := newRig(t)
	ro, err := r.engine.Start(context.Background(), "api", "v2", store.ActorAdmin)
	if err != nil {
		t.Fatal(err)
	}
	r.store.refuseFinishes.Store(true)
	for range 15 {
		r.feed(bin{stable: 900, canary: 100, stableErr: 2, canaryErr: 40})
	}
	if v := r.versions(t); v.Canary != "" || !r.engine.Status().Active {
		t.Fatalf("the config should be back on v1 while the outcome waits: versions %+v, status %+v", v, r.engine.Status())
	}
	r.store.refuseFinishes.Store(false)
	r.feed(bin{})
	if r.engine.Status().Active || r.store.outcomes[ro.ID] != store.OutcomeRolledBack {
		t.Fatalf("active %v, outcome %q", r.engine.Status().Active, r.store.outcomes[ro.ID])
	}
}

func TestClosingTheHubEndsEveryStream(t *testing.T) {
	r := newRig(t)
	hub := NewHub(quiet)
	srv := httptest.NewServer(NewAPI(r.cfgs, r.engine, r.store, hub, adminToken, quiet).Handler())
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
