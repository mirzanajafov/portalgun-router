package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/chaos"
	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
	"github.com/mirzanajafov/portalgun-router/internal/health"
	"github.com/mirzanajafov/portalgun-router/internal/store"
)

type fakeInjector struct {
	mu       sync.Mutex
	injected []chaos.Fault
	cleared  []chaos.Fault
	err      error
	gate     *gate
}

func (f *fakeInjector) Inject(_ context.Context, fault chaos.Fault) error {
	f.gate.pass()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.injected = append(f.injected, fault)
	return nil
}

func (f *fakeInjector) Clear(_ context.Context, fault chaos.Fault) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cleared = append(f.cleared, fault)
	return nil
}

func (f *fakeInjector) injections() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.injected)
}

func (f *fakeInjector) clears() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.cleared)
}

type fakeCounter struct {
	mu   sync.Mutex
	hits map[string]int64
	err  error
}

func (f *fakeCounter) Hit(_ context.Context, key string, _ time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return 0, f.err
	}
	f.hits[key]++
	return f.hits[key], nil
}

func (f *fakeCounter) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func downCounter() *fakeCounter {
	return &fakeCounter{hits: map[string]int64{}, err: errors.New("dial tcp redis:6379: connection refused")}
}

type logSink struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *logSink) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logSink) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func newChaos(limits ChaosLimits) (*ChaosService, *fakeInjector, *fakeCounter) {
	inj, counter := &fakeInjector{}, &fakeCounter{hits: map[string]int64{}}
	svc := NewChaosService(ChaosOptions{Injector: inj, Counter: counter, Limits: limits, Targets: []string{"dim-a", "dim-b"}, Logger: quiet})
	return svc, inj, counter
}

func chaosAPI(svc *ChaosService) http.Handler {
	return NewAPI(APIOptions{Chaos: svc, Token: adminToken, ProxyToken: proxyToken, Logger: quiet}).Handler()
}

func TestVisitorsGetBoundedChaos(t *testing.T) {
	svc, _, _ := newChaos(DefaultChaosLimits())
	ctx := context.Background()
	for _, req := range []ChaosRequest{
		{Mode: "crash", Target: "dim-a", DurationSec: 120},
		{Mode: "slow", Target: "dim-a", LatencyMs: 5000},
		{Mode: "partial", Target: "dim-a", ErrorRate: 0.9},
		{Mode: "crash", Target: "the-database"},
		{Mode: "meteor", Target: "dim-a"},
	} {
		if _, _, err := svc.Start(ctx, req, false, "1.2.3.4"); err == nil {
			t.Errorf("%+v accepted for a visitor", req)
		}
	}
	if _, _, err := svc.Start(ctx, ChaosRequest{Mode: "crash", Target: "dim-a", DurationSec: 120}, true, ""); err != nil {
		t.Fatalf("admin refused: %v", err)
	}
}

func TestOneFaultAtATimeThenCooldown(t *testing.T) {
	limits := DefaultChaosLimits()
	limits.Cooldown = time.Hour
	svc, inj, _ := newChaos(limits)
	ctx := context.Background()
	run, _, err := svc.Start(ctx, ChaosRequest{Mode: "crash", Target: "dim-a", DurationSec: 0.05}, false, "1.2.3.4")
	if err != nil {
		t.Fatal(err)
	}
	if run.Actor != store.ActorVisitor || run.Until.Sub(run.Started) != 50*time.Millisecond {
		t.Fatalf("run %+v", run)
	}
	if _, wait, err := svc.Start(ctx, ChaosRequest{Mode: "slow", Target: "dim-b", LatencyMs: 100}, false, "5.6.7.8"); !errors.Is(err, ErrChaosBusy) || wait <= 0 {
		t.Fatalf("second fault: %v, wait %v", err, wait)
	}
	deadline := time.Now().Add(2 * time.Second)
	for inj.clears() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the fault never reverted")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, _, err := svc.Start(ctx, ChaosRequest{Mode: "crash", Target: "dim-a"}, false, "5.6.7.8"); !errors.Is(err, ErrChaosCooldown) {
		t.Fatalf("during the cooldown: %v", err)
	}
	if _, _, err := svc.Start(ctx, ChaosRequest{Mode: "crash", Target: "dim-a", DurationSec: 1}, true, ""); err != nil {
		t.Fatalf("admin during the cooldown: %v", err)
	}
	if err := svc.Stop(ctx); err != nil || inj.clears() != 2 {
		t.Fatalf("stop: %v, clears %d", err, inj.clears())
	}
}

func TestVisitorsHaveAnHourlyLimit(t *testing.T) {
	limits := DefaultChaosLimits()
	limits.Cooldown = 0
	svc, _, _ := newChaos(limits)
	ctx := context.Background()
	for i := range 4 {
		_, _, err := svc.Start(ctx, ChaosRequest{Mode: "crash", Target: "dim-a", DurationSec: 1}, false, "1.2.3.4")
		if i < 3 && err != nil {
			t.Fatalf("run %d: %v", i+1, err)
		}
		if i == 3 && !errors.Is(err, ErrChaosLimit) {
			t.Fatalf("fourth run: %v", err)
		}
		_ = svc.Stop(ctx)
	}
}

func TestVisitorsAreRefusedWhileTheirFaultsCannotBeCounted(t *testing.T) {
	logs := &logSink{}
	counter := downCounter()
	inj := &fakeInjector{}
	svc := NewChaosService(ChaosOptions{Injector: inj, Counter: counter, Limits: DefaultChaosLimits(), Targets: []string{"dim-a"}, Logger: slog.New(slog.NewTextHandler(logs, nil))})
	var elapsed atomic.Int64
	start := time.Now()
	svc.visitors.now = func() time.Time { return start.Add(time.Duration(elapsed.Load())) }
	srv := httptest.NewServer(chaosAPI(svc))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	url, fault := srv.URL+"/api/chaos", `{"mode":"crash","target":"dim-a"}`
	visitor, admin := viaProxy("1.2.3.4"), asAdmin("1.2.3.4")
	for range 2 {
		if resp := send(t, http.MethodPost, url, fault, visitor); resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "60" {
			t.Fatalf("visitor with the counter down: %d, Retry-After %q", resp.StatusCode, resp.Header.Get("Retry-After"))
		}
	}
	if n := inj.injections(); n != 0 {
		t.Fatalf("injected %d faults without counting the visitor", n)
	}
	if n := strings.Count(logs.String(), "connection refused"); n != 1 {
		t.Fatalf("logged the counter's error %d times, want once: %q", n, logs.String())
	}
	if strings.Contains(logs.String(), "1.2.3.4") || !strings.Contains(logs.String(), "limit=chaos") {
		t.Fatalf("the log should name the limit and not the visitor: %q", logs.String())
	}
	if resp := send(t, http.MethodPost, url, fault, admin); resp.StatusCode != http.StatusCreated {
		t.Fatalf("admin with the counter down: %d", resp.StatusCode)
	}
	if resp := send(t, http.MethodDelete, url, "", admin); resp.StatusCode != http.StatusOK {
		t.Fatalf("admin stop: %d", resp.StatusCode)
	}
	counter.fail(nil)
	if resp := send(t, http.MethodPost, url, fault, visitor); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("visitor within a minute of the failure: %d, want the pause to hold", resp.StatusCode)
	}
	elapsed.Store(int64(recountAfter))
	if resp := send(t, http.MethodPost, url, fault, visitor); resp.StatusCode != http.StatusCreated {
		t.Fatalf("visitor a minute after the failure: %d", resp.StatusCode)
	}
}

func TestAVisitorWhoLeavesIsNotLoggedAsACounterFailure(t *testing.T) {
	logs := &logSink{}
	counter := &fakeCounter{hits: map[string]int64{}, err: context.Canceled}
	svc := NewChaosService(ChaosOptions{Injector: &fakeInjector{}, Counter: counter, Limits: DefaultChaosLimits(), Targets: []string{"dim-a"}, Logger: slog.New(slog.NewTextHandler(logs, nil))})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := svc.Start(ctx, ChaosRequest{Mode: "crash", Target: "dim-a"}, false, "1.2.3.4"); !errors.Is(err, ErrLimitUnchecked) {
		t.Fatalf("cancelled visitor: %v", err)
	}
	if logs.String() != "" {
		t.Fatalf("logged a cancelled request: %q", logs.String())
	}
	counter.fail(nil)
	if _, _, err := svc.Start(context.Background(), ChaosRequest{Mode: "crash", Target: "dim-a"}, false, "5.6.7.8"); err != nil {
		t.Fatalf("the next visitor was refused after a cancelled request: %v", err)
	}
}

func TestIncidentsMeasureDetectionMoveAndLoss(t *testing.T) {
	svc, _, _ := newChaos(DefaultChaosLimits())
	base := time.Unix(2_000_000, 0)
	svc.now = func() time.Time { return base.Add(300 * time.Millisecond) }
	if _, _, err := svc.Start(context.Background(), ChaosRequest{Mode: "crash", Target: "dim-a", DurationSec: 1}, true, ""); err != nil {
		t.Fatal(err)
	}
	tick := func(sec int64, attemptsA int64, healthy bool, outlier health.State, failed int64) {
		svc.Observe(Merged{
			Second: sec,
			Routes: map[string]gatewayapi.RouteSnapshot{"api": {Failed: failed}},
			Dimensions: []gatewayapi.DimensionSnapshot{
				{ID: "dim-a", Attempts: attemptsA, Healthy: healthy, Outlier: outlier},
				{ID: "dim-b", Attempts: 900 - attemptsA, Healthy: true, Outlier: health.Closed},
			},
		})
	}
	tick(base.Unix(), 300, true, health.Closed, 5)
	tick(base.Unix()+1, 4, true, health.Ejected, 2)
	tick(base.Unix()+2, 0, false, health.Ejected, 0)
	inc := svc.View().Incidents[0]
	if inc.DetectedS == nil || inc.MovedS == nil || inc.Lost != 7 {
		t.Fatalf("incident %+v", inc)
	}
	if *inc.DetectedS != 1.7 || *inc.MovedS != 1.7 {
		t.Fatalf("detected after %.1fs, moved after %.1fs; want both at the end of the second second, 1.7s", *inc.DetectedS, *inc.MovedS)
	}
}

func TestWhileAFaultIsBeingInjectedTheViewAnswersAndTheSecondCounts(t *testing.T) {
	svc, inj, _ := newChaos(DefaultChaosLimits())
	base := time.Unix(2_000_000, 0)
	svc.now = func() time.Time { return base }
	g := newGate()
	inj.gate = g
	t.Cleanup(g.release)
	started := make(chan error, 1)
	go func() {
		_, _, err := svc.Start(context.Background(), ChaosRequest{Mode: "crash", Target: "dim-a"}, true, "")
		started <- err
	}()
	<-g.reached
	answered := make(chan ChaosView, 1)
	go func() {
		svc.Observe(Merged{
			Second:     base.Unix(),
			Routes:     map[string]gatewayapi.RouteSnapshot{"api": {Failed: 4}},
			Dimensions: []gatewayapi.DimensionSnapshot{{ID: "dim-a", Attempts: 100, Healthy: true, Outlier: health.Closed}},
		})
		answered <- svc.View()
	}()
	select {
	case v := <-answered:
		if v.Current != nil {
			t.Fatal("a fault that isn't injected yet shows as running")
		}
	case <-time.After(time.Second):
		t.Fatal("the chaos view waited for the injection")
	}
	g.release()
	if err := <-started; err != nil {
		t.Fatal(err)
	}
	if lost := svc.View().Incidents[0].Lost; lost != 4 {
		t.Fatalf("the second observed during the injection counted %d lost requests, want 4", lost)
	}
	if err := svc.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestChaosEndpoints(t *testing.T) {
	svc, _, counter := newChaos(DefaultChaosLimits())
	srv := httptest.NewServer(chaosAPI(svc))
	t.Cleanup(srv.Close)
	url, visitor := srv.URL+"/api/chaos", viaProxy("9.9.9.9")
	if got := send(t, http.MethodPost, url, `{"mode":"crash","target":"dim-a"}`, visitor).StatusCode; got != 201 {
		t.Fatalf("visitor fault: %d", got)
	}
	if n := counter.hits["chaos:ip:9.9.9.9"]; n != 1 || len(counter.hits) != 1 {
		t.Fatalf("counts %v, want the fault on the address the dashboard forwarded", counter.hits)
	}
	if got := send(t, http.MethodPost, url, `{"mode":"crash","target":"dim-b"}`, visitor).StatusCode; got != 409 {
		t.Fatalf("second fault: %d", got)
	}
	if got := send(t, http.MethodDelete, url, "", visitor).StatusCode; got != 401 {
		t.Fatalf("visitor stop: %d", got)
	}
	if got := send(t, http.MethodDelete, url, "", asAdmin("9.9.9.9")).StatusCode; got != 200 {
		t.Fatalf("admin stop: %d", got)
	}
	if got := send(t, http.MethodPost, url, `{"mode":"crash","target":"dim-a"}`, visitor).StatusCode; got != 429 {
		t.Fatalf("during the cooldown: %d", got)
	}
}

func TestAFaultThatFailsToStartIs503AndOnlyTheAdminSeesWhy(t *testing.T) {
	for _, failure := range []error{
		fmt.Errorf(`dimension control: %w: Patch "http://dim-a:8081/state": 401 Unauthorized`, chaos.ErrControlToken),
		errors.New(`dimension control: Patch "http://dim-a:8081/state": dial tcp: lookup dim-a on 127.0.0.11:53: no such host`),
	} {
		svc, inj, _ := newChaos(DefaultChaosLimits())
		inj.err = failure
		api := chaosAPI(svc)
		for _, caller := range []struct {
			header http.Header
			admin  bool
		}{{viaProxy("9.9.9.9"), false}, {asAdmin("9.9.9.9"), true}} {
			req := httptest.NewRequest(http.MethodPost, "/api/chaos", strings.NewReader(`{"mode":"partial","target":"dim-a","errorRate":0.3}`))
			req.Header = caller.header
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if named := strings.Contains(w.Body.String(), "dim-a"); w.Code != http.StatusServiceUnavailable || named != caller.admin {
				t.Fatalf("%v came back to admin=%v as %d %s", failure, caller.admin, w.Code, w.Body)
			}
		}
		if n := len(svc.View().Incidents); n != 0 {
			t.Fatalf("%d incidents left from faults that never started", n)
		}
	}
}
