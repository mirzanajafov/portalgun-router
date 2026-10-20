package controller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/chaos"
	"github.com/mirzanajafov/portalgun-router/internal/proxy"
)

type fakeInjector struct {
	mu       sync.Mutex
	injected []chaos.Fault
	cleared  []chaos.Fault
}

func (f *fakeInjector) Inject(_ context.Context, fault chaos.Fault) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.injected = append(f.injected, fault)
	return nil
}

func (f *fakeInjector) Clear(_ context.Context, fault chaos.Fault) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cleared = append(f.cleared, fault)
	return nil
}

func (f *fakeInjector) ClearAll(context.Context) error { return nil }

func (f *fakeInjector) clears() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.cleared)
}

type fakeCounter struct {
	mu   sync.Mutex
	hits map[string]int64
}

func (f *fakeCounter) Hit(_ context.Context, key string, _ time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hits[key]++
	return f.hits[key], nil
}

type downCounter struct{}

func (downCounter) Hit(context.Context, string, time.Duration) (int64, error) {
	return 0, errors.New("dial tcp redis:6379: connection refused")
}

func newChaos(limits ChaosLimits) (*ChaosService, *fakeInjector) {
	inj := &fakeInjector{}
	svc := NewChaosService(inj, nil, &fakeCounter{hits: map[string]int64{}}, NewHub(), limits, []string{"dim-a", "dim-b"}, 1, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return svc, inj
}

func TestVisitorsGetBoundedChaos(t *testing.T) {
	svc, _ := newChaos(DefaultChaosLimits())
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
	svc, inj := newChaos(limits)
	ctx := context.Background()
	run, _, err := svc.Start(ctx, ChaosRequest{Mode: "crash", Target: "dim-a", DurationSec: 0.05}, false, "1.2.3.4")
	if err != nil {
		t.Fatal(err)
	}
	if run.Actor != "visitor" || run.Until.Sub(run.Started) != 50*time.Millisecond {
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
	svc, _ := newChaos(limits)
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

func TestVisitorsAreRefusedWhenTheirFaultsCannotBeCounted(t *testing.T) {
	var logs strings.Builder
	inj := &fakeInjector{}
	svc := NewChaosService(inj, nil, downCounter{}, NewHub(), DefaultChaosLimits(), []string{"dim-a"}, 1, slog.New(slog.NewTextHandler(&logs, nil)))
	ctx := context.Background()
	if _, _, err := svc.Start(ctx, ChaosRequest{Mode: "crash", Target: "dim-a"}, false, "1.2.3.4"); !errors.Is(err, ErrLimitUnchecked) {
		t.Fatalf("visitor with the counter down: %v", err)
	}
	if len(inj.injected) != 0 {
		t.Fatalf("injected %v without counting the visitor", inj.injected)
	}
	if !strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("the refusal was not logged: %q", logs.String())
	}
	if _, _, err := svc.Start(ctx, ChaosRequest{Mode: "crash", Target: "dim-a"}, true, ""); err != nil {
		t.Fatalf("admin with the counter down: %v", err)
	}
}

func TestIncidentsMeasureDetectionMoveAndLoss(t *testing.T) {
	svc, _ := newChaos(DefaultChaosLimits())
	base := time.Unix(2_000_000, 0)
	svc.now = func() time.Time { return base.Add(300 * time.Millisecond) }
	if _, _, err := svc.Start(context.Background(), ChaosRequest{Mode: "crash", Target: "dim-a", DurationSec: 1}, true, ""); err != nil {
		t.Fatal(err)
	}
	tick := func(sec int64, attemptsA int64, healthy bool, outlier string, failed int64) {
		svc.Observe(Merged{
			Second: sec,
			Routes: map[string]proxy.RouteSnapshot{"api": {Failed: failed}},
			Dimensions: []proxy.DimensionSnapshot{
				{ID: "dim-a", Attempts: attemptsA, Healthy: healthy, Outlier: outlier},
				{ID: "dim-b", Attempts: 900 - attemptsA, Healthy: true, Outlier: "closed"},
			},
		})
	}
	tick(base.Unix(), 300, true, "closed", 5)
	tick(base.Unix()+1, 4, true, "ejected", 2)
	tick(base.Unix()+2, 0, false, "ejected", 0)
	inc := svc.View().Incidents[0]
	if inc.DetectedS == nil || inc.MovedS == nil || inc.Lost != 7 {
		t.Fatalf("incident %+v", inc)
	}
	if *inc.DetectedS != 1.7 || *inc.MovedS != 1.7 {
		t.Fatalf("detected after %.1fs, moved after %.1fs; want both at the end of the second second, 1.7s", *inc.DetectedS, *inc.MovedS)
	}
}

func TestChaosEndpoints(t *testing.T) {
	svc, _ := newChaos(DefaultChaosLimits())
	mux := http.NewServeMux()
	api := &API{token: "s3cret"}
	svc.Routes(mux, api.admin, "s3cret")
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	call := func(method, body, auth string) int {
		req, _ := http.NewRequest(method, srv.URL+"/api/chaos", strings.NewReader(body))
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
	if got := call(http.MethodPost, `{"mode":"crash","target":"dim-a"}`, ""); got != 201 {
		t.Fatalf("visitor fault: %d", got)
	}
	if got := call(http.MethodPost, `{"mode":"crash","target":"dim-b"}`, ""); got != 409 {
		t.Fatalf("second fault: %d", got)
	}
	if got := call(http.MethodDelete, "", ""); got != 401 {
		t.Fatalf("visitor stop: %d", got)
	}
	if got := call(http.MethodDelete, "", "Bearer s3cret"); got != 200 {
		t.Fatalf("admin stop: %d", got)
	}
	if got := call(http.MethodPost, `{"mode":"crash","target":"dim-a"}`, ""); got != 429 {
		t.Fatalf("during the cooldown: %d", got)
	}
}
