package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
	"github.com/mirzanajafov/portalgun-router/internal/health"
	"github.com/mirzanajafov/portalgun-router/internal/store"
)

var update = flag.Bool("update", false, "rewrite testdata/dashboard.json from what the controller sends")

func TestTheDashboardReadsWhatTheControllerSends(t *testing.T) {
	r := newRig(t)
	idle := r.engine.Status()
	if _, err := r.engine.Start(context.Background(), "api", "v2", store.ActorVisitor); err != nil {
		t.Fatal(err)
	}
	r.feed(bin{stable: 900, canary: 100, stableErr: 2, canaryErr: 3, stableSlow: 45, canarySlow: 5})
	running := r.engine.Status()
	settling := newRig(t)
	settling.pendingPromotion(t)

	at := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	broken, _, _ := newChaos(DefaultChaosLimits())
	broken.now = func() time.Time { return at.Add(300 * time.Millisecond) }
	if _, _, err := broken.Start(context.Background(), ChaosRequest{Mode: "crash", Target: "dim-a"}, false, "203.0.113.7"); err != nil {
		t.Fatal(err)
	}
	for sec, outlier := range []health.State{health.Closed, health.Ejected} {
		broken.Observe(Merged{Second: at.Unix() + int64(sec), Dimensions: []gatewayapi.DimensionSnapshot{
			{ID: "dim-a", Attempts: int64(300 - 296*sec), Healthy: true, Outlier: outlier},
			{ID: "dim-b", Attempts: int64(600 + 296*sec), Healthy: true, Outlier: health.Closed},
		}})
	}
	t.Cleanup(func() { _ = broken.Stop(context.Background()) })
	untouched := NewChaosService(ChaosOptions{Injector: &fakeInjector{}, Limits: DefaultChaosLimits(), Logger: quiet})

	busy := merge(1_000_001, map[string]gatewayapi.Snapshot{"gw-1": {
		Instance: "gw-1",
		Routes: map[string]gatewayapi.RouteSnapshot{"portal": {
			Requests: 200, Limited: 1, Failed: 2, Stable: "v1", Canary: "v2", Weight: 0.01, Rollout: "ro-1",
			Versions: [2]gatewayapi.VersionSnapshot{gatewayapi.StableSlot: {Attempts: 198}, gatewayapi.CanarySlot: {Attempts: 2}},
		}},
		Dimensions: []gatewayapi.DimensionSnapshot{
			{ID: "c-137", Version: "v1", Attempts: 198, Errors: 1, P50Ms: 3, P99Ms: 40, Healthy: true, Outlier: health.Closed},
			{ID: "d-99", Version: "v2", Attempts: 2, P99Ms: 900, Outlier: health.Ejected, EjectedMs: 5000},
		},
	}})
	quietSecond := merge(1_000_002, map[string]gatewayapi.Snapshot{"gw-1": {Instance: "gw-1"}})
	events := []Event{
		{Type: EventConfig, At: at, Message: "config v4 by admin: edited", Actor: store.ActorAdmin},
		{Type: EventChaos, At: at, Message: "crash on dim-a cleared: the fault ran its course"},
		{Type: EventRollout, At: at, Message: "rollout ro-1: v2 at 1% of portal"},
	}
	brokenView, untouchedView := broken.View(), untouched.View()

	got, err := json.MarshalIndent(map[string]any{
		"state":        State{ConfigVersion: 4, Latest: &busy, Rollout: running, Viewers: 2, Events: events},
		"firstState":   State{Rollout: idle, Events: NewHub(quiet).Events()},
		"tick":         Tick{Latest: busy, Rollout: running, Viewers: 2, Chaos: &brokenView},
		"quietTick":    Tick{Latest: quietSecond, Rollout: idle, Viewers: 1, Chaos: &untouchedView},
		"settlingTick": Tick{Latest: busy, Rollout: settling.engine.Status(), Viewers: 2},
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	path := filepath.Join("testdata", "dashboard.json")
	if *update {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("the controller now sends the dashboard something other than %s, which the web tests parse: rerun with -update and run the web tests", path)
	}
}
