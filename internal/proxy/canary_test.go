package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/dimension"
)

func canaryConfig(t *testing.T, stable, canary string, weight float64, rollout string) *config.Config {
	return parse(t, config.Config{
		TrustedHops: 1,
		Routes: []config.Route{{Name: "all", PathPrefix: "/", Pool: "p",
			Versions: config.Versions{Stable: "v1", Canary: "v2", Weight: weight, Rollout: rollout}}},
		Pools: []config.Pool{{Name: "p", HealthCheck: config.HealthCheck{Disabled: true}, Dimensions: []config.Dimension{
			{ID: "stable", Address: stable, Version: "v1"},
			{ID: "canary", Address: canary, Version: "v2"},
		}}},
	})
}

func manual(t *testing.T, cfg *config.Config) (*Gateway, *httptest.Server) {
	t.Helper()
	g, err := New(cfg, Options{ManualTick: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.Close)
	srv := httptest.NewServer(g)
	t.Cleanup(srv.Close)
	return g, srv
}

func from(t *testing.T, url, client string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("X-Forwarded-For", client)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, resp.Header.Get(DimensionHeader)
}

func TestCanarySplitIsWeightedAndSticky(t *testing.T) {
	cfg := canaryConfig(t, dim(t, "stable", nil), dim(t, "canary", nil), 0.25, "r1")
	g, srv := manual(t, cfg)
	assigned := map[string]string{}
	canaries := 0
	for i := range 2000 {
		client := fmt.Sprintf("10.%d.%d.1", i/250, i%250)
		_, served := from(t, srv.URL, client)
		assigned[client] = served
		if served == "canary" {
			canaries++
		}
	}
	if canaries < 440 || canaries > 560 {
		t.Fatalf("%d of 2000 clients on the canary, want about 500", canaries)
	}
	for client, version := range assigned {
		if _, again := from(t, srv.URL, client); again != version {
			t.Fatalf("%s moved from %s to %s", client, version, again)
		}
		break
	}
	if err := g.Apply(canaryConfig(t, cfg.Pools[0].Dimensions[0].Address, cfg.Pools[0].Dimensions[1].Address, 0.25, "r2")); err != nil {
		t.Fatal(err)
	}
	moved := 0
	for client, version := range assigned {
		if _, now := from(t, srv.URL, client); now != version {
			moved++
		}
	}
	if moved < 200 {
		t.Fatalf("a new rollout moved only %d clients", moved)
	}
}

func TestRetriesFromACanaryLandOnStable(t *testing.T) {
	broken := dim(t, "canary", func(b *dimension.Behavior) { b.ErrorRate = 1 })
	g, srv := manual(t, canaryConfig(t, dim(t, "stable", nil), broken, 1, "r1"))
	for i := range 5 {
		code, served := from(t, srv.URL, fmt.Sprintf("10.0.0.%d", i+1))
		if code != 200 || served != "stable" {
			t.Fatalf("got %d from %s", code, served)
		}
	}
	snap := g.Tick(1)
	v := snap.Routes["all"].Versions
	if v[canarySlot].Attempts != 5 || v[canarySlot].Errors != 5 || v[stableSlot].Attempts != 5 || v[stableSlot].Errors != 0 {
		t.Fatalf("version counts %+v", v)
	}
	if snap.Routes["all"].Requests != 5 {
		t.Fatalf("requests %d", snap.Routes["all"].Requests)
	}
	dims := map[string]DimensionSnapshot{}
	for _, d := range snap.Dimensions {
		dims[d.ID] = d
	}
	if dims["canary"].Errors != 5 || dims["stable"].Attempts != 5 || dims["stable"].P50Ms <= 0 {
		t.Fatalf("dimensions %+v", snap.Dimensions)
	}
	if again := g.Tick(2); again.Routes["all"].Versions[canarySlot].Attempts != 0 {
		t.Fatal("a tick did not reset the counters")
	}
}

func TestBrakeTakesTheCanaryOutLocally(t *testing.T) {
	stable := dim(t, "stable", nil)
	broken := dim(t, "canary", func(b *dimension.Behavior) { b.ErrorRate = 1 })
	cfg := canaryConfig(t, stable, broken, 1, "r1")
	cfg.Pools[0].Outlier.Disabled = true
	g, srv := manual(t, cfg)
	for i := range 20 {
		from(t, srv.URL, fmt.Sprintf("10.0.0.%d", i+1))
	}
	if !g.Tick(1).Routes["all"].Brake {
		t.Fatal("brake not engaged after 20 failed canary attempts")
	}
	for i := range 10 {
		from(t, srv.URL, fmt.Sprintf("10.0.1.%d", i+1))
	}
	if v := g.Tick(2).Routes["all"].Versions; v[canarySlot].Attempts != 0 {
		t.Fatalf("canary still got %d attempts with the brake on", v[canarySlot].Attempts)
	}
	next := canaryConfig(t, stable, broken, 1, "r2")
	next.Pools[0].Outlier.Disabled = true
	if err := g.Apply(next); err != nil {
		t.Fatal(err)
	}
	from(t, srv.URL, "10.0.2.1")
	if snap := g.Tick(3); snap.Routes["all"].Brake || snap.Routes["all"].Versions[canarySlot].Attempts != 1 {
		t.Fatalf("a new rollout did not release the brake: %+v", snap.Routes["all"])
	}
}

func TestSlowEventsUseTheStableP95(t *testing.T) {
	slow := dim(t, "canary", func(b *dimension.Behavior) { b.DelayMs = 30 })
	cfg := canaryConfig(t, dim(t, "stable", nil), slow, 0, "r1")
	g, srv := manual(t, cfg)
	for i := range 150 {
		from(t, srv.URL, fmt.Sprintf("10.0.%d.%d", i/200, i%200+1))
	}
	if th := g.Tick(1).Routes["all"]; th.SlowThresholdMs <= 0 || th.SlowThresholdMs > 25 {
		t.Fatalf("threshold %.2fms after 150 fast stable requests", th.SlowThresholdMs)
	}
	if err := g.Apply(canaryConfig(t, cfg.Pools[0].Dimensions[0].Address, slow, 1, "r1")); err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		from(t, srv.URL, fmt.Sprintf("10.1.0.%d", i+1))
	}
	if v := g.Tick(2).Routes["all"].Versions; v[canarySlot].Slow != 10 || v[canarySlot].Attempts != 10 {
		t.Fatalf("canary counts %+v", v[canarySlot])
	}
}

func TestVersionsMustExistInThePool(t *testing.T) {
	_, err := config.Parse([]byte(`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p", "versions": {"stable": "v1", "canary": "v9", "weight": 0.1}}],
		"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:1"}]}]}`))
	if err == nil {
		t.Fatal("a canary version with no dimensions was accepted")
	}
}
