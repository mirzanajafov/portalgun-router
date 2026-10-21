package controller

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/registry"
)

func dimension(name, addr, version string) registry.Instance {
	return registry.Instance{Name: name, Addr: addr, Meta: map[string]string{"version": version}}
}

func discovering(t *testing.T) (*rig, *Discovery, *bytes.Buffer) {
	t.Helper()
	r := newRig(t)
	var logs bytes.Buffer
	return r, NewDiscovery("p", r.cfgs, slog.New(slog.NewTextHandler(&logs, nil))), &logs
}

func (r *rig) latest(t *testing.T) (int, string, string, []config.Dimension) {
	t.Helper()
	r.store.mu.Lock()
	n := len(r.store.configs)
	last := r.store.configs[n-1]
	r.store.mu.Unlock()
	cfg, err := config.Parse(last.Body)
	if err != nil {
		t.Fatal(err)
	}
	return n, last.Author, last.Reason, poolNamed(cfg, "p").Dimensions
}

func TestTheRegistryListBecomesThePoolAndTheReasonSaysWhoJoinedAndLeft(t *testing.T) {
	r, d, _ := discovering(t)
	d.reconcile(context.Background(), []registry.Instance{dimension("s1", "s1:8080", "v1"), dimension("s2", "s2:8080", "v1")})
	n, author, reason, dims := r.latest(t)
	if n != 2 || author != "registry" || reason != "registry: s2 joined, c1 left" {
		t.Fatalf("version %d by %s: %q", n, author, reason)
	}
	want := []config.Dimension{
		{ID: "s1", Address: "s1:8080", Version: "v1", MaxInflight: config.DefaultMaxInflight},
		{ID: "s2", Address: "s2:8080", Version: "v1", MaxInflight: config.DefaultMaxInflight},
	}
	if !sameDimensions(dims, want) {
		t.Fatalf("pool %+v", dims)
	}
	d.reconcile(context.Background(), []registry.Instance{dimension("s2", "s2:8080", "v1"), dimension("s1", "s1:8080", "v1")})
	if n, _, _, _ := r.latest(t); n != 2 {
		t.Fatalf("the same instances in another order made version %d", n)
	}
	d.reconcile(context.Background(), []registry.Instance{dimension("s1", "s1:9090", "v1"), {Name: "s2", Addr: "s2:8080"}})
	if n, _, reason, _ := r.latest(t); n != 3 || reason != "registry: s1 changed" {
		t.Fatalf("a moved address: version %d, %q", n, reason)
	}
}

func TestAnEmptyListKeepsThePoolAndIsLoggedOnce(t *testing.T) {
	r, d, logs := discovering(t)
	d.reconcile(context.Background(), []registry.Instance{})
	d.reconcile(context.Background(), []registry.Instance{})
	n, _, _, dims := r.latest(t)
	if n != 1 || len(dims) != 2 {
		t.Fatalf("version %d with %+v", n, dims)
	}
	if got := strings.Count(logs.String(), "not applied"); got != 1 {
		t.Fatalf("logged %d times:\n%s", got, logs.String())
	}
}

func TestAListWithoutTheCanaryWaitsUntilTheRolloutIsOver(t *testing.T) {
	r, d, _ := discovering(t)
	ctx := context.Background()
	if _, err := r.cfgs.Update(ctx, "admin", "rollout", func(c *config.Config) error {
		c.Routes[0].Versions = config.Versions{Stable: "v1", Canary: "v2", Weight: 0.1, Rollout: "ro-1"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	stable := []registry.Instance{dimension("s1", "s1:8080", "v1")}
	d.reconcile(ctx, stable)
	if n, _, _, dims := r.latest(t); n != 2 || len(dims) != 2 {
		t.Fatalf("applied during the rollout: version %d with %+v", n, dims)
	}
	if _, err := r.cfgs.Update(ctx, "controller", "rolled back", func(c *config.Config) error {
		c.Routes[0].Versions = config.Versions{Stable: "v1"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	d.reconcile(ctx, stable)
	if n, author, reason, dims := r.latest(t); n != 4 || author != "registry" || reason != "registry: c1 left" || len(dims) != 1 {
		t.Fatalf("after the rollback: version %d by %s %q with %+v", n, author, reason, dims)
	}
}

func TestDiscoveryAppliesEachListItReceives(t *testing.T) {
	r, d, _ := discovering(t)
	lists := make(chan []registry.Instance)
	done := make(chan struct{})
	go func() {
		d.Run(context.Background(), lists)
		close(done)
	}()
	lists <- []registry.Instance{dimension("s1", "s1:8080", "v1"), dimension("c1", "c1:8080", "v2"), dimension("e7", "e7:8080", "v2")}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if n, _, reason, _ := r.latest(t); n == 2 && reason == "registry: e7 joined" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the list was not applied")
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(lists)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after its lists ended")
	}
}
