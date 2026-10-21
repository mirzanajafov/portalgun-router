package controller

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/registry"
)

func dimension(name, addr, version string) registry.Instance {
	return registry.Instance{Name: name, Addr: addr, Meta: map[string]string{"version": version}}
}

type discoveryRig struct {
	*rig
	d    *Discovery
	logs *logSink
}

func discovering(t *testing.T, pool string) *discoveryRig {
	t.Helper()
	r := newRig(t)
	logs := &logSink{}
	d := NewDiscovery(pool, r.cfgs, slog.New(slog.NewTextHandler(logs, nil)))
	d.retry = 20 * time.Millisecond
	return &discoveryRig{rig: r, d: d, logs: logs}
}

func (r *discoveryRig) send(t *testing.T, lists ...[]registry.Instance) {
	t.Helper()
	ch := make(chan []registry.Instance)
	done := make(chan struct{})
	go func() {
		r.d.Run(context.Background(), ch)
		close(done)
	}()
	for _, l := range lists {
		ch <- l
	}
	close(ch)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its lists ended")
	}
}

func (r *discoveryRig) running(t *testing.T) chan<- []registry.Instance {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	lists := make(chan []registry.Instance)
	done := make(chan struct{})
	go func() {
		r.d.Run(ctx, lists)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return lists
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("never saw %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
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
	return n, last.Author, last.Reason, slices.SortedFunc(slices.Values(poolNamed(cfg, "p").Dimensions), byDimensionID)
}

func TestTheRegistryListBecomesThePoolAndTheReasonSaysWhoJoinedAndLeft(t *testing.T) {
	r := discovering(t, "p")
	r.send(t, []registry.Instance{dimension("s1", "s1:8080", "v1"), dimension("s2", "s2:8080", "v1")})
	n, author, reason, dims := r.latest(t)
	if n != 2 || author != "registry" || reason != "s2 joined, c1 left" {
		t.Fatalf("version %d by %s: %q", n, author, reason)
	}
	want := []config.Dimension{
		{ID: "s1", Address: "s1:8080", Version: "v1", MaxInflight: config.DefaultMaxInflight},
		{ID: "s2", Address: "s2:8080", Version: "v1", MaxInflight: config.DefaultMaxInflight},
	}
	if !slices.Equal(dims, want) {
		t.Fatalf("pool %+v", dims)
	}
}

func TestTheSameInstancesInAnotherOrderWriteNothing(t *testing.T) {
	r := discovering(t, "p")
	r.send(t, []registry.Instance{dimension("c1", "c1:8080", "v2"), dimension("s1", "s1:8080", "v1")})
	if n, _, _, _ := r.latest(t); n != 1 {
		t.Fatalf("version %d written for the pool as it is", n)
	}
}

func TestAMovedAddressIsAChange(t *testing.T) {
	r := discovering(t, "p")
	r.send(t, []registry.Instance{dimension("s1", "s1:9090", "v1"), dimension("c1", "c1:8080", "v2")})
	if n, _, reason, _ := r.latest(t); n != 2 || reason != "s1 changed" {
		t.Fatalf("version %d, %q", n, reason)
	}
}

func TestMaxInflightComesFromTheRegistrationOrStaysWithTheDimension(t *testing.T) {
	r := discovering(t, "p")
	if _, err := r.cfgs.Update(context.Background(), "admin", "caps", func(c *config.Config) error {
		c.Pools[0].Dimensions[0].MaxInflight = 64
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	announced := dimension("s2", "s2:8080", "v1")
	announced.Meta["maxInflight"] = "256"
	r.send(t, []registry.Instance{dimension("s1", "s1:8080", "v1"), announced, dimension("s3", "s3:8080", "v1")})
	_, _, _, dims := r.latest(t)
	got := map[string]int{}
	for _, d := range dims {
		got[d.ID] = d.MaxInflight
	}
	if got["s1"] != 64 || got["s2"] != 256 || got["s3"] != config.DefaultMaxInflight {
		t.Fatalf("maxInflight %v, want s1 kept at 64, s2 at its announced 256 and s3 at the default", got)
	}
}

func TestAnEmptyListKeepsThePoolAndIsLoggedOnce(t *testing.T) {
	r := discovering(t, "p")
	r.send(t, []registry.Instance{}, []registry.Instance{})
	n, _, _, dims := r.latest(t)
	if n != 1 || len(dims) != 2 {
		t.Fatalf("version %d with %+v", n, dims)
	}
	if got := strings.Count(r.logs.String(), "not applied"); got != 1 {
		t.Fatalf("logged %d times:\n%s", got, r.logs.String())
	}
}

func TestAnInstanceWithoutAPortIsLeftOutAndTheRestApplied(t *testing.T) {
	r := discovering(t, "p")
	r.send(t, []registry.Instance{dimension("s1", "s1:8080", "v1"), dimension("c1", "c1:8080", "v2"), dimension("bad", "bad", "v1")})
	if n, _, _, _ := r.latest(t); n != 1 {
		t.Fatalf("the usable instances are the pool already, but version %d was written", n)
	}
	r.send(t, []registry.Instance{dimension("s1", "s1:8080", "v1"), dimension("bad", "bad", "v1")})
	if n, _, reason, _ := r.latest(t); n != 2 || reason != "c1 left" {
		t.Fatalf("version %d, %q", n, reason)
	}
	if !strings.Contains(r.logs.String(), `bad=\"bad\"`) {
		t.Fatalf("the instance left out was not logged:\n%s", r.logs.String())
	}
}

func TestAListWithoutTheCanaryIsAppliedOnARetryOnceTheRolloutIsOver(t *testing.T) {
	r := discovering(t, "p")
	ctx := context.Background()
	if _, err := r.cfgs.Update(ctx, "admin", "rollout", func(c *config.Config) error {
		c.Routes[0].Versions = config.Versions{Stable: "v1", Canary: "v2", Weight: 0.1, Rollout: "ro-1"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r.running(t) <- []registry.Instance{dimension("s1", "s1:8080", "v1")}
	eventually(t, "the list refused during the rollout", func() bool { return strings.Contains(r.logs.String(), "not applied") })
	if n, _, _, dims := r.latest(t); n != 2 || len(dims) != 2 {
		t.Fatalf("applied during the rollout: version %d with %+v", n, dims)
	}
	if _, err := r.cfgs.Update(ctx, "controller", "rolled back", func(c *config.Config) error {
		c.Routes[0].Versions = config.Versions{Stable: "v1"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the list applied on a retry after the rollback", func() bool {
		n, author, reason, dims := r.latest(t)
		return n == 4 && author == "registry" && reason == "c1 left" && len(dims) == 1
	})
}

func TestAnAppliedListIsNotPutBackOverAnAdminChange(t *testing.T) {
	r := discovering(t, "p")
	ctx := context.Background()
	r.running(t) <- []registry.Instance{dimension("s1", "s1:8080", "v1"), dimension("c1", "c1:8080", "v2"), dimension("s2", "s2:8080", "v1")}
	eventually(t, "s2 joining", func() bool {
		n, _, reason, _ := r.latest(t)
		return n == 2 && reason == "s2 joined"
	})
	if _, err := r.cfgs.Update(ctx, "admin", "drain s2", func(c *config.Config) error {
		c.Pools[0].Dimensions = slices.DeleteFunc(c.Pools[0].Dimensions, func(d config.Dimension) bool { return d.ID == "s2" })
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * r.d.retry)
	if n, author, _, _ := r.latest(t); n != 3 || author != "admin" {
		t.Fatalf("version %d by %s, want the admin's change to stand until the registry says something new", n, author)
	}
}

func TestAPoolTheConfigDoesNotHaveIsLoggedOnceAndNothingWritten(t *testing.T) {
	r := discovering(t, "dimensions")
	r.send(t, []registry.Instance{dimension("s1", "s1:8080", "v1")}, []registry.Instance{dimension("s2", "s2:8080", "v1")})
	if n, _, _, _ := r.latest(t); n != 1 {
		t.Fatalf("version %d written for a pool the config doesn't have", n)
	}
	if got := strings.Count(r.logs.String(), "no pool"); got != 1 {
		t.Fatalf("logged %d times:\n%s", got, r.logs.String())
	}
}
