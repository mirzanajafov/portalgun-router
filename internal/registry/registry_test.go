package registry

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeRegistry struct {
	mu         sync.Mutex
	index      uint64
	changed    chan struct{}
	instances  map[string]Instance
	leases     map[uint64]string
	renewed    map[uint64]time.Time
	ttl        time.Duration
	keepalives int
}

func newFakeRegistry(ttl time.Duration) *fakeRegistry {
	return &fakeRegistry{index: 1, changed: make(chan struct{}), instances: map[string]Instance{}, leases: map[uint64]string{}, renewed: map[uint64]time.Time{}, ttl: ttl}
}

func (f *fakeRegistry) bumpLocked() {
	f.index++
	close(f.changed)
	f.changed = make(chan struct{})
}

func (f *fakeRegistry) add(inst Instance) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bumpLocked()
	f.instances[inst.Name] = inst
	f.leases[f.index] = inst.Name
	f.renewed[f.index] = time.Now()
	return f.index
}

func (f *fakeRegistry) dropLocked(lease uint64) {
	delete(f.instances, f.leases[lease])
	delete(f.leases, lease)
	delete(f.renewed, lease)
	f.bumpLocked()
}

func (f *fakeRegistry) drop(lease uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dropLocked(lease)
}

func (f *fakeRegistry) snapshot() ([]Instance, uint64, int, []uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	list := slices.SortedFunc(maps.Values(f.instances), func(a, b Instance) int { return strings.Compare(a.Name, b.Name) })
	return list, f.index, f.keepalives, slices.Sorted(maps.Keys(f.leases))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (f *fakeRegistry) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/registry/{service}", func(w http.ResponseWriter, r *http.Request) {
		index, _ := strconv.ParseUint(r.URL.Query().Get("index"), 10, 64)
		wait, _ := time.ParseDuration(r.URL.Query().Get("wait"))
		f.mu.Lock()
		current, changed := f.index, f.changed
		f.mu.Unlock()
		if index == current {
			select {
			case <-changed:
			case <-time.After(wait):
			case <-r.Context().Done():
			}
		}
		list, at, _, _ := f.snapshot()
		writeJSON(w, http.StatusOK, map[string]any{"index": at, "instances": list})
	})
	mux.HandleFunc("PUT /v1/registry/{service}/{instance}", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Addr string            `json:"addr"`
			Meta map[string]string `json:"meta"`
		}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &in)
		lease := f.add(Instance{Name: r.PathValue("instance"), Addr: in.Addr, Meta: in.Meta})
		writeJSON(w, http.StatusOK, map[string]any{"lease": lease, "ttl": f.ttl.String()})
	})
	mux.HandleFunc("POST /v1/leases/{id}/keepalive", func(w http.ResponseWriter, r *http.Request) {
		id, _ := strconv.ParseUint(r.PathValue("id"), 10, 64)
		f.mu.Lock()
		defer f.mu.Unlock()
		at, ok := f.renewed[id]
		if ok && time.Since(at) > f.ttl {
			f.dropLocked(id)
			ok = false
		}
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "gone", "reason": "LEASE_NOT_FOUND"})
			return
		}
		f.renewed[id] = time.Now()
		f.keepalives++
		writeJSON(w, http.StatusOK, map[string]string{"ttl": f.ttl.String()})
	})
	mux.HandleFunc("DELETE /v1/leases/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, _ := strconv.ParseUint(r.PathValue("id"), 10, 64)
		f.drop(id)
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

type mode int32

const (
	answering mode = iota
	failing
	hanging
)

type node struct {
	mode  atomic.Int32
	delay atomic.Int64
	next  http.Handler
}

func (n *node) set(m mode) { n.mode.Store(int32(m)) }

func (n *node) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(n.delay.Load()))
	switch mode(n.mode.Load()) {
	case failing:
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no leader"})
	case hanging:
		<-r.Context().Done()
	default:
		n.next.ServeHTTP(w, r)
	}
}

func cluster(t *testing.T, f *fakeRegistry, n int) (*Client, []*node) {
	t.Helper()
	var nodes []*node
	var endpoints []string
	for range n {
		nd := &node{next: f.handler()}
		srv := httptest.NewServer(nd)
		t.Cleanup(srv.Close)
		nodes = append(nodes, nd)
		endpoints = append(endpoints, srv.URL)
	}
	c, err := New(endpoints)
	if err != nil {
		t.Fatal(err)
	}
	return c, nodes
}

func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func names(list []Instance) string {
	var out []string
	for _, in := range list {
		out = append(out, in.Name)
	}
	return strings.Join(out, " ")
}

func next(t *testing.T, lists <-chan []Instance, within time.Duration) []Instance {
	t.Helper()
	select {
	case l := <-lists:
		return l
	case <-time.After(within):
		t.Fatalf("no list within %v", within)
		return nil
	}
}

func TestTheWatchSendsEachNewListAndNothingWhileTheRegistryIsDown(t *testing.T) {
	f := newFakeRegistry(time.Minute)
	f.add(Instance{Name: "a", Addr: "a:80"})
	c, nodes := cluster(t, f, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lists := c.Watch(ctx, "dimensions", 100*time.Millisecond, quiet())
	if got := names(next(t, lists, time.Second)); got != "a" {
		t.Fatalf("first list %q", got)
	}
	f.add(Instance{Name: "b", Addr: "b:80"})
	if got := names(next(t, lists, time.Second)); got != "a b" {
		t.Fatalf("after b registered: %q", got)
	}
	select {
	case l := <-lists:
		t.Fatalf("sent %q again with nothing changed", names(l))
	case <-time.After(400 * time.Millisecond):
	}
	for _, n := range nodes {
		n.set(failing)
	}
	select {
	case l := <-lists:
		t.Fatalf("sent %q while no node answered", names(l))
	case <-time.After(time.Second):
	}
	f.add(Instance{Name: "c", Addr: "c:80"})
	nodes[1].set(answering)
	if got := names(next(t, lists, 5*time.Second)); got != "a b c" {
		t.Fatalf("after the registry came back: %q", got)
	}
}

func TestARegistrationIsRenewedRestoredAndRevoked(t *testing.T) {
	f := newFakeRegistry(300 * time.Millisecond)
	c, _ := cluster(t, f, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		c.Keep(ctx, "dimensions", versioned("e-7", "dim-e:8080", "v1"), 300*time.Millisecond, quiet())
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	var leases []uint64
	for time.Now().Before(deadline) {
		var keepalives int
		_, _, keepalives, leases = f.snapshot()
		if keepalives >= 3 && len(leases) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(leases) != 1 {
		t.Fatalf("leases %v", leases)
	}
	f.drop(leases[0])
	for time.Now().Before(deadline) {
		if list, _, _, now := f.snapshot(); len(now) == 1 && now[0] != leases[0] && names(list) == "e-7" && list[0].Meta["version"] == "v1" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if list, _, _, now := f.snapshot(); len(now) != 1 || now[0] == leases[0] || names(list) != "e-7" || list[0].Meta["version"] != "v1" {
		t.Fatalf("after the lease was lost: %v on %v", list, now)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Keep did not return after its context ended")
	}
	if list, _, _, now := f.snapshot(); len(list) != 0 || len(now) != 0 {
		t.Fatalf("still registered after the context ended: %v on %v", list, now)
	}
}

func TestKeepalivesAndTheFinalRevokeMoveOffANodeThatHangs(t *testing.T) {
	f := newFakeRegistry(1200 * time.Millisecond)
	c, nodes := cluster(t, f, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		c.Keep(ctx, "dimensions", versioned("e-7", "dim-e:8080", "v1"), 1200*time.Millisecond, quiet())
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, _, leases := f.snapshot(); len(leases) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, _, _, before := f.snapshot()
	for _, hung := range []int{0, 1} {
		_, _, renewed, _ := f.snapshot()
		nodes[hung].set(hanging)
		nodes[1-hung].set(answering)
		time.Sleep(2500 * time.Millisecond)
		_, _, now, after := f.snapshot()
		if len(before) != 1 || !slices.Equal(before, after) {
			t.Fatalf("lease %v became %v while node %d hung", before, after, hung)
		}
		if now-renewed < 2 {
			t.Fatalf("%d keepalives in 2.5 s with a 1.2 s lease while node %d hung", now-renewed, hung)
		}
	}
	nodes[0].set(hanging)
	nodes[1].set(answering)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Keep did not return within its stop budget")
	}
	if list, _, _, leases := f.snapshot(); len(list) != 0 || len(leases) != 0 {
		t.Fatalf("still registered after stopping with the current node hung: %v on %v", list, leases)
	}
}

func TestAnAnswerWithoutALeaseOrATTLIsNotARegistration(t *testing.T) {
	for _, half := range []map[string]any{{"lease": 63}, {"ttl": "10s"}} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusOK, half)
		}))
		c, err := New([]string{srv.URL})
		if err != nil {
			t.Fatal(err)
		}
		if l, err := c.Register(context.Background(), "dimensions", versioned("e-7", "dim-e:8080", "v1"), 10*time.Second); err == nil || !strings.Contains(err.Error(), srv.URL) {
			t.Errorf("%v became %+v, %v", half, l, err)
		}
		srv.Close()
	}
}

func TestARegistrationInFlightWhenTheDimensionStopsIsRevoked(t *testing.T) {
	f := newFakeRegistry(10 * time.Second)
	c, nodes := cluster(t, f, 1)
	nodes[0].delay.Store(int64(300 * time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.Keep(ctx, "dimensions", versioned("e-7", "dim-e:8080", "v1"), 10*time.Second, quiet())
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Keep did not return after its context ended")
	}
	time.Sleep(500 * time.Millisecond)
	if list, _, _, leases := f.snapshot(); len(list) != 0 || len(leases) != 0 {
		t.Fatalf("a registration that landed after the stop is still there: %v on %v", list, leases)
	}
}
