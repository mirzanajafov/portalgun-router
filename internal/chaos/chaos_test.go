package chaos

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/dimension"
)

type call struct {
	method string
	path   string
	body   map[string]any
}

type fakeToxiproxy struct {
	mu      sync.Mutex
	calls   []call
	enabled map[string]bool
	toxics  map[string]map[string]Toxic
}

func newFakeToxiproxy(t *testing.T) (*fakeToxiproxy, *Toxiproxy) {
	f := &fakeToxiproxy{enabled: map[string]bool{"dim-a": true}, toxics: map[string]map[string]Toxic{"dim-a": {}}}
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)
	return f, NewToxiproxy(srv.URL)
}

func (f *fakeToxiproxy) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, _ := io.ReadAll(r.Body)
	c := call{method: r.Method, path: r.URL.Path}
	if len(raw) > 0 && raw[0] == '{' {
		_ = json.Unmarshal(raw, &c.body)
	}
	f.calls = append(f.calls, c)
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/reset":
		for name := range f.enabled {
			f.enabled[name] = true
			f.toxics[name] = map[string]Toxic{}
		}
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPost && r.URL.Path == "/populate":
		w.WriteHeader(http.StatusCreated)
	case r.Method == http.MethodPost && len(parts) == 2 && parts[0] == "proxies":
		f.enabled[parts[1]] = c.body["enabled"].(bool)
	case r.Method == http.MethodPost && len(parts) == 3 && parts[2] == "toxics":
		var tx Toxic
		_ = json.Unmarshal(raw, &tx)
		if _, dup := f.toxics[parts[1]][tx.Name]; dup {
			http.Error(w, "toxic already exists", http.StatusConflict)
			return
		}
		f.toxics[parts[1]][tx.Name] = tx
	case r.Method == http.MethodDelete && len(parts) == 4:
		if _, ok := f.toxics[parts[1]][parts[3]]; !ok {
			http.Error(w, "toxic not found", http.StatusNotFound)
			return
		}
		delete(f.toxics[parts[1]], parts[3])
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "unexpected", http.StatusTeapot)
	}
}

func (f *fakeToxiproxy) state(proxy string) (bool, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var names []string
	for n := range f.toxics[proxy] {
		names = append(names, n)
	}
	return f.enabled[proxy], names
}

func (f *fakeToxiproxy) toxic(proxy, name string) Toxic {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.toxics[proxy][name]
}

func (f *fakeToxiproxy) toggles(proxy string) (downs, ups int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if c.method != http.MethodPost || c.path != "/proxies/"+proxy {
			continue
		}
		switch c.body["enabled"] {
		case true:
			ups++
		case false:
			downs++
		}
	}
	return downs, ups
}

func TestCrashDisablesAndRestoresTheProxy(t *testing.T) {
	fake, toxi := newFakeToxiproxy(t)
	in := NewInjector(toxi, nil)
	f := Fault{Mode: Crash, Target: "dim-a"}
	if err := in.Inject(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	if on, _ := fake.state("dim-a"); on {
		t.Fatal("proxy still enabled")
	}
	if err := in.Clear(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	if on, _ := fake.state("dim-a"); !on {
		t.Fatal("proxy not restored")
	}
	if downs, ups := fake.toggles("dim-a"); downs != 1 || ups != 1 {
		t.Fatalf("downs %d ups %d, want one of each", downs, ups)
	}
}

func TestToxicFaultsAddAndRemoveTheirToxics(t *testing.T) {
	for _, tc := range []struct {
		fault Fault
		want  int
		typ   string
	}{
		{Fault{Mode: Reset, Target: "dim-a"}, 1, "reset_peer"},
		{Fault{Mode: Blackhole, Target: "dim-a"}, 2, "timeout"},
		{Fault{Mode: Slow, Target: "dim-a", Latency: 500 * time.Millisecond}, 1, "latency"},
	} {
		fake, toxi := newFakeToxiproxy(t)
		in := NewInjector(toxi, nil)
		if err := in.Inject(context.Background(), tc.fault); err != nil {
			t.Fatalf("%s: %v", tc.fault.Mode, err)
		}
		_, names := fake.state("dim-a")
		if len(names) != tc.want {
			t.Fatalf("%s: toxics %v", tc.fault.Mode, names)
		}
		for _, n := range names {
			if got := fake.toxic("dim-a", n).Type; got != tc.typ {
				t.Fatalf("%s: toxic type %s", tc.fault.Mode, got)
			}
		}
		if err := in.Clear(context.Background(), tc.fault); err != nil {
			t.Fatalf("%s: %v", tc.fault.Mode, err)
		}
		if _, names := fake.state("dim-a"); len(names) != 0 {
			t.Fatalf("%s: toxics left %v", tc.fault.Mode, names)
		}
	}
}

func TestSlowSetsTheLatencyInMilliseconds(t *testing.T) {
	fake, toxi := newFakeToxiproxy(t)
	in := NewInjector(toxi, nil)
	if err := in.Inject(context.Background(), Fault{Mode: Slow, Target: "dim-a", Latency: 750 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if got := fake.toxic("dim-a", "slow-downstream").Attributes["latency"]; got != 750 {
		t.Fatalf("latency %d, want 750", got)
	}
}

func TestPartialPatchesTheDimension(t *testing.T) {
	s, err := dimension.New("a", "v1", dimension.DefaultBehavior())
	if err != nil {
		t.Fatal(err)
	}
	control := httptest.NewServer(s.ControlHandler("control-token"))
	t.Cleanup(control.Close)
	_, toxi := newFakeToxiproxy(t)
	in := NewInjector(toxi, map[string]*DimensionControl{"dim-a": {BaseURL: control.URL, Token: "control-token"}})

	f := Fault{Mode: Partial, Target: "dim-a", ErrorRate: 1}
	if err := in.Inject(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d with the fault on", w.Code)
	}
	if err := in.Clear(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("got %d after clearing", w.Code)
	}
	if err := in.Inject(context.Background(), Fault{Mode: Partial, Target: "dim-z", ErrorRate: 1}); err == nil {
		t.Fatal("unknown dimension accepted")
	}
	stale := NewInjector(toxi, map[string]*DimensionControl{"dim-a": {BaseURL: control.URL, Token: "old-token"}})
	if err := stale.Inject(context.Background(), f); !errors.Is(err, ErrControlToken) {
		t.Fatalf("a refused token came back as %v", err)
	}
}

func TestFlappingTogglesUntilCleared(t *testing.T) {
	fake, toxi := newFakeToxiproxy(t)
	in := NewInjector(toxi, nil)
	f := Fault{Mode: Flapping, Target: "dim-a", Period: 100 * time.Millisecond}
	if err := in.Inject(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	if err := in.Inject(context.Background(), f); err == nil {
		t.Fatal("second flapping on the same target accepted")
	}
	time.Sleep(350 * time.Millisecond)
	if downs, ups := fake.toggles("dim-a"); downs < 2 || ups < 1 {
		t.Fatalf("downs %d ups %d", downs, ups)
	}
	if err := in.Clear(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	if on, _ := fake.state("dim-a"); !on {
		t.Fatal("proxy left down after flapping")
	}
}

func TestValidateRejectsBadFaults(t *testing.T) {
	for _, f := range []Fault{
		{Mode: Crash},
		{Mode: "meteor", Target: "dim-a"},
		{Mode: Slow, Target: "dim-a"},
		{Mode: Partial, Target: "dim-a", ErrorRate: 1.5},
		{Mode: Flapping, Target: "dim-a", Period: time.Millisecond},
	} {
		if f.Validate() == nil {
			t.Errorf("%+v accepted", f)
		}
	}
}

func TestClearAllResetsEverything(t *testing.T) {
	fake, toxi := newFakeToxiproxy(t)
	in := NewInjector(toxi, nil)
	ctx := context.Background()
	if err := in.Inject(ctx, Fault{Mode: Slow, Target: "dim-a", Latency: time.Second}); err != nil {
		t.Fatal(err)
	}
	if err := in.Inject(ctx, Fault{Mode: Flapping, Target: "dim-a", Period: time.Second}); err != nil {
		t.Fatal(err)
	}
	if err := in.ClearAll(ctx); err != nil {
		t.Fatal(err)
	}
	if on, names := fake.state("dim-a"); !on || len(names) != 0 {
		t.Fatalf("enabled %v toxics %v after reset", on, names)
	}
}
