package health

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const sec = int64(time.Second)

func outlierConfig() OutlierConfig {
	return OutlierConfig{
		Consecutive:    5,
		Window:         50,
		MinRequests:    20,
		FailureRatio:   0.5,
		BaseEjection:   5 * time.Second,
		MaxEjection:    time.Minute,
		HalfOpenTrials: 3,
	}
}

type recorder struct {
	mu sync.Mutex
	ts []Transition
}

func (r *recorder) add(t Transition) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ts = append(r.ts, t)
}

func TestConsecutiveFailuresEject(t *testing.T) {
	var rec recorder
	o := NewOutlier(outlierConfig(), rec.add)
	for i := range 4 {
		o.Record(int64(i), true, false, false)
	}
	if o.State() != Closed {
		t.Fatal("ejected before the fifth failure")
	}
	o.Record(10, true, false, false)
	if o.State() != Ejected || o.Eligible(11) || o.Until() != 10+5*sec {
		t.Fatalf("state %v until %d", o.State(), o.Until())
	}
	if len(rec.ts) != 1 || rec.ts[0].Reason != "consecutive failures" {
		t.Fatalf("transitions %+v", rec.ts)
	}
}

func TestSuccessBreaksTheStreak(t *testing.T) {
	o := NewOutlier(outlierConfig(), nil)
	for i := range 19 {
		o.Record(int64(i), i%3 != 0, false, false)
		if o.State() != Closed {
			t.Fatalf("ejected after %d attempts with a 2/3 failure rate, no streak of five and fewer than 20 samples", i+1)
		}
	}
}

func TestFailureRatioEjects(t *testing.T) {
	o := NewOutlier(outlierConfig(), nil)
	n := 0
	for o.State() == Closed && n < 100 {
		o.Record(int64(n), n%2 == 0, false, false)
		n++
	}
	if o.State() != Ejected || n != 20 {
		t.Fatalf("ejected after %d attempts, want 20 (the minimum sample at 50%%)", n)
	}
}

func TestRefusedEjectsWhenConfigured(t *testing.T) {
	cfg := outlierConfig()
	cfg.EjectOnRefused = true
	o := NewOutlier(cfg, nil)
	o.Record(0, true, true, false)
	if o.State() != Ejected {
		t.Fatal("a refused connection did not eject")
	}
	plain := NewOutlier(outlierConfig(), nil)
	plain.Record(0, true, true, false)
	if plain.State() != Closed {
		t.Fatal("refusal ejected without the option")
	}
}

func eject(o *Outlier, at int64) {
	for range 5 {
		o.Record(at, true, false, false)
	}
}

func TestHalfOpenLimitsTrialsAndCloses(t *testing.T) {
	var rec recorder
	o := NewOutlier(outlierConfig(), rec.add)
	eject(o, 0)
	if !o.Eligible(5 * sec) {
		t.Fatal("not eligible after the ejection ended")
	}
	if o.State() != HalfOpen {
		t.Fatalf("state %v", o.State())
	}
	for range 3 {
		if !o.Begin() {
			t.Fatal("trial not granted")
		}
	}
	if o.Eligible(5 * sec) {
		t.Fatal("eligible with every trial slot taken")
	}
	for range 3 {
		o.Record(6*sec, false, false, true)
	}
	if o.State() != Closed {
		t.Fatalf("state %v after three good trials", o.State())
	}
	got := []State{}
	for _, tr := range rec.ts {
		got = append(got, tr.To)
	}
	if len(got) != 3 || got[0] != Ejected || got[1] != HalfOpen || got[2] != Closed {
		t.Fatalf("transitions %v", got)
	}
}

func TestEjectionBacksOffAndCaps(t *testing.T) {
	o := NewOutlier(outlierConfig(), nil)
	now := int64(0)
	var lengths []time.Duration
	for range 6 {
		eject(o, now)
		lengths = append(lengths, time.Duration(o.Until()-now))
		now = o.Until()
		o.Eligible(now)
		o.Begin()
		o.Record(now, true, false, true)
		lengths = append(lengths, time.Duration(o.Until()-now))
		now = o.Until()
		o.Eligible(now)
		for range 3 {
			o.Begin()
			o.Record(now, false, false, true)
		}
	}
	want := []time.Duration{5, 10, 20, 40, 60, 60}
	for i, w := range want {
		if lengths[i] != w*time.Second {
			t.Fatalf("ejection %d lasted %v, want %v (all: %v)", i+1, lengths[i], w*time.Second, lengths)
		}
	}
}

func TestOldEjectionsAreForgotten(t *testing.T) {
	o := NewOutlier(outlierConfig(), nil)
	eject(o, 0)
	o.Eligible(5 * sec)
	for range 3 {
		o.Begin()
		o.Record(5*sec, false, false, true)
	}
	later := 10 * 60 * sec
	eject(o, later)
	if got := time.Duration(o.Until() - later); got != 5*time.Second {
		t.Fatalf("an ejection ten minutes after the last one lasted %v, want the base 5s", got)
	}
}

func TestBudgetAllowsTheFloorThenTheRatio(t *testing.T) {
	b := NewBudget(0.2, 1)
	allowed := 0
	for range 50 {
		if b.TryRetry(0) {
			allowed++
		}
	}
	if allowed != 10 {
		t.Fatalf("allowed %d retries with no traffic, want the floor of 1/s over 10s", allowed)
	}
	for range 1000 {
		b.Request(sec)
	}
	for range 500 {
		if b.TryRetry(sec) {
			allowed++
		}
	}
	if allowed != 200 {
		t.Fatalf("allowed %d retries after 1000 requests, want 20%%", allowed)
	}
	if !b.TryRetry(20 * sec) {
		t.Fatal("the window never moved on")
	}
}

func TestCheckerFollowsTheProbes(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !healthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer srv.Close()
	var changes atomic.Int32
	c := StartChecker(srv.Listener.Addr().String(), ProbeConfig{
		Path: "/healthz", Interval: 20 * time.Millisecond, Timeout: 50 * time.Millisecond, UnhealthyAfter: 2, HealthyAfter: 2,
	}, func(bool) { changes.Add(1) })
	defer c.Stop()

	waitFor(t, func() bool { return c.Healthy() })
	healthy.Store(false)
	waitFor(t, func() bool { return !c.Healthy() })
	healthy.Store(true)
	waitFor(t, func() bool { return c.Healthy() })
	if changes.Load() != 2 {
		t.Fatalf("%d changes, want 2", changes.Load())
	}
	c.Stop()
	c.Stop()
}

func TestCheckerMarksADeadAddressUnhealthy(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	addr := srv.Listener.Addr().String()
	srv.Close()
	c := StartChecker(addr, ProbeConfig{Path: "/healthz", Interval: 20 * time.Millisecond, Timeout: 50 * time.Millisecond, UnhealthyAfter: 1, HealthyAfter: 1}, nil)
	defer c.Stop()
	waitFor(t, func() bool { return !c.Healthy() })
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
