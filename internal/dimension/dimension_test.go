package dimension

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

func start(t *testing.T, b Behavior) (*Server, *httptest.Server) {
	t.Helper()
	s, err := New("c-137", "v1", b)
	if err != nil {
		t.Fatal(err)
	}
	return s, serve(t, s)
}

func serve(t *testing.T, s *Server) *httptest.Server {
	t.Helper()
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})
	control := httptest.NewServer(s.ControlHandler())
	t.Cleanup(control.Close)
	return control
}

func get(t *testing.T, url string) (int, http.Header, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, body
}

func patch(t *testing.T, control *httptest.Server, body string) (int, View) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPatch, control.URL+"/state", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var v View
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
			t.Fatal(err)
		}
	}
	return resp.StatusCode, v
}

func TestServesBodyAndHeaders(t *testing.T) {
	b := DefaultBehavior()
	b.BodyBytes = 300
	s, _ := start(t, b)

	status, h, body := get(t, "http://"+s.Addr()+"/anything")
	if status != http.StatusOK || len(body) != 300 {
		t.Fatalf("got %d with %d bytes, want 200 with 300", status, len(body))
	}
	if h.Get("Content-Length") != "300" || h.Get(IDHeader) != "c-137" || h.Get(VersionHeader) != "v1" {
		t.Fatalf("unexpected headers %v", h)
	}
}

func TestInjectsErrorsWithTheConfiguredStatus(t *testing.T) {
	b := DefaultBehavior()
	b.ErrorRate = 1
	b.ErrorStatus = http.StatusBadGateway
	s, _ := start(t, b)

	if status, _, _ := get(t, "http://"+s.Addr()+"/"); status != http.StatusBadGateway {
		t.Fatalf("got %d, want 502", status)
	}
}

func TestErrorRateIsRespected(t *testing.T) {
	b := DefaultBehavior()
	b.ErrorRate = 0.3
	s, err := New("c-137", "v1", b)
	if err != nil {
		t.Fatal(err)
	}
	const n = 4000
	errs := 0
	for range n {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
		if w.Code == http.StatusServiceUnavailable {
			errs++
		}
	}
	if share := float64(errs) / n; math.Abs(share-0.3) > 0.04 {
		t.Fatalf("error share %.3f, want about 0.3", share)
	}
}

func TestFixedDelay(t *testing.T) {
	b := DefaultBehavior()
	b.DelayMs = 30
	s, err := New("c-137", "v1", b)
	if err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if elapsed := time.Since(began); elapsed < 30*time.Millisecond {
		t.Fatalf("answered after %v, want at least 30ms", elapsed)
	}
}

func TestLognormalDelayHasTheConfiguredMedian(t *testing.T) {
	b := DefaultBehavior()
	b.DelayMs = 10
	b.DelayDist = "lognormal"
	b.DelaySigma = 0.5
	st := newState(b)
	samples := make([]time.Duration, 5001)
	for i := range samples {
		samples[i] = st.sampleDelay()
	}
	slices.Sort(samples)
	if median := samples[len(samples)/2]; median < 9*time.Millisecond || median > 11*time.Millisecond {
		t.Fatalf("median %v, want about 10ms", median)
	}
	if samples[len(samples)-1] < 20*time.Millisecond {
		t.Fatalf("max %v, want a tail above 20ms", samples[len(samples)-1])
	}
}

func TestHangHoldsUntilTheClientGivesUp(t *testing.T) {
	b := DefaultBehavior()
	b.Hang = true
	s, _ := start(t, b)

	client := &http.Client{Timeout: 100 * time.Millisecond}
	began := time.Now()
	if _, err := client.Get("http://" + s.Addr() + "/"); err == nil {
		t.Fatal("expected a timeout")
	}
	if elapsed := time.Since(began); elapsed > 2*time.Second {
		t.Fatalf("client waited %v", elapsed)
	}
}

func TestHealthzFollowsTheHealthyFlag(t *testing.T) {
	s, control := start(t, DefaultBehavior())
	if status, _, _ := get(t, "http://"+s.Addr()+"/healthz"); status != http.StatusOK {
		t.Fatalf("got %d, want 200", status)
	}
	if status, _ := patch(t, control, `{"healthy": false}`); status != http.StatusOK {
		t.Fatalf("patch got %d", status)
	}
	if status, _, _ := get(t, "http://"+s.Addr()+"/healthz"); status != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", status)
	}
	if status, _, _ := get(t, "http://"+s.Addr()+"/"); status != http.StatusOK {
		t.Fatalf("traffic got %d, want 200 while only the health check fails", status)
	}
}

func TestPatchRejectsInvalidInput(t *testing.T) {
	_, control := start(t, DefaultBehavior())
	for _, body := range []string{
		`{"errorRate": 1.5}`,
		`{"delayDist": "uniform"}`,
		`{"bodyBytes": -1}`,
		`{"errorStatus": 200}`,
		`{"colour": "green"}`,
		`not json`,
	} {
		if status, _ := patch(t, control, body); status != http.StatusBadRequest {
			t.Errorf("%s: got %d, want 400", body, status)
		}
	}
	status, v := patch(t, control, `{}`)
	if status != http.StatusOK || v.Behavior != DefaultBehavior() {
		t.Fatalf("rejected patches changed the state: %+v", v)
	}
}

func TestStopAndResumeListening(t *testing.T) {
	s, control := start(t, DefaultBehavior())
	addr := s.Addr()

	if status, v := patch(t, control, `{"listening": false}`); status != http.StatusOK || v.Listening {
		t.Fatalf("got %d listening=%v", status, v.Listening)
	}
	if _, err := http.Get("http://" + addr + "/"); err == nil {
		t.Fatal("expected the connection to be refused")
	}
	if status, v := patch(t, control, `{"listening": true}`); status != http.StatusOK || !v.Listening {
		t.Fatalf("got %d listening=%v", status, v.Listening)
	}
	if s.Addr() != addr {
		t.Fatalf("came back on %s, want %s", s.Addr(), addr)
	}
	if status, _, _ := get(t, "http://"+addr+"/"); status != http.StatusOK {
		t.Fatalf("got %d after resuming", status)
	}
}

func TestReadyOnlyWhileListeningAndHoldingTheLeaseItNeeds(t *testing.T) {
	ready := func(control *httptest.Server) int {
		status, _, _ := get(t, control.URL+"/ready")
		return status
	}
	_, unregistered := start(t, DefaultBehavior())
	if got := ready(unregistered); got != http.StatusOK {
		t.Fatalf("a dimension that doesn't register: %d", got)
	}
	s, err := New("c-137", "v1", DefaultBehavior())
	if err != nil {
		t.Fatal(err)
	}
	s.SetLeased(false)
	control := serve(t, s)
	if got := ready(control); got != http.StatusServiceUnavailable {
		t.Fatalf("before the first registration: %d", got)
	}
	s.SetLeased(true)
	if got := ready(control); got != http.StatusOK {
		t.Fatalf("registered: %d", got)
	}
	if status, _ := patch(t, control, `{"listening": false}`); status != http.StatusOK {
		t.Fatalf("stop listening: %d", status)
	}
	if got := ready(control); got != http.StatusServiceUnavailable {
		t.Fatalf("registered but not listening: %d", got)
	}
	if status, _ := patch(t, control, `{"listening": true}`); status != http.StatusOK {
		t.Fatalf("listen again: %d", status)
	}
	s.SetLeased(false)
	if status, _, body := get(t, control.URL+"/ready"); status != http.StatusServiceUnavailable || !strings.Contains(string(body), "not registered") {
		t.Fatalf("listening after the lease was lost: %d %q", status, body)
	}
}

func TestResetRestoresTheInitialBehavior(t *testing.T) {
	s, control := start(t, DefaultBehavior())
	if status, _ := patch(t, control, `{"errorRate": 1, "delayMs": 5, "listening": false}`); status != http.StatusOK {
		t.Fatalf("patch got %d", status)
	}
	resp, err := http.Post(control.URL+"/reset", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	var v View
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !v.Listening || v.Behavior != DefaultBehavior() {
		t.Fatalf("after reset: %+v", v)
	}
	if status, _, _ := get(t, "http://"+s.Addr()+"/"); status != http.StatusOK {
		t.Fatalf("got %d after reset", status)
	}
}

func TestValidateRejectsNaN(t *testing.T) {
	b := DefaultBehavior()
	b.ErrorRate = math.NaN()
	if b.Validate() == nil {
		t.Fatal("NaN error rate passed validation")
	}
	if _, err := New("", "v1", DefaultBehavior()); err == nil {
		t.Fatal("empty id passed")
	}
}
