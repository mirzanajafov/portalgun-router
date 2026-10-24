package controller

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOnlyTheTrustedProxyNamesTheVisitor(t *testing.T) {
	cases := []struct {
		name, proxy, peer, want string
	}{
		{"no proxy is trusted", "", "192.0.2.1:4000", "192.0.2.1"},
		{"the proxy by address", "192.0.2.1", "192.0.2.1:4000", "9.9.9.9"},
		{"the proxy by name", "localhost", "127.0.0.1:4000", "9.9.9.9"},
		{"any other caller", "192.0.2.7", "192.0.2.1:4000", "192.0.2.1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/chaos", nil)
			req.RemoteAddr = c.peer
			req.Header.Set("X-Forwarded-For", "6.6.6.6, 9.9.9.9")
			p := TrustedProxy{Host: c.proxy, Hops: 1, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			if got := p.Client(req); got != c.want {
				t.Fatalf("counted as %s, want %s", got, c.want)
			}
		})
	}
}

func TestACallerBesideTheProxyCannotRotatePastTheVisitorLimit(t *testing.T) {
	r := newRig(t)
	api := NewAPI(r.cfgs, r.engine, r.store, NewHub(), "s3cret")
	counter := &fakeCounter{hits: map[string]int64{}}
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	api.LetVisitorsRollOut(counter, RolloutLimits{PerHour: 1}, TrustedProxy{Host: "192.0.2.1", Hops: 1, Logger: discard}, discard)
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	post := func(path, auth, forwardedFor, body string) int {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(body))
		req.Header.Set("X-Forwarded-For", forwardedFor)
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
	start := `{"route":"api","canary":"v2"}`
	if got := post("/api/rollouts", "", "9.9.9.1", start); got != http.StatusCreated {
		t.Fatalf("first start: %d", got)
	}
	if got := post("/api/rollouts/abort", "Bearer s3cret", "", `{}`); got != http.StatusOK {
		t.Fatalf("admin abort: %d", got)
	}
	r.now = r.now.Add(time.Minute)
	if got := post("/api/rollouts", "", "9.9.9.2", start); got != http.StatusTooManyRequests {
		t.Fatalf("second start under a new X-Forwarded-For: %d, want the limit", got)
	}
	if n := counter.hits["rollout:ip:127.0.0.1"]; n != 2 {
		t.Fatalf("counts %v, want both starts on the caller's own address", counter.hits)
	}
}
