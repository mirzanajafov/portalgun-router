package controller

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	adminToken = "s3cret"
	proxyToken = "proxy-token"
)

func viaProxy(visitor string) http.Header {
	return http.Header{"X-Forwarded-For": {visitor}, proxyTokenHeader: {proxyToken}}
}

func asAdmin(visitor string) http.Header {
	h := viaProxy(visitor)
	h.Set("Authorization", "Bearer "+adminToken)
	return h
}

func send(t *testing.T, method, url, body string, header http.Header) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header = header
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp
}

func TestOnlyARequestWithTheProxyTokenNamesTheVisitor(t *testing.T) {
	cases := []struct {
		name, forwardedFor, configured, sent, want string
	}{
		{"no token configured", "6.6.6.6, 9.9.9.9", "", "", "192.0.2.1"},
		{"a token sent while none is configured", "6.6.6.6, 9.9.9.9", "", proxyToken, "192.0.2.1"},
		{"the proxy token", "6.6.6.6, 9.9.9.9", proxyToken, proxyToken, "9.9.9.9"},
		{"the proxy token without an address", "", proxyToken, proxyToken, "192.0.2.1"},
		{"the proxy token with something that isn't an address", "6.6.6.6, rotate-me", proxyToken, proxyToken, "192.0.2.1"},
		{"the proxy token with an IPv6 address spelled long", "2001:DB8:0:0::1", proxyToken, proxyToken, "2001:db8::1"},
		{"the proxy token with an IPv4 address mapped into IPv6", "::ffff:9.9.9.9", proxyToken, proxyToken, "9.9.9.9"},
		{"a wrong token", "6.6.6.6, 9.9.9.9", proxyToken, "proxy-tokem", "192.0.2.1"},
		{"no token sent", "6.6.6.6, 9.9.9.9", proxyToken, "", "192.0.2.1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/chaos", nil)
			req.RemoteAddr = "192.0.2.1:4000"
			if c.forwardedFor != "" {
				req.Header.Set("X-Forwarded-For", c.forwardedFor)
			}
			if c.sent != "" {
				req.Header.Set(proxyTokenHeader, c.sent)
			}
			if got := newVisitorLimit("chaos", 3, nil, c.configured, quiet).client(req); got != c.want {
				t.Fatalf("counted as %s, want %s", got, c.want)
			}
		})
	}
}

func TestALimitWithoutTheProxyTokenSaysSoAtStart(t *testing.T) {
	logs := &logSink{}
	newVisitorLimit("chaos", 3, nil, "", slog.New(slog.NewTextHandler(logs, nil)))
	newVisitorLimit("rollout", 3, nil, proxyToken, slog.New(slog.NewTextHandler(logs, nil)))
	if strings.Count(logs.String(), "no proxy token") != 1 || !strings.Contains(logs.String(), "limit=chaos") {
		t.Fatalf("want one warning, for the chaos limit: %q", logs.String())
	}
}

func TestAWrongProxyTokenIsLoggedOnceAMinuteWithoutTheAddress(t *testing.T) {
	logs := &logSink{}
	limit := newVisitorLimit("rollout", 3, nil, proxyToken, slog.New(slog.NewTextHandler(logs, nil)))
	start := time.Now()
	var elapsed time.Duration
	limit.now = func() time.Time { return start.Add(elapsed) }
	req := httptest.NewRequest(http.MethodPost, "/api/rollouts", nil)
	req.RemoteAddr = "192.0.2.1:4000"
	req.Header = http.Header{"X-Forwarded-For": {"9.9.9.9"}, proxyTokenHeader: {"an-old-token"}}
	for range 2 {
		if got := limit.client(req); got != "192.0.2.1" {
			t.Fatalf("counted as %s, want the peer", got)
		}
	}
	elapsed = warnEvery
	limit.client(req)
	if n := strings.Count(logs.String(), "doesn't match"); n != 2 || !strings.Contains(logs.String(), "limit=rollout") || strings.Contains(logs.String(), "9.9.9.9") {
		t.Fatalf("want two warnings a minute apart, naming the limit and not the visitor: %q", logs.String())
	}
}

func TestTheWebServerSendsTheTokenUnderTheHeaderTheLimitsRead(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "..", "web", "src", "lib", "server.ts"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "'"+proxyTokenHeader+"'") {
		t.Fatalf("web/src/lib/server.ts doesn't send %s", proxyTokenHeader)
	}
}

func TestACallerWithoutTheProxyTokenCannotRotatePastTheVisitorLimit(t *testing.T) {
	r := newRig(t)
	api := NewAPI(r.cfgs, r.engine, r.store, NewHub(quiet), adminToken, quiet)
	counter := &fakeCounter{hits: map[string]int64{}}
	api.LetVisitorsRollOut(counter, RolloutLimits{PerHour: 1}, proxyToken, quiet)
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	url, start := srv.URL+"/api/rollouts", `{"route":"api","canary":"v2"}`
	if got := send(t, http.MethodPost, url, start, http.Header{"X-Forwarded-For": {"9.9.9.1"}}).StatusCode; got != http.StatusCreated {
		t.Fatalf("first start: %d", got)
	}
	if got := send(t, http.MethodPost, url+"/abort", `{}`, asAdmin("9.9.9.1")).StatusCode; got != http.StatusOK {
		t.Fatalf("admin abort: %d", got)
	}
	r.now = r.now.Add(time.Minute)
	if got := send(t, http.MethodPost, url, start, http.Header{"X-Forwarded-For": {"9.9.9.2"}}).StatusCode; got != http.StatusTooManyRequests {
		t.Fatalf("second start under a new X-Forwarded-For: %d, want the limit", got)
	}
	if len(counter.hits) != 1 || counter.hits["rollout:ip:9.9.9.1"] != 0 || counter.hits["rollout:ip:9.9.9.2"] != 0 {
		t.Fatalf("counts %v, want both starts on one key, the caller's own address", counter.hits)
	}
}

func TestAChaosCallerWithoutTheProxyTokenIsCountedByItsOwnAddress(t *testing.T) {
	counter := &fakeCounter{hits: map[string]int64{}}
	svc := NewChaosService(&fakeInjector{}, nil, counter, NewHub(quiet), DefaultChaosLimits(), []string{"dim-a"}, proxyToken, quiet)
	mux := http.NewServeMux()
	svc.Routes(mux, (&API{token: adminToken}).admin, adminToken)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	t.Cleanup(func() { _ = svc.Stop(context.Background()) })
	if got := send(t, http.MethodPost, srv.URL+"/api/chaos", `{"mode":"crash","target":"dim-a"}`, http.Header{"X-Forwarded-For": {"9.9.9.1"}}).StatusCode; got != http.StatusCreated {
		t.Fatalf("visitor fault: %d", got)
	}
	if len(counter.hits) != 1 || counter.hits["chaos:ip:9.9.9.1"] != 0 {
		t.Fatalf("counts %v, want the fault on the caller's own address", counter.hits)
	}
}
