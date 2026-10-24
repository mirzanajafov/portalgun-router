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

const dashboardToken = "dashboard-token"

func behindDashboard() TrustedProxy {
	return TrustedProxy{Token: dashboardToken, Hops: 1}
}

func viaDashboard(visitor string) http.Header {
	return http.Header{"X-Forwarded-For": {visitor}, ProxyTokenHeader: {dashboardToken}}
}

func postAs(t *testing.T, url, body string, header http.Header) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header = header
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestOnlyTheDashboardsServerNamesTheVisitor(t *testing.T) {
	cases := []struct {
		name, configured, sent, want string
	}{
		{"no token configured", "", "", "192.0.2.1"},
		{"the dashboard's token", dashboardToken, dashboardToken, "9.9.9.9"},
		{"a wrong token", dashboardToken, "dashboard-tokem", "192.0.2.1"},
		{"no token sent", dashboardToken, "", "192.0.2.1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/chaos", nil)
			req.RemoteAddr = "192.0.2.1:4000"
			req.Header.Set("X-Forwarded-For", "6.6.6.6, 9.9.9.9")
			if c.sent != "" {
				req.Header.Set(ProxyTokenHeader, c.sent)
			}
			if got := (TrustedProxy{Token: c.configured, Hops: 1}).ClientIP(req); got != c.want {
				t.Fatalf("counted as %s, want %s", got, c.want)
			}
		})
	}
}

func TestACallerWithoutTheDashboardsTokenCannotRotatePastTheVisitorLimit(t *testing.T) {
	r := newRig(t)
	api := NewAPI(r.cfgs, r.engine, r.store, NewHub(), "s3cret")
	counter := &fakeCounter{hits: map[string]int64{}}
	api.LetVisitorsRollOut(counter, RolloutLimits{PerHour: 1}, behindDashboard(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(api.Handler())
	t.Cleanup(srv.Close)
	start := `{"route":"api","canary":"v2"}`
	if got := postAs(t, srv.URL+"/api/rollouts", start, http.Header{"X-Forwarded-For": {"9.9.9.1"}}); got != http.StatusCreated {
		t.Fatalf("first start: %d", got)
	}
	if got := postAs(t, srv.URL+"/api/rollouts/abort", `{}`, http.Header{"Authorization": {"Bearer s3cret"}}); got != http.StatusOK {
		t.Fatalf("admin abort: %d", got)
	}
	r.now = r.now.Add(time.Minute)
	if got := postAs(t, srv.URL+"/api/rollouts", start, http.Header{"X-Forwarded-For": {"9.9.9.2"}}); got != http.StatusTooManyRequests {
		t.Fatalf("second start under a new X-Forwarded-For: %d, want the limit", got)
	}
	if n := counter.hits["rollout:ip:127.0.0.1"]; n != 2 {
		t.Fatalf("counts %v, want both starts on the caller's own address", counter.hits)
	}
}
