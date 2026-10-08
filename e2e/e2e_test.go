package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const token = "e2e-token"

var (
	enabled    = os.Getenv("PORTAL_E2E") != ""
	gateways   = []string{"http://127.0.0.1:" + env("E2E_GATEWAY_1_PORT", "3131"), "http://127.0.0.1:" + env("E2E_GATEWAY_2_PORT", "3132")}
	controller = "http://127.0.0.1:" + env("E2E_CONTROLLER_PORT", "3133")
	client     = &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: 256, DisableCompression: true},
	}
)

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func compose(args ...string) ([]byte, error) {
	cmd := exec.Command("docker", append([]string{"compose", "-f", "compose.yml"}, args...)...)
	return cmd.CombinedOutput()
}

func TestMain(m *testing.M) {
	if !enabled {
		os.Exit(m.Run())
	}
	if os.Getenv("PORTAL_E2E_UP") != "0" {
		if out, err := compose("up", "-d", "--build", "--wait"); err != nil {
			fmt.Fprintf(os.Stderr, "starting the e2e stack: %v\n%s", err, out)
			os.Exit(1)
		}
	}
	code := m.Run()
	if os.Getenv("PORTAL_E2E_KEEP") == "" {
		if out, err := compose("down", "-v"); err != nil {
			fmt.Fprintf(os.Stderr, "stopping the e2e stack: %v\n%s", err, out)
		}
	}
	os.Exit(code)
}

func needStack(t *testing.T) {
	t.Helper()
	if !enabled {
		t.Skip("PORTAL_E2E not set")
	}
	waitFor(t, 60*time.Second, "every dimension healthy on both gateways", func() bool {
		var st struct {
			Latest *struct {
				Instances  []string `json:"instances"`
				Dimensions []struct {
					ID      string `json:"id"`
					Healthy bool   `json:"healthy"`
					Outlier string `json:"outlier"`
				} `json:"dimensions"`
			} `json:"latest"`
		}
		if call(http.MethodGet, controller+"/api/state", nil, &st) != http.StatusOK || st.Latest == nil {
			return false
		}
		if len(st.Latest.Instances) != 2 || len(st.Latest.Dimensions) != 4 {
			return false
		}
		for _, d := range st.Latest.Dimensions {
			if !d.Healthy || d.Outlier == "ejected" {
				return false
			}
		}
		return true
	})
}

func waitFor(t *testing.T, limit time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("waited %v for %s", limit, what)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func call(method, url string, body any, out any) int {
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		return 0
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	if out != nil {
		_ = json.NewDecoder(resp.Body).Decode(out)
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}
	return resp.StatusCode
}

type sample struct {
	at        time.Duration
	status    int
	dimension string
	err       string
}

type tally struct {
	mu      sync.Mutex
	samples []sample
}

func (t *tally) add(s sample) {
	t.mu.Lock()
	t.samples = append(t.samples, s)
	t.mu.Unlock()
}

func (t *tally) count(keep func(sample) bool) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, s := range t.samples {
		if keep(s) {
			n++
		}
	}
	return n
}

func (t *tally) failures() []sample {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []sample
	for _, s := range t.samples {
		if s.err != "" || s.status >= 500 {
			out = append(out, s)
		}
	}
	return out
}

type load struct {
	rate     float64
	duration time.Duration
	path     string
	header   func(i int) http.Header
}

func (l load) run(ctx context.Context) *tally {
	t := &tally{}
	began := time.Now()
	tick := time.NewTicker(time.Duration(float64(time.Second) / l.rate))
	defer tick.Stop()
	var wg sync.WaitGroup
	for i := 0; time.Since(began) < l.duration; i++ {
		select {
		case <-ctx.Done():
			wg.Wait()
			return t
		case <-tick.C:
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, gateways[i%len(gateways)]+l.path, nil)
		if err != nil {
			continue
		}
		if l.header != nil {
			req.Header = l.header(i)
		} else {
			req.Header.Set("X-Forwarded-For", fmt.Sprintf("10.1.%d.%d", i%2000/250, i%250+1))
		}
		wg.Go(func() {
			at := time.Since(began)
			resp, err := client.Do(req)
			if err != nil {
				if ctx.Err() == nil {
					t.add(sample{at: at, err: err.Error()})
				}
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			t.add(sample{at: at, status: resp.StatusCode, dimension: resp.Header.Get("X-Portal-Dimension")})
		})
	}
	wg.Wait()
	return t
}

func noFailures(t *testing.T, tl *tally) {
	t.Helper()
	fails := tl.failures()
	if len(fails) == 0 {
		return
	}
	for _, f := range fails[:min(len(fails), 5)] {
		t.Logf("at %v: status %d %s", f.at.Round(time.Millisecond), f.status, f.err)
	}
	t.Fatalf("%d of %d requests failed", len(fails), tl.count(func(sample) bool { return true }))
}

func chaos(t *testing.T, mode, target string, errorRate, seconds float64) {
	t.Helper()
	body := map[string]any{"mode": mode, "target": target, "durationSec": seconds}
	if errorRate > 0 {
		body["errorRate"] = errorRate
	}
	if code := call(http.MethodPost, controller+"/api/chaos", body, nil); code != http.StatusCreated {
		t.Fatalf("injecting %s on %s: status %d", mode, target, code)
	}
	t.Cleanup(func() { call(http.MethodDelete, controller+"/api/chaos", nil, nil) })
}

func TestCrashUnderLoadLosesNoGets(t *testing.T) {
	needStack(t)
	done := make(chan *tally)
	go func() { done <- load{rate: 300, duration: 12 * time.Second, path: "/items"}.run(context.Background()) }()
	time.Sleep(3 * time.Second)
	chaos(t, "crash", "a", 0, 5)
	tl := <-done
	noFailures(t, tl)
	if n := tl.count(func(s sample) bool { return s.dimension == "a" && s.at < 3*time.Second }); n == 0 {
		t.Fatal("dimension a served nothing before the crash, so the test proved nothing")
	}
	if n := tl.count(func(s sample) bool { return s.dimension == "a" && s.at > 4*time.Second && s.at < 8*time.Second }); n != 0 {
		t.Fatalf("a crashed dimension answered %d requests", n)
	}
}

func TestBadCanaryIsRolledBackWithoutClientErrors(t *testing.T) {
	needStack(t)
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan *tally)
	go func() { done <- load{rate: 300, duration: 90 * time.Second, path: "/items"}.run(ctx) }()
	chaos(t, "partial", "d", 1, 90)
	var ro struct {
		ID string `json:"id"`
	}
	if code := call(http.MethodPost, controller+"/api/rollouts", map[string]string{"route": "api", "canary": "v2"}, &ro); code != http.StatusCreated {
		stop()
		<-done
		t.Fatalf("starting the rollout: status %d", code)
	}
	began := time.Now()
	waitFor(t, 60*time.Second, "the rollout to end", func() bool {
		var st struct {
			Rollout struct {
				Active bool `json:"active"`
			} `json:"rollout"`
		}
		return call(http.MethodGet, controller+"/api/state", nil, &st) == http.StatusOK && !st.Rollout.Active
	})
	took := time.Since(began)
	stop()
	tl := <-done
	noFailures(t, tl)
	t.Logf("rolled back %v after the start, %d requests served", took.Round(100*time.Millisecond), tl.count(func(sample) bool { return true }))

	var rollouts []struct {
		ID      string `json:"id"`
		Outcome string `json:"outcome"`
		Reason  string `json:"reason"`
	}
	if call(http.MethodGet, controller+"/api/rollouts", nil, &rollouts) != http.StatusOK || len(rollouts) == 0 {
		t.Fatal("no rollout history")
	}
	if r := rollouts[0]; r.ID != ro.ID || r.Outcome != "rolled_back" || r.Reason == "" {
		t.Fatalf("latest rollout %+v, want %s rolled back with a reason", r, ro.ID)
	}
	var history []struct {
		Author string `json:"author"`
		Reason string `json:"reason"`
	}
	if call(http.MethodGet, controller+"/api/config/history", nil, &history) != http.StatusOK || len(history) == 0 {
		t.Fatal("no config history")
	}
	if h := history[0]; h.Author != "controller" || !strings.Contains(h.Reason, ro.ID+" rolled_back") {
		t.Fatalf("latest config version %+v does not say the rollout was rolled back", h)
	}
}

func TestControllerDownKeepsTrafficFlowing(t *testing.T) {
	needStack(t)
	if out, err := compose("stop", "controller"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	t.Cleanup(func() {
		if out, err := compose("start", "controller"); err != nil {
			t.Errorf("%v\n%s", err, out)
		}
		waitFor(t, 60*time.Second, "the controller", func() bool { return call(http.MethodGet, controller+"/healthz", nil, nil) == http.StatusOK })
	})
	tl := load{rate: 200, duration: 6 * time.Second, path: "/items"}.run(context.Background())
	noFailures(t, tl)
}

func degraded(t *testing.T, gateway string) float64 {
	t.Helper()
	out, err := compose("exec", "-T", gateway, "wget", "-q", "-O", "-", "http://127.0.0.1:9090/metrics")
	if err != nil {
		t.Fatalf("reading %s metrics: %v\n%s", gateway, err, out)
	}
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, `portal_rate_limit_degraded_total{route="limited"}`) {
			v, _ := strconv.ParseFloat(strings.TrimSpace(line[strings.LastIndexByte(line, ' '):]), 64)
			return v
		}
	}
	return 0
}

func TestRedisDownKeepsTrafficFlowingAndLimitsLocally(t *testing.T) {
	needStack(t)
	before := degraded(t, "gateway-1") + degraded(t, "gateway-2")
	if out, err := compose("stop", "redis"); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	restarted := false
	restart := func() {
		if restarted {
			return
		}
		restarted = true
		if out, err := compose("start", "redis"); err != nil {
			t.Errorf("%v\n%s", err, out)
		}
	}
	t.Cleanup(restart)

	key := func(int) http.Header { return http.Header{"X-Client": {"rick"}} }
	var plain, limited *tally
	var wg sync.WaitGroup
	wg.Go(func() { plain = load{rate: 200, duration: 8 * time.Second, path: "/items"}.run(context.Background()) })
	wg.Go(func() {
		limited = load{rate: 100, duration: 8 * time.Second, path: "/limited/x", header: key}.run(context.Background())
	})
	wg.Wait()
	noFailures(t, plain)
	noFailures(t, limited)
	admitted := limited.count(func(s sample) bool { return s.status == http.StatusOK })
	refused := limited.count(func(s sample) bool { return s.status == http.StatusTooManyRequests })
	t.Logf("with Redis down: %d admitted, %d refused at 100/s against 50/s", admitted, refused)
	if admitted > 50*8+50+40 || admitted < 200 || refused == 0 {
		t.Fatalf("%d admitted and %d refused, want the static split near 50/s", admitted, refused)
	}
	during := degraded(t, "gateway-1") + degraded(t, "gateway-2")
	if during <= before {
		t.Fatalf("no decision was marked degraded (%v before, %v after)", before, during)
	}

	restart()
	waitFor(t, 30*time.Second, "the limiter to sync with Redis again", func() bool {
		start := degraded(t, "gateway-1") + degraded(t, "gateway-2")
		load{rate: 20, duration: time.Second, path: "/limited/y", header: key}.run(context.Background())
		return degraded(t, "gateway-1")+degraded(t, "gateway-2") == start
	})
}
