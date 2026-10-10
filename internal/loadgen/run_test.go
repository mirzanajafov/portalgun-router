package loadgen

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/dimension"
)

func dimensionServer(t *testing.T, mutate func(*dimension.Behavior)) *httptest.Server {
	t.Helper()
	b := dimension.DefaultBehavior()
	if mutate != nil {
		mutate(&b)
	}
	s, err := dimension.New("c-137", "v1", b)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return srv
}

func TestRunCountsEveryRequest(t *testing.T) {
	srv := dimensionServer(t, func(b *dimension.Behavior) { b.BodyBytes = 256 })
	res, err := Run(context.Background(), Config{
		URLs:        []string{srv.URL},
		Rate:        400,
		Warmup:      250 * time.Millisecond,
		Duration:    500 * time.Millisecond,
		KeepRecords: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	sum := res.Summary
	if sum.Scheduled != 200 || sum.Completed != 200 || sum.Status["200"] != 200 {
		t.Fatalf("scheduled %d, completed %d, status %v; want 200 each", sum.Scheduled, sum.Completed, sum.Status)
	}
	if sum.Dimensions["c-137"] != 200 {
		t.Fatalf("dimensions %v", sum.Dimensions)
	}
	if sum.LatencyUs.Count != 200 || sum.LatencyUs.P50 <= 0 {
		t.Fatalf("latency %+v", sum.LatencyUs)
	}
	if len(res.Log.Records) != 300 || !reflect.DeepEqual(res.Log.Dimensions, []string{"c-137"}) {
		t.Fatalf("log has %d records and dimensions %v, want 300 and [c-137]", len(res.Log.Records), res.Log.Dimensions)
	}
	for _, r := range res.Log.Records {
		if r.Bytes != 256 || r.Dimension != 1 || r.Err != ErrNone || r.Connected == 0 || r.Ended < r.Connected {
			t.Fatalf("bad record %+v", r)
		}
	}
}

func TestLatencyIsMeasuredFromTheSchedule(t *testing.T) {
	srv := dimensionServer(t, func(b *dimension.Behavior) { b.DelayMs = 20 })
	res, err := Run(context.Background(), Config{
		URLs:     []string{srv.URL},
		Rate:     100,
		Duration: time.Second,
		MaxConns: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	sum := res.Summary
	if sum.Completed != 100 {
		t.Fatalf("completed %d, want 100", sum.Completed)
	}
	if sum.ServiceUs.P99 > 100_000 {
		t.Fatalf("service p99 %.0fus, want close to the 20ms service time", sum.ServiceUs.P99)
	}
	if sum.LatencyUs.P99 < 500_000 {
		t.Fatalf("latency p99 %.0fus, want the queue behind one connection to show up", sum.LatencyUs.P99)
	}
}

func TestSuccessLatencyLeavesFastRefusalsOut(t *testing.T) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1)%3 != 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}))
	t.Cleanup(srv.Close)
	res, err := Run(context.Background(), Config{URLs: []string{srv.URL}, Rate: 200, Duration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	sum := res.Summary
	if sum.OKLatencyUs.Count != sum.Status["200"] || sum.Status["200"]+sum.Status["503"] != sum.Completed {
		t.Fatalf("ok latency counted %d requests, status %v", sum.OKLatencyUs.Count, sum.Status)
	}
	if sum.OKLatencyUs.P50 < 20_000 || sum.LatencyUs.P50 >= 20_000 {
		t.Fatalf("ok p50 %.0fus, all p50 %.0fus; want only the 200s above 20ms", sum.OKLatencyUs.P50, sum.LatencyUs.P50)
	}
}

func TestSpreadsRequestsOverURLsInTurn(t *testing.T) {
	var urls []string
	for _, id := range []string{"a", "b", "c"} {
		s, err := dimension.New(id, "v1", dimension.DefaultBehavior())
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(s.Handler())
		t.Cleanup(srv.Close)
		urls = append(urls, srv.URL)
	}
	res, err := Run(context.Background(), Config{URLs: urls, Rate: 300, Duration: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if d := res.Summary.Dimensions; d["a"] != 30 || d["b"] != 30 || d["c"] != 30 {
		t.Fatalf("dimensions %v, want 30 each", d)
	}
}

func TestSendsBodyAndHeaders(t *testing.T) {
	var seen, wrong atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		seen.Add(1)
		if r.Method != http.MethodPost || len(body) != 1000 || r.Header.Get("X-Api-Key") != "morty" {
			wrong.Add(1)
		}
	}))
	t.Cleanup(srv.Close)
	_, err := Run(context.Background(), Config{
		URLs:      []string{srv.URL},
		Method:    http.MethodPost,
		BodyBytes: 1000,
		Header:    http.Header{"X-Api-Key": {"morty"}},
		Rate:      200,
		Duration:  200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen.Load() != 40 || wrong.Load() != 0 {
		t.Fatalf("server saw %d requests, %d malformed", seen.Load(), wrong.Load())
	}
}

func TestRefusedConnectionsAreClassified(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("refusal errors are classified on Linux")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	res, err := Run(context.Background(), Config{URLs: []string{"http://" + addr}, Rate: 50, Duration: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	sum := res.Summary
	if sum.Scheduled != 10 || sum.Failed != 10 || sum.Errors["refused"] != 10 || sum.Completed != 0 {
		t.Fatalf("scheduled %d failed %d errors %v completed %d", sum.Scheduled, sum.Failed, sum.Errors, sum.Completed)
	}
}

func TestInflightLimitDropsAndInvalidatesTheRun(t *testing.T) {
	srv := dimensionServer(t, func(b *dimension.Behavior) { b.Hang = true })
	res, err := Run(context.Background(), Config{
		URLs:        []string{srv.URL},
		Rate:        200,
		Duration:    300 * time.Millisecond,
		MaxInflight: 5,
		Timeout:     200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	sum := res.Summary
	if sum.Dropped == 0 || sum.Valid {
		t.Fatalf("dropped %d, valid %v", sum.Dropped, sum.Valid)
	}
	if !strings.Contains(strings.Join(sum.Invalid, ";"), "dropped") {
		t.Fatalf("invalid reasons %v", sum.Invalid)
	}
	if sum.Errors["timeout"] == 0 {
		t.Fatalf("errors %v, want timeouts from the hanging dimension", sum.Errors)
	}
}

func TestCancelStopsTheRun(t *testing.T) {
	srv := dimensionServer(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	began := time.Now()
	res, err := Run(ctx, Config{URLs: []string{srv.URL}, Rate: 100, Duration: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(began); elapsed > 2*time.Second {
		t.Fatalf("run lasted %v after cancel", elapsed)
	}
	if res.Summary.WindowSec > 1 || res.Summary.Completed == 0 {
		t.Fatalf("window %.2fs with %d completed", res.Summary.WindowSec, res.Summary.Completed)
	}
}

func TestValidateFlagsAnUnreliableGenerator(t *testing.T) {
	g := &generator{}
	ok := Summary{WindowSec: 10, LagUs: Percentiles{P99: 200}, CPU: &CPU{Utilization: 0.4}}
	g.validate(&ok)
	if !ok.Valid {
		t.Fatalf("healthy run marked invalid: %v", ok.Invalid)
	}
	bad := Summary{WindowSec: 10, LagUs: Percentiles{P99: 1500}, CPU: &CPU{Utilization: 0.9}}
	g.validate(&bad)
	if bad.Valid || len(bad.Invalid) != 2 {
		t.Fatalf("got valid=%v reasons=%v, want lag and CPU", bad.Valid, bad.Invalid)
	}
}

func TestNormalizeRejectsBadConfig(t *testing.T) {
	for name, c := range map[string]Config{
		"scheme":   {URLs: []string{"ftp://x"}, Duration: time.Second},
		"host":     {URLs: []string{"http://"}, Duration: time.Second},
		"duration": {URLs: []string{"http://x"}},
		"warmup":   {URLs: []string{"http://x"}, Duration: time.Second, Warmup: -time.Second},
		"body":     {URLs: []string{"http://x"}, Duration: time.Second, BodyBytes: -1},
		"no urls":  {Duration: time.Second},
	} {
		if _, _, err := c.normalize(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestLogRoundTrip(t *testing.T) {
	l := &Log{
		StartUnixNano: 1_790_000_000_000_000_000,
		Dimensions:    []string{"c-137", "j19-zeta-7"},
		Records: []Record{
			{Scheduled: 0, Started: 10, Connected: 20, Ended: 900, Bytes: 1024, Status: 200, Dimension: 1, Attempts: 1},
			{Scheduled: 5000, Started: 5010, Connected: 0, Ended: 6000, Err: ErrRefused},
			{Scheduled: 9000, Started: 9001, Connected: 9100, Ended: 99000, Bytes: 7, Status: 503, Dimension: 2, Attempts: 3},
		},
	}
	var buf bytes.Buffer
	if err := WriteLog(&buf, l); err != nil {
		t.Fatal(err)
	}
	got, err := ReadLog(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, l) {
		t.Fatalf("round trip changed the log:\n%+v\n%+v", got, l)
	}
	if _, err := ReadLog(strings.NewReader("nope, not a log")); err == nil {
		t.Fatal("garbage accepted")
	}
}

func TestParseProcStat(t *testing.T) {
	line := []byte("1234 (load gen) S 1 1 1 0 -1 4194560 100 0 0 0 150 50 0 0 20 0 1 0")
	got, ok := parseProcStat(line)
	if !ok || got != 2*time.Second {
		t.Fatalf("got %v %v, want 2s", got, ok)
	}
	if _, ok := parseProcStat([]byte("garbage")); ok {
		t.Fatal("garbage parsed")
	}
}
