package proxy

import (
	"context"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"syscall"
	"testing"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/redis/go-redis/v9"
)

func gathered(t *testing.T, m *Metrics, name string) float64 {
	t.Helper()
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return f.GetMetric()[0].GetCounter().GetValue()
		}
	}
	t.Fatalf("no metric %s", name)
	return 0
}

func TestRedisHookCountsRoundTrips(t *testing.T) {
	m := NewMetrics()
	h := m.RedisHook()
	one := h.ProcessHook(func(context.Context, redis.Cmder) error { return nil })
	many := h.ProcessPipelineHook(func(context.Context, []redis.Cmder) error { return nil })
	ctx := context.Background()
	_ = one(ctx, redis.NewStatusCmd(ctx, "ping"))
	_ = one(ctx, redis.NewStatusCmd(ctx, "ping"))
	_ = many(ctx, []redis.Cmder{redis.NewStatusCmd(ctx, "ping"), redis.NewStatusCmd(ctx, "ping"), redis.NewStatusCmd(ctx, "ping")})
	if n := gathered(t, m, "portal_redis_round_trips_total"); n != 3 {
		t.Fatalf("%v round trips, want 3: two commands and one pipeline", n)
	}
}

func TestRedisHookTimesCallsAndDialsByOutcome(t *testing.T) {
	m := NewMetrics()
	h := m.RedisHook()
	ctx := context.Background()
	errs := []error{nil, redis.Nil, context.DeadlineExceeded, &net.OpError{Op: "read", Err: os.ErrDeadlineExceeded}, errors.New("connection reset by peer")}
	for _, err := range errs {
		call := h.ProcessHook(func(context.Context, redis.Cmder) error { return err })
		_ = call(ctx, redis.NewStatusCmd(ctx, "ping"))
	}
	dial := h.DialHook(func(context.Context, string, string) (net.Conn, error) { return nil, syscall.ECONNREFUSED })
	_, _ = dial(ctx, "tcp", "redis:6379")
	want := map[string]uint64{"ok": 2, "timeout": 2, "error": 1}
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]uint64{}
	dials := map[string]uint64{}
	for _, f := range families {
		into := map[string]map[string]uint64{"portal_redis_call_seconds": got, "portal_redis_dial_seconds": dials}[f.GetName()]
		if into == nil {
			continue
		}
		for _, s := range f.GetMetric() {
			into[s.GetLabel()[0].GetValue()] = s.GetHistogram().GetSampleCount()
		}
	}
	if !maps.Equal(got, want) || dials["error"] != 1 {
		t.Fatalf("calls %v, dials %v: a missing key is a reply, and both a context deadline and a socket deadline are timeouts", got, dials)
	}
}

func TestAResponseCutOffMidBodyIsCounted(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		_, _ = w.Write([]byte("only ten b"))
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}))
	t.Cleanup(up.Close)
	g, srv := serve(t, onePool(t, config.BalancerRoundRobin, config.Dimension{ID: "a", Address: up.Listener.Addr().String(), Version: "v1"}))
	if resp, err := http.Get(srv.URL + "/"); err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if n := gathered(t, g.metrics, "portal_response_cut_off_total"); n != 1 {
		t.Fatalf("%v responses counted as cut off, want 1", n)
	}
}
