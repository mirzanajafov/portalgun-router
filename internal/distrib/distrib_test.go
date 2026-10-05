package distrib

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/proxy"
	"github.com/redis/go-redis/v9"
)

func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("PORTAL_TEST_REDIS")
	if addr == "" {
		t.Skip("PORTAL_TEST_REDIS not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 3})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return rdb
}

type fakeTarget struct {
	mu      sync.Mutex
	applied []*config.Config
	cluster int64
}

func (f *fakeTarget) Apply(c *config.Config) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, c)
	return nil
}

func (f *fakeTarget) SetClusterVersion(v int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cluster = v
}

func (f *fakeTarget) ClusterVersion() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cluster
}

func (f *fakeTarget) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.applied)
}

const cfgJSON = `{"routes":[{"name":"all","pathPrefix":"/","pool":"p"}],"pools":[{"name":"p","dimensions":[{"id":"a","address":"a:8080"}]}]}`

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestWatcherAppliesNewerConfigsOnly(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	target := &fakeTarget{}
	w := NewWatcher(rdb, target, quiet(), time.Hour)

	if err := w.Sync(ctx); err != nil || target.count() != 0 {
		t.Fatalf("empty Redis: err %v, applied %d", err, target.count())
	}
	if err := PublishConfig(ctx, rdb, Envelope{Version: 3, Config: json.RawMessage(cfgJSON)}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(ctx); err != nil || target.count() != 1 || target.ClusterVersion() != 3 {
		t.Fatalf("err %v, applied %d, version %d", err, target.count(), target.ClusterVersion())
	}
	if err := w.Sync(ctx); err != nil || target.count() != 1 {
		t.Fatal("the same version was applied twice")
	}
	_ = PublishConfig(ctx, rdb, Envelope{Version: 4, Config: json.RawMessage(`{"routes":[]}`)})
	if err := w.Sync(ctx); err == nil || target.count() != 1 || target.ClusterVersion() != 4 {
		t.Fatalf("a broken config: err %v, applied %d, version %d", err, target.count(), target.ClusterVersion())
	}
}

func TestWatcherReactsToTheChannel(t *testing.T) {
	rdb := testRedis(t)
	ctx := t.Context()
	target := &fakeTarget{}
	go NewWatcher(rdb, target, quiet(), time.Hour).Run(ctx)
	time.Sleep(100 * time.Millisecond)
	if err := PublishConfig(ctx, rdb, Envelope{Version: 1, Config: json.RawMessage(cfgJSON)}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for target.count() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the change notification was not acted on")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPublisherWritesSnapshots(t *testing.T) {
	rdb := testRedis(t)
	ctx := t.Context()
	p := NewPublisher(rdb, quiet())
	go p.Run(ctx)
	p.Offer(proxy.Snapshot{Instance: "gw-1", Second: 42, Routes: map[string]proxy.RouteSnapshot{"all": {Requests: 7}}})
	var msgs []redis.XMessage
	deadline := time.Now().Add(2 * time.Second)
	for len(msgs) == 0 && time.Now().Before(deadline) {
		msgs, _ = rdb.XRange(ctx, SnapshotStream, "-", "+").Result()
		time.Sleep(10 * time.Millisecond)
	}
	if len(msgs) != 1 {
		t.Fatalf("%d entries in the stream", len(msgs))
	}
	s, err := DecodeSnapshot(msgs[0])
	if err != nil || s.Instance != "gw-1" || s.Second != 42 || s.Routes["all"].Requests != 7 {
		t.Fatalf("decoded %+v, %v", s, err)
	}
}
