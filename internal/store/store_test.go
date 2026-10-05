package store

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

func open(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("PORTAL_TEST_POSTGRES")
	if url == "" {
		t.Skip("PORTAL_TEST_POSTGRES not set")
	}
	ctx := context.Background()
	s, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if _, err := s.pool.Exec(ctx, `truncate config_versions, rollouts, rollout_steps, chaos_events restart identity cascade`); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestMigrationsRunOnce(t *testing.T) {
	s := open(t)
	if err := s.migrate(context.Background()); err != nil {
		t.Fatalf("second migration run: %v", err)
	}
	var n int
	if err := s.pool.QueryRow(context.Background(), `select count(*) from schema_migrations`).Scan(&n); err != nil || n != len(migrations) {
		t.Fatalf("%d migrations recorded, want %d (%v)", n, len(migrations), err)
	}
}

func TestConfigVersions(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	if c, err := s.LatestConfig(ctx); err != nil || c != nil {
		t.Fatalf("empty store: %+v %v", c, err)
	}
	first, err := s.SaveConfig(ctx, "bootstrap", "first", json.RawMessage(`{"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.SaveConfig(ctx, "admin", "second", json.RawMessage(`{"a":2}`))
	if err != nil || second.Version != first.Version+1 {
		t.Fatalf("versions %d then %d (%v)", first.Version, second.Version, err)
	}
	latest, err := s.LatestConfig(ctx)
	if err != nil || latest.Version != second.Version || latest.Reason != "second" || string(latest.Body) != `{"a": 2}` {
		t.Fatalf("latest %+v %v", latest, err)
	}
	hist, err := s.ConfigHistory(ctx, 10)
	if err != nil || len(hist) != 2 || hist[0].Version != second.Version || hist[0].Body != nil {
		t.Fatalf("history %+v %v", hist, err)
	}
}

func TestRolloutLifecycle(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	start := time.Now().UTC().Truncate(time.Millisecond)
	if err := s.StartRollout(ctx, Rollout{ID: "ro-1", Route: "api", Stable: "v1", Canary: "v2", StartedAt: start}); err != nil {
		t.Fatal(err)
	}
	step := Step{Step: 0, Weight: 0.01, StartedAt: start, LLRErrors: 1.5, CanaryRequests: 10}
	if err := s.SaveStep(ctx, "ro-1", step); err != nil {
		t.Fatal(err)
	}
	ended := start.Add(time.Minute)
	step.EndedAt, step.Decision, step.Reason, step.CanaryRequests = &ended, "promote", "accepted", 600
	if err := s.SaveStep(ctx, "ro-1", step); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishRollout(ctx, "ro-1", "promoted", "all steps passed", ended); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishRollout(ctx, "ro-1", "promoted", "twice", ended); err == nil {
		t.Fatal("a finished rollout finished again")
	}
	ros, err := s.Rollouts(ctx, 5)
	if err != nil || len(ros) != 1 {
		t.Fatalf("rollouts %+v %v", ros, err)
	}
	r := ros[0]
	if r.Outcome != "promoted" || r.FinishedAt == nil || len(r.Steps) != 1 || r.Steps[0].Decision != "promote" || r.Steps[0].CanaryRequests != 600 {
		t.Fatalf("rollout %+v", r)
	}
}

func TestAbandonActiveRollouts(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	now := time.Now().UTC()
	_ = s.StartRollout(ctx, Rollout{ID: "done", Route: "api", Stable: "v1", Canary: "v2", StartedAt: now})
	_ = s.FinishRollout(ctx, "done", "promoted", "", now)
	_ = s.StartRollout(ctx, Rollout{ID: "live", Route: "api", Stable: "v1", Canary: "v3", StartedAt: now})
	ids, err := s.AbandonActive(ctx, "restart", now)
	if err != nil || len(ids) != 1 || ids[0] != "live" {
		t.Fatalf("abandoned %v %v", ids, err)
	}
}

func TestChaosEvents(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	if err := s.LogChaos(ctx, ChaosEvent{At: time.Now().UTC(), Actor: "visitor", Mode: "crash", Target: "dim-a", Action: "inject"}); err != nil {
		t.Fatal(err)
	}
	evs, err := s.ChaosEvents(ctx, 10)
	if err != nil || len(evs) != 1 || evs[0].Mode != "crash" || string(evs[0].Detail) != "{}" {
		t.Fatalf("events %+v %v", evs, err)
	}
}
