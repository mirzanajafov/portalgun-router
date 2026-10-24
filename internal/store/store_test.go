package store

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"slices"
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
	if c, err := s.LatestConfig(ctx); !errors.Is(err, ErrNoConfig) {
		t.Fatalf("empty store: %+v %v", c, err)
	}
	first, err := s.SaveConfig(ctx, ActorBootstrap, "first", json.RawMessage(`{"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.SaveConfig(ctx, ActorAdmin, "second", json.RawMessage(`{"a":2}`))
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
	step.EndedAt, step.Decision, step.Reason, step.CanaryRequests = &ended, DecisionPromote, "accepted", 600
	if err := s.SaveStep(ctx, "ro-1", step); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishRollout(ctx, "ro-1", OutcomePromoted, "all steps passed", ended); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishRollout(ctx, "ro-1", OutcomePromoted, "twice", ended); err == nil {
		t.Fatal("a finished rollout finished again")
	}
	ros, err := s.Rollouts(ctx, 5)
	if err != nil || len(ros) != 1 {
		t.Fatalf("rollouts %+v %v", ros, err)
	}
	r := ros[0]
	if r.Outcome != OutcomePromoted || r.FinishedAt == nil || len(r.Steps) != 1 || r.Steps[0].Decision != DecisionPromote || r.Steps[0].CanaryRequests != 600 {
		t.Fatalf("rollout %+v", r)
	}
}

func TestAbandonActiveRollouts(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	now := time.Now().UTC()
	_ = s.StartRollout(ctx, Rollout{ID: "done", Route: "api", Stable: "v1", Canary: "v2", StartedAt: now})
	_ = s.FinishRollout(ctx, "done", OutcomePromoted, "", now)
	_ = s.StartRollout(ctx, Rollout{ID: "live", Route: "api", Stable: "v1", Canary: "v3", StartedAt: now})
	ids, err := s.AbandonActive(ctx, "restart", now)
	if err != nil || len(ids) != 1 || ids[0] != "live" {
		t.Fatalf("abandoned %v %v", ids, err)
	}
}

func TestChaosEvents(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	if err := s.LogChaos(ctx, ChaosEvent{At: time.Now().UTC(), Actor: ActorVisitor, Mode: "crash", Target: "dim-a", Action: "inject"}); err != nil {
		t.Fatal(err)
	}
	evs, err := s.ChaosEvents(ctx, 10)
	if err != nil || len(evs) != 1 || evs[0].Mode != "crash" || string(evs[0].Detail) != "{}" {
		t.Fatalf("events %+v %v", evs, err)
	}
}

func TestEveryListedRolloutGetsItsOwnSteps(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	start := time.Now().UTC().Truncate(time.Millisecond)
	for i, id := range []string{"ro-a", "ro-b", "ro-c"} {
		if err := s.StartRollout(ctx, Rollout{ID: id, Route: "api", Stable: "v1", Canary: "v2", StartedAt: start.Add(time.Duration(i) * time.Second)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, saved := range []struct {
		id   string
		step int
	}{{"ro-a", 0}, {"ro-c", 0}, {"ro-c", 1}, {"ro-a", 1}, {"ro-c", 2}} {
		if err := s.SaveStep(ctx, saved.id, Step{Step: saved.step, Weight: 0.01, StartedAt: start}); err != nil {
			t.Fatal(err)
		}
	}
	ros, err := s.Rollouts(ctx, 10)
	if err != nil || len(ros) != 3 {
		t.Fatalf("rollouts %+v, %v", ros, err)
	}
	got := map[string][]int{}
	for _, ro := range ros {
		got[ro.ID] = []int{}
		for _, st := range ro.Steps {
			got[ro.ID] = append(got[ro.ID], st.Step)
		}
		if ro.Steps == nil {
			t.Fatalf("%s has nil steps, which the dashboard would read as null", ro.ID)
		}
	}
	want := map[string][]int{"ro-c": {0, 1, 2}, "ro-b": {}, "ro-a": {0, 1}}
	if ros[0].ID != "ro-c" || !maps.EqualFunc(got, want, slices.Equal) {
		t.Fatalf("rollouts %v in order %s, %s, %s", got, ros[0].ID, ros[1].ID, ros[2].ID)
	}
}

func TestACallThatWaitsOnTheDatabaseGivesUpInTime(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	if err := s.StartRollout(ctx, Rollout{ID: "ro-held", Route: "api", Stable: "v1", Canary: "v2", StartedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name, hold string
		call       func() error
	}{
		{"finishing a rollout whose row is locked", `select 1 from rollouts where id = 'ro-held' for update`, func() error {
			return s.FinishRollout(ctx, "ro-held", OutcomeAborted, "the row is locked", time.Now().UTC())
		}},
		{"saving a config while its table is locked", `lock table config_versions in exclusive mode`, func() error {
			_, err := s.SaveConfig(ctx, ActorAdmin, "the table is locked", json.RawMessage(`{}`))
			return err
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			tx, err := s.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			if _, err := tx.Exec(ctx, c.hold); err != nil {
				t.Fatal(err)
			}
			began := time.Now()
			if err := c.call(); !errors.Is(err, context.DeadlineExceeded) || time.Since(began) > callTimeout+time.Second {
				t.Fatalf("took %v and came back as %v", time.Since(began), err)
			}
		})
	}
}
