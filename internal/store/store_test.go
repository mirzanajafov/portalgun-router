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
	first, err := s.SaveConfig(ctx, ConfigChange{Author: ActorBootstrap, Reason: "first", Body: json.RawMessage(`{"a":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.SaveConfig(ctx, ConfigChange{Author: ActorAdmin, Reason: "second", Body: json.RawMessage(`{"a":2}`)})
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

func change(reason string) ConfigChange {
	return ConfigChange{Author: ActorController, Reason: reason, Body: json.RawMessage(`{}`)}
}

func TestRolloutLifecycle(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	start := time.Now().UTC().Truncate(time.Millisecond)
	begun, err := s.StartRollout(ctx, Rollout{ID: "ro-1", Route: "api", Stable: "v1", Canary: "v2", StartedAt: start}, change("starts"))
	if err != nil {
		t.Fatal(err)
	}
	step := Step{Step: 0, Weight: 0.01, StartedAt: start, LLRErrors: 1.5, CanaryRequests: 10}
	if err := s.SaveStep(ctx, "ro-1", step); err != nil {
		t.Fatal(err)
	}
	ended := start.Add(time.Minute)
	step.EndedAt, step.Decision, step.Reason, step.CanaryRequests = &ended, DecisionPromote, "accepted", 600
	promoted := change("promoted")
	done, err := s.EndRollout(ctx, RolloutEnd{ID: "ro-1", Outcome: OutcomePromoted, Reason: "all steps passed", At: ended, Last: step}, &promoted)
	if err != nil || done.Version != begun.Version+1 || done.Reason != "promoted" {
		t.Fatalf("the start saved config %d, the end %+v (%v)", begun.Version, done, err)
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

func TestAnEndSavedAgainReplacesTheFirst(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	if _, err := s.StartRollout(ctx, Rollout{ID: "ro-1", Route: "api", Stable: "v1", Canary: "v2", StartedAt: now}, change("starts")); err != nil {
		t.Fatal(err)
	}
	end := RolloutEnd{ID: "ro-1", Outcome: OutcomePromoted, Reason: "all steps passed", At: now, Last: Step{Weight: 0.5, StartedAt: now, Decision: DecisionPromote}}
	if _, err := s.EndRollout(ctx, end, nil); err != nil {
		t.Fatal(err)
	}
	end.Outcome, end.Reason, end.Last.Decision = OutcomeRolledBack, "the brake", DecisionRollback
	if _, err := s.EndRollout(ctx, end, nil); err != nil {
		t.Fatal(err)
	}
	ros, err := s.Rollouts(ctx, 5)
	if err != nil || len(ros) != 1 || ros[0].Outcome != OutcomeRolledBack || ros[0].Reason != "the brake" || ros[0].Steps[0].Decision != DecisionRollback {
		t.Fatalf("rollouts %+v %v", ros, err)
	}
	if _, err := s.EndRollout(ctx, RolloutEnd{ID: "ro-never", Outcome: OutcomeAborted, At: now}, nil); err == nil {
		t.Fatal("a rollout that never started was ended")
	}
}

func TestAStartOrAnEndThatFailsLeavesNothingBehind(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	now := time.Now().UTC()
	broken := ConfigChange{Author: ActorController, Reason: "broken", Body: json.RawMessage(`{`)}
	if _, err := s.StartRollout(ctx, Rollout{ID: "ro-1", Route: "api", Stable: "v1", Canary: "v2", StartedAt: now}, broken); err == nil {
		t.Fatal("a start with a config Postgres refuses was saved")
	}
	if ros, err := s.Rollouts(ctx, 5); err != nil || len(ros) != 0 {
		t.Fatalf("a failed start left %+v (%v)", ros, err)
	}
	if _, err := s.StartRollout(ctx, Rollout{ID: "ro-1", Route: "api", Stable: "v1", Canary: "v2", StartedAt: now}, change("starts")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StartRollout(ctx, Rollout{ID: "ro-2", Route: "api", Stable: "v1", Canary: "v2", StartedAt: now}, broken); err == nil {
		t.Fatal("a second start with a config Postgres refuses was saved")
	}
	end := RolloutEnd{ID: "ro-1", Outcome: OutcomeRolledBack, Reason: "errors", At: now, Last: Step{Weight: 0.01, StartedAt: now, Decision: DecisionRollback}}
	if _, err := s.EndRollout(ctx, end, &broken); err == nil {
		t.Fatal("an end with a config Postgres refuses was saved")
	}
	ros, err := s.Rollouts(ctx, 5)
	if err != nil || len(ros) != 1 || ros[0].FinishedAt != nil || len(ros[0].Steps) != 0 {
		t.Fatalf("a failed end left %+v (%v)", ros, err)
	}
	if latest, err := s.LatestConfig(ctx); err != nil || latest.Reason != "starts" {
		t.Fatalf("latest config %+v (%v)", latest, err)
	}
}

func TestAStartClosesARolloutLeftOpen(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	for i, id := range []string{"unconfirmed", "next"} {
		if _, err := s.StartRollout(ctx, Rollout{ID: id, Route: "api", Stable: "v1", Canary: "v2", StartedAt: now.Add(time.Duration(i) * time.Second)}, change("starts")); err != nil {
			t.Fatal(err)
		}
	}
	ros, err := s.Rollouts(ctx, 5)
	if err != nil || len(ros) != 2 || ros[0].ID != "next" || ros[0].FinishedAt != nil {
		t.Fatalf("rollouts %+v (%v)", ros, err)
	}
	if closed := ros[1]; closed.Outcome != OutcomeAborted || closed.Reason != "a newer rollout started while it was still open" || closed.FinishedAt == nil || !closed.FinishedAt.Equal(ros[0].StartedAt) {
		t.Fatalf("the rollout left open was closed as %+v", closed)
	}
}

func TestAbandonActiveRollouts(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	now := time.Now().UTC()
	_, _ = s.StartRollout(ctx, Rollout{ID: "done", Route: "api", Stable: "v1", Canary: "v2", StartedAt: now}, change("starts"))
	_, _ = s.EndRollout(ctx, RolloutEnd{ID: "done", Outcome: OutcomePromoted, At: now, Last: Step{StartedAt: now}}, nil)
	_, _ = s.StartRollout(ctx, Rollout{ID: "live", Route: "api", Stable: "v1", Canary: "v3", StartedAt: now}, change("starts"))
	ids, err := s.AbandonActive(ctx, "restart", now)
	if err != nil || len(ids) != 1 || ids[0] != "live" {
		t.Fatalf("abandoned %v %v", ids, err)
	}
}

func TestAChaosEventIsWrittenWithAnEmptyDetailWhenItHasNone(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	if err := s.LogChaos(ctx, ChaosEvent{At: time.Now().UTC(), Actor: ActorVisitor, Mode: "crash", Target: "dim-a", Action: "inject"}); err != nil {
		t.Fatal(err)
	}
	var n int
	var mode, detail string
	err := s.pool.QueryRow(ctx, `select count(*), min(mode), min(detail::text) from chaos_events`).Scan(&n, &mode, &detail)
	if err != nil || n != 1 || mode != "crash" || detail != "{}" {
		t.Fatalf("%d events, mode %q, detail %q: %v", n, mode, detail, err)
	}
}

func TestEveryListedRolloutGetsItsOwnSteps(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	start := time.Now().UTC().Truncate(time.Millisecond)
	for i, id := range []string{"ro-a", "ro-b", "ro-c"} {
		if _, err := s.StartRollout(ctx, Rollout{ID: id, Route: "api", Stable: "v1", Canary: "v2", StartedAt: start.Add(time.Duration(i) * time.Second)}, change("starts")); err != nil {
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

func TestAWriteThatWaitsTooLongIsCancelledByPostgresAndNeverLands(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	if _, err := s.StartRollout(ctx, Rollout{ID: "ro-held", Route: "api", Stable: "v1", Canary: "v2", StartedAt: time.Now().UTC()}, change("starts")); err != nil {
		t.Fatal(err)
	}
	count := func(query string) int {
		var n int
		if err := s.pool.QueryRow(ctx, query).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	for _, c := range []struct {
		name, hold, landed string
		write              func() error
	}{
		{"ending a rollout whose row is locked", `select 1 from rollouts where id = 'ro-held' for update`, `select count(*) from rollouts where finished_at is not null`, func() error {
			_, err := s.EndRollout(ctx, RolloutEnd{ID: "ro-held", Outcome: OutcomeAborted, Reason: "the row is locked", At: time.Now().UTC()}, nil)
			return err
		}},
		{"saving a config while its table is locked", `lock table config_versions in exclusive mode`, `select count(*) from config_versions`, func() error {
			_, err := s.SaveConfig(ctx, change("the table is locked"))
			return err
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			before := count(c.landed)
			tx, err := s.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, c.hold); err != nil {
				t.Fatal(err)
			}
			began := time.Now()
			err = c.write()
			took := time.Since(began)
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			if err == nil || took > callTimeout {
				t.Fatalf("took %v and came back as %v", took, err)
			}
			time.Sleep(200 * time.Millisecond)
			if after := count(c.landed); after != before {
				t.Fatalf("the write landed after it was reported as failed: %d rows, was %d", after, before)
			}
		})
	}
}
