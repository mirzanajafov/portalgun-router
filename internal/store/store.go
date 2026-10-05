package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var migrations = []string{
	`create table config_versions (
		version bigserial primary key,
		created_at timestamptz not null default now(),
		author text not null,
		reason text not null,
		body jsonb not null
	)`,
	`create table rollouts (
		id text primary key,
		route text not null,
		stable text not null,
		canary text not null,
		started_at timestamptz not null default now(),
		finished_at timestamptz,
		outcome text,
		reason text
	)`,
	`create table rollout_steps (
		rollout_id text not null references rollouts(id) on delete cascade,
		step int not null,
		weight double precision not null,
		started_at timestamptz not null,
		ended_at timestamptz,
		decision text,
		reason text,
		llr_errors double precision not null default 0,
		llr_slow double precision not null default 0,
		stable_requests bigint not null default 0,
		canary_requests bigint not null default 0,
		stable_errors bigint not null default 0,
		canary_errors bigint not null default 0,
		primary key (rollout_id, step)
	)`,
	`create table chaos_events (
		id bigserial primary key,
		at timestamptz not null default now(),
		actor text not null,
		mode text not null,
		target text not null,
		action text not null,
		detail jsonb not null default '{}'
	)`,
	`create index rollouts_started on rollouts (started_at desc)`,
}

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) migrate(ctx context.Context) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `select pg_advisory_xact_lock(4207)`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `create table if not exists schema_migrations (version int primary key, applied_at timestamptz not null default now())`); err != nil {
			return err
		}
		var applied int
		if err := tx.QueryRow(ctx, `select coalesce(max(version), 0) from schema_migrations`).Scan(&applied); err != nil {
			return err
		}
		for i := applied; i < len(migrations); i++ {
			if _, err := tx.Exec(ctx, migrations[i]); err != nil {
				return fmt.Errorf("migration %d: %w", i+1, err)
			}
			if _, err := tx.Exec(ctx, `insert into schema_migrations (version) values ($1)`, i+1); err != nil {
				return err
			}
		}
		return nil
	})
}

type ConfigVersion struct {
	Version   int64           `json:"version"`
	CreatedAt time.Time       `json:"createdAt"`
	Author    string          `json:"author"`
	Reason    string          `json:"reason"`
	Body      json.RawMessage `json:"body,omitempty"`
}

func (s *Store) LatestConfig(ctx context.Context) (*ConfigVersion, error) {
	var c ConfigVersion
	err := s.pool.QueryRow(ctx, `select version, created_at, author, reason, body from config_versions order by version desc limit 1`).
		Scan(&c.Version, &c.CreatedAt, &c.Author, &c.Reason, &c.Body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &c, err
}

func (s *Store) SaveConfig(ctx context.Context, author, reason string, body json.RawMessage) (ConfigVersion, error) {
	c := ConfigVersion{Author: author, Reason: reason, Body: body}
	err := s.pool.QueryRow(ctx, `insert into config_versions (author, reason, body) values ($1, $2, $3) returning version, created_at`,
		author, reason, body).Scan(&c.Version, &c.CreatedAt)
	return c, err
}

func (s *Store) ConfigHistory(ctx context.Context, limit int) ([]ConfigVersion, error) {
	rows, err := s.pool.Query(ctx, `select version, created_at, author, reason from config_versions order by version desc limit $1`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (ConfigVersion, error) {
		var c ConfigVersion
		err := r.Scan(&c.Version, &c.CreatedAt, &c.Author, &c.Reason)
		return c, err
	})
}

type Rollout struct {
	ID         string     `json:"id"`
	Route      string     `json:"route"`
	Stable     string     `json:"stable"`
	Canary     string     `json:"canary"`
	StartedAt  time.Time  `json:"startedAt"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
	Outcome    string     `json:"outcome,omitempty"`
	Reason     string     `json:"reason,omitempty"`
	Steps      []Step     `json:"steps"`
}

type Step struct {
	Step           int        `json:"step"`
	Weight         float64    `json:"weight"`
	StartedAt      time.Time  `json:"startedAt"`
	EndedAt        *time.Time `json:"endedAt,omitempty"`
	Decision       string     `json:"decision,omitempty"`
	Reason         string     `json:"reason,omitempty"`
	LLRErrors      float64    `json:"llrErrors"`
	LLRSlow        float64    `json:"llrSlow"`
	StableRequests int64      `json:"stableRequests"`
	CanaryRequests int64      `json:"canaryRequests"`
	StableErrors   int64      `json:"stableErrors"`
	CanaryErrors   int64      `json:"canaryErrors"`
}

func (s *Store) StartRollout(ctx context.Context, r Rollout) error {
	_, err := s.pool.Exec(ctx, `insert into rollouts (id, route, stable, canary, started_at) values ($1, $2, $3, $4, $5)`,
		r.ID, r.Route, r.Stable, r.Canary, r.StartedAt)
	return err
}

func (s *Store) SaveStep(ctx context.Context, rollout string, st Step) error {
	_, err := s.pool.Exec(ctx, `insert into rollout_steps
		(rollout_id, step, weight, started_at, ended_at, decision, reason, llr_errors, llr_slow, stable_requests, canary_requests, stable_errors, canary_errors)
		values ($1, $2, $3, $4, $5, nullif($6, ''), nullif($7, ''), $8, $9, $10, $11, $12, $13)
		on conflict (rollout_id, step) do update set
		ended_at = excluded.ended_at, decision = excluded.decision, reason = excluded.reason,
		llr_errors = excluded.llr_errors, llr_slow = excluded.llr_slow,
		stable_requests = excluded.stable_requests, canary_requests = excluded.canary_requests,
		stable_errors = excluded.stable_errors, canary_errors = excluded.canary_errors`,
		rollout, st.Step, st.Weight, st.StartedAt, st.EndedAt, st.Decision, st.Reason, st.LLRErrors, st.LLRSlow,
		st.StableRequests, st.CanaryRequests, st.StableErrors, st.CanaryErrors)
	return err
}

func (s *Store) FinishRollout(ctx context.Context, id, outcome, reason string, at time.Time) error {
	tag, err := s.pool.Exec(ctx, `update rollouts set finished_at = $2, outcome = $3, reason = $4 where id = $1 and finished_at is null`, id, at, outcome, reason)
	if err == nil && tag.RowsAffected() == 0 {
		err = fmt.Errorf("rollout %s is not active", id)
	}
	return err
}

func (s *Store) Rollouts(ctx context.Context, limit int) ([]Rollout, error) {
	rows, err := s.pool.Query(ctx, `select id, route, stable, canary, started_at, finished_at, coalesce(outcome, ''), coalesce(reason, '')
		from rollouts order by started_at desc limit $1`, limit)
	if err != nil {
		return nil, err
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Rollout, error) {
		var ro Rollout
		err := r.Scan(&ro.ID, &ro.Route, &ro.Stable, &ro.Canary, &ro.StartedAt, &ro.FinishedAt, &ro.Outcome, &ro.Reason)
		return ro, err
	})
	if err != nil {
		return nil, err
	}
	for i := range out {
		if out[i].Steps, err = s.steps(ctx, out[i].ID); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *Store) steps(ctx context.Context, rollout string) ([]Step, error) {
	rows, err := s.pool.Query(ctx, `select step, weight, started_at, ended_at, coalesce(decision, ''), coalesce(reason, ''),
		llr_errors, llr_slow, stable_requests, canary_requests, stable_errors, canary_errors
		from rollout_steps where rollout_id = $1 order by step`, rollout)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Step, error) {
		var st Step
		err := r.Scan(&st.Step, &st.Weight, &st.StartedAt, &st.EndedAt, &st.Decision, &st.Reason,
			&st.LLRErrors, &st.LLRSlow, &st.StableRequests, &st.CanaryRequests, &st.StableErrors, &st.CanaryErrors)
		return st, err
	})
}

func (s *Store) AbandonActive(ctx context.Context, reason string, at time.Time) ([]string, error) {
	rows, err := s.pool.Query(ctx, `update rollouts set finished_at = $1, outcome = 'aborted', reason = $2 where finished_at is null returning id`, at, reason)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

type ChaosEvent struct {
	At     time.Time       `json:"at"`
	Actor  string          `json:"actor"`
	Mode   string          `json:"mode"`
	Target string          `json:"target"`
	Action string          `json:"action"`
	Detail json.RawMessage `json:"detail,omitempty"`
}

func (s *Store) LogChaos(ctx context.Context, e ChaosEvent) error {
	detail := e.Detail
	if len(detail) == 0 {
		detail = json.RawMessage(`{}`)
	}
	_, err := s.pool.Exec(ctx, `insert into chaos_events (at, actor, mode, target, action, detail) values ($1, $2, $3, $4, $5, $6)`,
		e.At, e.Actor, e.Mode, e.Target, e.Action, detail)
	return err
}

func (s *Store) ChaosEvents(ctx context.Context, limit int) ([]ChaosEvent, error) {
	rows, err := s.pool.Query(ctx, `select at, actor, mode, target, action, detail from chaos_events order by id desc limit $1`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (ChaosEvent, error) {
		var e ChaosEvent
		err := r.Scan(&e.At, &e.Actor, &e.Mode, &e.Target, &e.Action, &e.Detail)
		return e, err
	})
}
