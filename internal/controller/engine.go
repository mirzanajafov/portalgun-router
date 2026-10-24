package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/canary"
	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
	"github.com/mirzanajafov/portalgun-router/internal/health"
	"github.com/mirzanajafov/portalgun-router/internal/store"
)

type RolloutStore interface {
	StartRollout(ctx context.Context, r store.Rollout) error
	SaveStep(ctx context.Context, rollout string, st store.Step) error
	FinishRollout(ctx context.Context, id string, outcome store.Outcome, reason string, at time.Time) error
}

type EngineOptions struct {
	Steps       []float64
	MaxStep     time.Duration
	MinStep     time.Duration
	DeadSeconds int
	Alpha       float64
	Beta        float64
	Rho         float64
}

func DefaultEngineOptions() EngineOptions {
	return EngineOptions{
		Steps:       []float64{0.01, 0.05, 0.25, 0.5},
		MaxStep:     5 * time.Minute,
		MinStep:     30 * time.Second,
		DeadSeconds: 10,
		Alpha:       0.01,
		Beta:        0.05,
		Rho:         2,
	}
}

type Event struct {
	Type    string    `json:"type"`
	At      time.Time `json:"at"`
	Message string    `json:"message"`
}

type active struct {
	rollout     store.Rollout
	step        store.Step
	stepVersion int64
	policy      *canary.Hybrid
	seconds     int
	dead        int
	settling    *settlement
}

type settlement struct {
	outcome     store.Outcome
	reason      string
	versions    config.Versions
	decided     time.Time
	configSaved bool
	logged      time.Time
	err         error
}

const (
	storeTimeout = 2 * time.Second
	settleEvery  = 5 * time.Second
)

type Engine struct {
	opts   EngineOptions
	cfgs   *ConfigManager
	store  RolloutStore
	logger *slog.Logger
	notify func(Event)
	now    func() time.Time

	mu     sync.Mutex
	active *active
}

func NewEngine(opts EngineOptions, cfgs *ConfigManager, s RolloutStore, logger *slog.Logger, notify func(Event)) *Engine {
	return &Engine{opts: opts, cfgs: cfgs, store: s, logger: logger, notify: notify, now: time.Now}
}

var (
	ErrBusy          = errors.New("a rollout is already running")
	ErrNoRollout     = errors.New("no rollout is running")
	ErrRolloutFailed = errors.New("the rollout could not be started")
	ErrEndNotSaved   = errors.New("the rollout's end is decided but not saved yet, and it is tried again every few seconds")
	ErrPromoted      = errors.New("the rollout is already promoted, only its record is waiting to be saved")
)

func bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), storeTimeout)
}

func (e *Engine) Start(ctx context.Context, route, canaryVersion string, author store.Actor) (store.Rollout, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active != nil {
		return store.Rollout{}, ErrBusy
	}
	cfg, _ := e.cfgs.Current()
	if cfg == nil {
		return store.Rollout{}, fmt.Errorf("%w: no config loaded", ErrRolloutFailed)
	}
	rc, pc, err := findRoute(cfg, route)
	if err != nil {
		return store.Rollout{}, err
	}
	stable := rc.Versions.Stable
	if stable == "" {
		var others []string
		for _, d := range pc.Dimensions {
			if d.Version != canaryVersion && !slices.Contains(others, d.Version) {
				others = append(others, d.Version)
			}
		}
		if len(others) != 1 {
			return store.Rollout{}, fmt.Errorf("route %q needs an explicit stable version, the pool runs %v", route, others)
		}
		stable = others[0]
	}
	if canaryVersion == stable {
		return store.Rollout{}, fmt.Errorf("%s is already the stable version", canaryVersion)
	}
	now := e.now().UTC()
	id := "ro-" + now.Format("20060102-150405")
	weight := e.opts.Steps[0]
	begin := func(c *config.Config) error {
		r, _, err := findRoute(c, route)
		if err != nil {
			return err
		}
		r.Versions = config.Versions{Stable: stable, Canary: canaryVersion, Weight: weight, Rollout: id}
		return nil
	}
	if err := e.cfgs.Check(begin); err != nil {
		return store.Rollout{}, err
	}
	sctx, cancel := bounded(ctx)
	defer cancel()
	ro := store.Rollout{ID: id, Route: route, Stable: stable, Canary: canaryVersion, StartedAt: now}
	if err := e.store.StartRollout(sctx, ro); err != nil {
		return store.Rollout{}, fmt.Errorf("%w: %w", ErrRolloutFailed, err)
	}
	version, err := e.cfgs.Update(sctx, author, fmt.Sprintf("rollout %s starts: %s at %s on %s", id, canaryVersion, pct(weight), route), begin)
	if err != nil {
		reason, failed := "its config was refused", err
		if errors.Is(err, ErrConfigNotSaved) {
			reason, failed = "its config could not be saved", fmt.Errorf("%w: %w", ErrRolloutFailed, err)
		}
		fctx, fcancel := bounded(ctx)
		defer fcancel()
		if ferr := e.store.FinishRollout(fctx, id, store.OutcomeAborted, reason, e.now().UTC()); ferr != nil {
			e.logger.Error("closing a rollout that did not start", "rollout", id, "err", ferr)
		}
		return store.Rollout{}, failed
	}
	e.active = &active{rollout: ro}
	e.beginStep(sctx, 0, version)
	e.emit(fmt.Sprintf("rollout %s: %s at %s of %s", id, canaryVersion, pct(weight), route))
	return ro, nil
}

func (e *Engine) beginStep(ctx context.Context, step int, version int64) {
	a := e.active
	a.step = store.Step{Step: step, Weight: e.opts.Steps[step], StartedAt: e.now().UTC()}
	a.stepVersion = version
	a.policy = canary.NewHybrid(e.opts.Alpha, e.opts.Beta, e.opts.Rho, e.opts.MinStep)
	a.seconds = 0
	e.saveStep(ctx)
}

func (a *active) record() store.Step {
	seq := a.policy.Sequential()
	a.step.LLRErrors, a.step.LLRSlow = seq.Errors().LLR(), seq.Slow().LLR()
	return a.step
}

func (e *Engine) saveStep(ctx context.Context) {
	a := e.active
	if err := e.store.SaveStep(ctx, a.rollout.ID, a.record()); err != nil {
		e.logger.Error("saving a rollout step", "rollout", a.rollout.ID, "err", err)
	}
}

func (e *Engine) Observe(ctx context.Context, m Merged) {
	e.mu.Lock()
	defer e.mu.Unlock()
	a := e.active
	if a == nil {
		return
	}
	ctx, cancel := bounded(ctx)
	defer cancel()
	rs, ok := m.Routes[a.rollout.Route]
	current := ok && rs.Rollout == a.rollout.ID && m.MinConfig() >= a.stepVersion
	if s := a.settling; s != nil {
		if current && s.outcome == store.OutcomePromoted && !s.configSaved {
			if reason := e.broken(a, m, rs); reason != "" {
				e.finish(ctx, store.OutcomeRolledBack, reason)
			}
		}
		return
	}
	if !current {
		return
	}
	if reason := e.broken(a, m, rs); reason != "" {
		e.finish(ctx, store.OutcomeRolledBack, reason)
		return
	}
	bin := canary.Bin{
		Requests: [2]int64{rs.Versions[0].Attempts, rs.Versions[1].Attempts},
		Errors:   [2]int64{rs.Versions[0].Errors, rs.Versions[1].Errors},
		Slow:     [2]int64{rs.Versions[0].Slow, rs.Versions[1].Slow},
	}
	a.seconds++
	a.step.StableRequests += bin.Requests[0]
	a.step.CanaryRequests += bin.Requests[1]
	a.step.StableErrors += bin.Errors[0]
	a.step.CanaryErrors += bin.Errors[1]
	elapsed := e.now().Sub(a.step.StartedAt)
	d := a.policy.Observe(bin, elapsed)
	if d.Verdict == canary.Continue && elapsed >= e.opts.MaxStep {
		d = a.policy.Finish()
	}
	switch d.Verdict {
	case canary.Promote:
		e.advance(ctx, d.Reason)
	case canary.Rollback:
		e.finish(ctx, store.OutcomeRolledBack, d.Reason)
	default:
		if a.seconds%10 == 0 {
			e.saveStep(ctx)
		}
	}
}

func (e *Engine) broken(a *active, m Merged, rs gatewayapi.RouteSnapshot) string {
	if rs.Brake {
		return "an instance's emergency brake stopped the canary"
	}
	if alive(m.Dimensions, a.rollout.Canary) || !alive(m.Dimensions, a.rollout.Stable) {
		a.dead = 0
		return ""
	}
	a.dead++
	if a.dead < e.opts.DeadSeconds {
		return ""
	}
	return fmt.Sprintf("no healthy dimension ran the canary for %d s while the stable version had one", a.dead)
}

func alive(dims []gatewayapi.DimensionSnapshot, version string) bool {
	seen := false
	for _, d := range dims {
		if d.Version != version {
			continue
		}
		seen = true
		if d.Healthy && d.Outlier != health.Ejected {
			return true
		}
	}
	return !seen
}

func (e *Engine) closeStep(decision store.Decision, reason string) {
	a := e.active
	ended := e.now().UTC()
	a.step.EndedAt = &ended
	a.step.Decision, a.step.Reason = decision, reason
}

func (e *Engine) advance(ctx context.Context, reason string) {
	a := e.active
	e.closeStep(store.DecisionPromote, reason)
	next := a.step.Step + 1
	if next == len(e.opts.Steps) {
		e.settle(ctx, store.OutcomePromoted, reason, config.Versions{Stable: a.rollout.Canary, Rollout: a.rollout.ID})
		return
	}
	e.saveStep(ctx)
	weight := e.opts.Steps[next]
	version, err := e.cfgs.Update(ctx, store.ActorController, fmt.Sprintf("rollout %s: step %d, %s at %s", a.rollout.ID, next+1, a.rollout.Canary, pct(weight)), func(c *config.Config) error {
		r, _, err := findRoute(c, a.rollout.Route)
		if err != nil {
			return err
		}
		r.Versions.Weight = weight
		return nil
	})
	if err != nil {
		e.logger.Error("raising the canary weight", "rollout", a.rollout.ID, "err", err)
		e.finish(ctx, store.OutcomeAborted, "the next step's config could not be saved")
		return
	}
	e.beginStep(ctx, next, version)
	e.emit(fmt.Sprintf("rollout %s: %s promoted to %s (%s)", a.rollout.ID, a.rollout.Canary, pct(weight), reason))
}

func (e *Engine) finish(ctx context.Context, outcome store.Outcome, reason string) {
	a := e.active
	e.closeStep(store.DecisionRollback, reason)
	e.settle(ctx, outcome, reason, config.Versions{Stable: a.rollout.Stable, Rollout: a.rollout.ID})
}

func (e *Engine) settle(ctx context.Context, outcome store.Outcome, reason string, v config.Versions) {
	e.active.settling = &settlement{outcome: outcome, reason: reason, versions: v, decided: e.now().UTC()}
	e.trySettle(ctx)
}

func (e *Engine) Run(ctx context.Context) {
	t := time.NewTicker(settleEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.retry(ctx)
		}
	}
}

func (e *Engine) retry(ctx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active == nil || e.active.settling == nil {
		return
	}
	ctx, cancel := bounded(ctx)
	defer cancel()
	e.trySettle(ctx)
}

func (e *Engine) trySettle(ctx context.Context) {
	a := e.active
	s := a.settling
	if err := e.saveSettlement(ctx, a, s); err != nil {
		if now := e.now(); s.err == nil || now.Sub(s.logged) >= time.Minute {
			e.logger.Error("the rollout's end could not be saved, it is tried again every few seconds", "rollout", a.rollout.ID, "outcome", s.outcome, "err", err)
			s.logged = now
		}
		s.err = err
		return
	}
	e.active = nil
	e.emit(fmt.Sprintf("rollout %s %s: %s", a.rollout.ID, s.outcome, s.reason))
}

func (e *Engine) saveSettlement(ctx context.Context, a *active, s *settlement) error {
	if !s.configSaved {
		_, err := e.cfgs.Update(ctx, store.ActorController, fmt.Sprintf("rollout %s %s: %s", a.rollout.ID, s.outcome, s.reason), func(c *config.Config) error {
			r, _, err := findRoute(c, a.rollout.Route)
			if err != nil {
				return err
			}
			r.Versions = s.versions
			return nil
		})
		if errors.Is(err, ErrConfigNotSaved) {
			return err
		}
		if err != nil {
			e.logger.Error("the rollout ended without changing the config", "rollout", a.rollout.ID, "outcome", s.outcome, "err", err)
		}
		s.configSaved = true
	}
	if err := e.store.SaveStep(ctx, a.rollout.ID, a.record()); err != nil {
		return err
	}
	if err := e.store.FinishRollout(ctx, a.rollout.ID, s.outcome, s.reason, s.decided); err != nil && !errors.Is(err, store.ErrRolloutNotActive) {
		return err
	}
	return nil
}

func (e *Engine) Abort(ctx context.Context, reason string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	a := e.active
	if a == nil {
		return ErrNoRollout
	}
	ctx, cancel := bounded(ctx)
	defer cancel()
	switch s := a.settling; {
	case s == nil, s.outcome == store.OutcomePromoted && !s.configSaved:
		e.finish(ctx, store.OutcomeAborted, reason)
	case s.outcome == store.OutcomePromoted:
		return ErrPromoted
	default:
		e.trySettle(ctx)
	}
	if e.active != nil {
		return fmt.Errorf("%w: %w", ErrEndNotSaved, e.active.settling.err)
	}
	return nil
}

type Status struct {
	Active     bool          `json:"active"`
	Rollout    store.Rollout `json:"rollout,omitzero"`
	Step       store.Step    `json:"step,omitzero"`
	Steps      []float64     `json:"steps"`
	DataSec    int           `json:"dataSec"`
	ElapsedSec float64       `json:"elapsedSec"`
	LLRErrors  float64       `json:"llrErrors"`
	LLRSlow    float64       `json:"llrSlow"`
	Lower      float64       `json:"lower"`
	Upper      float64       `json:"upper"`
	Pending    store.Outcome `json:"pending,omitempty"`
}

func (e *Engine) Status() Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	st := Status{Steps: e.opts.Steps}
	a := e.active
	if a == nil {
		return st
	}
	seq := a.policy.Sequential()
	st.Active, st.Rollout, st.Step, st.DataSec = true, a.rollout, a.step, a.seconds
	st.ElapsedSec = e.now().Sub(a.step.StartedAt).Seconds()
	st.LLRErrors, st.LLRSlow = seq.Errors().LLR(), seq.Slow().LLR()
	st.Lower, st.Upper = seq.Errors().Bounds()
	if a.settling != nil {
		st.Pending = a.settling.outcome
	}
	return st
}

func (e *Engine) emit(msg string) {
	e.logger.Info(msg)
	if e.notify != nil {
		e.notify(Event{Type: "rollout", At: e.now().UTC(), Message: msg})
	}
}

func pct(w float64) string { return fmt.Sprintf("%g%%", w*100) }
