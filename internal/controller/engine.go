package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/canary"
	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
	"github.com/mirzanajafov/portalgun-router/internal/health"
	"github.com/mirzanajafov/portalgun-router/internal/store"
)

type RolloutStore interface {
	StartRollout(ctx context.Context, r store.Rollout, c store.ConfigChange) (store.ConfigVersion, error)
	SaveStep(ctx context.Context, rollout string, st store.Step) error
	EndRollout(ctx context.Context, end store.RolloutEnd, c *store.ConfigChange) (store.ConfigVersion, error)
	AbandonActive(ctx context.Context, reason string, at time.Time) ([]string, error)
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
	outcome    store.Outcome
	reason     string
	versions   config.Versions
	decided    time.Time
	keepConfig bool
	log        everyMinute
	err        error
}

const settleEvery = 5 * time.Second

type Engine struct {
	opts   EngineOptions
	cfgs   *ConfigManager
	store  RolloutStore
	logger *slog.Logger
	notify func(Event)
	now    func() time.Time

	mu     sync.Mutex
	active *active
	status atomic.Pointer[Status]
}

func NewEngine(opts EngineOptions, cfgs *ConfigManager, s RolloutStore, logger *slog.Logger, notify func(Event)) *Engine {
	e := &Engine{opts: opts, cfgs: cfgs, store: s, logger: logger, notify: notify, now: time.Now}
	e.storeStatus()
	return e
}

var (
	ErrBusy          = errors.New("a rollout is already running")
	ErrNoRollout     = errors.New("no rollout is running")
	ErrRolloutFailed = errors.New("the rollout could not be started")
	ErrEndNotSaved   = errors.New("the rollout's end is decided but not saved yet, and it is tried again every few seconds")
)

func (e *Engine) Recover(ctx context.Context) error {
	abandoned, err := e.store.AbandonActive(ctx, "the controller restarted during the rollout", e.now().UTC())
	if err != nil {
		return err
	}
	cfg, _ := e.cfgs.Current()
	live := false
	for _, r := range cfg.Routes {
		live = live || (r.Versions.Canary != "" && r.Versions.Weight > 0)
	}
	if len(abandoned) == 0 && !live {
		return nil
	}
	e.logger.Warn("taking traffic off canaries left from before a restart", "rollouts", abandoned)
	_, err = e.cfgs.Update(ctx, store.ActorController, "restarted during a rollout: canary traffic set to zero", func(c *config.Config) error {
		for i := range c.Routes {
			v := &c.Routes[i].Versions
			if v.Canary != "" {
				v.Canary, v.Weight = "", 0
			}
		}
		return nil
	})
	return err
}

func (e *Engine) Start(ctx context.Context, route, canaryVersion string, author store.Actor) (store.Rollout, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	defer e.storeStatus()
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
	ro := store.Rollout{ID: id, Route: route, Stable: stable, Canary: canaryVersion, StartedAt: now}
	ctx = context.WithoutCancel(ctx)
	version, err := e.cfgs.commit(ctx, author, fmt.Sprintf("rollout %s starts: %s at %s on %s", id, canaryVersion, pct(weight), route), begin,
		func(ctx context.Context, c store.ConfigChange) (store.ConfigVersion, error) {
			return e.store.StartRollout(ctx, ro, c)
		})
	if errors.Is(err, ErrConfigNotSaved) {
		return store.Rollout{}, fmt.Errorf("%w: %w", ErrRolloutFailed, err)
	}
	if err != nil {
		return store.Rollout{}, err
	}
	e.active = &active{rollout: ro}
	e.beginStep(ctx, 0, version)
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
	st, seq := a.step, a.policy.Sequential()
	st.LLRErrors, st.LLRSlow = seq.Errors().LLR(), seq.Slow().LLR()
	return st
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
	defer e.storeStatus()
	a := e.active
	if a == nil {
		return
	}
	ctx = context.WithoutCancel(ctx)
	rs, ok := m.Routes[a.rollout.Route]
	current := ok && rs.Rollout == a.rollout.ID && m.MinConfig() >= a.stepVersion
	if s := a.settling; s != nil {
		if current && s.outcome == store.OutcomePromoted {
			a.countDead(m)
			if reason := e.broken(a, rs); reason != "" {
				e.finish(ctx, store.OutcomeRolledBack, reason)
			}
		}
		return
	}
	if !current {
		return
	}
	a.countDead(m)
	if reason := e.broken(a, rs); reason != "" {
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
	elapsed := time.Duration(a.seconds) * time.Second
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

func (a *active) countDead(m Merged) {
	if alive(m.Dimensions, a.rollout.Canary) || !alive(m.Dimensions, a.rollout.Stable) {
		a.dead = 0
		return
	}
	a.dead++
}

func (e *Engine) broken(a *active, rs gatewayapi.RouteSnapshot) string {
	switch {
	case rs.Brake:
		return "an instance's emergency brake stopped the canary"
	case a.dead >= e.opts.DeadSeconds:
		return fmt.Sprintf("no healthy dimension ran the canary for %d s while the stable version had one", a.dead)
	}
	return ""
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
		reason := "the next step's config was refused"
		if errors.Is(err, ErrConfigNotSaved) {
			reason = "the next step's config could not be saved"
		}
		e.finish(ctx, store.OutcomeAborted, reason)
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
			e.retry(context.WithoutCancel(ctx))
			return
		case <-t.C:
			e.retry(ctx)
		}
	}
}

func (e *Engine) retry(ctx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()
	defer e.storeStatus()
	if e.active != nil && e.active.settling != nil {
		e.trySettle(ctx)
	}
}

func (e *Engine) trySettle(ctx context.Context) {
	a := e.active
	s := a.settling
	if err := e.saveSettlement(ctx, a, s); err != nil {
		if s.log.allow(e.now()) {
			e.logger.Error("the rollout's end could not be saved, it is tried again every few seconds", "rollout", a.rollout.ID, "outcome", s.outcome, "err", err)
		}
		s.err = err
		return
	}
	e.active = nil
	e.emit(fmt.Sprintf("rollout %s %s: %s", a.rollout.ID, s.outcome, s.reason))
}

func (e *Engine) saveSettlement(ctx context.Context, a *active, s *settlement) error {
	end := store.RolloutEnd{ID: a.rollout.ID, Outcome: s.outcome, Reason: s.reason, At: s.decided, Last: a.record()}
	if !s.keepConfig {
		_, err := e.cfgs.commit(ctx, store.ActorController, fmt.Sprintf("rollout %s %s: %s", a.rollout.ID, s.outcome, s.reason), func(c *config.Config) error {
			r, _, err := findRoute(c, a.rollout.Route)
			if err != nil {
				return err
			}
			r.Versions = s.versions
			return nil
		}, func(ctx context.Context, c store.ConfigChange) (store.ConfigVersion, error) {
			return e.store.EndRollout(ctx, end, &c)
		})
		if err == nil || errors.Is(err, ErrConfigNotSaved) {
			return err
		}
		e.logger.Error("the rollout ends without changing the config", "rollout", a.rollout.ID, "outcome", s.outcome, "err", err)
		s.keepConfig = true
	}
	_, err := e.store.EndRollout(ctx, end, nil)
	return err
}

func (e *Engine) Abort(ctx context.Context, reason string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	defer e.storeStatus()
	a := e.active
	if a == nil {
		return ErrNoRollout
	}
	ctx = context.WithoutCancel(ctx)
	if s := a.settling; s == nil || s.outcome == store.OutcomePromoted {
		e.finish(ctx, store.OutcomeAborted, reason)
	} else {
		e.trySettle(ctx)
	}
	if e.active != nil {
		return fmt.Errorf("%w: %w", ErrEndNotSaved, e.active.settling.err)
	}
	return nil
}

type Status struct {
	Active    bool          `json:"active"`
	Rollout   store.Rollout `json:"rollout,omitzero"`
	Step      store.Step    `json:"step,omitzero"`
	Steps     []float64     `json:"steps"`
	DataSec   int           `json:"dataSec"`
	LLRErrors float64       `json:"llrErrors"`
	LLRSlow   float64       `json:"llrSlow"`
	Lower     float64       `json:"lower"`
	Upper     float64       `json:"upper"`
	Pending   store.Outcome `json:"pending,omitempty"`
}

func (e *Engine) Status() Status { return *e.status.Load() }

func (e *Engine) Busy() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.active != nil
}

func (e *Engine) storeStatus() {
	st := Status{Steps: e.opts.Steps}
	if a := e.active; a != nil {
		st.Active, st.Rollout, st.Step, st.DataSec = true, a.rollout, a.record(), a.seconds
		st.LLRErrors, st.LLRSlow = st.Step.LLRErrors, st.Step.LLRSlow
		st.Lower, st.Upper = a.policy.Sequential().Errors().Bounds()
		if a.settling != nil {
			st.Pending = a.settling.outcome
		}
	}
	e.status.Store(&st)
}

func (e *Engine) emit(msg string) {
	e.logger.Info(msg)
	if e.notify != nil {
		e.notify(Event{Type: "rollout", At: e.now().UTC(), Message: msg})
	}
}

func pct(w float64) string { return fmt.Sprintf("%g%%", w*100) }
