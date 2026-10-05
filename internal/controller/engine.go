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
	"github.com/mirzanajafov/portalgun-router/internal/proxy"
	"github.com/mirzanajafov/portalgun-router/internal/store"
)

type RolloutStore interface {
	StartRollout(ctx context.Context, r store.Rollout) error
	SaveStep(ctx context.Context, rollout string, st store.Step) error
	FinishRollout(ctx context.Context, id, outcome, reason string, at time.Time) error
}

type EngineOptions struct {
	Steps   []float64
	MaxStep time.Duration
	MinStep time.Duration
	Alpha   float64
	Beta    float64
	Rho     float64
}

func DefaultEngineOptions() EngineOptions {
	return EngineOptions{
		Steps:   []float64{0.01, 0.05, 0.25, 0.5},
		MaxStep: 5 * time.Minute,
		MinStep: 30 * time.Second,
		Alpha:   0.01,
		Beta:    0.05,
		Rho:     2,
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
}

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

var ErrBusy = errors.New("a rollout is already running")

func (e *Engine) Start(ctx context.Context, route, canaryVersion, author string) (store.Rollout, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active != nil {
		return store.Rollout{}, ErrBusy
	}
	cfg, _ := e.cfgs.Current()
	if cfg == nil {
		return store.Rollout{}, errors.New("no config loaded")
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
	version, err := e.cfgs.Update(ctx, author, fmt.Sprintf("rollout %s starts: %s at %s on %s", id, canaryVersion, pct(weight), route), func(c *config.Config) error {
		r, _, err := findRoute(c, route)
		if err != nil {
			return err
		}
		r.Versions = config.Versions{Stable: stable, Canary: canaryVersion, Weight: weight, Rollout: id}
		return nil
	})
	if err != nil {
		return store.Rollout{}, err
	}
	ro := store.Rollout{ID: id, Route: route, Stable: stable, Canary: canaryVersion, StartedAt: now}
	if err := e.store.StartRollout(ctx, ro); err != nil {
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

func (e *Engine) saveStep(ctx context.Context) {
	a := e.active
	seq := a.policy.Sequential()
	a.step.LLRErrors, a.step.LLRSlow = seq.Errors().LLR(), seq.Slow().LLR()
	if err := e.store.SaveStep(ctx, a.rollout.ID, a.step); err != nil {
		e.logger.Error("saving a rollout step", "err", err)
	}
}

func (e *Engine) Observe(ctx context.Context, m Merged) {
	e.mu.Lock()
	defer e.mu.Unlock()
	a := e.active
	if a == nil {
		return
	}
	rs, ok := m.Routes[a.rollout.Route]
	if !ok || rs.Rollout != a.rollout.ID || m.MinConfig() < a.stepVersion {
		return
	}
	if rs.Brake {
		e.finish(ctx, "rolled_back", "an instance's emergency brake stopped the canary")
		return
	}
	if !canaryAlive(m.Dimensions, a.rollout.Canary) {
		e.finish(ctx, "rolled_back", "no healthy dimension runs the canary")
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
		e.finish(ctx, "rolled_back", d.Reason)
	default:
		if a.seconds%10 == 0 {
			e.saveStep(ctx)
		}
	}
}

func canaryAlive(dims []proxy.DimensionSnapshot, version string) bool {
	seen := false
	for _, d := range dims {
		if d.Version != version {
			continue
		}
		seen = true
		if d.Healthy && d.Outlier != "ejected" {
			return true
		}
	}
	return !seen
}

func (e *Engine) closeStep(ctx context.Context, decision, reason string) {
	a := e.active
	ended := e.now().UTC()
	a.step.EndedAt = &ended
	a.step.Decision, a.step.Reason = decision, reason
	e.saveStep(ctx)
}

func (e *Engine) advance(ctx context.Context, reason string) {
	a := e.active
	e.closeStep(ctx, "promote", reason)
	next := a.step.Step + 1
	if next == len(e.opts.Steps) {
		e.settle(ctx, "promoted", reason, config.Versions{Stable: a.rollout.Canary, Rollout: a.rollout.ID})
		return
	}
	weight := e.opts.Steps[next]
	version, err := e.cfgs.Update(ctx, "controller", fmt.Sprintf("rollout %s: step %d, %s at %s", a.rollout.ID, next+1, a.rollout.Canary, pct(weight)), func(c *config.Config) error {
		r, _, err := findRoute(c, a.rollout.Route)
		if err != nil {
			return err
		}
		r.Versions.Weight = weight
		return nil
	})
	if err != nil {
		e.logger.Error("raising the canary weight", "err", err)
		e.finish(ctx, "aborted", "the next step's config could not be published: "+err.Error())
		return
	}
	e.beginStep(ctx, next, version)
	e.emit(fmt.Sprintf("rollout %s: %s promoted to %s (%s)", a.rollout.ID, a.rollout.Canary, pct(weight), reason))
}

func (e *Engine) finish(ctx context.Context, outcome, reason string) {
	a := e.active
	e.closeStep(ctx, "rollback", reason)
	e.settle(ctx, outcome, reason, config.Versions{Stable: a.rollout.Stable, Rollout: a.rollout.ID})
}

func (e *Engine) settle(ctx context.Context, outcome, reason string, v config.Versions) {
	a := e.active
	e.active = nil
	_, err := e.cfgs.Update(ctx, "controller", fmt.Sprintf("rollout %s %s: %s", a.rollout.ID, outcome, reason), func(c *config.Config) error {
		r, _, err := findRoute(c, a.rollout.Route)
		if err != nil {
			return err
		}
		r.Versions = v
		return nil
	})
	if err != nil {
		e.logger.Error("settling a rollout", "err", err)
	}
	if err := e.store.FinishRollout(ctx, a.rollout.ID, outcome, reason, e.now().UTC()); err != nil {
		e.logger.Error("recording the rollout outcome", "err", err)
	}
	e.emit(fmt.Sprintf("rollout %s %s: %s", a.rollout.ID, outcome, reason))
}

func (e *Engine) Abort(ctx context.Context, reason string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active == nil {
		return errors.New("no rollout is running")
	}
	e.finish(ctx, "aborted", reason)
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
	return st
}

func (e *Engine) emit(msg string) {
	e.logger.Info(msg)
	if e.notify != nil {
		e.notify(Event{Type: "rollout", At: e.now().UTC(), Message: msg})
	}
}

func pct(w float64) string { return fmt.Sprintf("%g%%", w*100) }
