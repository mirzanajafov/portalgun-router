package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/chaos"
	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
	"github.com/mirzanajafov/portalgun-router/internal/health"
	"github.com/mirzanajafov/portalgun-router/internal/store"
)

type Injector interface {
	Inject(ctx context.Context, f chaos.Fault) error
	Clear(ctx context.Context, f chaos.Fault) error
}

type ChaosLog interface {
	LogChaos(ctx context.Context, e store.ChaosEvent) error
}

type ChaosLimits struct {
	MaxDuration  time.Duration
	Default      time.Duration
	Cooldown     time.Duration
	PerHour      int64
	MaxLatency   time.Duration
	MaxErrorRate float64
}

func DefaultChaosLimits() ChaosLimits {
	return ChaosLimits{
		MaxDuration:  30 * time.Second,
		Default:      20 * time.Second,
		Cooldown:     2 * time.Minute,
		PerHour:      3,
		MaxLatency:   time.Second,
		MaxErrorRate: 0.5,
	}
}

type ChaosRequest struct {
	Mode        string  `json:"mode"`
	Target      string  `json:"target"`
	LatencyMs   float64 `json:"latencyMs,omitempty"`
	ErrorRate   float64 `json:"errorRate,omitempty"`
	DurationSec float64 `json:"durationSec,omitempty"`
}

type Running struct {
	Fault   chaos.Fault `json:"fault"`
	Actor   store.Actor `json:"actor"`
	Started time.Time   `json:"startedAt"`
	Until   time.Time   `json:"until"`
}

type Incident struct {
	Fault      chaos.Fault `json:"fault"`
	Actor      store.Actor `json:"actor"`
	InjectedAt time.Time   `json:"injectedAt"`
	ClearedAt  *time.Time  `json:"clearedAt,omitempty"`
	DetectedS  *float64    `json:"detectedAfterSec,omitempty"`
	MovedS     *float64    `json:"movedAfterSec,omitempty"`
	Lost       int64       `json:"lostRequests"`
	Closed     bool        `json:"closed"`
}

var (
	ErrChaosBusy     = errors.New("another fault is running")
	ErrChaosCooldown = errors.New("the portal gun is cooling down")
	ErrChaosLimit    = errors.New("that is enough chaos from this address for an hour")
	ErrChaosFailed   = errors.New("the fault could not be started")
)

type ChaosService struct {
	inj        Injector
	store      ChaosLog
	chaosLimit *visitorLimit
	notify     func(Event)
	limits     ChaosLimits
	targets    []string
	logger     *slog.Logger
	now        func() time.Time

	actions sync.Mutex
	timer   *time.Timer

	mu        sync.Mutex
	current   *Running
	cooldown  time.Time
	incidents []*Incident
	starting  *Incident
}

type ChaosOptions struct {
	Injector Injector
	Store    ChaosLog
	Counter  Counter
	Limits   ChaosLimits
	Targets  []string
	Notify   func(Event)
	Logger   *slog.Logger
}

func NewChaosService(o ChaosOptions) *ChaosService {
	return &ChaosService{
		inj:        o.Injector,
		store:      o.Store,
		chaosLimit: newVisitorLimit("chaos", o.Limits.PerHour, o.Counter, o.Logger),
		notify:     o.Notify,
		limits:     o.Limits,
		targets:    o.Targets,
		logger:     o.Logger,
		now:        time.Now,
	}
}

func (c *ChaosService) fault(req ChaosRequest, admin bool) (chaos.Fault, time.Duration, error) {
	f := chaos.Fault{Mode: chaos.Mode(req.Mode), Target: req.Target, Latency: time.Duration(req.LatencyMs * float64(time.Millisecond)), ErrorRate: req.ErrorRate}
	if f.Mode == chaos.Flapping {
		f.Period = 2 * time.Second
	}
	if !slices.Contains(c.targets, f.Target) {
		return f, 0, fmt.Errorf("unknown target %q", f.Target)
	}
	d := time.Duration(req.DurationSec * float64(time.Second))
	if d <= 0 {
		d = c.limits.Default
	}
	if !admin {
		switch {
		case d > c.limits.MaxDuration:
			return f, 0, fmt.Errorf("a visitor's fault lasts at most %v", c.limits.MaxDuration)
		case f.Latency > c.limits.MaxLatency:
			return f, 0, fmt.Errorf("a visitor can add at most %v of latency", c.limits.MaxLatency)
		case f.ErrorRate > c.limits.MaxErrorRate:
			return f, 0, fmt.Errorf("a visitor can fail at most %.0f%% of requests", c.limits.MaxErrorRate*100)
		}
	}
	return f, d, f.Validate()
}

func (c *ChaosService) Start(ctx context.Context, req ChaosRequest, admin bool, client string) (Running, time.Duration, error) {
	f, d, err := c.fault(req, admin)
	if err != nil {
		return Running{}, 0, err
	}
	c.actions.Lock()
	defer c.actions.Unlock()
	now := c.now()
	c.mu.Lock()
	current, cooldown := c.current, c.cooldown
	c.mu.Unlock()
	if current != nil {
		return Running{}, current.Until.Sub(now), ErrChaosBusy
	}
	actor := store.ActorAdmin
	if !admin {
		actor = store.ActorVisitor
		if wait := cooldown.Sub(now); wait > 0 {
			return Running{}, wait, ErrChaosCooldown
		}
	}
	inc := &Incident{Fault: f, Actor: actor, InjectedAt: now}
	c.setStarting(inc)
	defer c.setStarting(nil)
	if !admin {
		exceeded, err := c.chaosLimit.exceeded(ctx, client)
		if err != nil {
			return Running{}, 0, err
		}
		if exceeded {
			return Running{}, time.Hour, ErrChaosLimit
		}
	}
	if err := c.inj.Inject(ctx, f); err != nil {
		return Running{}, 0, fmt.Errorf("%w: %w", ErrChaosFailed, err)
	}
	run := Running{Fault: f, Actor: actor, Started: now, Until: now.Add(d)}
	c.mu.Lock()
	c.starting = nil
	c.current = &run
	c.incidents = append(c.incidents, inc)
	if len(c.incidents) > 20 {
		c.incidents = c.incidents[len(c.incidents)-20:]
	}
	c.mu.Unlock()
	c.timer = time.AfterFunc(d, func() { c.expire(run) })
	c.record(ctx, actor, f, "inject")
	c.emit(fmt.Sprintf("%s: %s on %s for %v", actor, f.Mode, f.Target, d))
	return run, 0, nil
}

func (c *ChaosService) setStarting(inc *Incident) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.starting = inc
}

func (c *ChaosService) running() *Running {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

func (c *ChaosService) expire(run Running) {
	c.actions.Lock()
	defer c.actions.Unlock()
	current := c.running()
	if current == nil || !current.Started.Equal(run.Started) {
		return
	}
	c.end(context.Background(), current, "the fault ran its course")
}

func (c *ChaosService) end(ctx context.Context, run *Running, why string) {
	if err := c.inj.Clear(ctx, run.Fault); err != nil {
		c.logger.Error("clearing a fault", "err", err)
	}
	now := c.now()
	c.mu.Lock()
	for _, inc := range c.incidents {
		if inc.InjectedAt.Equal(run.Started) {
			inc.ClearedAt = &now
		}
	}
	if run.Actor == store.ActorVisitor {
		c.cooldown = now.Add(c.limits.Cooldown)
	}
	c.current = nil
	c.mu.Unlock()
	c.record(ctx, run.Actor, run.Fault, "clear")
	c.emit(fmt.Sprintf("%s on %s cleared: %s", run.Fault.Mode, run.Fault.Target, why))
}

func (c *ChaosService) Stop(ctx context.Context) error {
	c.actions.Lock()
	defer c.actions.Unlock()
	run := c.running()
	if run == nil {
		return errors.New("no fault is running")
	}
	c.timer.Stop()
	c.end(ctx, run, "stopped by an admin")
	return nil
}

func (c *ChaosService) emit(msg string) {
	if c.notify != nil {
		c.notify(Event{Type: "chaos", Message: msg})
	}
}

func (c *ChaosService) record(ctx context.Context, actor store.Actor, f chaos.Fault, action string) {
	if c.store == nil {
		return
	}
	detail, _ := json.Marshal(f)
	if err := c.store.LogChaos(ctx, store.ChaosEvent{At: c.now().UTC(), Actor: actor, Mode: string(f.Mode), Target: f.Target, Action: action, Detail: detail}); err != nil {
		c.logger.Warn("logging a chaos event", "err", err)
	}
}

func (c *ChaosService) Observe(m Merged) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, inc := range c.incidents {
		inc.observe(m)
	}
	if c.starting != nil {
		c.starting.observe(m)
	}
}

func (inc *Incident) observe(m Merged) {
	start := time.Unix(m.Second, 0)
	if inc.Closed || start.Add(time.Second).Before(inc.InjectedAt) {
		return
	}
	i := slices.IndexFunc(m.Dimensions, func(d gatewayapi.DimensionSnapshot) bool { return d.ID == inc.Fault.Target })
	if i < 0 {
		return
	}
	d := m.Dimensions[i]
	after := start.Add(time.Second).Sub(inc.InjectedAt).Seconds()
	if inc.DetectedS == nil && (!d.Healthy || d.Outlier == health.Ejected) {
		inc.DetectedS = &after
	}
	if inc.MovedS == nil && (inc.ClearedAt == nil || !start.After(*inc.ClearedAt)) {
		var total, failed int64
		for _, r := range m.Routes {
			failed += r.Failed
		}
		for _, dim := range m.Dimensions {
			total += dim.Attempts
		}
		inc.Lost += failed
		if total > 0 && float64(d.Attempts) < 0.01*float64(total) {
			inc.MovedS = &after
		}
	}
	if inc.ClearedAt != nil && start.After(inc.ClearedAt.Add(10*time.Second)) {
		inc.Closed = true
	}
}

type ChaosView struct {
	Current   *Running   `json:"current"`
	Cooldown  float64    `json:"cooldownSec"`
	Targets   []string   `json:"targets"`
	Incidents []Incident `json:"incidents"`
}

func (c *ChaosService) View() ChaosView {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := ChaosView{Current: c.current, Targets: c.targets, Cooldown: max(0, c.cooldown.Sub(c.now()).Seconds())}
	for i := len(c.incidents) - 1; i >= 0; i-- {
		v.Incidents = append(v.Incidents, *c.incidents[i])
	}
	return v
}
