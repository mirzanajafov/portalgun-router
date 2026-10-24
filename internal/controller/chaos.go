package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/chaos"
	"github.com/mirzanajafov/portalgun-router/internal/proxy"
	"github.com/mirzanajafov/portalgun-router/internal/store"
)

type Injector interface {
	Inject(ctx context.Context, f chaos.Fault) error
	Clear(ctx context.Context, f chaos.Fault) error
	ClearAll(ctx context.Context) error
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
	Actor   string      `json:"actor"`
	Started time.Time   `json:"startedAt"`
	Until   time.Time   `json:"until"`
}

type Incident struct {
	Fault      chaos.Fault `json:"fault"`
	Actor      string      `json:"actor"`
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
)

type ChaosService struct {
	inj      Injector
	log      ChaosLog
	visitors *visitorLimit
	hub      *Hub
	limits   ChaosLimits
	targets  []string
	logger   *slog.Logger
	now      func() time.Time
	trusted  TrustedProxy

	mu        sync.Mutex
	current   *Running
	timer     *time.Timer
	cooldown  time.Time
	incidents []*Incident
}

func NewChaosService(inj Injector, log ChaosLog, counter Counter, hub *Hub, limits ChaosLimits, targets []string, trusted TrustedProxy, logger *slog.Logger) *ChaosService {
	return &ChaosService{inj: inj, log: log, visitors: newVisitorLimit("chaos", limits.PerHour, counter, logger), hub: hub, limits: limits, targets: targets, trusted: trusted, logger: logger, now: time.Now}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if c.current != nil {
		return Running{}, c.current.Until.Sub(now), ErrChaosBusy
	}
	actor := "admin"
	if !admin {
		actor = "visitor"
		if wait := c.cooldown.Sub(now); wait > 0 {
			return Running{}, wait, ErrChaosCooldown
		}
		exceeded, err := c.visitors.exceeded(ctx, client)
		if err != nil {
			return Running{}, 0, err
		}
		if exceeded {
			return Running{}, time.Hour, ErrChaosLimit
		}
	}
	if err := c.inj.Inject(ctx, f); err != nil {
		return Running{}, 0, err
	}
	run := Running{Fault: f, Actor: actor, Started: now, Until: now.Add(d)}
	c.current = &run
	c.timer = time.AfterFunc(d, func() { c.expire(run) })
	c.incidents = append(c.incidents, &Incident{Fault: f, Actor: actor, InjectedAt: now})
	if len(c.incidents) > 20 {
		c.incidents = c.incidents[len(c.incidents)-20:]
	}
	c.record(ctx, actor, f, "inject")
	c.hub.Event(Event{Type: "chaos", Message: fmt.Sprintf("%s: %s on %s for %v", actor, f.Mode, f.Target, d)})
	return run, 0, nil
}

func (c *ChaosService) expire(run Running) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current == nil || c.current.Started != run.Started {
		return
	}
	c.clearLocked(context.Background(), "the fault ran its course")
}

func (c *ChaosService) clearLocked(ctx context.Context, why string) {
	run := c.current
	if err := c.inj.Clear(ctx, run.Fault); err != nil {
		c.logger.Error("clearing a fault", "err", err)
	}
	now := c.now()
	for _, inc := range c.incidents {
		if inc.InjectedAt == run.Started {
			inc.ClearedAt = &now
		}
	}
	if run.Actor == "visitor" {
		c.cooldown = now.Add(c.limits.Cooldown)
	}
	c.current = nil
	c.record(ctx, run.Actor, run.Fault, "clear")
	c.hub.Event(Event{Type: "chaos", Message: fmt.Sprintf("%s on %s cleared: %s", run.Fault.Mode, run.Fault.Target, why)})
}

func (c *ChaosService) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current == nil {
		return errors.New("no fault is running")
	}
	c.timer.Stop()
	c.clearLocked(ctx, "stopped by an admin")
	return nil
}

func (c *ChaosService) Reset(ctx context.Context) error {
	return c.inj.ClearAll(ctx)
}

func (c *ChaosService) record(ctx context.Context, actor string, f chaos.Fault, action string) {
	if c.log == nil {
		return
	}
	detail, _ := json.Marshal(f)
	if err := c.log.LogChaos(ctx, store.ChaosEvent{At: c.now().UTC(), Actor: actor, Mode: string(f.Mode), Target: f.Target, Action: action, Detail: detail}); err != nil {
		c.logger.Warn("logging a chaos event", "err", err)
	}
}

func (c *ChaosService) Observe(m Merged) {
	c.mu.Lock()
	defer c.mu.Unlock()
	start := time.Unix(m.Second, 0)
	var total, failed int64
	for _, r := range m.Routes {
		failed += r.Failed
	}
	byID := map[string]proxy.DimensionSnapshot{}
	for _, d := range m.Dimensions {
		total += d.Attempts
		byID[d.ID] = d
	}
	for _, inc := range c.incidents {
		if inc.Closed || start.Add(time.Second).Before(inc.InjectedAt) {
			continue
		}
		after := start.Add(time.Second).Sub(inc.InjectedAt).Seconds()
		d, ok := byID[inc.Fault.Target]
		if !ok {
			continue
		}
		if inc.DetectedS == nil && (!d.Healthy || d.Outlier == "ejected") {
			inc.DetectedS = &after
		}
		if inc.MovedS == nil && (inc.ClearedAt == nil || !start.After(*inc.ClearedAt)) {
			inc.Lost += failed
			if total > 0 && float64(d.Attempts) < 0.01*float64(total) {
				inc.MovedS = &after
			}
		}
		if inc.ClearedAt != nil && start.After(inc.ClearedAt.Add(10*time.Second)) {
			inc.Closed = true
		}
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

func (c *ChaosService) Routes(mux *http.ServeMux, admin func(http.HandlerFunc) http.HandlerFunc, token string) {
	mux.HandleFunc("GET /api/chaos", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, c.View()) })
	mux.HandleFunc("POST /api/chaos", func(w http.ResponseWriter, r *http.Request) {
		var req ChaosRequest
		if err := decode(w, r, &req); err != nil {
			return
		}
		run, wait, err := c.Start(r.Context(), req, Authorized(r, token), c.trusted.ClientIP(r))
		switch {
		case errors.Is(err, ErrChaosBusy), errors.Is(err, ErrChaosCooldown), errors.Is(err, ErrChaosLimit):
			w.Header().Set("Retry-After", fmt.Sprint(max(1, int(wait.Seconds()+0.5))))
			status := http.StatusTooManyRequests
			if errors.Is(err, ErrChaosBusy) {
				status = http.StatusConflict
			}
			writeJSON(w, status, map[string]any{"error": err.Error(), "retryAfterSec": wait.Seconds()})
		case errors.Is(err, ErrLimitUnchecked):
			uncounted(w)
		case err != nil:
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		default:
			writeJSON(w, http.StatusCreated, run)
		}
	})
	mux.HandleFunc("DELETE /api/chaos", admin(func(w http.ResponseWriter, r *http.Request) {
		if err := c.Stop(r.Context()); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, c.View())
	}))
}
