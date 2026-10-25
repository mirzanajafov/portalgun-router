package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
	"github.com/mirzanajafov/portalgun-router/internal/health"
	"github.com/mirzanajafov/portalgun-router/internal/limit"
)

type Options struct {
	Metrics   *Metrics
	Logger    *slog.Logger
	Redis     limit.Pipeliner
	Instance  string
	Decisions int
}

type Gateway struct {
	routing   atomic.Pointer[table]
	metrics   *Metrics
	logger    *slog.Logger
	transport func(address string) http.RoundTripper
	redis     limit.Pipeliner
	instance  string
	epoch     time.Time
	buffers   *bufferPool
	decisions *decisionLog
	events    eventLog
	inflight  atomic.Int64

	cluster  atomic.Int64
	latest   atomic.Pointer[gatewayapi.Snapshot]
	stopTick chan struct{}
	tickDone chan struct{}

	mu          sync.Mutex
	version     int64
	upstreams   map[string]*upstream
	limiters    map[string]*routeLimiter
	routeStats  map[string]*routeStats
	subscribers []func(gatewayapi.Snapshot)
	closed      bool
}

func New(cfg *config.Config, opts Options) (*Gateway, error) {
	g, err := newGateway(cfg, opts, defaultTransport)
	if err != nil {
		return nil, err
	}
	go g.tickLoop()
	return g, nil
}

func newGateway(cfg *config.Config, opts Options, transport func(address string) http.RoundTripper) (*Gateway, error) {
	g := &Gateway{
		metrics:    opts.Metrics,
		logger:     opts.Logger,
		transport:  transport,
		redis:      opts.Redis,
		instance:   opts.Instance,
		epoch:      time.Now(),
		buffers:    newBufferPool(),
		decisions:  newDecisionLog(opts.Decisions),
		upstreams:  map[string]*upstream{},
		limiters:   map[string]*routeLimiter{},
		routeStats: map[string]*routeStats{},
		stopTick:   make(chan struct{}),
		tickDone:   make(chan struct{}),
	}
	if g.instance == "" {
		g.instance, _ = os.Hostname()
	}
	if g.metrics == nil {
		g.metrics = NewMetrics()
	}
	if g.logger == nil {
		g.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	g.metrics.watch(g)
	if err := g.Apply(cfg); err != nil {
		return nil, err
	}
	return g, nil
}

func (g *Gateway) SetClusterVersion(v int64) { g.cluster.Store(v) }

func (g *Gateway) ClusterVersion() int64 { return g.cluster.Load() }

func (g *Gateway) Latest() *gatewayapi.Snapshot { return g.latest.Load() }

func (g *Gateway) Subscribe(fn func(gatewayapi.Snapshot)) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.subscribers = append(g.subscribers, fn)
}

func (g *Gateway) now() int64 { return int64(time.Since(g.epoch)) }

func (g *Gateway) Metrics() *Metrics { return g.metrics }

func (g *Gateway) Version() int64 { return g.routing.Load().version }

func (g *Gateway) Config() *config.Config { return g.routing.Load().config }

func (g *Gateway) Events(since int64) []gatewayapi.DimensionEvent { return g.events.since(since) }

func (g *Gateway) Apply(cfg *config.Config) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return errors.New("gateway is closed")
	}
	limiters, err := g.buildLimiters(cfg)
	if err != nil {
		return err
	}
	pools, upstreams := g.buildPools(cfg)
	routes, stats := g.buildRoutes(cfg, pools, limiters)
	g.retire(upstreams)
	g.upstreams = upstreams
	g.version++
	g.routing.Store(&table{version: g.version, config: cfg, routes: routes, trustedHops: cfg.TrustedHops, maxInflight: int64(*cfg.MaxInflight)})
	closeReplaced(g.limiters, limiters)
	g.limiters = limiters
	g.routeStats = stats
	return nil
}

func (g *Gateway) Close() {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return
	}
	g.closed = true
	g.mu.Unlock()
	close(g.stopTick)
	<-g.tickDone
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, u := range g.upstreams {
		u.stop()
	}
	for _, l := range g.limiters {
		l.Close()
	}
}

func (g *Gateway) onOutlier(u *upstream, t health.Transition) {
	if t.To == health.Closed {
		u.readyAt.Store(t.At)
	}
	state := gatewayapi.OutlierEvent(t.To)
	g.metrics.transition(u.id, state)
	g.logEvent(u.id, t.At, state, t.Reason)
	attrs := []any{"dimension", u.id, "from", t.From.String(), "to", t.To.String(), "reason", t.Reason}
	if t.To == health.Ejected {
		attrs = append(attrs, "ejected_for", time.Duration(t.Until-t.At).String())
	}
	g.logger.Info("dimension state", attrs...)
}

func (g *Gateway) onHealth(u *upstream, healthy bool) {
	now := g.now()
	state := gatewayapi.EventUnhealthy
	if healthy {
		state = gatewayapi.EventHealthy
		u.readyAt.Store(now)
	}
	g.metrics.transition(u.id, state)
	g.logEvent(u.id, now, state, "probes")
	g.logger.Info("dimension health", "dimension", u.id, "state", state)
}

func (g *Gateway) logEvent(id string, at int64, state gatewayapi.EventState, reason string) {
	g.events.add(gatewayapi.DimensionEvent{UnixNano: g.epoch.UnixNano() + at, Dimension: id, State: state, Reason: reason})
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t := g.routing.Load()
	if n := g.inflight.Add(1); t.maxInflight > 0 && n > t.maxInflight {
		g.inflight.Add(-1)
		g.metrics.shed.Inc()
		w.Header().Set("Retry-After", "1")
		http.Error(w, "overloaded", http.StatusServiceUnavailable)
		return
	}
	defer g.inflight.Add(-1)
	rt := t.match(r.Host, r.URL.Path)
	if rt == nil {
		g.metrics.unrouted.Inc()
		http.Error(w, "no route", http.StatusNotFound)
		return
	}
	rt.stats.requests.Add(1)
	split := rt.splitting()
	var key string
	if rt.limiter != nil || split {
		key = keyFor(r, rt.keyHeader, t.trustedHops)
	}
	if rt.limiter != nil {
		d := rt.limiter.Allow(r.Context(), key)
		g.decisions.add(key, d.Allowed)
		if d.Degraded {
			rt.metrics.limitDegraded.Inc()
		}
		if !d.Allowed {
			rt.stats.limited.Add(1)
			rt.metrics.limited.Inc()
			w.Header().Set("Retry-After", strconv.Itoa(max(1, int(math.Ceil(d.RetryAfter.Seconds())))))
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
	}
	began := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), rt.timeout)
	defer cancel()
	rt.pick(key, split).ServeHTTP(w, r.WithContext(ctx))
	rt.metrics.duration.Observe(time.Since(began).Seconds())
}

func (g *Gateway) tickLoop() {
	defer close(g.tickDone)
	for {
		next := time.Now().Truncate(time.Second).Add(time.Second)
		t := time.NewTimer(time.Until(next))
		select {
		case <-g.stopTick:
			t.Stop()
			return
		case <-t.C:
		}
		g.Tick(next.Unix() - 1)
	}
}

func (g *Gateway) Tick(second int64) gatewayapi.Snapshot {
	now := g.now()
	t := g.routing.Load()
	snap := gatewayapi.Snapshot{
		Instance:      g.instance,
		Second:        second,
		ConfigVersion: t.version,
		ClusterConfig: g.cluster.Load(),
		Routes:        map[string]gatewayapi.RouteSnapshot{},
	}
	g.mu.Lock()
	for _, r := range t.routes {
		rs := r.stats.tick()
		rs.Stable, rs.Canary, rs.Weight, rs.Rollout = r.stableVersion, r.canaryVersion, r.weight, r.rollout
		snap.Routes[r.name] = rs
	}
	for _, u := range g.upstreams {
		lat := u.counters.latency.Drain()
		c := u.condition(now)
		snap.Dimensions = append(snap.Dimensions, gatewayapi.DimensionSnapshot{
			ID:        u.id,
			Version:   u.spec.version,
			Attempts:  u.counters.attempts.Swap(0),
			Errors:    u.counters.errors.Swap(0),
			P50Ms:     ms(lat.Quantile(0.5)),
			P99Ms:     ms(lat.Quantile(0.99)),
			Inflight:  u.load.Inflight(),
			Healthy:   c.healthy,
			Outlier:   c.outlier,
			EjectedMs: c.ejectedMs,
		})
	}
	subs := slices.Clone(g.subscribers)
	g.mu.Unlock()
	slices.SortFunc(snap.Dimensions, func(a, b gatewayapi.DimensionSnapshot) int { return strings.Compare(a.ID, b.ID) })
	g.latest.Store(&snap)
	for _, fn := range subs {
		fn(snap)
	}
	return snap
}

func (g *Gateway) State() gatewayapi.State {
	now := g.now()
	g.mu.Lock()
	defer g.mu.Unlock()
	st := gatewayapi.State{Version: g.version, Inflight: g.inflight.Load(), MaxInflight: g.routing.Load().maxInflight}
	for _, u := range g.upstreams {
		c := u.condition(now)
		st.Upstreams = append(st.Upstreams, gatewayapi.Upstream{
			ID:          u.id,
			Version:     u.spec.version,
			Address:     u.spec.address,
			Inflight:    u.load.Inflight(),
			MaxInflight: u.load.MaxInflight(),
			LatencyMs:   ms(u.load.Latency()),
			Healthy:     c.healthy,
			Outlier:     c.outlier,
			EjectedMs:   c.ejectedMs,
		})
	}
	slices.SortFunc(st.Upstreams, func(a, b gatewayapi.Upstream) int { return strings.Compare(a.ID, b.ID) })
	return st
}
