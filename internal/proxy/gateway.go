package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httputil"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/balance"
	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/health"
)

const (
	DimensionHeader = "X-Portal-Dimension"
	AttemptsHeader  = "X-Portal-Attempts"
)

type upstreamSpec struct {
	address     string
	version     string
	maxInflight int
	health      config.HealthCheck
	outlier     config.Outlier
	slowStart   config.Duration
	ewmaDecay   config.Duration
}

type upstream struct {
	id        string
	spec      upstreamSpec
	version   string
	address   string
	load      *balance.Load
	rt        http.RoundTripper
	idHeader  []string
	outlier   *health.Outlier
	checker   *health.Checker
	slowStart int64
	readyAt   atomic.Int64
}

func (u *upstream) Load() *balance.Load { return u.load }

func (u *upstream) healthy(now int64) bool {
	if u.checker != nil && !u.checker.Healthy() {
		return false
	}
	return u.outlier == nil || u.outlier.Eligible(now)
}

func (u *upstream) warm(now int64) bool {
	since := now - u.readyAt.Load()
	if u.slowStart <= 0 || since >= u.slowStart {
		return true
	}
	return rand.Float64() < max(0.1, float64(since)/float64(u.slowStart))
}

func (u *upstream) stop() {
	if u.checker != nil {
		u.checker.Stop()
	}
}

func filterWarm(u *upstream, now int64) bool    { return u.healthy(now) && u.warm(now) }
func filterHealthy(u *upstream, now int64) bool { return u.healthy(now) }
func filterAny(*upstream, int64) bool           { return true }

var (
	normalFilters = []balance.Filter[*upstream]{filterWarm, filterHealthy}
	panicFilters  = []balance.Filter[*upstream]{filterAny}
)

type pool struct {
	name      string
	upstreams []*upstream
	balancer  balance.Balancer[*upstream]
	panic     float64
}

func (p *pool) panicking(now int64) bool {
	healthy := 0
	for _, u := range p.upstreams {
		if u.healthy(now) {
			healthy++
		}
	}
	return float64(healthy) < p.panic*float64(len(p.upstreams))
}

func (p *pool) acquire(now int64, tried []*upstream) (*upstream, bool, bool) {
	panicking := p.panicking(now)
	filters := normalFilters
	if panicking {
		filters = panicFilters
	}
	for _, f := range filters {
		ok := f
		if len(tried) > 0 {
			ok = func(u *upstream, now int64) bool { return !slices.Contains(tried, u) && f(u, now) }
		}
		for range 3 {
			u, found := p.balancer.Pick(p.upstreams, now, ok)
			if !found {
				break
			}
			if u.load.Acquire() {
				return u, true, panicking
			}
		}
	}
	return nil, false, panicking
}

type route struct {
	name    string
	host    string
	prefix  string
	pool    *pool
	timeout time.Duration
	perTry  time.Duration
	retries int
	budget  *health.Budget
	proxy   *httputil.ReverseProxy
	metrics *routeMetrics
}

type snapshot struct {
	version int64
	config  *config.Config
	routes  []*route
}

func (s *snapshot) match(host, path string) *route {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.ToLower(host)
	for _, r := range s.routes {
		if (r.host == "" || r.host == host) && strings.HasPrefix(path, r.prefix) {
			return r
		}
	}
	return nil
}

type Options struct {
	Metrics   *Metrics
	Logger    *slog.Logger
	Transport func(address string) http.RoundTripper
}

type Gateway struct {
	snap      atomic.Pointer[snapshot]
	metrics   *Metrics
	logger    *slog.Logger
	transport func(address string) http.RoundTripper
	epoch     time.Time
	buffers   *bufferPool

	mu        sync.Mutex
	version   int64
	upstreams map[string]*upstream
	closed    bool
}

func New(cfg *config.Config, opts Options) (*Gateway, error) {
	g := &Gateway{
		metrics:   opts.Metrics,
		logger:    opts.Logger,
		transport: opts.Transport,
		epoch:     time.Now(),
		buffers:   newBufferPool(),
		upstreams: map[string]*upstream{},
	}
	if g.metrics == nil {
		g.metrics = NewMetrics()
	}
	if g.logger == nil {
		g.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if g.transport == nil {
		g.transport = DefaultTransport
	}
	g.metrics.watch(g)
	if err := g.Apply(cfg); err != nil {
		return nil, err
	}
	return g, nil
}

func (g *Gateway) now() int64 { return int64(time.Since(g.epoch)) }

func (g *Gateway) Metrics() *Metrics { return g.metrics }

func (g *Gateway) Version() int64 { return g.snap.Load().version }

func (g *Gateway) Config() *config.Config { return g.snap.Load().config }

func (g *Gateway) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	for _, u := range g.upstreams {
		u.stop()
	}
}

func (g *Gateway) Apply(cfg *config.Config) error {
	if cfg == nil {
		return errors.New("nil config")
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return errors.New("gateway is closed")
	}

	keep := map[string]*upstream{}
	pools := map[string]*pool{}
	for _, pc := range cfg.Pools {
		bal, err := balance.New[*upstream](pc.Balancer)
		if err != nil {
			for id, u := range keep {
				if g.upstreams[id] != u {
					u.stop()
				}
			}
			return err
		}
		p := &pool{name: pc.Name, balancer: bal, panic: *pc.PanicThreshold}
		for _, d := range pc.Dimensions {
			spec := upstreamSpec{
				address:     d.Address,
				version:     d.Version,
				maxInflight: d.MaxInflight,
				health:      pc.HealthCheck,
				outlier:     pc.Outlier,
				slowStart:   *pc.SlowStart,
				ewmaDecay:   pc.EwmaDecay,
			}
			u, existed := g.upstreams[d.ID]
			if !existed || u.spec != spec {
				var rt http.RoundTripper
				if existed && u.address == d.Address {
					rt = u.rt
				} else {
					rt = g.transport(d.Address)
				}
				u = g.newUpstream(d.ID, spec, rt)
			}
			keep[d.ID] = u
			p.upstreams = append(p.upstreams, u)
		}
		pools[pc.Name] = p
	}

	routes := make([]*route, 0, len(cfg.Routes))
	for _, rc := range cfg.Routes {
		r := &route{
			name:    rc.Name,
			host:    rc.Host,
			prefix:  rc.PathPrefix,
			pool:    pools[rc.Pool],
			timeout: rc.Timeout.D(),
			perTry:  rc.PerTryTimeout.D(),
			retries: *rc.Retries.Max,
			budget:  health.NewBudget(rc.Retries.BudgetRatio, rc.Retries.MinPerSecond),
			metrics: g.metrics.forRoute(rc.Name),
		}
		r.proxy = g.newReverseProxy(r)
		routes = append(routes, r)
	}
	slices.SortStableFunc(routes, func(a, b *route) int {
		if (a.host == "") != (b.host == "") {
			if a.host != "" {
				return -1
			}
			return 1
		}
		return len(b.prefix) - len(a.prefix)
	})

	for id, u := range g.upstreams {
		next, kept := keep[id]
		if kept && next == u {
			continue
		}
		u.stop()
		if kept && next.rt == u.rt {
			continue
		}
		if t, ok := u.rt.(interface{ CloseIdleConnections() }); ok {
			t.CloseIdleConnections()
		}
		if !kept {
			g.metrics.forget(id)
		}
	}
	g.upstreams = keep
	g.version++
	g.snap.Store(&snapshot{version: g.version, config: cfg, routes: routes})
	return nil
}

func (g *Gateway) newUpstream(id string, spec upstreamSpec, rt http.RoundTripper) *upstream {
	u := &upstream{
		id:        id,
		spec:      spec,
		version:   spec.version,
		address:   spec.address,
		load:      balance.NewLoad(int64(spec.maxInflight), spec.ewmaDecay.D()),
		rt:        rt,
		idHeader:  []string{id},
		slowStart: int64(spec.slowStart),
	}
	u.readyAt.Store(math.MinInt64 / 2)
	if o := spec.outlier; !o.Disabled {
		u.outlier = health.NewOutlier(health.OutlierConfig{
			Consecutive:    o.Consecutive,
			Window:         o.Window,
			MinRequests:    o.MinRequests,
			FailureRatio:   o.FailureRatio,
			BaseEjection:   o.BaseEjection.D(),
			MaxEjection:    o.MaxEjection.D(),
			HalfOpenTrials: o.HalfOpenTrials,
			EjectOnRefused: o.EjectOnRefused,
		}, func(t health.Transition) { g.onOutlier(u, t) })
	}
	if h := spec.health; !h.Disabled {
		u.checker = health.StartChecker(spec.address, health.ProbeConfig{
			Path:           h.Path,
			Interval:       h.Interval.D(),
			Timeout:        h.Timeout.D(),
			UnhealthyAfter: h.UnhealthyAfter,
			HealthyAfter:   h.HealthyAfter,
		}, func(healthy bool) { g.onHealth(u, healthy) })
	}
	return u
}

func (g *Gateway) onOutlier(u *upstream, t health.Transition) {
	if t.To == health.Closed {
		u.readyAt.Store(t.At)
	}
	g.metrics.transition(u.id, t.To.String())
	g.logger.Info("dimension state", "dimension", u.id, "from", t.From.String(), "to", t.To.String(),
		"reason", t.Reason, "ejected_for", time.Duration(t.Until-t.At).String())
}

func (g *Gateway) onHealth(u *upstream, healthy bool) {
	state := "unhealthy"
	if healthy {
		state = "healthy"
		u.readyAt.Store(g.now())
	}
	g.metrics.transition(u.id, state)
	g.logger.Info("dimension health", "dimension", u.id, "state", state)
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s := g.snap.Load()
	rt := s.match(r.Host, r.URL.Path)
	if rt == nil {
		g.metrics.unrouted.Inc()
		http.Error(w, "no route", http.StatusNotFound)
		return
	}
	began := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), rt.timeout)
	defer cancel()
	rt.proxy.ServeHTTP(w, r.WithContext(ctx))
	rt.metrics.duration.Observe(time.Since(began).Seconds())
}

type Upstream struct {
	ID          string  `json:"id"`
	Version     string  `json:"version"`
	Address     string  `json:"address"`
	Inflight    int64   `json:"inflight"`
	MaxInflight int64   `json:"maxInflight"`
	LatencyMs   float64 `json:"latencyMs"`
	Healthy     bool    `json:"healthy"`
	Outlier     string  `json:"outlier"`
	EjectedMs   float64 `json:"ejectedForMs,omitempty"`
}

type State struct {
	Version   int64      `json:"version"`
	Upstreams []Upstream `json:"upstreams"`
}

func (g *Gateway) State() State {
	now := g.now()
	g.mu.Lock()
	defer g.mu.Unlock()
	st := State{Version: g.version}
	for _, u := range g.upstreams {
		v := Upstream{
			ID:          u.id,
			Version:     u.version,
			Address:     u.address,
			Inflight:    u.load.Inflight(),
			MaxInflight: u.load.MaxInflight(),
			LatencyMs:   float64(u.load.Latency().Microseconds()) / 1000,
			Healthy:     u.checker == nil || u.checker.Healthy(),
			Outlier:     "disabled",
		}
		if u.outlier != nil {
			v.Outlier = u.outlier.State().String()
			if left := u.outlier.Until() - now; v.Outlier == "ejected" && left > 0 {
				v.EjectedMs = float64(left) / 1e6
			}
		}
		st.Upstreams = append(st.Upstreams, v)
	}
	slices.SortFunc(st.Upstreams, func(a, b Upstream) int { return strings.Compare(a.ID, b.ID) })
	return st
}
