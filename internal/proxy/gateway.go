package proxy

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/balance"
	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/health"
	"github.com/mirzanajafov/portalgun-router/internal/limit"
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
	counters  upstreamCounters
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

type subset struct {
	upstreams []*upstream
	balancer  balance.Balancer[*upstream]
	panic     float64
}

func (p *subset) panicking(now int64) bool {
	healthy := 0
	for _, u := range p.upstreams {
		if u.healthy(now) {
			healthy++
		}
	}
	return float64(healthy) < p.panic*float64(len(p.upstreams))
}

type tried struct {
	n  int
	at [8]*upstream
}

func (t *tried) add(u *upstream) { t.at[t.n], t.n = u, t.n+1 }

func excluding(f balance.Filter[*upstream], t tried) balance.Filter[*upstream] {
	return func(u *upstream, now int64) bool { return !slices.Contains(t.at[:t.n], u) && f(u, now) }
}

func (p *subset) acquire(now int64, t tried) (*upstream, bool, bool) {
	panicking := p.panicking(now)
	filters := normalFilters
	if panicking {
		filters = panicFilters
	}
	for _, f := range filters {
		ok := f
		if t.n > 0 {
			ok = excluding(f, t)
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
	name          string
	host          string
	prefix        string
	stable        *subset
	canary        *subset
	canaryVersion string
	canaryCut     uint64
	rollout       string
	weight        float64
	stableVersion string
	timeout       time.Duration
	perTry        time.Duration
	retries       int
	budget        *health.Budget
	limiter       limit.Limiter
	keyHeader     string
	proxy         *httputil.ReverseProxy
	canaryProxy   *httputil.ReverseProxy
	stats         *routeStats
	metrics       *routeMetrics
}

func keyFor(req *http.Request, header string, hops int) string {
	if header != "" {
		if v := req.Header.Get(header); v != "" {
			return "key:" + v
		}
	}
	return "ip:" + ClientIP(req, hops)
}

func (r *route) splitting() bool {
	return r.canary != nil && r.canaryCut != 0 && !r.stats.brakeOn.Load()
}

func (r *route) pick(key string, split bool) *httputil.ReverseProxy {
	if split && sticky(r.rollout, key) < r.canaryCut {
		return r.canaryProxy
	}
	return r.proxy
}

func ClientIP(req *http.Request, hops int) string {
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		host = req.RemoteAddr
	}
	if hops <= 0 {
		return host
	}
	values := req.Header["X-Forwarded-For"]
	seen := 0
	for i := len(values) - 1; i >= 0; i-- {
		rest := values[i]
		for rest != "" {
			part := rest
			if j := strings.LastIndexByte(rest, ','); j >= 0 {
				part, rest = rest[j+1:], rest[:j]
			} else {
				rest = ""
			}
			if p := strings.TrimSpace(part); p != "" {
				if seen++; seen == hops {
					return p
				}
			}
		}
	}
	return host
}

type routeLimiter struct {
	cfg config.RateLimit
	l   limit.Limiter
}

type snapshot struct {
	version     int64
	config      *config.Config
	routes      []*route
	trustedHops int
}

func (s *snapshot) match(host, path string) *route {
	if strings.IndexByte(host, ':') >= 0 {
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
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
	Metrics    *Metrics
	Logger     *slog.Logger
	Transport  func(address string) http.RoundTripper
	Redis      limit.Pipeliner
	Instance   string
	ManualTick bool
	Decisions  int
}

type Gateway struct {
	snap      atomic.Pointer[snapshot]
	metrics   *Metrics
	logger    *slog.Logger
	transport func(address string) http.RoundTripper
	redis     limit.Pipeliner
	instance  string
	epoch     time.Time
	buffers   *bufferPool
	decisions *decisionLog

	cluster  atomic.Int64
	latest   atomic.Pointer[Snapshot]
	stopTick chan struct{}
	tickDone chan struct{}

	mu          sync.Mutex
	version     int64
	upstreams   map[string]*upstream
	limiters    map[string]*routeLimiter
	routeStats  map[string]*routeStats
	subscribers []func(Snapshot)
	closed      bool

	eventsMu sync.Mutex
	events   []DimensionEvent
}

const eventLog = 4096

type DimensionEvent struct {
	UnixNano  int64  `json:"unixNano"`
	Dimension string `json:"dimension"`
	State     string `json:"state"`
	Reason    string `json:"reason,omitempty"`
}

func (g *Gateway) logEvent(id string, at int64, state, reason string) {
	e := DimensionEvent{UnixNano: g.epoch.UnixNano() + at, Dimension: id, State: state, Reason: reason}
	g.eventsMu.Lock()
	defer g.eventsMu.Unlock()
	if len(g.events) >= eventLog {
		g.events = slices.Delete(g.events, 0, len(g.events)-eventLog+1)
	}
	g.events = append(g.events, e)
}

func (g *Gateway) Events(since int64) []DimensionEvent {
	g.eventsMu.Lock()
	defer g.eventsMu.Unlock()
	out := []DimensionEvent{}
	for _, e := range g.events {
		if e.UnixNano >= since {
			out = append(out, e)
		}
	}
	slices.SortStableFunc(out, func(a, b DimensionEvent) int { return cmp.Compare(a.UnixNano, b.UnixNano) })
	return out
}

func New(cfg *config.Config, opts Options) (*Gateway, error) {
	g := &Gateway{
		metrics:    opts.Metrics,
		logger:     opts.Logger,
		transport:  opts.Transport,
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
	if g.transport == nil {
		g.transport = DefaultTransport
	}
	g.metrics.watch(g)
	if err := g.Apply(cfg); err != nil {
		return nil, err
	}
	if opts.ManualTick {
		close(g.tickDone)
	} else {
		go g.tickLoop()
	}
	return g, nil
}

func (g *Gateway) SetClusterVersion(v int64) { g.cluster.Store(v) }

func (g *Gateway) ClusterVersion() int64 { return g.cluster.Load() }

func (g *Gateway) Latest() *Snapshot { return g.latest.Load() }

func (g *Gateway) Subscribe(fn func(Snapshot)) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.subscribers = append(g.subscribers, fn)
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

func (g *Gateway) Tick(second int64) Snapshot {
	now := g.now()
	s := g.snap.Load()
	snap := Snapshot{
		Instance:      g.instance,
		Second:        second,
		ConfigVersion: s.version,
		ClusterConfig: g.cluster.Load(),
		Routes:        map[string]RouteSnapshot{},
	}
	g.mu.Lock()
	for _, r := range s.routes {
		rs := r.stats.tick()
		rs.Stable, rs.Canary, rs.Weight, rs.Rollout = r.stableVersion, r.canaryVersion, r.weight, r.rollout
		snap.Routes[r.name] = rs
	}
	for _, u := range g.upstreams {
		lat := u.counters.latency.Drain()
		d := DimensionSnapshot{
			ID:       u.id,
			Version:  u.version,
			Attempts: u.counters.attempts.Swap(0),
			Errors:   u.counters.errors.Swap(0),
			P50Ms:    ms(lat.Quantile(0.5)),
			P99Ms:    ms(lat.Quantile(0.99)),
			Inflight: u.load.Inflight(),
			Healthy:  u.checker == nil || u.checker.Healthy(),
			Outlier:  "disabled",
		}
		if u.outlier != nil {
			d.Outlier = u.outlier.State().String()
			if left := u.outlier.Until() - now; d.Outlier == "ejected" && left > 0 {
				d.EjectedMs = float64(left) / 1e6
			}
		}
		snap.Dimensions = append(snap.Dimensions, d)
	}
	subs := slices.Clone(g.subscribers)
	g.mu.Unlock()
	slices.SortFunc(snap.Dimensions, func(a, b DimensionSnapshot) int { return strings.Compare(a.ID, b.ID) })
	g.latest.Store(&snap)
	for _, fn := range subs {
		fn(snap)
	}
	return snap
}

func (g *Gateway) now() int64 { return int64(time.Since(g.epoch)) }

func (g *Gateway) Metrics() *Metrics { return g.metrics }

func (g *Gateway) Version() int64 { return g.snap.Load().version }

func (g *Gateway) Config() *config.Config { return g.snap.Load().config }

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
		l.l.Close()
	}
}

func (g *Gateway) newLimiter(route string, c config.RateLimit) (limit.Limiter, error) {
	rule := limit.Rule{Rate: c.Rate, Burst: c.Burst}
	prefix := "pf:" + route + ":"
	switch c.Design {
	case config.LimitStaticSplit:
		return limit.NewStatic(rule, c.Instances, g.now), nil
	case config.LimitRedisExact:
		return limit.NewRedisExact(g.redis, prefix, rule, c.Instances, c.RedisTimeout.D(), g.now)
	case config.LimitLocalSync:
		return limit.NewLocalSync(g.redis, prefix, rule, c.Instances, g.instance, c.SyncInterval.D(), c.RedisTimeout.D(), g.now)
	case config.LimitLease:
		return limit.NewLease(g.redis, prefix, rule, c.Instances, c.LeaseTarget.D(), c.LeaseTTL.D(), c.RedisTimeout.D(), g.now)
	}
	return nil, fmt.Errorf("unknown rate limit design %q", c.Design)
}

func (g *Gateway) buildLimiters(cfg *config.Config) (map[string]*routeLimiter, error) {
	out := map[string]*routeLimiter{}
	fail := func(err error) (map[string]*routeLimiter, error) {
		for name, l := range out {
			if g.limiters[name] != l {
				l.l.Close()
			}
		}
		return nil, err
	}
	for _, rc := range cfg.Routes {
		c := rc.RateLimit
		if c == nil {
			continue
		}
		if c.UsesRedis() && g.redis == nil {
			return fail(fmt.Errorf("route %q: the %s rate limiter needs Redis", rc.Name, c.Design))
		}
		if old, ok := g.limiters[rc.Name]; ok && old.cfg == *c {
			out[rc.Name] = old
			continue
		}
		l, err := g.newLimiter(rc.Name, *c)
		if err != nil {
			return fail(err)
		}
		out[rc.Name] = &routeLimiter{cfg: *c, l: l}
	}
	return out, nil
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
	limiters, err := g.buildLimiters(cfg)
	if err != nil {
		return err
	}

	keep := map[string]*upstream{}
	type poolSpec struct {
		upstreams []*upstream
		balancer  string
		panic     float64
	}
	pools := map[string]*poolSpec{}
	abort := func(err error) error {
		for id, u := range keep {
			if g.upstreams[id] != u {
				u.stop()
			}
		}
		for name, l := range limiters {
			if g.limiters[name] != l {
				l.l.Close()
			}
		}
		return err
	}
	newSubset := func(ps *poolSpec, version string) (*subset, error) {
		bal, err := balance.New[*upstream](ps.balancer)
		if err != nil {
			return nil, err
		}
		s := &subset{balancer: bal, panic: ps.panic}
		for _, u := range ps.upstreams {
			if version == "" || u.version == version {
				s.upstreams = append(s.upstreams, u)
			}
		}
		return s, nil
	}
	for _, pc := range cfg.Pools {
		p := &poolSpec{balancer: pc.Balancer, panic: *pc.PanicThreshold}
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
	stats := map[string]*routeStats{}
	for _, rc := range cfg.Routes {
		v := rc.Versions
		stable, err := newSubset(pools[rc.Pool], v.Stable)
		if err != nil {
			return abort(err)
		}
		rs, ok := g.routeStats[rc.Name]
		if !ok {
			rs = newRouteStats()
		}
		rs.resetBrake(v.Rollout)
		stats[rc.Name] = rs
		r := &route{
			name:          rc.Name,
			host:          rc.Host,
			prefix:        rc.PathPrefix,
			stable:        stable,
			stableVersion: v.Stable,
			canaryVersion: v.Canary,
			weight:        v.Weight,
			canaryCut:     uint64(math.Round(v.Weight * 10000)),
			rollout:       v.Rollout,
			timeout:       rc.Timeout.D(),
			perTry:        rc.PerTryTimeout.D(),
			retries:       *rc.Retries.Max,
			budget:        health.NewBudget(rc.Retries.BudgetRatio, rc.Retries.MinPerSecond),
			stats:         rs,
			metrics:       g.metrics.forRoute(rc.Name),
		}
		if l, ok := limiters[rc.Name]; ok {
			r.limiter = l.l
			r.keyHeader, _ = strings.CutPrefix(rc.RateLimit.Key, "header:")
			if rc.RateLimit.Key == "ip" {
				r.keyHeader = ""
			}
		}
		r.proxy = g.newReverseProxy(r, stable)
		if v.Canary != "" {
			if r.canary, err = newSubset(pools[rc.Pool], v.Canary); err != nil {
				return abort(err)
			}
			r.canaryProxy = g.newReverseProxy(r, r.canary)
		}
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
	g.snap.Store(&snapshot{version: g.version, config: cfg, routes: routes, trustedHops: cfg.TrustedHops})
	for name, l := range g.limiters {
		if limiters[name] != l {
			l.l.Close()
		}
	}
	g.limiters = limiters
	g.routeStats = stats
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
	g.logEvent(u.id, t.At, t.To.String(), t.Reason)
	g.logger.Info("dimension state", "dimension", u.id, "from", t.From.String(), "to", t.To.String(),
		"reason", t.Reason, "ejected_for", time.Duration(t.Until-t.At).String())
}

func (g *Gateway) onHealth(u *upstream, healthy bool) {
	now := g.now()
	state := "unhealthy"
	if healthy {
		state = "healthy"
		u.readyAt.Store(now)
	}
	g.metrics.transition(u.id, state)
	g.logEvent(u.id, now, state, "probes")
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
	rt.stats.requests.Add(1)
	split := rt.splitting()
	var key string
	if rt.limiter != nil || split {
		key = keyFor(r, rt.keyHeader, s.trustedHops)
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
