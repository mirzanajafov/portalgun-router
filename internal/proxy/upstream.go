package proxy

import (
	"math"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync/atomic"

	"github.com/mirzanajafov/portalgun-router/internal/balance"
	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/health"
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
	id       string
	spec     upstreamSpec
	load     *balance.Load
	rt       http.RoundTripper
	idHeader []string
	outlier  *health.Outlier
	checker  *health.Checker
	readyAt  atomic.Int64
	counters upstreamCounters
}

func (u *upstream) Load() *balance.Load { return u.load }

func (u *upstream) healthy(now int64) bool {
	if u.checker != nil && !u.checker.Healthy() {
		return false
	}
	return u.outlier == nil || u.outlier.Eligible(now)
}

func (u *upstream) warm(now int64) bool {
	since, slowStart := now-u.readyAt.Load(), int64(u.spec.slowStart)
	if slowStart <= 0 || since >= slowStart {
		return true
	}
	return rand.Float64() < max(0.1, float64(since)/float64(slowStart))
}

func (u *upstream) stop() {
	if u.checker != nil {
		u.checker.Stop()
	}
}

type condition struct {
	healthy   bool
	outlier   health.State
	ejectedMs float64
}

func (u *upstream) condition(now int64) condition {
	c := condition{healthy: u.checker == nil || u.checker.Healthy(), outlier: health.Disabled}
	if u.outlier != nil {
		c.outlier = u.outlier.StateAt(now)
		if left := u.outlier.Until() - now; c.outlier == health.Ejected && left > 0 {
			c.ejectedMs = float64(left) / 1e6
		}
	}
	return c
}

func filterWarm(u *upstream, now int64) bool    { return u.healthy(now) && u.warm(now) }
func filterHealthy(u *upstream, now int64) bool { return u.healthy(now) }
func filterAny(*upstream, int64) bool           { return true }

var (
	normalFilters = []balance.Filter[*upstream]{filterWarm, filterHealthy}
	panicFilters  = []balance.Filter[*upstream]{filterAny}
)

type pool struct {
	config    config.Pool
	upstreams []*upstream
}

func (p *pool) subset(version string) *subset {
	s := &subset{balancer: newBalancer(p.config.Balancer), panic: *p.config.PanicThreshold}
	for _, u := range p.upstreams {
		if version == "" || u.spec.version == version {
			s.upstreams = append(s.upstreams, u)
		}
	}
	return s
}

func newBalancer(kind config.Balancer) balance.Balancer[*upstream] {
	switch kind {
	case config.BalancerRoundRobin:
		return &balance.RoundRobin[*upstream]{}
	case config.BalancerLeastOutstanding:
		return balance.LeastOutstanding[*upstream]{}
	}
	return balance.P2C[*upstream]{}
}

type subset struct {
	upstreams []*upstream
	balancer  balance.Balancer[*upstream]
	panic     float64
}

func (s *subset) panicking(now int64) bool {
	healthy := 0
	for _, u := range s.upstreams {
		if u.healthy(now) {
			healthy++
		}
	}
	return float64(healthy) < s.panic*float64(len(s.upstreams))
}

type tried struct {
	n  int
	at [8]*upstream
}

func (t *tried) add(u *upstream) { t.at[t.n], t.n = u, t.n+1 }

func excluding(f balance.Filter[*upstream], t tried) balance.Filter[*upstream] {
	return func(u *upstream, now int64) bool { return !slices.Contains(t.at[:t.n], u) && f(u, now) }
}

func (s *subset) acquire(now int64, t tried) (*upstream, bool) {
	panicking := s.panicking(now)
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
			u, found := s.balancer.Pick(s.upstreams, now, ok)
			if !found {
				break
			}
			if u.load.Acquire() {
				return u, panicking
			}
		}
	}
	return nil, panicking
}

func (g *Gateway) buildPools(cfg *config.Config) (map[string]*pool, map[string]*upstream) {
	pools := map[string]*pool{}
	upstreams := map[string]*upstream{}
	for _, pc := range cfg.Pools {
		p := &pool{config: pc}
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
				if existed && u.spec.address == d.Address {
					rt = u.rt
				} else {
					rt = g.transport(d.Address)
				}
				u = g.newUpstream(d.ID, spec, rt)
			}
			upstreams[d.ID] = u
			p.upstreams = append(p.upstreams, u)
		}
		pools[pc.Name] = p
	}
	return pools, upstreams
}

func (g *Gateway) retire(keep map[string]*upstream) {
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
}

func (g *Gateway) newUpstream(id string, spec upstreamSpec, rt http.RoundTripper) *upstream {
	u := &upstream{
		id:       id,
		spec:     spec,
		load:     balance.NewLoad(int64(spec.maxInflight), spec.ewmaDecay.D()),
		rt:       rt,
		idHeader: []string{id},
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
