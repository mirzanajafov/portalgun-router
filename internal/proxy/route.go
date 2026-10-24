package proxy

import (
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"slices"
	"strings"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/health"
	"github.com/mirzanajafov/portalgun-router/internal/httpx"
	"github.com/mirzanajafov/portalgun-router/internal/limit"
)

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
	return "ip:" + httpx.ClientIP(req, hops)
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

type table struct {
	version     int64
	config      *config.Config
	routes      []*route
	trustedHops int
	maxInflight int64
}

func (t *table) match(host, path string) *route {
	if strings.IndexByte(host, ':') >= 0 {
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
	}
	host = strings.ToLower(host)
	for _, r := range t.routes {
		if (r.host == "" || r.host == host) && strings.HasPrefix(path, r.prefix) {
			return r
		}
	}
	return nil
}

func (g *Gateway) buildRoutes(cfg *config.Config, pools map[string]*pool, limiters map[string]*routeLimiter) ([]*route, map[string]*routeStats) {
	routes := make([]*route, 0, len(cfg.Routes))
	stats := map[string]*routeStats{}
	for _, rc := range cfg.Routes {
		v := rc.Versions
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
			stable:        pools[rc.Pool].subset(v.Stable),
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
			r.limiter = l.Limiter
			r.keyHeader = rc.RateLimit.KeyHeader()
		}
		r.proxy = g.newReverseProxy(r, r.stable)
		if v.Canary != "" {
			r.canary = pools[rc.Pool].subset(v.Canary)
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
	return routes, stats
}
