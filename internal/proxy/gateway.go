package proxy

import (
	"context"
	"errors"
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
)

const (
	DimensionHeader = "X-Portal-Dimension"
	AttemptsHeader  = "X-Portal-Attempts"
)

type upstream struct {
	id       string
	version  string
	address  string
	load     *balance.Load
	rt       http.RoundTripper
	idHeader []string
}

func (u *upstream) Load() *balance.Load { return u.load }

type pool struct {
	name      string
	upstreams []*upstream
	balancer  balance.Balancer[*upstream]
}

type route struct {
	name    string
	host    string
	prefix  string
	pool    *pool
	timeout time.Duration
	perTry  time.Duration
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
	Transport func(address string) http.RoundTripper
}

type Gateway struct {
	snap      atomic.Pointer[snapshot]
	metrics   *Metrics
	transport func(address string) http.RoundTripper
	epoch     time.Time
	buffers   *bufferPool

	mu        sync.Mutex
	version   int64
	upstreams map[string]*upstream
}

func New(cfg *config.Config, opts Options) (*Gateway, error) {
	g := &Gateway{
		metrics:   opts.Metrics,
		transport: opts.Transport,
		epoch:     time.Now(),
		buffers:   newBufferPool(),
		upstreams: map[string]*upstream{},
	}
	if g.metrics == nil {
		g.metrics = NewMetrics()
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

func (g *Gateway) Apply(cfg *config.Config) error {
	if cfg == nil {
		return errors.New("nil config")
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	keep := map[string]*upstream{}
	pools := map[string]*pool{}
	for _, pc := range cfg.Pools {
		bal, err := balance.New[*upstream](pc.Balancer)
		if err != nil {
			return err
		}
		p := &pool{name: pc.Name, balancer: bal}
		for _, d := range pc.Dimensions {
			old, existed := g.upstreams[d.ID]
			u := old
			if !existed || old.address != d.Address || old.version != d.Version || old.load.MaxInflight() != int64(d.MaxInflight) {
				var rt http.RoundTripper
				if existed && old.address == d.Address {
					rt = old.rt
				} else {
					rt = g.transport(d.Address)
				}
				u = &upstream{
					id:       d.ID,
					version:  d.Version,
					address:  d.Address,
					load:     balance.NewLoad(int64(d.MaxInflight)),
					rt:       rt,
					idHeader: []string{d.ID},
				}
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
}

type State struct {
	Version   int64      `json:"version"`
	Upstreams []Upstream `json:"upstreams"`
}

func (g *Gateway) State() State {
	g.mu.Lock()
	defer g.mu.Unlock()
	st := State{Version: g.version}
	for _, u := range g.upstreams {
		st.Upstreams = append(st.Upstreams, Upstream{
			ID:          u.id,
			Version:     u.version,
			Address:     u.address,
			Inflight:    u.load.Inflight(),
			MaxInflight: u.load.MaxInflight(),
			LatencyMs:   float64(u.load.Latency().Microseconds()) / 1000,
		})
	}
	slices.SortFunc(st.Upstreams, func(a, b Upstream) int { return strings.Compare(a.ID, b.ID) })
	return st
}
