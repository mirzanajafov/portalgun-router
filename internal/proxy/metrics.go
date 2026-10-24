package proxy

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/limit"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/redis/go-redis/v9"
)

var classes = [6]string{"error", "1xx", "2xx", "3xx", "4xx", "5xx"}

func class(status int) int {
	if c := status / 100; c >= 1 && c <= 5 {
		return c
	}
	return 0
}

type Metrics struct {
	Registry   *prometheus.Registry
	requests   *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	noUpstream *prometheus.CounterVec
	clientGone *prometheus.CounterVec
	cutOff     *prometheus.CounterVec
	retries    *prometheus.CounterVec
	exhausted  *prometheus.CounterVec
	panics     *prometheus.CounterVec
	changes    *prometheus.CounterVec
	limited    *prometheus.CounterVec
	degraded   *prometheus.CounterVec
	unrouted   prometheus.Counter
	shed       prometheus.Counter
	redisTrips prometheus.Counter
	redisCalls *prometheus.HistogramVec
	redisDials *prometheus.HistogramVec
	gateway    atomic.Pointer[Gateway]
	inflight   *prometheus.Desc
	latency    *prometheus.Desc
	held       *prometheus.Desc
}

func NewMetrics() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "portal_requests_total",
			Help: "Attempts by route, dimension, version and status class.",
		}, []string{"route", "dimension", "version", "class"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "portal_request_duration_seconds",
			Help:    "Time from receiving a request to finishing the response.",
			Buckets: prometheus.ExponentialBucketsRange(0.0001, 10, 16),
		}, []string{"route"}),
		noUpstream: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "portal_no_upstream_total",
			Help: "Requests refused because every dimension of the pool was unavailable.",
		}, []string{"route"}),
		clientGone: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "portal_client_gone_total",
			Help: "Requests the client abandoned before the upstream answered.",
		}, []string{"route"}),
		cutOff: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "portal_response_cut_off_total",
			Help: "Responses whose body broke off while it was copied from the dimension, after the status had gone out.",
		}, []string{"route"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "portal_retries_total",
			Help: "Attempts after the first one.",
		}, []string{"route"}),
		exhausted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "portal_retry_budget_exhausted_total",
			Help: "Retries skipped because the route's retry budget was spent.",
		}, []string{"route"}),
		panics: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "portal_panic_routing_total",
			Help: "Picks made while too little of the pool was healthy, ignoring health.",
		}, []string{"route"}),
		changes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "portal_dimension_transitions_total",
			Help: "Health and ejection state changes by dimension and new state.",
		}, []string{"dimension", "state"}),
		limited: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "portal_rate_limited_total",
			Help: "Requests refused with 429 by the route's rate limiter.",
		}, []string{"route"}),
		degraded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "portal_rate_limit_degraded_total",
			Help: "Rate limit decisions made by the local fallback because Redis was slow or down.",
		}, []string{"route"}),
		unrouted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "portal_unrouted_total",
			Help: "Requests that matched no route.",
		}),
		shed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "portal_shed_total",
			Help: "Requests refused with 503 because the gateway already held its maximum in flight.",
		}),
		redisTrips: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "portal_redis_round_trips_total",
			Help: "Commands and pipelines sent to Redis, each one a round trip.",
		}),
		redisCalls: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "portal_redis_call_seconds",
			Help:    "Time of a Redis command or pipeline as the gateway sees it, including the wait for a connection and any dial, by outcome.",
			Buckets: redisBuckets,
		}, []string{"outcome"}),
		redisDials: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "portal_redis_dial_seconds",
			Help:    "Time to open a new connection to Redis, by outcome.",
			Buckets: redisBuckets,
		}, []string{"outcome"}),
		inflight: prometheus.NewDesc("portal_dimension_inflight", "Requests in flight to a dimension.", []string{"dimension", "version"}, nil),
		latency:  prometheus.NewDesc("portal_dimension_latency_seconds", "Peak EWMA latency the balancer sees for a dimension.", []string{"dimension", "version"}, nil),
		held:     prometheus.NewDesc("portal_inflight", "Requests the gateway holds right now, across all routes.", nil, nil),
	}
	m.Registry.MustRegister(m.requests, m.duration, m.noUpstream, m.clientGone, m.cutOff, m.retries, m.exhausted, m.panics, m.changes, m.limited, m.degraded, m.unrouted, m.shed, m.redisTrips, m.redisCalls, m.redisDials, m,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

func (m *Metrics) watch(g *Gateway) { m.gateway.Store(g) }

func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.inflight
	ch <- m.latency
	ch <- m.held
}

func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	g := m.gateway.Load()
	if g == nil {
		return
	}
	st := g.State()
	ch <- prometheus.MustNewConstMetric(m.held, prometheus.GaugeValue, float64(st.Inflight))
	for _, u := range st.Upstreams {
		ch <- prometheus.MustNewConstMetric(m.inflight, prometheus.GaugeValue, float64(u.Inflight), u.ID, u.Version)
		ch <- prometheus.MustNewConstMetric(m.latency, prometheus.GaugeValue, u.LatencyMs/1000, u.ID, u.Version)
	}
}

func (m *Metrics) forget(dimension string) {
	m.requests.DeletePartialMatch(prometheus.Labels{"dimension": dimension})
	m.changes.DeletePartialMatch(prometheus.Labels{"dimension": dimension})
}

func (m *Metrics) transition(dimension, state string) {
	m.changes.WithLabelValues(dimension, state).Inc()
}

type routeMetrics struct {
	m               *Metrics
	route           string
	duration        prometheus.Observer
	noUpstream      prometheus.Counter
	clientGone      prometheus.Counter
	cutOff          prometheus.Counter
	retries         prometheus.Counter
	budgetExhausted prometheus.Counter
	panics          prometheus.Counter
	limited         prometheus.Counter
	limitDegraded   prometheus.Counter
	counters        sync.Map
}

func (m *Metrics) forRoute(route string) *routeMetrics {
	return &routeMetrics{
		m:               m,
		route:           route,
		duration:        m.duration.WithLabelValues(route),
		noUpstream:      m.noUpstream.WithLabelValues(route),
		clientGone:      m.clientGone.WithLabelValues(route),
		cutOff:          m.cutOff.WithLabelValues(route),
		retries:         m.retries.WithLabelValues(route),
		budgetExhausted: m.exhausted.WithLabelValues(route),
		panics:          m.panics.WithLabelValues(route),
		limited:         m.limited.WithLabelValues(route),
		limitDegraded:   m.degraded.WithLabelValues(route),
	}
}

func (rm *routeMetrics) result(u *upstream, status int) {
	c, ok := rm.counters.Load(u)
	if !ok {
		var set [6]prometheus.Counter
		for i, name := range classes {
			set[i] = rm.m.requests.WithLabelValues(rm.route, u.id, u.spec.version, name)
		}
		c, _ = rm.counters.LoadOrStore(u, &set)
	}
	c.(*[6]prometheus.Counter)[class(status)].Inc()
}

var redisBuckets = []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0015, 0.002, 0.003, 0.005, 0.01, 0.05, 0.25, 1}

func outcome(err error) string {
	switch {
	case err == nil || errors.Is(err, redis.Nil):
		return "ok"
	case limit.TimedOut(err):
		return "timeout"
	}
	return "error"
}

type redisHook struct {
	trips prometheus.Counter
	calls *prometheus.HistogramVec
	dials *prometheus.HistogramVec
}

func (h redisHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		began := time.Now()
		c, err := next(ctx, network, addr)
		h.dials.WithLabelValues(outcome(err)).Observe(time.Since(began).Seconds())
		return c, err
	}
}

func (h redisHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.trips.Inc()
		began := time.Now()
		err := next(ctx, cmd)
		h.calls.WithLabelValues(outcome(err)).Observe(time.Since(began).Seconds())
		return err
	}
}

func (h redisHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.trips.Inc()
		began := time.Now()
		err := next(ctx, cmds)
		h.calls.WithLabelValues(outcome(err)).Observe(time.Since(began).Seconds())
		return err
	}
}

func (m *Metrics) RedisHook() redis.Hook { return redisHook{m.redisTrips, m.redisCalls, m.redisDials} }

type countedLog struct{ c prometheus.Counter }

func (l countedLog) Write(p []byte) (int, error) {
	l.c.Inc()
	return len(p), nil
}
