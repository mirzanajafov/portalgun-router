package proxy

import (
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
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
	unrouted   prometheus.Counter
	gateway    atomic.Pointer[Gateway]
	inflight   *prometheus.Desc
	latency    *prometheus.Desc
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
		unrouted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "portal_unrouted_total",
			Help: "Requests that matched no route.",
		}),
		inflight: prometheus.NewDesc("portal_dimension_inflight", "Requests in flight to a dimension.", []string{"dimension", "version"}, nil),
		latency:  prometheus.NewDesc("portal_dimension_latency_seconds", "Peak EWMA latency the balancer sees for a dimension.", []string{"dimension", "version"}, nil),
	}
	m.Registry.MustRegister(m.requests, m.duration, m.noUpstream, m.clientGone, m.unrouted, m,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

func (m *Metrics) watch(g *Gateway) { m.gateway.Store(g) }

func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.inflight
	ch <- m.latency
}

func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	g := m.gateway.Load()
	if g == nil {
		return
	}
	for _, u := range g.State().Upstreams {
		ch <- prometheus.MustNewConstMetric(m.inflight, prometheus.GaugeValue, float64(u.Inflight), u.ID, u.Version)
		ch <- prometheus.MustNewConstMetric(m.latency, prometheus.GaugeValue, u.LatencyMs/1000, u.ID, u.Version)
	}
}

func (m *Metrics) forget(dimension string) {
	m.requests.DeletePartialMatch(prometheus.Labels{"dimension": dimension})
}

type routeMetrics struct {
	m          *Metrics
	route      string
	duration   prometheus.Observer
	noUpstream prometheus.Counter
	clientGone prometheus.Counter
	counters   sync.Map
}

func (m *Metrics) forRoute(route string) *routeMetrics {
	return &routeMetrics{
		m:          m,
		route:      route,
		duration:   m.duration.WithLabelValues(route),
		noUpstream: m.noUpstream.WithLabelValues(route),
		clientGone: m.clientGone.WithLabelValues(route),
	}
}

func (rm *routeMetrics) result(u *upstream, status int) {
	c, ok := rm.counters.Load(u)
	if !ok {
		var set [6]prometheus.Counter
		for i, name := range classes {
			set[i] = rm.m.requests.WithLabelValues(rm.route, u.id, u.version, name)
		}
		c, _ = rm.counters.LoadOrStore(u, &set)
	}
	c.(*[6]prometheus.Counter)[class(status)].Inc()
}
