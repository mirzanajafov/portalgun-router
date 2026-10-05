package loadgen

import (
	"math"
	"strconv"

	"github.com/HdrHistogram/hdrhistogram-go"
)

const maxTrackedNanos = int64(120e9)

type Percentiles struct {
	Count int64   `json:"count"`
	Mean  float64 `json:"mean"`
	P50   float64 `json:"p50"`
	P90   float64 `json:"p90"`
	P99   float64 `json:"p99"`
	P999  float64 `json:"p999"`
	Max   float64 `json:"max"`
}

type CPU struct {
	Seconds     float64 `json:"seconds"`
	Cores       int     `json:"cores"`
	Utilization float64 `json:"utilization"`
}

type Summary struct {
	Label         string             `json:"label,omitempty"`
	URLs          []string           `json:"urls"`
	Method        string             `json:"method"`
	Rate          float64            `json:"rate"`
	Arrival       Arrival            `json:"arrival"`
	WarmupSec     float64            `json:"warmupSec"`
	WindowSec     float64            `json:"windowSec"`
	StartUnixNano int64              `json:"startUnixNano"`
	Scheduled     int64              `json:"scheduled"`
	Completed     int64              `json:"completed"`
	Failed        int64              `json:"failed"`
	Dropped       int64              `json:"dropped"`
	ThroughputRps float64            `json:"throughputRps"`
	Status        map[string]int64   `json:"status"`
	Errors        map[string]int64   `json:"errors,omitempty"`
	Dimensions    map[string]int64   `json:"dimensions,omitempty"`
	LatencyUs     Percentiles        `json:"latencyUs"`
	ServiceUs     Percentiles        `json:"serviceUs"`
	LagUs         Percentiles        `json:"lagUs"`
	CPU           *CPU               `json:"cpu,omitempty"`
	Watched       map[string]Watched `json:"watched,omitempty"`
	Valid         bool               `json:"valid"`
	Invalid       []string           `json:"invalid,omitempty"`
}

type stats struct {
	latency   *hdrhistogram.Histogram
	service   *hdrhistogram.Histogram
	lag       *hdrhistogram.Histogram
	scheduled int64
	completed int64
	failed    int64
	dropped   int64
	status    map[int]int64
	errors    map[ErrKind]int64
	dims      map[string]int64
}

func newStats() *stats {
	return &stats{
		latency: hdrhistogram.New(1, maxTrackedNanos, 3),
		service: hdrhistogram.New(1, maxTrackedNanos, 3),
		lag:     hdrhistogram.New(1, maxTrackedNanos, 3),
		status:  map[int]int64{},
		errors:  map[ErrKind]int64{},
		dims:    map[string]int64{},
	}
}

func record(h *hdrhistogram.Histogram, nanos int64) {
	_ = h.RecordValue(min(max(nanos, 1), maxTrackedNanos))
}

func (s *stats) add(r Record, dim string) {
	s.scheduled++
	switch r.Err {
	case ErrDropped:
		s.dropped++
		s.errors[r.Err]++
		return
	case ErrNone:
		s.completed++
		s.status[int(r.Status)]++
		if dim != "" {
			s.dims[dim]++
		}
		record(s.latency, r.Ended-r.Scheduled)
		if r.Connected > 0 {
			record(s.service, r.Ended-r.Connected)
		}
	default:
		s.failed++
		s.errors[r.Err]++
	}
	record(s.lag, r.Started-r.Scheduled)
}

func percentiles(h *hdrhistogram.Histogram) Percentiles {
	us := func(v float64) float64 { return math.Round(v/100) / 10 }
	return Percentiles{
		Count: h.TotalCount(),
		Mean:  us(h.Mean()),
		P50:   us(float64(h.ValueAtQuantile(50))),
		P90:   us(float64(h.ValueAtQuantile(90))),
		P99:   us(float64(h.ValueAtQuantile(99))),
		P999:  us(float64(h.ValueAtQuantile(99.9))),
		Max:   us(float64(h.Max())),
	}
}

func (s *stats) fill(sum *Summary) {
	sum.Scheduled = s.scheduled
	sum.Completed = s.completed
	sum.Failed = s.failed
	sum.Dropped = s.dropped
	sum.Status = map[string]int64{}
	for code, n := range s.status {
		sum.Status[strconv.Itoa(code)] = n
	}
	if len(s.errors) > 0 {
		sum.Errors = map[string]int64{}
		for k, n := range s.errors {
			sum.Errors[k.String()] = n
		}
	}
	if len(s.dims) > 0 {
		sum.Dimensions = s.dims
	}
	sum.LatencyUs = percentiles(s.latency)
	sum.ServiceUs = percentiles(s.service)
	sum.LagUs = percentiles(s.lag)
}
