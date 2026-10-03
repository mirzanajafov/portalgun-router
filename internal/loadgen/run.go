package loadgen

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DimensionHeader = "X-Dimension-Id"
	AttemptsHeader  = "X-Portal-Attempts"

	sleepThreshold = 3 * time.Millisecond
	spinWindow     = 2 * time.Millisecond

	maxLagP99         = time.Millisecond
	maxUtilization    = 0.8
	resultsBufferSize = 1 << 16
)

type Config struct {
	URLs        []string
	Method      string
	Header      http.Header
	BodyBytes   int
	Rate        float64
	Arrival     Arrival
	Duration    time.Duration
	Warmup      time.Duration
	Seed        uint64
	MaxConns    int
	MaxInflight int
	Timeout     time.Duration
	NoKeepAlive bool
	KeepRecords bool
	Label       string
}

func (c Config) normalize() (Config, []*url.URL, error) {
	if c.Method == "" {
		c.Method = http.MethodGet
	}
	if c.Arrival == "" {
		c.Arrival = Constant
	}
	if c.MaxInflight == 0 {
		c.MaxInflight = 20000
	}
	if c.Timeout == 0 {
		c.Timeout = 10 * time.Second
	}
	if len(c.URLs) == 0 {
		return c, nil, errors.New("at least one url is required")
	}
	urls := make([]*url.URL, 0, len(c.URLs))
	for _, raw := range c.URLs {
		u, err := url.Parse(raw)
		switch {
		case err != nil:
			return c, nil, fmt.Errorf("url: %w", err)
		case u.Scheme != "http" && u.Scheme != "https":
			return c, nil, fmt.Errorf("url %q must be http or https", raw)
		case u.Host == "":
			return c, nil, fmt.Errorf("url %q needs a host", raw)
		}
		urls = append(urls, u)
	}
	switch {
	case c.Duration <= 0:
		return c, nil, errors.New("duration must be positive")
	case c.Warmup < 0:
		return c, nil, errors.New("warmup must not be negative")
	case c.BodyBytes < 0:
		return c, nil, errors.New("body must not be negative")
	case c.MaxConns < 0 || c.MaxInflight < 0:
		return c, nil, errors.New("connection and in-flight limits must not be negative")
	}
	return c, urls, nil
}

type Result struct {
	Summary Summary
	Log     *Log
}

type sample struct {
	rec Record
	dim string
}

type generator struct {
	cfg      Config
	urls     []*url.URL
	body     []byte
	client   *http.Client
	start    time.Time
	timer    *time.Timer
	inflight atomic.Int64
	results  chan sample
}

func Run(ctx context.Context, cfg Config) (*Result, error) {
	cfg, urls, err := cfg.normalize()
	if err != nil {
		return nil, err
	}
	sched, err := NewSchedule(cfg.Rate, cfg.Arrival, cfg.Seed)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        0,
		MaxIdleConnsPerHost: max(cfg.MaxConns, 1024),
		MaxConnsPerHost:     cfg.MaxConns,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   cfg.NoKeepAlive,
		DisableCompression:  true,
	}
	defer transport.CloseIdleConnections()
	g := &generator{
		cfg:  cfg,
		urls: urls,
		client: &http.Client{
			Transport:     transport,
			Timeout:       cfg.Timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		results: make(chan sample, resultsBufferSize),
	}
	if cfg.BodyBytes > 0 {
		g.body = bytes.Repeat([]byte{'p'}, cfg.BodyBytes)
	}
	return g.run(ctx, sched), nil
}

func (g *generator) now() int64 { return int64(time.Since(g.start)) }

func (g *generator) run(ctx context.Context, sched *Schedule) *Result {
	warm := int64(g.cfg.Warmup)
	total := warm + int64(g.cfg.Duration)
	collected := make(chan *Result, 1)

	g.start = time.Now()
	g.timer = time.NewTimer(time.Hour)
	g.timer.Stop()
	go g.collect(warm, collected)

	var wg sync.WaitGroup
	measuring := false
	var windowStart int64
	var cpuStart time.Duration
	cpuOK := false
	for i := 0; ; i++ {
		at := sched.Next()
		if at >= total || !g.waitUntil(ctx, at) {
			break
		}
		u := g.urls[i%len(g.urls)]
		if !measuring && at >= warm {
			measuring = true
			windowStart = g.now()
			cpuStart, cpuOK = processCPU()
		}
		if g.inflight.Load() >= int64(g.cfg.MaxInflight) {
			now := g.now()
			g.results <- sample{rec: Record{Scheduled: at, Started: now, Ended: now, Err: ErrDropped}}
			continue
		}
		g.inflight.Add(1)
		wg.Go(func() { g.fire(ctx, u, at) })
	}
	windowEnd := g.now()
	cpuEnd, cpuEndOK := processCPU()
	wg.Wait()
	close(g.results)
	res := <-collected

	sum := &res.Summary
	sum.Label = g.cfg.Label
	sum.URLs = g.cfg.URLs
	sum.Method = g.cfg.Method
	sum.Rate = g.cfg.Rate
	sum.Arrival = g.cfg.Arrival
	sum.WarmupSec = g.cfg.Warmup.Seconds()
	sum.StartUnixNano = g.start.UnixNano()
	if res.Log != nil {
		res.Log.StartUnixNano = sum.StartUnixNano
	}
	if measuring {
		window := time.Duration(windowEnd - windowStart).Seconds()
		sum.WindowSec = round(window, 3)
		if window > 0 {
			sum.ThroughputRps = round(float64(sum.Completed)/window, 1)
		}
		if cpuOK && cpuEndOK && window > 0 {
			cores := runtime.GOMAXPROCS(0)
			used := (cpuEnd - cpuStart).Seconds()
			sum.CPU = &CPU{Seconds: round(used, 2), Cores: cores, Utilization: round(used/(window*float64(cores)), 3)}
		}
	}
	g.validate(sum)
	return res
}

func (g *generator) waitUntil(ctx context.Context, at int64) bool {
	for {
		d := time.Duration(at - g.now())
		if d <= 0 {
			return ctx.Err() == nil
		}
		if d <= sleepThreshold {
			runtime.Gosched()
			continue
		}
		g.timer.Reset(d - spinWindow)
		select {
		case <-g.timer.C:
		case <-ctx.Done():
			g.timer.Stop()
			return false
		}
	}
}

func (g *generator) newRequest(ctx context.Context, u *url.URL) *http.Request {
	req := &http.Request{
		Method:     g.cfg.Method,
		URL:        u,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     g.cfg.Header.Clone(),
		Host:       u.Host,
	}
	if req.Header == nil {
		req.Header = http.Header{}
	}
	if g.body != nil {
		req.Body = io.NopCloser(bytes.NewReader(g.body))
		req.ContentLength = int64(len(g.body))
	}
	return req.WithContext(ctx)
}

func (g *generator) fire(ctx context.Context, u *url.URL, scheduled int64) {
	defer g.inflight.Add(-1)
	rec := Record{Scheduled: scheduled, Started: g.now()}
	var connected atomic.Int64
	trace := &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { connected.CompareAndSwap(0, g.now()) }}
	var dim string
	resp, err := g.client.Do(g.newRequest(httptrace.WithClientTrace(ctx, trace), u))
	if err == nil {
		var n int64
		n, err = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		rec.Status = uint16(resp.StatusCode)
		rec.Bytes = uint32(min(n, math.MaxUint32))
		rec.Attempts = attempts(resp.Header.Get(AttemptsHeader))
		dim = resp.Header.Get(DimensionHeader)
	}
	rec.Ended = g.now()
	rec.Connected = connected.Load()
	rec.Err = classify(err)
	g.results <- sample{rec: rec, dim: dim}
}

func attempts(v string) uint8 {
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0
	}
	return uint8(min(n, math.MaxUint8))
}

func (g *generator) collect(warm int64, out chan<- *Result) {
	st := newStats()
	var lg *Log
	if g.cfg.KeepRecords {
		lg = &Log{Records: make([]Record, 0, int(min(g.cfg.Rate*(g.cfg.Warmup+g.cfg.Duration).Seconds()*1.05+1024, 1<<22)))}
	}
	index := map[string]uint8{}
	for s := range g.results {
		if lg != nil {
			s.rec.Dimension = dimensionIndex(index, lg, s.dim)
			lg.Records = append(lg.Records, s.rec)
		}
		if s.rec.Scheduled >= warm {
			st.add(s.rec, s.dim)
		}
	}
	res := &Result{Log: lg}
	st.fill(&res.Summary)
	out <- res
}

func dimensionIndex(index map[string]uint8, lg *Log, dim string) uint8 {
	if dim == "" {
		return 0
	}
	if i, ok := index[dim]; ok {
		return i
	}
	if len(lg.Dimensions) >= math.MaxUint8 {
		return math.MaxUint8
	}
	lg.Dimensions = append(lg.Dimensions, dim)
	i := uint8(len(lg.Dimensions))
	index[dim] = i
	return i
}

func (g *generator) validate(sum *Summary) {
	if lag := time.Duration(sum.LagUs.P99 * float64(time.Microsecond)); lag > maxLagP99 {
		sum.Invalid = append(sum.Invalid, fmt.Sprintf("generator lag p99 %v is above %v", lag, maxLagP99))
	}
	if sum.CPU != nil && sum.CPU.Utilization > maxUtilization {
		sum.Invalid = append(sum.Invalid, fmt.Sprintf("generator used %.0f%% of its CPUs", sum.CPU.Utilization*100))
	}
	if sum.Dropped > 0 {
		sum.Invalid = append(sum.Invalid, fmt.Sprintf("%d requests dropped at the in-flight limit", sum.Dropped))
	}
	if sum.WindowSec == 0 {
		sum.Invalid = append(sum.Invalid, "the run ended before the measurement window")
	}
	sum.Valid = len(sum.Invalid) == 0
}

func round(v float64, digits int) float64 {
	p := math.Pow(10, float64(digits))
	return math.Round(v*p) / p
}
