package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const maxReplayBody = 64 << 10

var (
	errNoUpstream    = errors.New("no upstream available")
	errPerTryTimeout = errors.New("upstream did not answer in time")
	attemptHeaders   = func() [][]string {
		h := make([][]string, 8)
		for i := range h {
			h[i] = []string{strconv.Itoa(i)}
		}
		return h
	}()
)

func DefaultTransport(string) http.RoundTripper {
	return &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConnsPerHost: 1024,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
}

type bufferPool struct{ p sync.Pool }

func newBufferPool() *bufferPool {
	return &bufferPool{p: sync.Pool{New: func() any { b := make([]byte, 32<<10); return &b }}}
}

func (b *bufferPool) Get() []byte  { return *b.p.Get().(*[]byte) }
func (b *bufferPool) Put(v []byte) { b.p.Put(&v) }

func (g *Gateway) newReverseProxy(r *route) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = "http"
			pr.SetXForwarded()
		},
		Transport:    &attempt{g: g, r: r},
		BufferPool:   g.buffers,
		ErrorHandler: g.errorHandler(r),
		ErrorLog:     log.New(io.Discard, "", 0),
	}
}

func (g *Gateway) errorHandler(r *route) func(http.ResponseWriter, *http.Request, error) {
	return func(w http.ResponseWriter, req *http.Request, err error) {
		status := http.StatusBadGateway
		switch {
		case errors.Is(err, errNoUpstream):
			status = http.StatusServiceUnavailable
		case errors.Is(err, errPerTryTimeout), errors.Is(err, context.DeadlineExceeded):
			status = http.StatusGatewayTimeout
		case errors.Is(err, context.Canceled) && req.Context().Err() != nil:
			r.metrics.clientGone.Inc()
			return
		}
		w.WriteHeader(status)
	}
}

type failure int

const (
	failNone failure = iota
	failConnect
	failReset
	failTimeout
	failStatus
	failCanceled
	failOther
)

func classify(err error, status int, timedOut bool, client context.Context) failure {
	if err == nil {
		switch status {
		case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return failStatus
		}
		return failNone
	}
	var op *net.OpError
	switch {
	case client.Err() != nil:
		return failCanceled
	case timedOut:
		return failTimeout
	case errors.As(err, &op) && op.Op == "dial":
		return failConnect
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return failReset
	}
	return failOther
}

func (f failure) retryable(idempotent bool) bool {
	switch f {
	case failConnect:
		return true
	case failReset, failTimeout, failStatus, failOther:
		return idempotent
	}
	return false
}

func idempotent(req *http.Request) bool {
	switch req.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return true
	}
	return req.Header.Get("Idempotency-Key") != ""
}

func takeBody(req *http.Request) ([]byte, bool, error) {
	if req.Body == nil || req.Body == http.NoBody {
		return nil, true, nil
	}
	if req.ContentLength <= 0 || req.ContentLength > maxReplayBody {
		return nil, false, nil
	}
	buf := make([]byte, req.ContentLength)
	_, err := io.ReadFull(req.Body, buf)
	_ = req.Body.Close()
	if err != nil {
		return nil, false, err
	}
	return buf, true, nil
}

type attempt struct {
	g *Gateway
	r *route
}

func (a *attempt) RoundTrip(req *http.Request) (*http.Response, error) {
	g, r := a.g, a.r
	r.budget.Request(g.now())
	body, replayable, err := takeBody(req)
	if err != nil {
		return nil, err
	}
	limit := 1
	if replayable {
		limit += r.retries
	}
	safe := idempotent(req)

	var triedBuf [8]*upstream
	tried := triedBuf[:0]
	var resp *http.Response
	for {
		u, ok, panicking := r.pool.acquire(g.now(), tried)
		if panicking {
			r.metrics.panics.Inc()
		}
		if !ok {
			if len(tried) == 0 {
				r.metrics.noUpstream.Inc()
				return nil, errNoUpstream
			}
			return finish(resp, err, len(tried))
		}
		if len(tried) > 0 {
			discard(resp)
			r.metrics.retries.Inc()
		}
		tried = append(tried, u)
		var f failure
		resp, err, f = a.try(req, u, body)
		if f == failNone || len(tried) >= limit || !f.retryable(safe) {
			return finish(resp, err, len(tried))
		}
		if !r.budget.TryRetry(g.now()) {
			r.metrics.budgetExhausted.Inc()
			return finish(resp, err, len(tried))
		}
	}
}

func finish(resp *http.Response, err error, attempts int) (*http.Response, error) {
	if resp == nil {
		return nil, err
	}
	resp.Header[AttemptsHeader] = attemptHeaders[min(attempts, len(attemptHeaders)-1)]
	return resp, nil
}

func discard(resp *http.Response) {
	if resp != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		_ = resp.Body.Close()
	}
}

func (a *attempt) try(req *http.Request, u *upstream, body []byte) (*http.Response, error, failure) {
	g, r := a.g, a.r
	started := g.now()
	trial := u.outlier != nil && u.outlier.Begin()

	ctx, cancel := context.WithCancel(req.Context())
	var timedOut atomic.Bool
	timer := time.AfterFunc(r.perTry, func() {
		timedOut.Store(true)
		cancel()
	})
	out := req.WithContext(ctx)
	target := *req.URL
	target.Scheme = "http"
	target.Host = u.address
	out.URL = &target
	if body != nil {
		out.Body = io.NopCloser(bytes.NewReader(body))
		out.ContentLength = int64(len(body))
		out.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	}

	resp, err := u.rt.RoundTrip(out)
	if !timer.Stop() && err == nil {
		_ = resp.Body.Close()
		resp, err = nil, errPerTryTimeout
	}
	finished := g.now()
	status := 0
	if resp != nil {
		status = resp.StatusCode
	}
	f := classify(err, status, timedOut.Load(), req.Context())
	if u.outlier != nil && f != failCanceled {
		u.outlier.Record(finished, f != failNone, errors.Is(err, syscall.ECONNREFUSED), trial)
	}
	if err != nil {
		cancel()
		u.load.Release()
		if timedOut.Load() {
			err = errPerTryTimeout
		}
		r.metrics.result(u, 0)
		return nil, err, f
	}

	if f == failNone {
		u.load.Observe(time.Duration(finished-started), finished)
	}
	resp.Header[DimensionHeader] = u.idHeader
	r.metrics.result(u, resp.StatusCode)
	if resp.StatusCode == http.StatusSwitchingProtocols {
		u.load.Release()
		return resp, nil, f
	}
	resp.Body = &trackedBody{ReadCloser: resp.Body, release: u.load.Release, cancel: cancel}
	return resp, nil, f
}

type trackedBody struct {
	io.ReadCloser
	once    sync.Once
	release func()
	cancel  context.CancelFunc
}

func (b *trackedBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() {
		b.release()
		b.cancel()
	})
	return err
}
