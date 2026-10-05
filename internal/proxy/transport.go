package proxy

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"sync"
	"sync/atomic"
	"time"
)

var (
	errNoUpstream    = errors.New("no upstream available")
	errPerTryTimeout = errors.New("upstream did not answer in time")
	oneAttempt       = []string{"1"}
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

type attempt struct {
	g *Gateway
	r *route
}

func (a *attempt) RoundTrip(req *http.Request) (*http.Response, error) {
	g, r := a.g, a.r
	started := g.now()
	u, ok := r.pool.balancer.Pick(r.pool.upstreams, started)
	if !ok || !u.load.Acquire() {
		r.metrics.noUpstream.Inc()
		return nil, errNoUpstream
	}

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

	resp, err := u.rt.RoundTrip(out)
	if !timer.Stop() && err == nil {
		_ = resp.Body.Close()
		err = errPerTryTimeout
	}
	if err != nil {
		cancel()
		u.load.Release()
		if timedOut.Load() {
			err = errPerTryTimeout
		}
		r.metrics.result(u, 0)
		return nil, err
	}

	finished := g.now()
	u.load.Observe(time.Duration(finished-started), finished)
	resp.Header[DimensionHeader] = u.idHeader
	resp.Header[AttemptsHeader] = oneAttempt
	r.metrics.result(u, resp.StatusCode)
	if resp.StatusCode == http.StatusSwitchingProtocols {
		u.load.Release()
		return resp, nil
	}
	resp.Body = &trackedBody{ReadCloser: resp.Body, release: u.load.Release, cancel: cancel}
	return resp, nil
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
