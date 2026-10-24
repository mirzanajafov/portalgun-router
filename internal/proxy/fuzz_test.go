package proxy

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
)

var requestSeeds = []string{
	"GET / HTTP/1.1\r\nHost: portal.test\r\n\r\n",
	"GET /v1/items HTTP/1.1\r\nHost: api.portal.test\r\nX-Api-Key: rick\r\n\r\n",
	"POST /items HTTP/1.1\r\nHost: portal.test\r\nContent-Length: 5\r\n\r\nhello",
	"POST /items HTTP/1.1\r\nHost: portal.test\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
	"POST / HTTP/1.1\r\nHost: portal.test\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\nGET /smuggled HTTP/1.1\r\nHost: portal.test\r\n\r\n",
	"POST / HTTP/1.1\r\nHost: portal.test\r\nTransfer-Encoding: chunked\r\nContent-Length: 50\r\n\r\n3\r\nabc\r\n0\r\n\r\n",
	"POST / HTTP/1.1\r\nHost: portal.test\r\nContent-Length: 3\r\nContent-Length: 4\r\n\r\nabcd",
	"POST / HTTP/1.1\r\nHost: portal.test\r\nTransfer-Encoding: gzip, chunked\r\n\r\n0\r\n\r\n",
	"POST / HTTP/1.0\r\nHost: portal.test\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\n\r\n3\r\nabc\r\n0\r\n\r\n",
	"POST / HTTP/1.1\r\nHost: portal.test\r\nContent-Length: 99999999999\r\n\r\nshort",
	"PUT /items/1 HTTP/1.1\r\nHost: portal.test\r\nIdempotency-Key: k1\r\nContent-Length: 4\r\n\r\nbody",
	"GET / HTTP/1.1\r\nHost: portal.test\r\n\r\nGET /second HTTP/1.1\r\nHost: portal.test\r\n\r\n",
	"GET http://portal.test/absolute HTTP/1.1\r\nHost: other.test\r\n\r\n",
	"OPTIONS * HTTP/1.1\r\nHost: portal.test\r\n\r\n",
	"CONNECT portal.test:443 HTTP/1.1\r\nHost: portal.test:443\r\n\r\n",
	"GET /ws HTTP/1.1\r\nHost: portal.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: portal.test\r\nConnection: X-Secret, close\r\nX-Secret: hop\r\nTe: trailers\r\nKeep-Alive: 300\r\nProxy-Connection: keep-alive\r\nTrailer: X-Sum\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: portal.test\r\nX-Forwarded-For: 6.6.6.6, , 1.2.3.4\r\nX-Forwarded-For: 10.0.0.1\r\nForwarded: for=6.6.6.6\r\n\r\n",
	"GET /%2e%2e/%00/../etc HTTP/1.1\r\nHost: portal.test\r\n\r\n",
	"GET //double//slash HTTP/1.1\r\nHost: [::1]:8080\r\n\r\n",
	"GET / HTTP/1.1\nHost: portal.test\n\n",
	"GET / HTTP/1.1\r\nHost: portal.test\r\nX-Folded: a\r\n b\r\n\r\n",
	"POST / HTTP/1.1\r\nHost: portal.test\r\nExpect: 100-continue\r\nContent-Length: 2\r\n\r\nhi",
	"GET /limited HTTP/1.1\r\nHost: portal.test\r\n\r\nGET /limited HTTP/1.1\r\nHost: portal.test\r\n\r\n",
	"GET / HTTP/1.1\r\n\r\n",
	"GET / HTTP/2.0\r\nHost: portal.test\r\n\r\n",
	"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n",
}

func fuzzConfig(tb testing.TB, addrs []string) *config.Config {
	return parse(tb, config.Config{
		TrustedHops: 1,
		Routes: []config.Route{
			{Name: "api", Host: "api.portal.test", PathPrefix: "/v1/", Pool: "p",
				RateLimit: &config.RateLimit{Design: config.LimitStaticSplit, Rate: 1e9, Burst: 1e9, Key: "header:X-Api-Key"}},
			{Name: "limited", PathPrefix: "/limited", Pool: "p",
				RateLimit: &config.RateLimit{Design: config.LimitStaticSplit, Rate: 0.001, Burst: 1}},
			{Name: "all", PathPrefix: "/", Pool: "p",
				Versions:  config.Versions{Stable: "v1", Canary: "v2", Weight: 0.5, Rollout: "r1"},
				RateLimit: &config.RateLimit{Design: config.LimitStaticSplit, Rate: 1e9, Burst: 1e9}},
		},
		Pools: []config.Pool{{Name: "p", HealthCheck: config.HealthCheck{Disabled: true}, Dimensions: []config.Dimension{
			{ID: "a", Address: addrs[0]},
			{ID: "b", Address: addrs[1]},
			{ID: "c", Address: addrs[2], Version: "v2"},
		}}},
	})
}

type upstreamCheck struct {
	mu   sync.Mutex
	in   *http.Request
	errs []string
}

func (c *upstreamCheck) begin(in *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.in, c.errs = in, nil
}

func (c *upstreamCheck) failures() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.errs
}

func (c *upstreamCheck) failf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, fmt.Sprintf(format, args...))
}

var neverForwarded = []string{"Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Trailer", "Transfer-Encoding", "Forwarded"}

func (c *upstreamCheck) inspect(addr string, out *http.Request) {
	c.mu.Lock()
	in := c.in
	c.mu.Unlock()
	if out.URL.Scheme != "http" || out.URL.Host != addr {
		c.failf("sent to %s://%s through the transport for %s", out.URL.Scheme, out.URL.Host, addr)
	}
	for _, h := range neverForwarded {
		if _, ok := out.Header[h]; ok {
			c.failf("hop-by-hop header %s forwarded: %q", h, out.Header[h])
		}
	}
	if te, ok := out.Header["Te"]; ok && !slices.Equal(te, []string{"trailers"}) {
		c.failf("Te forwarded as %q", te)
	}
	upgrade := out.Header.Get("Upgrade")
	if conn, ok := out.Header["Connection"]; ok && (upgrade == "" || !slices.Equal(conn, []string{"Upgrade"})) {
		c.failf("Connection forwarded as %q with Upgrade %q", conn, upgrade)
	}
	if in != nil {
		for _, v := range in.Header["Connection"] {
			for token := range strings.SplitSeq(v, ",") {
				name := http.CanonicalHeaderKey(textproto.TrimString(token))
				switch name {
				case "", "Connection", "Upgrade", "Te", "User-Agent", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto":
					continue
				}
				if _, ok := out.Header[name]; ok {
					c.failf("%s was named in Connection and still forwarded", name)
				}
			}
		}
	}
	if xff := out.Header["X-Forwarded-For"]; !slices.Equal(xff, []string{"192.0.2.1"}) {
		c.failf("X-Forwarded-For forwarded as %q, want only the direct peer", xff)
	}
	if out.Body == nil {
		return
	}
	body, err := io.ReadAll(out.Body)
	_ = out.Body.Close()
	if out.ContentLength >= 0 && int64(len(body)) > out.ContentLength {
		c.failf("body of %d bytes on a request that declares %d", len(body), out.ContentLength)
	}
	if err == nil && out.ContentLength >= 0 && int64(len(body)) != out.ContentLength {
		c.failf("body of %d bytes on a request that declares %d", len(body), out.ContentLength)
	}
}

type checkingTransport struct {
	addr  string
	check *upstreamCheck
}

func (t checkingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.check.inspect(t.addr, req)
	return canned{}.RoundTrip(req)
}

func FuzzServeHTTP(f *testing.F) {
	for _, s := range requestSeeds {
		f.Add([]byte(s))
	}
	check := &upstreamCheck{}
	addrs := []string{"dim-a:8080", "dim-b:8080", "dim-c:8080"}
	g, err := New(fuzzConfig(f, addrs), Options{ManualTick: true, Transport: func(addr string) http.RoundTripper {
		return checkingTransport{addr: addr, check: check}
	}})
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(g.Close)
	f.Fuzz(func(t *testing.T, raw []byte) {
		req, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(raw)))
		if err != nil {
			return
		}
		req.RemoteAddr = "192.0.2.1:40000"
		check.begin(req)
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, req)
		for _, e := range check.failures() {
			t.Error(e)
		}
		switch rec.Code {
		case http.StatusOK:
			if rec.Header().Get(gatewayapi.DimensionHeader) == "" || rec.Header().Get(gatewayapi.AttemptsHeader) == "" {
				t.Errorf("200 without the dimension and attempt headers: %v", rec.Header())
			}
		case http.StatusNotFound, http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		default:
			t.Errorf("unexpected status %d", rec.Code)
		}
		for _, u := range g.State().Upstreams {
			if u.Inflight != 0 {
				t.Fatalf("%s still has %d in flight after the response", u.ID, u.Inflight)
			}
		}
	})
}

type countingTransport struct {
	rt    http.RoundTripper
	trips *atomic.Int64
}

func (c countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	c.trips.Add(1)
	return c.rt.RoundTrip(req)
}

type panicWatch struct{ panics *atomic.Int64 }

func (p panicWatch) Write(b []byte) (int, error) {
	if bytes.Contains(b, []byte("panic")) {
		p.panics.Add(1)
	}
	return len(b), nil
}

func FuzzWire(f *testing.F) {
	for _, s := range requestSeeds {
		f.Add([]byte(s))
	}
	var seen atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Add(1)
		n, _ := io.Copy(io.Discard, r.Body)
		w.Header().Set("X-Got-Bytes", strconv.FormatInt(n, 10))
		_, _ = w.Write([]byte("ok"))
	}))
	f.Cleanup(upstream.Close)
	addr := upstream.Listener.Addr().String()
	var trips, handled, active, panics atomic.Int64
	g, err := New(fuzzConfig(f, []string{addr, addr, addr}), Options{ManualTick: true, Transport: func(a string) http.RoundTripper {
		return countingTransport{rt: DefaultTransport(a), trips: &trips}
	}})
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(g.Close)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		active.Add(1)
		defer active.Add(-1)
		handled.Add(1)
		g.ServeHTTP(w, r)
	}))
	srv.Config.ErrorLog = log.New(panicWatch{&panics}, "", 0)
	srv.Config.ReadHeaderTimeout = time.Second
	srv.Config.MaxHeaderBytes = 64 << 10
	srv.Start()
	f.Cleanup(srv.Close)

	f.Fuzz(func(t *testing.T, raw []byte) {
		handledBefore, tripsBefore := handled.Load(), trips.Load()
		c, err := net.Dial("tcp", srv.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		_, _ = c.Write(raw)
		_ = c.(*net.TCPConn).CloseWrite()
		_, _ = io.Copy(io.Discard, c)
		_ = c.Close()
		deadline := time.Now().Add(3 * time.Second)
		for active.Load() != 0 || inflight(g) != 0 {
			if time.Now().After(deadline) {
				t.Fatalf("still busy 3 s after the client left: %d handlers, %d in flight", active.Load(), inflight(g))
			}
			time.Sleep(time.Millisecond)
		}
		if n := panics.Load(); n != 0 {
			t.Fatalf("the server logged %d panics", n)
		}
		if s, tr := seen.Load(), trips.Load(); s > tr {
			t.Fatalf("the upstream saw %d requests, the gateway sent %d: a request was smuggled", s, tr)
		}
		requests, sent := handled.Load()-handledBefore, trips.Load()-tripsBefore
		if sent > 3*requests {
			t.Fatalf("%d attempts for %d requests", sent, requests)
		}
	})
}

func inflight(g *Gateway) int64 {
	var n int64
	for _, u := range g.State().Upstreams {
		n += u.Inflight
	}
	return n
}
