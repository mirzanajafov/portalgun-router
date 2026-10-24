package proxy

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
)

var payload = bytes.Repeat([]byte("x"), 1024)

type canned struct{}

func (canned) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body)
		req.Body.Close()
	}
	return &http.Response{
		StatusCode:    http.StatusOK,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{"Content-Type": {"application/octet-stream"}, "Content-Length": {"1024"}},
		ContentLength: int64(len(payload)),
		Body:          io.NopCloser(bytes.NewReader(payload)),
		Request:       req,
	}, nil
}

type sink struct {
	header http.Header
	status int
	n      int
}

func (s *sink) Header() http.Header    { return s.header }
func (s *sink) WriteHeader(status int) { s.status = status }
func (s *sink) Write(p []byte) (int, error) {
	s.n += len(p)
	return len(p), nil
}

func bareProxy(rt http.RoundTripper, addrs []string) http.Handler {
	targets := make([]*url.URL, len(addrs))
	for i, a := range addrs {
		targets[i] = &url.URL{Scheme: "http", Host: a}
	}
	var next atomic.Uint64
	return &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(targets[next.Add(1)%uint64(len(targets))])
		},
		Transport:  rt,
		BufferPool: newBufferPool(),
		ErrorLog:   log.New(io.Discard, "", 0),
	}
}

func benchConfig(tb testing.TB, addrs []string, full bool) *config.Config {
	dims := []config.Dimension{
		{ID: "a", Address: addrs[0], MaxInflight: 4096},
		{ID: "b", Address: addrs[1], MaxInflight: 4096},
		{ID: "c", Address: addrs[2], MaxInflight: 4096},
	}
	route := config.Route{Name: "all", PathPrefix: "/", Pool: "dimensions"}
	c := config.Config{
		Routes: []config.Route{route},
		Pools:  []config.Pool{{Name: "dimensions", HealthCheck: config.HealthCheck{Disabled: true}, Dimensions: dims}},
	}
	if full {
		c.TrustedHops = 1
		c.Pools[0].Dimensions = append(c.Pools[0].Dimensions, config.Dimension{ID: "d", Address: addrs[2], Version: "v2", MaxInflight: 4096})
		c.Routes[0].Versions = config.Versions{Stable: "v1", Canary: "v2", Weight: 0.1, Rollout: "r1"}
		c.Routes[0].RateLimit = &config.RateLimit{Design: config.LimitStaticSplit, Rate: 1e9, Burst: 1e9}
	}
	return parse(tb, c)
}

func benchGateway(tb testing.TB, addrs []string, full bool, rt func(string) http.RoundTripper) *Gateway {
	g, err := New(benchConfig(tb, addrs, full), Options{ManualTick: true, Transport: rt})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(g.Close)
	return g
}

func clients(n int) []*http.Request {
	out := make([]*http.Request, n)
	for i := range out {
		r := httptest.NewRequest(http.MethodGet, "http://portal.test/items/42", nil)
		r.RemoteAddr = "172.18.0.2:40000"
		r.Header.Set("X-Forwarded-For", fmt.Sprintf("10.0.%d.%d", i/250, i%250+1))
		r.Header.Set("Accept", "*/*")
		r.Header.Set("User-Agent", "loadgen")
		out[i] = r
	}
	return out
}

func cpuPerOp(b *testing.B) func() {
	start, ok := loadgen.ProcessCPU()
	return func() {
		if end, ok2 := loadgen.ProcessCPU(); ok && ok2 && b.N > 0 {
			b.ReportMetric(float64(end-start)/float64(b.N), "cpu-ns/op")
		}
	}
}

type system struct {
	name    string
	handler func(testing.TB) http.Handler
}

var fakeAddrs = []string{"dim-a:8080", "dim-b:8080", "dim-c:8080"}

func inMemorySystems() []system {
	fake := func(string) http.RoundTripper { return canned{} }
	return []system{
		{"bare", func(testing.TB) http.Handler { return bareProxy(canned{}, fakeAddrs) }},
		{"portalgun", func(tb testing.TB) http.Handler { return benchGateway(tb, fakeAddrs, false, fake) }},
		{"portalgun-full", func(tb testing.TB) http.Handler { return benchGateway(tb, fakeAddrs, true, fake) }},
	}
}

func BenchmarkServeHTTP(b *testing.B) {
	reqs := clients(256)
	for _, sys := range inMemorySystems() {
		b.Run(sys.name, func(b *testing.B) {
			h := sys.handler(b)
			w := &sink{header: http.Header{}}
			b.ReportAllocs()
			done := cpuPerOp(b)
			i := 0
			for b.Loop() {
				clear(w.header)
				h.ServeHTTP(w, reqs[i&255])
				i++
			}
			done()
			if w.status != http.StatusOK || w.n != len(payload)*i {
				b.Fatalf("status %d, %d bytes after %d requests", w.status, w.n, i)
			}
		})
	}
}

func BenchmarkServeHTTPParallel(b *testing.B) {
	reqs := clients(256)
	for _, sys := range inMemorySystems() {
		b.Run(sys.name, func(b *testing.B) {
			h := sys.handler(b)
			var seq atomic.Uint64
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				w := &sink{header: http.Header{}}
				for pb.Next() {
					clear(w.header)
					h.ServeHTTP(w, reqs[seq.Add(1)&255])
					if w.status != http.StatusOK {
						b.Errorf("status %d", w.status)
						return
					}
				}
			})
		})
	}
}

func BenchmarkLoopback(b *testing.B) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1024")
		_, _ = w.Write(payload)
	}))
	b.Cleanup(upstream.Close)
	addr := upstream.Listener.Addr().String()
	addrs := []string{addr, addr, addr}
	serve := func(tb testing.TB, h http.Handler) string {
		srv := httptest.NewServer(h)
		tb.Cleanup(srv.Close)
		return srv.URL
	}
	for _, sys := range []struct {
		name string
		url  func(testing.TB) string
	}{
		{"direct", func(testing.TB) string { return upstream.URL }},
		{"bare", func(tb testing.TB) string { return serve(tb, bareProxy(DefaultTransport(addr), addrs)) }},
		{"portalgun", func(tb testing.TB) string { return serve(tb, benchGateway(tb, addrs, false, nil)) }},
		{"portalgun-full", func(tb testing.TB) string { return serve(tb, benchGateway(tb, addrs, true, nil)) }},
	} {
		b.Run(sys.name, func(b *testing.B) {
			target := sys.url(b) + "/items/42"
			tr := &http.Transport{MaxIdleConnsPerHost: 16, DisableCompression: true}
			b.Cleanup(tr.CloseIdleConnections)
			client := &http.Client{Transport: tr}
			b.ReportAllocs()
			done := cpuPerOp(b)
			for b.Loop() {
				resp, err := client.Get(target)
				if err != nil {
					b.Fatal(err)
				}
				n, _ := io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK || n != int64(len(payload)) {
					b.Fatalf("status %d, %d bytes", resp.StatusCode, n)
				}
			}
			done()
		})
	}
}
