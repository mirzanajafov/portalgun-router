package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type bufferPool struct{ p sync.Pool }

const bufferSize = 32 << 10

func (b *bufferPool) Get() []byte { return b.p.Get().(*[bufferSize]byte)[:] }

func (b *bufferPool) Put(v []byte) {
	if len(v) == bufferSize {
		b.p.Put((*[bufferSize]byte)(v))
	}
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	upstreams := flag.String("upstreams", "", "comma-separated host:port list")
	version := flag.Bool("version", false, "print the Go version and exit")
	flag.Parse()
	if *version {
		fmt.Println("stdproxy", runtime.Version())
		return
	}

	var targets []*url.URL
	for h := range strings.SplitSeq(*upstreams, ",") {
		if h = strings.TrimSpace(h); h != "" {
			targets = append(targets, &url.URL{Scheme: "http", Host: h})
		}
	}
	if len(targets) == 0 {
		log.Fatal("stdproxy: -upstreams is required")
	}

	var next atomic.Uint64
	proxy := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(targets[next.Add(1)%uint64(len(targets))])
		},
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConnsPerHost: 1024,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
		BufferPool: &bufferPool{p: sync.Pool{New: func() any { return new([bufferSize]byte) }}},
	}
	srv := &http.Server{Addr: *addr, Handler: proxy, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
