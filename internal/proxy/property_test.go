package proxy

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"pgregory.net/rapid"
)

type scripted struct{ fail bool }

func (s scripted) RoundTrip(req *http.Request) (*http.Response, error) {
	if !s.fail {
		return canned{}.RoundTrip(req)
	}
	return &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     http.Header{},
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Request:    req,
	}, nil
}

func TestEjectionNeverPushesAPoolPastThePanicThreshold(t *testing.T) {
	var cases, panicked, ejectedCalm int
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(1, 8).Draw(rt, "dimensions")
		threshold := rapid.SampledFrom([]float64{0, 0.2, 0.5, 0.75, 1}).Draw(rt, "panic")
		failing := rapid.SliceOfN(rapid.Bool(), n, n).Draw(rt, "failing")
		retries := rapid.IntRange(0, 2).Draw(rt, "retries")
		consecutive := rapid.IntRange(1, 3).Draw(rt, "consecutive")
		requests := rapid.IntRange(1, 40).Draw(rt, "requests")

		dims := make([]config.Dimension, n)
		broken := map[string]bool{}
		for i := range dims {
			dims[i] = config.Dimension{ID: fmt.Sprint("d", i), Address: fmt.Sprintf("d%d:80", i)}
			broken[dims[i].Address] = failing[i]
		}
		cfg := parse(t, config.Config{
			Routes: []config.Route{{Name: "all", PathPrefix: "/", Pool: "p", Retries: config.Retries{Max: &retries}}},
			Pools: []config.Pool{{
				Name:           "p",
				Balancer:       config.BalancerRoundRobin,
				HealthCheck:    config.HealthCheck{Disabled: true},
				Outlier:        config.Outlier{Consecutive: consecutive, BaseEjection: config.Duration(time.Hour), MaxEjection: config.Duration(time.Hour)},
				PanicThreshold: &threshold,
				SlowStart:      new(config.Duration(0)),
				Dimensions:     dims,
			}},
		})
		g, err := New(cfg, Options{ManualTick: true, Transport: func(addr string) http.RoundTripper { return scripted{fail: broken[addr]} }})
		if err != nil {
			rt.Fatal(err)
		}
		defer g.Close()
		cases++
		sawPanic, sawEjection := false, false
		floor := int(math.Ceil(threshold * float64(n)))
		for i := range requests {
			g.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
			stable := g.routing.Load().routes[0].stable
			now := g.now()
			healthy := map[*upstream]bool{}
			for _, u := range stable.upstreams {
				if u.healthy(now) {
					healthy[u] = true
				}
			}
			reached := map[*upstream]bool{}
			for range 2 * n {
				if u, _ := stable.acquire(now, tried{}); u != nil {
					reached[u] = true
					u.load.Release()
				}
			}
			if len(reached) < floor {
				rt.Fatalf("after request %d traffic reaches %d of %d dimensions, under the panic floor of %d", i+1, len(reached), n, floor)
			}
			if len(healthy) < floor {
				sawPanic = true
			} else if len(healthy) < n {
				sawEjection = true
			}
			if len(healthy) >= floor {
				for u := range reached {
					if !healthy[u] {
						rt.Fatalf("after request %d %s gets traffic while ejected and %d of %d are healthy", i+1, u.id, len(healthy), n)
					}
				}
				if len(reached) != len(healthy) {
					rt.Fatalf("after request %d traffic reaches %d dimensions, %d are healthy", i+1, len(reached), len(healthy))
				}
			} else if len(reached) != n {
				rt.Fatalf("after request %d only %d of %d healthy, under the threshold, yet traffic reaches %d, not the whole pool", i+1, len(healthy), n, len(reached))
			}
		}
		if sawPanic {
			panicked++
		}
		if sawEjection {
			ejectedCalm++
		}
	})
	if panicked < cases/10 || ejectedCalm < cases/10 {
		t.Fatalf("of %d cases %d reached panic mode and %d had an ejection without it, so the property was barely exercised", cases, panicked, ejectedCalm)
	}
}
