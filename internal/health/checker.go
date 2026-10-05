package health

import (
	"context"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type ProbeConfig struct {
	Path           string
	Interval       time.Duration
	Timeout        time.Duration
	UnhealthyAfter int
	HealthyAfter   int
}

type Checker struct {
	cfg      ProbeConfig
	url      string
	client   *http.Client
	onChange func(healthy bool)

	healthy atomic.Bool
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once
}

func StartChecker(address string, cfg ProbeConfig, onChange func(healthy bool)) *Checker {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Checker{
		cfg: cfg,
		url: "http://" + address + cfg.Path,
		client: &http.Client{Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: cfg.Timeout}).DialContext,
			MaxIdleConnsPerHost: 1,
			DisableCompression:  true,
		}},
		onChange: onChange,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	c.healthy.Store(true)
	go c.loop(ctx)
	return c
}

func (c *Checker) Healthy() bool { return c.healthy.Load() }

func (c *Checker) Stop() {
	c.once.Do(func() {
		c.cancel()
		<-c.done
		c.client.CloseIdleConnections()
	})
}

func (c *Checker) loop(ctx context.Context) {
	defer close(c.done)
	fails, oks := 0, 0
	t := time.NewTimer(jitter(c.cfg.Interval))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if c.probe(ctx) {
			oks, fails = oks+1, 0
			if !c.healthy.Load() && oks >= c.cfg.HealthyAfter {
				c.flip(true)
			}
		} else {
			fails, oks = fails+1, 0
			if c.healthy.Load() && fails >= c.cfg.UnhealthyAfter {
				c.flip(false)
			}
		}
		t.Reset(jitter(c.cfg.Interval))
	}
}

func (c *Checker) flip(healthy bool) {
	c.healthy.Store(healthy)
	if c.onChange != nil {
		c.onChange(healthy)
	}
}

func (c *Checker) probe(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return false
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return false
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

func jitter(d time.Duration) time.Duration {
	return d + time.Duration((rand.Float64()-0.5)*0.2*float64(d))
}
