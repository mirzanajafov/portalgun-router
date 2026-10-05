package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	controller := flag.String("controller", "http://controller:9100", "controller API, asked whether anyone is watching")
	gateways := flag.String("gateways", "http://gateway-1:8080,http://gateway-2:8080", "comma-separated gateway URLs")
	rate := flag.Float64("rate", 200, "requests per second while someone watches")
	clients := flag.Int("clients", 500, "distinct client addresses to send as")
	linger := flag.Duration("linger", time.Minute, "keep going this long after the last viewer leaves")
	always := flag.Bool("always", false, "send traffic whether or not anyone watches")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	targets := strings.Split(*gateways, ",")
	addrs := make([]string, *clients)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("100.%d.%d.%d", 64+i/65536, (i/256)%256, i%256)
	}
	var watching atomic.Bool
	watching.Store(*always)
	if !*always {
		go watch(ctx, logger, *controller, *linger, &watching)
	}
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 256}}
	var inflight atomic.Int64
	paths := []string{"/portal/schwifty", "/portal/plumbus", "/portal/meeseeks", "/portal/szechuan"}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	credit := 0.0
	logger.Info("traffic up", "rate", *rate, "gateways", len(targets), "always", *always)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if !watching.Load() {
			credit = 0
			continue
		}
		credit += *rate / 100
		for credit >= 1 {
			credit--
			if inflight.Load() > 500 {
				continue
			}
			inflight.Add(1)
			go func() {
				defer inflight.Add(-1)
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, targets[rand.IntN(len(targets))]+paths[rand.IntN(len(paths))], nil)
				if err != nil {
					return
				}
				req.Header.Set("X-Forwarded-For", addrs[rand.IntN(len(addrs))])
				if resp, err := client.Do(req); err == nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}()
		}
	}
}

func watch(ctx context.Context, logger *slog.Logger, controller string, linger time.Duration, watching *atomic.Bool) {
	last := time.Time{}
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		viewers := viewers(ctx, controller)
		if viewers > 0 {
			last = time.Now()
		}
		on := time.Since(last) < linger
		if on != watching.Load() {
			logger.Info("traffic", "on", on, "viewers", viewers)
		}
		watching.Store(on)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func viewers(ctx context.Context, controller string) int {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, controller+"/api/state", nil)
	if err != nil {
		return 0
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	var st struct {
		Viewers int `json:"viewers"`
	}
	if json.NewDecoder(resp.Body).Decode(&st) != nil {
		return 0
	}
	return st.Viewers
}
