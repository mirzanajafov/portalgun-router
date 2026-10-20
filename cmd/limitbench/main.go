package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/limitbench"
	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
)

func main() {
	var c limitbench.Cell
	l := &c.Load
	flag.StringVar(&c.Label, "label", "", "label copied into the report")
	flag.Func("url", "gateway traffic URL, repeatable", func(v string) error {
		c.Gateways = append(c.Gateways, v)
		return nil
	})
	flag.Func("admin", "gateway admin URL, repeatable, in -url order", func(v string) error {
		c.Admins = append(c.Admins, strings.TrimSuffix(v, "/"))
		return nil
	})
	flag.StringVar(&c.Toxiproxy, "toxiproxy", "http://toxiproxy:8474", "toxiproxy API")
	flag.StringVar(&c.Redis, "redis", "redis:6379", "Redis address for INFO, not through toxiproxy")
	flag.StringVar(&c.RedisFault, "redis-fault", "", "slow or down, empty for none")
	flag.DurationVar(&c.RedisLatency, "redis-latency", 5*time.Millisecond, "latency added to Redis replies when slow")
	flag.BoolVar(&c.Limited, "limited", true, "the route is rate limited (false for the baseline without a limiter)")
	flag.Float64Var(&c.Rule.Rate, "rule-rate", 1000, "the configured rate, for the oracle")
	flag.Float64Var(&c.Rule.Burst, "rule-burst", 100, "the configured burst, for the oracle")
	flag.Float64Var(&l.Rate, "rate", 1500, "offered requests per second")
	arrival := flag.String("arrival", string(loadgen.Poisson), "constant or poisson")
	flag.DurationVar(&l.Warmup, "warmup", 5*time.Second, "load before the latency window")
	flag.DurationVar(&l.Duration, "duration", 30*time.Second, "latency window")
	flag.Uint64Var(&l.Seed, "seed", 1, "seed for arrivals, splits and keys")
	flag.Func("weights", "share of requests per gateway, in -url order", func(v string) error {
		for w := range strings.SplitSeq(v, ",") {
			f, err := strconv.ParseFloat(strings.TrimSpace(w), 64)
			if err != nil {
				return err
			}
			l.Weights = append(l.Weights, f)
		}
		return nil
	})
	flag.DurationVar(&l.ShiftEvery, "shift-every", 0, "reverse the weights every period")
	flag.StringVar(&l.KeyHeader, "key-header", "X-Key", "header the route limits on")
	flag.StringVar(&l.KeyPrefix, "key-prefix", "k", "prefix of every client key, unique per run")
	flag.IntVar(&l.Keys, "keys", 1, "number of distinct client keys")
	flag.Float64Var(&l.ZipfS, "zipf", 0, "zipf exponent over the keys, above 1, or 0 for uniform")
	burst := loadgen.Burst{}
	flag.Float64Var(&burst.Rate, "burst-rate", 0, "rate inside bursts, 0 for no bursts")
	flag.DurationVar(&burst.Length, "burst-length", 200*time.Millisecond, "length of each burst")
	flag.DurationVar(&burst.Every, "burst-every", 2*time.Second, "time from one burst to the next")
	flag.Parse()
	l.Arrival = loadgen.Arrival(*arrival)
	l.Timeout = 10 * time.Second
	if burst.Rate > 0 {
		l.Burst = &burst
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	rep, err := limitbench.Run(ctx, c)
	if err != nil {
		fmt.Fprintln(os.Stderr, "limitbench:", err)
		os.Exit(1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rep); err != nil {
		fmt.Fprintln(os.Stderr, "limitbench:", err)
		os.Exit(1)
	}
}
