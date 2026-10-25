package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
)

func main() {
	var cfg loadgen.Config
	flag.Func("url", "target URL, repeatable, with requests going to each in turn", func(v string) error {
		cfg.URLs = append(cfg.URLs, v)
		return nil
	})
	flag.StringVar(&cfg.Method, "method", http.MethodGet, "request method")
	flag.IntVar(&cfg.BodyBytes, "body", 0, "request body size in bytes")
	flag.Float64Var(&cfg.Rate, "rate", 1000, "requests per second")
	arrival := flag.String("arrival", string(loadgen.Constant), "constant or poisson")
	flag.DurationVar(&cfg.Duration, "duration", 30*time.Second, "length of the measured window")
	flag.DurationVar(&cfg.Warmup, "warmup", 5*time.Second, "load before the measured window")
	flag.Uint64Var(&cfg.Seed, "seed", 1, "seed for poisson arrivals")
	flag.IntVar(&cfg.MaxConns, "max-conns", 0, "connection limit, 0 for none")
	flag.IntVar(&cfg.MaxInflight, "max-inflight", 20000, "requests in flight before new ones are dropped")
	flag.DurationVar(&cfg.Timeout, "timeout", 10*time.Second, "per-request timeout")
	flag.BoolVar(&cfg.NoKeepAlive, "no-keepalive", false, "open a new connection for every request")
	flag.StringVar(&cfg.Label, "label", "", "label copied into the summary")
	flag.Func("weights", "comma-separated share of requests per url, in -url order", func(v string) error {
		for w := range strings.SplitSeq(v, ",") {
			f, err := strconv.ParseFloat(strings.TrimSpace(w), 64)
			if err != nil {
				return err
			}
			cfg.Weights = append(cfg.Weights, f)
		}
		return nil
	})
	flag.DurationVar(&cfg.ShiftEvery, "shift-every", 0, "reverse the weights every period, 0 for never")
	flag.StringVar(&cfg.KeyHeader, "key-header", "", "send a client key in this header")
	flag.StringVar(&cfg.KeyPrefix, "key-prefix", "k", "prefix of every client key")
	flag.IntVar(&cfg.Keys, "keys", 1, "number of distinct client keys")
	flag.Float64Var(&cfg.ZipfS, "zipf", 0, "zipf exponent over the keys, above 1, or 0 for uniform")
	burst := loadgen.Burst{}
	flag.Float64Var(&burst.Rate, "burst-rate", 0, "rate inside bursts, 0 for no bursts")
	flag.DurationVar(&burst.Length, "burst-length", 200*time.Millisecond, "length of each burst")
	flag.DurationVar(&burst.Every, "burst-every", 2*time.Second, "time from one burst to the next")
	records := flag.String("records", "", "write every request to this file")
	cgroupRoot := flag.String("cgroup-root", "", "host cgroup mount used to resolve -watch containers")
	watch := map[string]string{}
	flag.Func("watch", "name=containerID whose CPU is read over the measured window, repeatable", func(v string) error {
		name, id, ok := strings.Cut(v, "=")
		if !ok || name == "" || id == "" {
			return errors.New("expected name=containerID")
		}
		watch[name] = id
		return nil
	})
	header := http.Header{}
	flag.Func("header", "request header as 'Name: value', repeatable", func(v string) error {
		name, value, ok := strings.Cut(v, ":")
		if !ok || strings.TrimSpace(name) == "" {
			return errors.New("expected 'Name: value'")
		}
		header.Add(strings.TrimSpace(name), strings.TrimSpace(value))
		return nil
	})
	flag.Parse()
	cfg.Arrival = loadgen.Arrival(*arrival)
	if burst.Rate > 0 {
		cfg.Burst = &burst
	}
	if len(header) > 0 {
		cfg.Header = header
	}
	cfg.KeepRecords = *records != ""
	if len(watch) > 0 {
		if *cgroupRoot == "" {
			fmt.Fprintln(os.Stderr, "loadgen: -watch needs -cgroup-root")
			os.Exit(2)
		}
		cfg.Watch = map[string]string{}
		for name, id := range watch {
			dir, err := loadgen.FindCgroup(*cgroupRoot, id)
			if err != nil {
				fmt.Fprintln(os.Stderr, "loadgen:", err)
				os.Exit(1)
			}
			cfg.Watch[name] = dir
		}
	}

	if err := run(cfg, *records); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func run(cfg loadgen.Config, records string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	res, err := loadgen.Run(ctx, cfg)
	if err != nil {
		return err
	}
	if records != "" {
		if err := loadgen.WriteLogFile(records, res.Log); err != nil {
			return err
		}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(res.Summary)
}
