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
	"strings"
	"syscall"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
)

func main() {
	var cfg loadgen.Config
	flag.StringVar(&cfg.URL, "url", "", "target URL")
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
	records := flag.String("records", "", "write every request to this file")
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
	if len(header) > 0 {
		cfg.Header = header
	}
	cfg.KeepRecords = *records != ""

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
		if err := writeLog(records, res.Log); err != nil {
			return err
		}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(res.Summary)
}

func writeLog(path string, l *loadgen.Log) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := loadgen.WriteLog(f, l); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
