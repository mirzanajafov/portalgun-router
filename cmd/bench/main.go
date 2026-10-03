package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/bench"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch os.Args[1] {
	case "e1":
		err = e1(ctx, os.Args[2:])
	case "report":
		err = report(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bench e1 [flags] | bench report <results dir>")
	os.Exit(2)
}

func e1(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("e1", flag.ExitOnError)
	workload := fs.String("workload", "w1", "w1 to w5")
	systems := fs.String("systems", "direct,stdlib,nginx,caddy,envoy,haproxy", "systems to compare")
	rates := fs.String("rates", "1000,2000,5000,10000,15000,20000,30000,40000", "offered rates")
	reps := fs.Int("reps", 3, "repetitions of the whole sweep")
	duration := fs.Duration("duration", 20*time.Second, "measured window per point")
	warmup := fs.Duration("warmup", 5*time.Second, "warm-up per point")
	maxConns := fs.Int("max-conns", 0, "generator connection limit")
	compose := fs.String("compose", filepath.Join("bench", "compose.yml"), "compose file")
	out := fs.String("out", filepath.Join("bench", "results", "e1"), "results directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	w, ok := bench.Workloads[*workload]
	if !ok {
		return fmt.Errorf("unknown workload %q", *workload)
	}
	sys, err := bench.SystemsByName(strings.Split(*systems, ","))
	if err != nil {
		return err
	}
	var rs []float64
	for v := range strings.SplitSeq(*rates, ",") {
		r, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil || r <= 0 {
			return fmt.Errorf("bad rate %q", v)
		}
		rs = append(rs, r)
	}
	runner := &bench.Runner{Compose: bench.Compose{File: *compose}, Out: *out, Log: os.Stdout}
	dir, err := runner.RunE1(ctx, bench.Plan{
		Workload: w,
		Systems:  sys,
		Rates:    rs,
		Reps:     *reps,
		Duration: *duration,
		Warmup:   *warmup,
		MaxConns: *maxConns,
	})
	if dir != "" {
		if rerr := report([]string{dir}); rerr != nil {
			err = errors.Join(err, rerr)
		}
	}
	return err
}

func report(args []string) error {
	if len(args) != 1 {
		return errors.New("report needs a results directory")
	}
	points, err := bench.LoadPoints(args[0])
	if err != nil {
		return err
	}
	rep := bench.Summarize(points)
	if err := rep.WriteText(os.Stdout); err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(args[0], "summary.json"))
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(rep)
}
