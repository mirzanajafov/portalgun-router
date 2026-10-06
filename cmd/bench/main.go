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
	"slices"
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
	case "e2":
		err = e2(ctx, os.Args[2:])
	case "report-e2":
		err = reportE2(os.Args[2:])
	case "e3":
		err = e3(ctx, os.Args[2:])
	case "report-e3":
		err = reportE3(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bench e1|e2|e3 [flags] | bench report|report-e2|report-e3 <dir>")
	os.Exit(2)
}

func e1(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("e1", flag.ExitOnError)
	workload := fs.String("workload", "w1", "w1 to w5")
	systems := fs.String("systems", "direct,stdlib,portalgun,nginx,caddy,envoy,haproxy", "systems to compare")
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
	return writeSummary(filepath.Join(args[0], "summary.json"), rep)
}

func e3(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("e3", flag.ExitOnError)
	studies := fs.String("studies", "detect,per-try,refused,balancers,slow-start", "studies to run: "+strings.Join(bench.StudyNames(), ", "))
	only := fs.String("cells", "", "comma-separated cell names to keep, empty for all")
	reps := fs.Int("reps", 5, "repetitions of every cell")
	rate := fs.Float64("rate", 2000, "Poisson request rate")
	gateways := fs.Int("gateways", 1, "gateway instances, 1 or 2")
	bins := fs.Bool("bins", true, "keep the 100 ms series of the first repetition")
	records := fs.Bool("records", false, "copy every request log into raw/")
	dry := fs.Bool("dry-run", false, "print the cells and the time they take, then stop")
	compose := fs.String("compose", filepath.Join("bench", "failover", "compose.yml"), "compose file")
	out := fs.String("out", filepath.Join("bench", "results", "e3"), "results directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cells, err := bench.FailoverCells(strings.Split(*studies, ","))
	if err != nil {
		return err
	}
	if *only != "" {
		keep := map[string]bool{}
		for c := range strings.SplitSeq(*only, ",") {
			keep[strings.TrimSpace(c)] = true
		}
		cells = slices.DeleteFunc(cells, func(c bench.FailoverCell) bool { return !keep[c.Name] })
		if len(cells) != len(keep) {
			return fmt.Errorf("only %d of the %d requested cells are in the chosen studies", len(cells), len(keep))
		}
	}
	var each time.Duration
	for _, c := range cells {
		each += c.Settle + c.Before + c.During + c.After + 5*time.Second
	}
	fmt.Printf("%d cells x %d reps, about %v\n", len(cells), *reps, (each * time.Duration(*reps)).Round(time.Minute))
	if *dry {
		for _, c := range cells {
			fmt.Println(" ", c.Name)
		}
		return nil
	}
	runner := &bench.Runner{Compose: bench.Compose{File: *compose}, Out: *out, Log: os.Stdout}
	dir, err := runner.RunE3(ctx, bench.FailoverPlan{Cells: cells, Reps: *reps, Rate: *rate, Gateways: *gateways, Bins: *bins, Records: *records})
	if dir != "" {
		if rerr := reportE3([]string{dir}); rerr != nil {
			err = errors.Join(err, rerr)
		}
	}
	return err
}

func reportE3(args []string) error {
	if len(args) != 1 {
		return errors.New("report-e3 needs a results directory")
	}
	runs, err := bench.LoadFailoverRuns(args[0])
	if err != nil {
		return err
	}
	sums := bench.SummarizeFailover(runs)
	if err := bench.WriteFailoverText(os.Stdout, sums); err != nil {
		return err
	}
	return writeSummary(filepath.Join(args[0], "summary.json"), sums)
}

func writeSummary(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func e2(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("e2", flag.ExitOnError)
	studies := fs.String("studies", "accuracy,skew,instances,redis,zipf", "studies to run: "+strings.Join(bench.LimiterStudyNames(), ", "))
	only := fs.String("cells", "", "comma-separated cell names to keep, empty for all")
	reps := fs.Int("reps", 5, "repetitions of every cell")
	dry := fs.Bool("dry-run", false, "print the cells and the time they take, then stop")
	compose := fs.String("compose", filepath.Join("bench", "limiter", "compose.yml"), "compose file")
	out := fs.String("out", filepath.Join("bench", "results", "e2"), "results directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cells, err := bench.LimiterCells(strings.Split(*studies, ","))
	if err != nil {
		return err
	}
	if *only != "" {
		keep := map[string]bool{}
		for c := range strings.SplitSeq(*only, ",") {
			keep[strings.TrimSpace(c)] = true
		}
		cells = slices.DeleteFunc(cells, func(c bench.LimiterCell) bool { return !keep[c.Name] })
		if len(cells) != len(keep) {
			return fmt.Errorf("only %d of the %d requested cells are in the chosen studies", len(cells), len(keep))
		}
	}
	var each time.Duration
	for _, c := range cells {
		each += c.Warmup + c.Duration + 5*time.Second
	}
	fmt.Printf("%d cells x %d reps, about %v\n", len(cells), *reps, (each * time.Duration(*reps)).Round(time.Minute))
	if *dry {
		for _, c := range cells {
			fmt.Println(" ", c.Name)
		}
		return nil
	}
	runner := &bench.Runner{Compose: bench.Compose{File: *compose}, Out: *out, Log: os.Stdout}
	dir, err := runner.RunE2(ctx, bench.LimiterPlan{Cells: cells, Reps: *reps})
	if dir != "" {
		if rerr := reportE2([]string{dir}); rerr != nil {
			err = errors.Join(err, rerr)
		}
	}
	return err
}

func reportE2(args []string) error {
	if len(args) != 1 {
		return errors.New("report-e2 needs a results directory")
	}
	runs, err := bench.LoadLimiterRuns(args[0])
	if err != nil {
		return err
	}
	sums := bench.SummarizeLimiter(runs)
	if err := bench.WriteLimiterText(os.Stdout, sums); err != nil {
		return err
	}
	return writeSummary(filepath.Join(args[0], "summary.json"), sums)
}
