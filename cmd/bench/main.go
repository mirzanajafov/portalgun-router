package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
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

var progress = log.New(os.Stdout, "", 0)

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
	case "report-overload":
		err = reportOverload(os.Args[2:])
	case "e2":
		err = e2(ctx, os.Args[2:])
	case "report-e2":
		err = reportE2(os.Args[2:])
	case "e3":
		err = e3(ctx, os.Args[2:])
	case "report-e3":
		err = reportE3(os.Args[2:])
	case "e5":
		err = e5(ctx, os.Args[2:])
	case "report-e5":
		err = reportE5(os.Args[2:])
	case "hotpath":
		err = hotpath(ctx, os.Args[2:])
	case "report-hotpath":
		err = reportHotpath(os.Args[2:])
	case "merge":
		if len(os.Args) < 3 {
			usage()
		}
		err = bench.MergeRuns(os.Args[2], os.Args[3:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bench e1|e2|e3|e5|hotpath [flags] | bench report|report-overload|report-e2|report-e3|report-e5|report-hotpath <dir> | bench merge <out> <dir>...")
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
	overload := fs.Bool("overload", false, "keep going past saturation, with no confirmation runs")
	delay := fs.Float64("delay-ms", -1, "dimension delay in ms instead of the workload's own")
	compose := fs.String("compose", filepath.Join("bench", "compose.yml"), "compose file")
	out := fs.String("out", filepath.Join("bench", "results", "e1"), "results directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	w, ok := bench.Workloads[*workload]
	if !ok {
		return fmt.Errorf("unknown workload %q", *workload)
	}
	if *delay >= 0 {
		w.DelayMs = *delay
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
	runner := &bench.Runner{Compose: bench.Compose{File: *compose}, Out: *out, Log: progress}
	dir, err := runner.RunE1(ctx, bench.Plan{
		Workload: w,
		Systems:  sys,
		Rates:    rs,
		Reps:     *reps,
		Duration: *duration,
		Warmup:   *warmup,
		MaxConns: *maxConns,
		Overload: *overload,
	})
	if dir != "" {
		write := report
		if *overload {
			write = reportOverload
		}
		if rerr := write([]string{dir}); rerr != nil {
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
	return bench.WriteJSON(filepath.Join(args[0], "summary.json"), rep)
}

func reportOverload(args []string) error {
	if len(args) != 1 {
		return errors.New("report-overload needs a results directory")
	}
	points, err := bench.LoadPoints(args[0])
	if err != nil {
		return err
	}
	rep := bench.Summarize(points)
	if err := rep.WriteOverloadText(os.Stdout); err != nil {
		return err
	}
	return bench.WriteJSON(filepath.Join(args[0], "summary.json"), rep)
}

func e5(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("e5", flag.ExitOnError)
	plan := bench.RegistryPlan{Image: cmp.Or(os.Getenv("FF_IMAGE"), "fivefamilies-node"), ImageCommit: os.Getenv("FF_COMMIT")}
	fs.IntVar(&plan.Join, "join", 20, "runs of a dimension registering and leaving")
	fs.IntVar(&plan.Leader, "leader", 10, "runs of the registry's leader being killed")
	fs.IntVar(&plan.Failover, "failover", 10, "runs of a dimension registering as the leader is killed")
	fs.IntVar(&plan.Down, "down", 3, "runs of the whole registry down for 30 s")
	fs.Float64Var(&plan.Rate, "rate", 500, "Poisson request rate across both gateways")
	fs.StringVar(&plan.Label, "label", "laptop", "name of the run")
	compose := fs.String("compose", filepath.Join("bench", "registry", "compose.yml"), "compose file")
	out := fs.String("out", filepath.Join("bench", "results", "e5"), "results directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	runner := &bench.Runner{Compose: bench.Compose{File: *compose}, Out: *out, Log: progress}
	dir, err := runner.RunE5(ctx, plan)
	if dir != "" {
		fmt.Println("results in", dir)
		if rerr := reportE5([]string{dir}); rerr != nil {
			err = errors.Join(err, rerr)
		}
	}
	return err
}

func reportE5(args []string) error {
	if len(args) != 1 {
		return errors.New("report-e5 needs a results directory")
	}
	runs, err := bench.LoadRegistryRuns(args[0])
	if err != nil {
		return err
	}
	sums := bench.SummarizeE5(runs)
	if err := bench.WriteRegistryText(os.Stdout, sums); err != nil {
		return err
	}
	return bench.WriteJSON(filepath.Join(args[0], "summary.json"), sums)
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
	runner := &bench.Runner{Compose: bench.Compose{File: *compose}, Out: *out, Log: progress}
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
	return bench.WriteJSON(filepath.Join(args[0], "summary.json"), sums)
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
	runner := &bench.Runner{Compose: bench.Compose{File: *compose}, Out: *out, Log: progress}
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
	return bench.WriteJSON(filepath.Join(args[0], "summary.json"), sums)
}

func hotpath(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("hotpath", flag.ExitOnError)
	reps := fs.Int("reps", 10, "repetitions, each one running every system once in rotated order")
	benchtime := fs.String("benchtime", "1s", "go test -benchtime for each run")
	pkg := fs.String("package", "./internal/proxy/", "package holding the benchmarks")
	out := fs.String("out", filepath.Join("bench", "results", "hotpath"), "results directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	dir, err := bench.RunHotpath(ctx, bench.HotpathPlan{Package: *pkg, Reps: *reps, BenchTime: *benchtime, Out: *out, Log: progress})
	if dir != "" && err == nil {
		err = reportHotpath([]string{dir})
	}
	return err
}

func reportHotpath(args []string) error {
	if len(args) != 1 {
		return errors.New("report-hotpath needs a results directory")
	}
	rows, err := bench.LoadHotpathRows(args[0])
	if err != nil {
		return err
	}
	sum := bench.SummarizeHotpath(rows)
	if err := bench.WriteHotpathText(os.Stdout, sum); err != nil {
		return err
	}
	return bench.WriteJSON(filepath.Join(args[0], "summary.json"), sum)
}
