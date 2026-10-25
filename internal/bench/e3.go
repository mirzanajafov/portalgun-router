package bench

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/chaos"
	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/failoverbench"
)

var failoverDims = []string{"a", "b", "c"}

type Variant struct {
	Probes       bool            `json:"probes"`
	Passive      bool            `json:"passive"`
	Retries      int             `json:"retries"`
	PerTry       time.Duration   `json:"perTry"`
	EjectRefused bool            `json:"ejectRefused"`
	Balancer     config.Balancer `json:"balancer"`
	SlowStart    time.Duration   `json:"slowStart"`
	Panic        float64         `json:"panic"`
}

func baseVariant() Variant {
	return Variant{Probes: true, Passive: true, Retries: 2, PerTry: time.Second, Balancer: config.BalancerP2C, SlowStart: config.DefaultSlowStart, Panic: config.DefaultPanic}
}

func (v Variant) Name() string {
	var parts []string
	switch {
	case v.Probes && v.Passive:
		parts = append(parts, "both")
	case v.Probes:
		parts = append(parts, "probes")
	case v.Passive:
		parts = append(parts, "passive")
	default:
		parts = append(parts, "blind")
	}
	if v.Retries > 0 {
		parts = append(parts, "retries")
	}
	base := baseVariant()
	if v.PerTry != base.PerTry {
		parts = append(parts, "try"+shortDuration(v.PerTry))
	}
	if v.EjectRefused {
		parts = append(parts, "refused")
	}
	if v.Balancer != base.Balancer {
		parts = append(parts, string(v.Balancer))
	}
	if v.SlowStart != base.SlowStart {
		parts = append(parts, "warm"+shortDuration(v.SlowStart))
	}
	if v.Panic != base.Panic {
		parts = append(parts, "panic"+strconv.FormatFloat(v.Panic, 'f', -1, 64))
	}
	return strings.Join(parts, "-")
}

func shortDuration(d time.Duration) string {
	switch {
	case d%time.Second == 0:
		return fmt.Sprintf("%ds", d/time.Second)
	case d%time.Millisecond == 0:
		return fmt.Sprintf("%dms", d/time.Millisecond)
	}
	return d.String()
}

func (v Variant) Config() config.Config {
	pool := config.Pool{
		Name:           "dimensions",
		Balancer:       v.Balancer,
		HealthCheck:    config.HealthCheck{Disabled: !v.Probes},
		Outlier:        config.Outlier{Disabled: !v.Passive, EjectOnRefused: v.EjectRefused},
		PanicThreshold: new(v.Panic),
		SlowStart:      new(config.Duration(v.SlowStart)),
	}
	for i, d := range failoverDims {
		pool.Dimensions = append(pool.Dimensions, config.Dimension{ID: d, Address: fmt.Sprintf("toxiproxy:%d", 18080+i), MaxInflight: 1024})
	}
	return config.Config{
		MaxInflight: new(0),
		Routes: []config.Route{{
			Name:          "all",
			PathPrefix:    "/",
			Pool:          pool.Name,
			Timeout:       config.Duration(10 * time.Second),
			PerTryTimeout: config.Duration(v.PerTry),
			Retries:       config.Retries{Max: new(v.Retries)},
		}},
		Pools: []config.Pool{pool},
	}
}

type FailoverCell struct {
	Name       string        `json:"name"`
	Fault      chaos.Fault   `json:"fault"`
	Variant    Variant       `json:"variant"`
	Background string        `json:"background,omitempty"`
	Settle     time.Duration `json:"settle"`
	Before     time.Duration `json:"before"`
	During     time.Duration `json:"during"`
	After      time.Duration `json:"after"`
}

func (c FailoverCell) file() string {
	return strings.NewReplacer("/", "_", ".", "_").Replace(c.Name)
}

func faultOf(mode chaos.Mode) chaos.Fault {
	f := chaos.Fault{Mode: mode, Target: "a"}
	switch mode {
	case chaos.Slow:
		f.Latency = 500 * time.Millisecond
	case chaos.Partial:
		f.ErrorRate = 0.3
	case chaos.Flapping:
		f.Period = 2 * time.Second
	}
	return f
}

func cell(f chaos.Fault, v Variant) FailoverCell {
	return FailoverCell{
		Name:    fmt.Sprintf("%s/%s", f.Mode, v.Name()),
		Fault:   f,
		Variant: v,
		Settle:  5 * time.Second,
		Before:  30 * time.Second,
		During:  30 * time.Second,
		After:   30 * time.Second,
	}
}

var FailoverModes = []chaos.Mode{chaos.Crash, chaos.Reset, chaos.Blackhole, chaos.Slow, chaos.Partial, chaos.Flapping}

var Studies = map[string]func() []FailoverCell{
	"detect": func() []FailoverCell {
		var cs []FailoverCell
		for _, m := range FailoverModes {
			for _, v := range []Variant{
				{Probes: true}, {Passive: true}, {Probes: true, Passive: true}, {Probes: true, Passive: true, Retries: 2},
			} {
				b := baseVariant()
				b.Probes, b.Passive, b.Retries = v.Probes, v.Passive, v.Retries
				cs = append(cs, cell(faultOf(m), b))
			}
		}
		return cs
	},
	"per-try": func() []FailoverCell {
		var cs []FailoverCell
		for _, d := range []time.Duration{100 * time.Millisecond, 250 * time.Millisecond, time.Second} {
			v := baseVariant()
			v.PerTry = d
			cs = append(cs, cell(faultOf(chaos.Blackhole), v))
		}
		return cs
	},
	"refused": func() []FailoverCell {
		v := baseVariant()
		v.EjectRefused = true
		return []FailoverCell{cell(faultOf(chaos.Crash), baseVariant()), cell(faultOf(chaos.Crash), v)}
	},
	"balancers": func() []FailoverCell {
		var cs []FailoverCell
		for _, b := range []config.Balancer{config.BalancerRoundRobin, config.BalancerLeastOutstanding, config.BalancerP2C} {
			v := baseVariant()
			v.Balancer = b
			cs = append(cs, cell(faultOf(chaos.Slow), v))
		}
		return cs
	},
	"slow-start": func() []FailoverCell {
		v := baseVariant()
		v.SlowStart = 0
		return []FailoverCell{cell(faultOf(chaos.Crash), baseVariant()), cell(faultOf(chaos.Crash), v)}
	},
	"false": func() []FailoverCell {
		c := cell(chaos.Fault{Mode: failoverbench.NoFault, Target: "a"}, baseVariant())
		c.Background = `{"errorRate":0.01,"delayDist":"lognormal","delayMs":5,"delaySigma":0.6}`
		c.Before, c.During, c.After = 10*time.Minute, 0, 5*time.Second
		return []FailoverCell{c}
	},
	"panic": func() []FailoverCell {
		var cs []FailoverCell
		for _, p := range []float64{0, config.DefaultPanic} {
			v := baseVariant()
			v.Panic = p
			f := faultOf(chaos.Partial)
			f.Target, f.ErrorRate = "all", 0.2
			c := cell(f, v)
			c.Name = fmt.Sprintf("partial-all/%s", v.Name())
			cs = append(cs, c)
		}
		return cs
	},
}

func StudyNames() []string {
	names := make([]string, 0, len(Studies))
	for n := range Studies {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}

func FailoverCells(studies []string) ([]FailoverCell, error) {
	var cs []FailoverCell
	seen := map[string]bool{}
	for _, s := range studies {
		build, ok := Studies[s]
		if !ok {
			return nil, fmt.Errorf("unknown study %q, have %s", s, strings.Join(StudyNames(), ", "))
		}
		for _, c := range build() {
			if !seen[c.Name] {
				seen[c.Name] = true
				cs = append(cs, c)
			}
		}
	}
	return cs, nil
}

type FailoverPlan struct {
	Cells    []FailoverCell `json:"cells"`
	Reps     int            `json:"reps"`
	Rate     float64        `json:"rate"`
	Gateways int            `json:"gateways"`
	Bins     bool           `json:"bins"`
	Records  bool           `json:"records"`
}

type FailoverRun struct {
	Cell      string                `json:"cell"`
	Rep       int                   `json:"rep"`
	Slot      int                   `json:"slot"`
	Gateways  int                   `json:"gateways"`
	StartedAt time.Time             `json:"startedAt"`
	Report    *failoverbench.Report `json:"report"`
	Error     string                `json:"error,omitempty"`
}

const maxFailedInARow = 3

func tryTwice(ctx context.Context, try func() (FailoverRun, error), heal func(error) error) (FailoverRun, error) {
	run, err := try()
	if err == nil || ctx.Err() != nil {
		return run, err
	}
	if herr := heal(err); herr != nil {
		return run, errors.Join(err, herr)
	}
	return try()
}

func (r *Runner) RunE3(ctx context.Context, plan FailoverPlan) (dir string, err error) {
	if plan.Reps < 1 || len(plan.Cells) == 0 || plan.Rate <= 0 || plan.Gateways < 1 || plan.Gateways > 2 {
		return "", errors.New("the plan needs cells, a rate, one or two gateways and at least one repetition")
	}
	dir, err = runDir(r.Out, "")
	if err != nil {
		return "", err
	}
	byFile := map[string]any{}
	for _, c := range plan.Cells {
		byFile[c.file()] = c.Variant.Config()
	}
	configs, err := writeConfigs(dir, byFile)
	if err != nil {
		return "", err
	}
	compose := r.Compose.withEnv("E3_CONFIGS="+configs, "E3_CELL="+plan.Cells[0].file())
	if plan.Gateways == 2 {
		compose = compose.withEnv("COMPOSE_PROFILES=two")
	}
	down, err := r.startStack(ctx, compose, fmt.Sprintf("the failover stack with %d gateway(s)", plan.Gateways))
	if err != nil {
		return "", err
	}
	defer down()
	f, err := startRuns(dir, map[string]any{"plan": plan, "commit": commit(ctx), "startedAt": time.Now().UTC(), "env": compose.Env})
	if err != nil {
		return dir, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	enc := json.NewEncoder(f)

	total := plan.Reps * len(plan.Cells)
	done, failedInARow := 0, 0
	for rep := range plan.Reps {
		for slot, c := range rotate(plan.Cells, rep) {
			done++
			run, err := tryTwice(ctx, func() (FailoverRun, error) {
				return r.failoverCell(ctx, compose, plan, c, rep, slot, dir)
			}, func(err error) error {
				r.Log.Printf("%s rep %d: %v; restarting toxiproxy and trying once more", c.Name, rep+1, err)
				_, herr := compose.run(ctx, "restart", "toxiproxy")
				return herr
			})
			if err != nil {
				if ctx.Err() != nil {
					return dir, fmt.Errorf("%s rep %d: %w", c.Name, rep+1, err)
				}
				failedInARow++
				if failedInARow >= maxFailedInARow {
					return dir, fmt.Errorf("%d cells in a row failed, the last %s rep %d: %w", failedInARow, c.Name, rep+1, err)
				}
				failed := FailoverRun{Cell: c.Name, Rep: rep, Slot: slot, Gateways: plan.Gateways, StartedAt: time.Now().UTC(), Error: err.Error()}
				if err := enc.Encode(failed); err != nil {
					return dir, err
				}
				r.Log.Printf("[%d/%d] rep %d %-34s failed twice, going on: %v", done, total, rep+1, c.Name, err)
				continue
			}
			failedInARow = 0
			if err := enc.Encode(run); err != nil {
				return dir, err
			}
			res := run.Report.Result
			r.Log.Printf("[%d/%d] rep %d %-34s detect %8s  move %8s  lost %5d  p99 %7.1f -> %7.1f ms  recover %8s  %s",
				done, total, rep+1, c.Name, msOrDash(res.DetectMs), msOrDash(res.MoveMs), res.Lost,
				res.P99BeforeMs, res.P99FaultMs, msOrDash(res.RecoverMs), strings.Join(run.Report.Generator.Invalid, ", "))
		}
	}
	return dir, nil
}

func msOrDash(v *float64) string {
	if v == nil {
		return "-"
	}
	return strconv.FormatFloat(*v, 'f', 0, 64) + "ms"
}

func (r *Runner) failoverCell(ctx context.Context, compose Compose, plan FailoverPlan, c FailoverCell, rep, slot int, dir string) (FailoverRun, error) {
	compose = compose.withEnv("E3_CELL=" + c.file())
	args := append([]string{"up", "-d", "--force-recreate", "--no-deps"}, gatewayNames(plan.Gateways)...)
	if _, err := compose.run(ctx, args...); err != nil {
		return FailoverRun{}, err
	}
	fa := []string{
		"exec", "-T", "loadgen", "failoverbench",
		"-label", c.Name,
		"-mode", string(c.Fault.Mode),
		"-target", c.Fault.Target,
		"-rate", strconv.FormatFloat(plan.Rate, 'f', -1, 64),
		"-seed", strconv.Itoa(rep + 1),
		"-settle", c.Settle.String(),
		"-before", c.Before.String(),
		"-fault", c.During.String(),
		"-after", c.After.String(),
	}
	if c.Fault.Latency > 0 {
		fa = append(fa, "-latency", c.Fault.Latency.String())
	}
	if c.Fault.ErrorRate > 0 {
		fa = append(fa, "-error-rate", strconv.FormatFloat(c.Fault.ErrorRate, 'f', -1, 64))
	}
	if c.Fault.Period > 0 {
		fa = append(fa, "-period", c.Fault.Period.String())
	}
	if c.Background != "" {
		fa = append(fa, "-background", c.Background)
	}
	for _, g := range gatewayNames(plan.Gateways) {
		fa = append(fa, "-url", "http://"+g+":8080/", "-admin", "http://"+g+":9090")
	}
	for _, d := range failoverDims {
		fa = append(fa, "-control", fmt.Sprintf("%s=http://dim-%s:8081", d, d))
	}
	if plan.Bins && rep == 0 {
		fa = append(fa, "-bins")
	}
	const inside = "/tmp/records.bin"
	if plan.Records {
		fa = append(fa, "-records", inside)
	}
	run := FailoverRun{Cell: c.Name, Rep: rep, Slot: slot, Gateways: plan.Gateways, StartedAt: time.Now().UTC()}
	out, err := compose.run(ctx, fa...)
	if err != nil {
		return run, err
	}
	run.Report = &failoverbench.Report{}
	if err := json.Unmarshal(out, run.Report); err != nil {
		return run, fmt.Errorf("failoverbench output: %w", err)
	}
	if plan.Records {
		raw := filepath.Join(dir, "raw")
		if err := os.MkdirAll(raw, 0o755); err != nil {
			return run, err
		}
		dst := filepath.Join(raw, fmt.Sprintf("%s-r%d.bin", c.file(), rep+1))
		if _, err := compose.run(ctx, "cp", "loadgen:"+inside, dst); err != nil {
			return run, err
		}
	}
	return run, nil
}
