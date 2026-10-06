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
	"github.com/mirzanajafov/portalgun-router/internal/failover"
)

var failoverDims = []string{"a", "b", "c"}

type Variant struct {
	Probes       bool          `json:"probes"`
	Passive      bool          `json:"passive"`
	Retries      int           `json:"retries"`
	PerTry       time.Duration `json:"perTry"`
	EjectRefused bool          `json:"ejectRefused"`
	Balancer     string        `json:"balancer"`
	SlowStart    time.Duration `json:"slowStart"`
	Panic        float64       `json:"panic"`
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
		parts = append(parts, v.Balancer)
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
		for _, b := range []string{config.BalancerRoundRobin, config.BalancerLeastOutstanding, config.BalancerP2C} {
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
		c := cell(chaos.Fault{Mode: failover.NoFault, Target: "a"}, baseVariant())
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
	Cell      string           `json:"cell"`
	Rep       int              `json:"rep"`
	Slot      int              `json:"slot"`
	Gateways  int              `json:"gateways"`
	StartedAt time.Time        `json:"startedAt"`
	Report    *failover.Report `json:"report"`
}

func (p FailoverPlan) gatewayServices() []string {
	gs := []string{"gateway-1"}
	if p.Gateways == 2 {
		gs = append(gs, "gateway-2")
	}
	return gs
}

func (r *Runner) RunE3(ctx context.Context, plan FailoverPlan) (string, error) {
	if plan.Reps < 1 || len(plan.Cells) == 0 || plan.Rate <= 0 || plan.Gateways < 1 || plan.Gateways > 2 {
		return "", errors.New("the plan needs cells, a rate, one or two gateways and at least one repetition")
	}
	dir, err := filepath.Abs(filepath.Join(r.Out, time.Now().UTC().Format("20060102-150405")))
	if err != nil {
		return "", err
	}
	configs := filepath.Join(dir, "configs")
	if err := os.MkdirAll(configs, 0o755); err != nil {
		return "", err
	}
	for _, c := range plan.Cells {
		if err := writeJSON(filepath.Join(configs, c.file()+".json"), c.Variant.Config()); err != nil {
			return "", err
		}
	}
	compose := r.Compose
	compose.Env = append(slices.Clone(compose.Env), "E3_CONFIGS="+configs, "E3_CELL="+plan.Cells[0].file())
	if plan.Gateways == 2 {
		compose.Env = append(compose.Env, "COMPOSE_PROFILES=two")
	}
	r.logf("starting the failover stack with %d gateway(s)", plan.Gateways)
	if err := compose.Up(ctx); err != nil {
		return "", err
	}
	defer func() {
		if err := compose.Down(context.Background()); err != nil {
			r.logf("tearing down: %v", err)
		}
	}()
	meta := map[string]any{"plan": plan, "commit": commit(ctx), "startedAt": time.Now().UTC(), "env": compose.Env}
	if err := writeJSON(filepath.Join(dir, "meta.json"), meta); err != nil {
		return dir, err
	}
	f, err := os.Create(filepath.Join(dir, "runs.jsonl"))
	if err != nil {
		return dir, err
	}
	defer f.Close()
	enc := json.NewEncoder(f)

	total := plan.Reps * len(plan.Cells)
	done := 0
	for rep := range plan.Reps {
		for slot, c := range rotate(plan.Cells, rep) {
			done++
			run, err := r.failoverCell(ctx, compose, plan, c, rep, slot, dir)
			if err != nil {
				return dir, fmt.Errorf("%s rep %d: %w", c.Name, rep+1, err)
			}
			if err := enc.Encode(run); err != nil {
				return dir, err
			}
			res := run.Report.Result
			r.logf("[%d/%d] rep %d %-34s detect %8s  move %8s  lost %5d  p99 %7.1f -> %7.1f ms  recover %8s  %s",
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
	compose.Env = append(slices.Clone(compose.Env), "E3_CELL="+c.file())
	args := append([]string{"up", "-d", "--force-recreate", "--no-deps"}, plan.gatewayServices()...)
	if _, err := compose.run(ctx, args...); err != nil {
		return FailoverRun{}, err
	}
	fa := []string{
		"exec", "-T", "loadgen", "failover",
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
	for _, g := range plan.gatewayServices() {
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
	run.Report = &failover.Report{}
	if err := json.Unmarshal(out, run.Report); err != nil {
		return run, fmt.Errorf("failover output: %w", err)
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
