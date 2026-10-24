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

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/limit"
	"github.com/mirzanajafov/portalgun-router/internal/limitbench"
)

const NoLimiter = "none"

type Design struct {
	Kind  config.LimitDesign `json:"kind"`
	Sync  time.Duration      `json:"sync,omitempty"`
	Lease time.Duration      `json:"lease,omitempty"`
}

func (d Design) HasLimiter() bool { return d.Kind != NoLimiter }

func (d Design) Name() string {
	switch {
	case d.Sync > 0:
		return string(d.Kind) + "-" + shortDuration(d.Sync)
	case d.Lease > 0:
		return string(d.Kind) + "-" + shortDuration(d.Lease)
	}
	return string(d.Kind)
}

var Designs = []Design{
	{Kind: NoLimiter},
	{Kind: config.LimitStaticSplit},
	{Kind: config.LimitRedisExact},
	{Kind: config.LimitLocalSync, Sync: 10 * time.Millisecond},
	{Kind: config.LimitLocalSync, Sync: 50 * time.Millisecond},
	{Kind: config.LimitLocalSync, Sync: 100 * time.Millisecond},
	{Kind: config.LimitLocalSync, Sync: 500 * time.Millisecond},
	{Kind: config.LimitLease, Lease: 5 * time.Millisecond},
	{Kind: config.LimitLease, Lease: 20 * time.Millisecond},
	{Kind: config.LimitLease, Lease: 100 * time.Millisecond},
}

const (
	LoadUnder = "under"
	LoadOver  = "over"
	LoadBurst = "burst"

	SplitEven  = "even"
	SplitSkew  = "skew"
	SplitShift = "shift"
)

type LimiterCell struct {
	Name       string        `json:"name"`
	Design     Design        `json:"design"`
	Instances  int           `json:"instances"`
	Load       string        `json:"load"`
	Split      string        `json:"split"`
	Rule       limit.Rule    `json:"rule"`
	Keys       int           `json:"keys"`
	Zipf       float64       `json:"zipf,omitempty"`
	Offered    float64       `json:"offered"`
	RedisFault string        `json:"redisFault,omitempty"`
	Warmup     time.Duration `json:"warmup"`
	Duration   time.Duration `json:"duration"`
}

func (c LimiterCell) file() string {
	return strings.NewReplacer("/", "_", ".", "_").Replace(c.Name)
}

func limiterCell(d Design, instances int, load, split string) LimiterCell {
	rule := limit.Rule{Rate: 1000, Burst: 100}
	offered := map[string]float64{LoadUnder: 0.8, LoadOver: 1.5, LoadBurst: 0.8}[load] * rule.Rate
	return LimiterCell{
		Name:      fmt.Sprintf("%s/%s-%s-%dgw", d.Name(), load, split, instances),
		Design:    d,
		Instances: instances,
		Load:      load,
		Split:     split,
		Rule:      rule,
		Keys:      1,
		Offered:   offered,
		Warmup:    5 * time.Second,
		Duration:  30 * time.Second,
	}
}

func (c LimiterCell) Config() config.Config {
	route := config.Route{
		Name:          "fluid",
		PathPrefix:    "/",
		Pool:          "dim",
		Timeout:       config.Duration(10 * time.Second),
		PerTryTimeout: config.Duration(2 * time.Second),
	}
	if c.Design.HasLimiter() {
		route.RateLimit = &config.RateLimit{
			Design:       c.Design.Kind,
			Rate:         c.Rule.Rate,
			Burst:        c.Rule.Burst,
			Key:          "header:X-Key",
			Instances:    c.Instances,
			SyncInterval: config.Duration(c.Design.Sync),
			LeaseTarget:  config.Duration(c.Design.Lease),
		}
	}
	return config.Config{
		MaxInflight: new(0),
		Routes:      []config.Route{route},
		Pools:       []config.Pool{{Name: "dim", Dimensions: []config.Dimension{{ID: "dim", Address: "dim:8080", MaxInflight: 4096}}}},
	}
}

func (c LimiterCell) weights() []float64 {
	if c.Split == SplitEven {
		return nil
	}
	ws := make([]float64, c.Instances)
	ws[0] = 0.9
	for i := 1; i < c.Instances; i++ {
		ws[i] = 0.1 / float64(c.Instances-1)
	}
	return ws
}

func limited(f func(Design) bool) []Design {
	return slices.DeleteFunc(slices.Clone(Designs), func(d Design) bool { return !f(d) })
}

var LimiterStudies = map[string]func() []LimiterCell{
	"accuracy": func() []LimiterCell {
		var cs []LimiterCell
		for _, d := range Designs {
			for _, load := range []string{LoadUnder, LoadOver, LoadBurst} {
				for _, split := range []string{SplitEven, SplitShift} {
					cs = append(cs, limiterCell(d, 2, load, split))
				}
			}
		}
		return cs
	},
	"skew": func() []LimiterCell {
		var cs []LimiterCell
		for _, d := range Designs {
			cs = append(cs, limiterCell(d, 2, LoadOver, SplitSkew))
		}
		return cs
	},
	"instances": func() []LimiterCell {
		var cs []LimiterCell
		for _, d := range Designs {
			cs = append(cs, limiterCell(d, 4, LoadOver, SplitShift))
		}
		return cs
	},
	"redis": func() []LimiterCell {
		var cs []LimiterCell
		for _, d := range limited(func(d Design) bool {
			return d.Kind == config.LimitRedisExact || d.Sync == 100*time.Millisecond || d.Lease == 20*time.Millisecond
		}) {
			for _, fault := range []string{limitbench.RedisSlow, limitbench.RedisDown} {
				c := limiterCell(d, 2, LoadOver, SplitEven)
				c.RedisFault = fault
				c.Name += "-redis-" + fault
				cs = append(cs, c)
			}
		}
		return cs
	},
	"zipf": func() []LimiterCell {
		var cs []LimiterCell
		for _, d := range Designs {
			c := limiterCell(d, 2, LoadOver, SplitEven)
			c.Rule = limit.Rule{Rate: 20, Burst: 40}
			c.Keys, c.Zipf, c.Offered = 10000, 1.1, 3000
			c.Name = fmt.Sprintf("%s/zipf-10k-2gw", d.Name())
			cs = append(cs, c)
		}
		return cs
	},
}

func LimiterStudyNames() []string {
	names := make([]string, 0, len(LimiterStudies))
	for n := range LimiterStudies {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}

func LimiterCells(studies []string) ([]LimiterCell, error) {
	var cs []LimiterCell
	seen := map[string]bool{}
	for _, s := range studies {
		build, ok := LimiterStudies[s]
		if !ok {
			return nil, fmt.Errorf("unknown study %q, have %s", s, strings.Join(LimiterStudyNames(), ", "))
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

type LimiterPlan struct {
	Cells []LimiterCell `json:"cells"`
	Reps  int           `json:"reps"`
}

type LimiterRun struct {
	Cell      string             `json:"cell"`
	Rep       int                `json:"rep"`
	Slot      int                `json:"slot"`
	StartedAt time.Time          `json:"startedAt"`
	Spec      LimiterCell        `json:"spec"`
	Report    *limitbench.Report `json:"report"`
}

func gatewayNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("gateway-%d", i+1)
	}
	return out
}

func (r *Runner) RunE2(ctx context.Context, plan LimiterPlan) (dir string, err error) {
	if plan.Reps < 1 || len(plan.Cells) == 0 {
		return "", errors.New("the plan needs cells and at least one repetition")
	}
	dir, err = filepath.Abs(filepath.Join(r.Out, time.Now().UTC().Format("20060102-150405")))
	if err != nil {
		return "", err
	}
	configs := filepath.Join(dir, "configs")
	if err := os.MkdirAll(configs, 0o755); err != nil {
		return "", err
	}
	most := 0
	for _, c := range plan.Cells {
		most = max(most, c.Instances)
		if err := writeJSON(filepath.Join(configs, c.file()+".json"), c.Config()); err != nil {
			return "", err
		}
	}
	compose := r.Compose
	compose.Env = append(slices.Clone(compose.Env), "E2_CONFIGS="+configs, "E2_CELL="+plan.Cells[0].file())
	if most > 2 {
		compose.Env = append(compose.Env, "COMPOSE_PROFILES=four")
	}
	r.logf("starting the limiter stack")
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
	defer func() { err = errors.Join(err, f.Close()) }()
	enc := json.NewEncoder(f)
	total := plan.Reps * len(plan.Cells)
	done := 0
	for rep := range plan.Reps {
		for slot, c := range rotate(plan.Cells, rep) {
			done++
			run, err := r.limiterCell(ctx, compose, c, rep, slot)
			if err != nil {
				return dir, fmt.Errorf("%s rep %d: %w", c.Name, rep+1, err)
			}
			if err := enc.Encode(run); err != nil {
				return dir, err
			}
			g := run.Report.Generator
			line := fmt.Sprintf("[%d/%d] rep %d %-40s p50 %6.0f us  p99 %7.0f us  429 %6d", done, total, rep+1, c.Name, g.LatencyUs.P50, g.LatencyUs.P99, g.Status["429"])
			if v := run.Report.Verdict; v != nil {
				line += fmt.Sprintf("  over %+6.1f%%  ahead %d behind %d  worst %.2fx", v.OverAdmission*100, v.MaxAhead, v.MaxBehind, v.WorstRatio)
			}
			r.logf("%s  redis %.2f trips/req  %s", line, run.Report.Redis.TripsReq, strings.Join(g.Invalid, ", "))
		}
	}
	return dir, nil
}

func (r *Runner) limiterCell(ctx context.Context, compose Compose, c LimiterCell, rep, slot int) (LimiterRun, error) {
	compose.Env = append(slices.Clone(compose.Env), "E2_CELL="+c.file())
	gws := gatewayNames(c.Instances)
	if _, err := compose.run(ctx, append([]string{"up", "-d", "--force-recreate", "--no-deps"}, gws...)...); err != nil {
		return LimiterRun{}, err
	}
	args := []string{
		"exec", "-T", "loadgen", "limitbench",
		"-label", c.Name,
		"-rate", strconv.FormatFloat(c.Offered, 'f', -1, 64),
		"-rule-rate", strconv.FormatFloat(c.Rule.Rate, 'f', -1, 64),
		"-rule-burst", strconv.FormatFloat(c.Rule.Burst, 'f', -1, 64),
		"-limited=" + strconv.FormatBool(c.Design.HasLimiter()),
		"-seed", strconv.Itoa(rep + 1),
		"-warmup", c.Warmup.String(),
		"-duration", c.Duration.String(),
		"-key-prefix", fmt.Sprintf("%s-r%d-", c.file(), rep+1),
		"-keys", strconv.Itoa(max(c.Keys, 1)),
	}
	if c.Zipf > 0 {
		args = append(args, "-zipf", strconv.FormatFloat(c.Zipf, 'f', -1, 64))
	}
	if c.Load == LoadBurst {
		args = append(args, "-burst-rate", strconv.FormatFloat(5*c.Rule.Rate, 'f', -1, 64))
	}
	if ws := c.weights(); ws != nil {
		parts := make([]string, len(ws))
		for i, w := range ws {
			parts[i] = strconv.FormatFloat(w, 'f', -1, 64)
		}
		args = append(args, "-weights", strings.Join(parts, ","))
	}
	if c.Split == SplitShift {
		args = append(args, "-shift-every", "5s")
	}
	if c.RedisFault != "" {
		args = append(args, "-redis-fault", c.RedisFault)
	}
	for _, g := range gws {
		args = append(args, "-url", "http://"+g+":8080/", "-admin", "http://"+g+":9090")
	}
	run := LimiterRun{Cell: c.Name, Rep: rep, Slot: slot, StartedAt: time.Now().UTC(), Spec: c}
	out, err := compose.run(ctx, args...)
	if err != nil {
		return run, err
	}
	run.Report = &limitbench.Report{}
	if err := json.Unmarshal(out, run.Report); err != nil {
		return run, fmt.Errorf("limitbench output: %w", err)
	}
	return run, nil
}
