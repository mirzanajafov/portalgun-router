package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
)

var dimensions = []string{"dim-a", "dim-b", "dim-c"}

type System struct {
	Name    string   `json:"name"`
	Service string   `json:"service,omitempty"`
	URLs    []string `json:"urls"`
}

func proxied(name string) System {
	return System{Name: name, Service: name, URLs: []string{"http://" + name + ":8080/"}}
}

var Systems = []System{
	{Name: "direct", URLs: []string{"http://dim-a:8080/", "http://dim-b:8080/", "http://dim-c:8080/"}},
	proxied("stdlib"),
	proxied("portalgun"),
	proxied("nginx"),
	proxied("caddy"),
	proxied("envoy"),
	proxied("haproxy"),
	proxied("portalgun-cap1024"),
	proxied("portalgun-cap128"),
	proxied("portalgun-cap256"),
	proxied("portalgun-cap512"),
}

func SystemsByName(names []string) ([]System, error) {
	var out []System
	for _, n := range names {
		i := slices.IndexFunc(Systems, func(s System) bool { return s.Name == n })
		if i < 0 {
			return nil, fmt.Errorf("unknown system %q", n)
		}
		out = append(out, Systems[i])
	}
	return out, nil
}

type Workload struct {
	Name        string  `json:"name"`
	Method      string  `json:"method"`
	BodyBytes   int     `json:"bodyBytes"`
	RespBytes   int     `json:"respBytes"`
	DelayMs     float64 `json:"delayMs"`
	NoKeepAlive bool    `json:"noKeepAlive"`
}

var Workloads = map[string]Workload{
	"w1": {Name: "w1", Method: "GET", RespBytes: 1 << 10},
	"w2": {Name: "w2", Method: "GET", RespBytes: 1 << 10, DelayMs: 5},
	"w3": {Name: "w3", Method: "GET", RespBytes: 128 << 10},
	"w4": {Name: "w4", Method: "POST", BodyBytes: 16 << 10, RespBytes: 1 << 10},
	"w5": {Name: "w5", Method: "GET", RespBytes: 1 << 10, NoKeepAlive: true},
}

func (w Workload) env() []string {
	return []string{
		"BODY_BYTES=" + strconv.Itoa(w.RespBytes),
		"DELAY_MS=" + strconv.FormatFloat(w.DelayMs, 'f', -1, 64),
	}
}

type Plan struct {
	Workload Workload      `json:"workload"`
	Systems  []System      `json:"systems"`
	Rates    []float64     `json:"rates"`
	Reps     int           `json:"reps"`
	Duration time.Duration `json:"duration"`
	Warmup   time.Duration `json:"warmup"`
	MaxConns int           `json:"maxConns"`
	Overload bool          `json:"overload,omitempty"`
}

type Point struct {
	Workload           string             `json:"workload"`
	System             string             `json:"system"`
	Rate               float64            `json:"rate"`
	Rep                int                `json:"rep"`
	Slot               int                `json:"slot"`
	StartedAt          time.Time          `json:"startedAt"`
	ElapsedSec         float64            `json:"elapsedSec"`
	SystemCPUSec       float64            `json:"systemCpuSec"`
	CPUPerRequestUs    float64            `json:"cpuPerRequestUs"`
	UserPerRequestUs   float64            `json:"userPerRequestUs"`
	KernelPerRequestUs float64            `json:"kernelPerRequestUs"`
	MemoryPeakMB       float64            `json:"memoryPeakMb,omitempty"`
	SwapPeakMB         float64            `json:"swapPeakMb,omitempty"`
	AnonPeakMB         float64            `json:"anonPeakMb,omitempty"`
	SockPeakMB         float64            `json:"sockPeakMb,omitempty"`
	DimensionCPU       map[string]float64 `json:"dimensionCpuSec"`
	DimensionUtil      float64            `json:"dimensionUtil"`
	Saturated          bool               `json:"saturated"`
	Died               string             `json:"died,omitempty"`
	Reasons            []string           `json:"reasons,omitempty"`
	GeneratorOK        bool               `json:"generatorOk"`
	Attempt            int                `json:"attempt"`
	Confirmation       bool               `json:"confirmation,omitempty"`
	Loadgen            loadgen.Summary    `json:"loadgen"`
}

const (
	maxP99Us         = 50_000
	maxFailureShare  = 0.001
	minThroughput    = 0.95
	maxGeneratorLag  = 1000
	maxGeneratorUtil = 0.8
	maxDimensionUtil = 0.5
	primeDuration    = 10 * time.Second
	maxAttempts      = 3
)

func judge(p *Point) {
	s := p.Loadgen
	p.GeneratorOK = s.LagUs.P99 <= maxGeneratorLag && (s.CPU == nil || s.CPU.Utilization <= maxGeneratorUtil)
	if s.Scheduled > 0 {
		if share := float64(s.Failed+s.Dropped) / float64(s.Scheduled); share > maxFailureShare {
			p.Reasons = append(p.Reasons, fmt.Sprintf("%.2f%% failed", share*100))
		}
	}
	if s.ThroughputRps < minThroughput*p.Rate {
		p.Reasons = append(p.Reasons, fmt.Sprintf("throughput %.0f of %.0f", s.ThroughputRps, p.Rate))
	}
	if p.GeneratorOK && s.LatencyUs.P99 > maxP99Us {
		p.Reasons = append(p.Reasons, fmt.Sprintf("p99 %.1f ms", s.LatencyUs.P99/1000))
	}
	p.Saturated = len(p.Reasons) > 0
	if !p.GeneratorOK {
		p.Reasons = append(p.Reasons, "generator limited")
	}
	if p.DimensionUtil > maxDimensionUtil {
		p.Reasons = append(p.Reasons, fmt.Sprintf("dimension at %.0f%%", p.DimensionUtil*100))
	}
}

func rotate[T any](s []T, k int) []T {
	if len(s) == 0 {
		return nil
	}
	k %= len(s)
	return append(slices.Clone(s[k:]), s[:k]...)
}

type Runner struct {
	Compose Compose
	Out     string
	Log     io.Writer
	ids     map[string]string
}

func (r *Runner) RunE1(ctx context.Context, plan Plan) (string, error) {
	if plan.Reps < 1 || len(plan.Rates) == 0 || len(plan.Systems) == 0 {
		return "", errors.New("the plan needs systems, rates and at least one repetition")
	}
	dir := filepath.Join(r.Out, plan.Workload.Name, time.Now().UTC().Format("20060102-150405"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	compose := r.Compose
	compose.Env = append(slices.Clone(compose.Env), plan.Workload.env()...)
	r.logf("starting the stack for %s", plan.Workload.Name)
	if err := compose.Up(ctx); err != nil {
		return "", err
	}
	defer func() {
		if err := compose.Down(context.Background()); err != nil {
			r.logf("tearing down: %v", err)
		}
	}()
	if err := r.waitReady(ctx, compose, plan); err != nil {
		return "", err
	}
	services := slices.Clone(dimensions)
	for _, s := range plan.Systems {
		if s.Service != "" {
			services = append(services, s.Service)
		}
	}
	ids, err := compose.ContainerIDs(ctx, services)
	if err != nil {
		return "", err
	}
	r.ids = ids
	if err := r.prime(ctx, compose, plan); err != nil {
		return "", err
	}
	meta := map[string]any{
		"plan":      plan,
		"versions":  versions(ctx, compose, plan.Systems),
		"commit":    commit(ctx),
		"startedAt": time.Now().UTC(),
		"env":       compose.Env,
	}
	if err := writeJSON(filepath.Join(dir, "meta.json"), meta); err != nil {
		return "", err
	}
	f, err := os.Create(filepath.Join(dir, "runs.jsonl"))
	if err != nil {
		return "", err
	}
	defer f.Close()
	enc := json.NewEncoder(f)

	total := plan.Reps * len(plan.Rates) * len(plan.Systems)
	done := 0
	for rep := range plan.Reps {
		limit := map[string]float64{}
		for ri, rate := range plan.Rates {
			for slot, sys := range rotate(plan.Systems, rep+ri) {
				done++
				if l, ok := limit[sys.Name]; ok && rate > l && !plan.Overload {
					continue
				}
				confirmations := []bool{false, true}
				if plan.Overload {
					confirmations = confirmations[:1]
				}
				if plan.Overload {
					if err := r.restart(ctx, compose, plan, sys); err != nil {
						return dir, err
					}
				}
				saturated := true
				for _, confirmation := range confirmations {
					p, err := r.measure(ctx, compose, plan, sys, rate, rep, slot)
					if err != nil || p.Saturated {
						died, rerr := r.revive(ctx, compose, plan, sys)
						switch {
						case rerr != nil:
							return dir, errors.Join(err, rerr)
						case died != "":
							p.Died, p.Saturated = died, true
							p.Reasons = append(p.Reasons, sys.Name+" "+died)
						case err != nil:
							return dir, err
						}
					}
					p.Confirmation = confirmation
					if err := enc.Encode(p); err != nil {
						return dir, err
					}
					r.logf("[%d/%d] rep %d %-8s %7.0f rps  p50 %7.1f us  p99 %8.1f us  cpu %6.1f us/req  %s",
						done, total, rep+1, sys.Name, rate, p.Loadgen.LatencyUs.P50, p.Loadgen.LatencyUs.P99, p.CPUPerRequestUs, strings.Join(p.Reasons, ", "))
					if p.Died != "" {
						break
					}
					if !p.Saturated {
						saturated = false
						break
					}
				}
				if saturated {
					limit[sys.Name] = rate
				}
			}
		}
	}
	return dir, nil
}

func (r *Runner) measure(ctx context.Context, compose Compose, plan Plan, sys System, rate float64, rep, slot int) (Point, error) {
	var p Point
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		var err error
		p, err = r.point(ctx, compose, plan, sys, rate, rep, slot)
		if err != nil {
			return p, err
		}
		p.Attempt = attempt
		if p.GeneratorOK || p.Saturated {
			return p, nil
		}
		r.logf("  %s at %.0f rps: the generator fell behind, repeating", sys.Name, rate)
	}
	return p, nil
}

func (r *Runner) point(ctx context.Context, compose Compose, plan Plan, sys System, rate float64, rep, slot int) (Point, error) {
	p := Point{Workload: plan.Workload.Name, System: sys.Name, Rate: rate, Rep: rep, Slot: slot, StartedAt: time.Now().UTC()}
	began := time.Now()
	sum, err := compose.Loadgen(ctx, append(loadgenArgs(plan, sys, rate), watchArgs(r.ids, sys)...)...)
	if err != nil {
		return p, err
	}
	p.Loadgen = sum
	p.ElapsedSec = time.Since(began).Seconds()
	p.DimensionCPU = map[string]float64{}
	for _, d := range dimensions {
		w := sum.Watched[d]
		p.DimensionCPU[d] = w.Seconds
		p.DimensionUtil = max(p.DimensionUtil, w.Utilization)
	}
	if w, ok := sum.Watched["system"]; ok && sum.Completed > 0 {
		served := float64(sum.Completed)
		p.SystemCPUSec = w.Seconds
		p.CPUPerRequestUs = w.Seconds / served * 1e6
		p.UserPerRequestUs = w.UserSeconds / served * 1e6
		p.KernelPerRequestUs = w.SystemSeconds / served * 1e6
	}
	if w, ok := sum.Watched["system"]; ok {
		p.MemoryPeakMB, p.SwapPeakMB = w.MemoryPeakMB, w.SwapPeakMB
		p.AnonPeakMB, p.SockPeakMB = w.AnonPeakMB, w.SockPeakMB
	}
	judge(&p)
	return p, nil
}

func (r *Runner) revive(ctx context.Context, compose Compose, plan Plan, sys System) (string, error) {
	id, ok := r.ids[sys.Service]
	if !ok {
		return "", nil
	}
	out, err := exec.CommandContext(ctx, "docker", "inspect", "-f", "{{.State.Running}} {{.State.OOMKilled}} {{.State.ExitCode}}", id).Output()
	if err != nil {
		return "", fmt.Errorf("inspecting %s: %w", sys.Name, err)
	}
	died := death(string(out))
	if died == "" {
		return "", nil
	}
	r.logf("  %s %s, starting it again", sys.Name, died)
	if _, err := compose.run(ctx, "start", sys.Service); err != nil {
		return died, err
	}
	p := plan
	p.Systems = []System{sys}
	return died, r.waitReady(ctx, compose, p)
}

func (r *Runner) restart(ctx context.Context, compose Compose, plan Plan, sys System) error {
	if sys.Service == "" {
		return nil
	}
	if _, err := compose.run(ctx, "restart", sys.Service); err != nil {
		return err
	}
	p := plan
	p.Systems = []System{sys}
	return r.waitReady(ctx, compose, p)
}

func death(state string) string {
	f := strings.Fields(state)
	switch {
	case len(f) != 3 || f[0] == "true":
		return ""
	case f[1] == "true":
		return "OOM-killed"
	}
	return "exited with " + f[2]
}

func watchArgs(ids map[string]string, sys System) []string {
	if len(ids) == 0 {
		return nil
	}
	args := []string{"-cgroup-root", "/host/cgroup"}
	for _, d := range dimensions {
		args = append(args, "-watch", d+"="+ids[d])
	}
	if sys.Service != "" {
		args = append(args, "-watch", "system="+ids[sys.Service])
	}
	return args
}

func loadgenArgs(plan Plan, sys System, rate float64) []string {
	w := plan.Workload
	args := []string{
		"-rate", strconv.FormatFloat(rate, 'f', -1, 64),
		"-duration", plan.Duration.String(),
		"-warmup", plan.Warmup.String(),
		"-method", w.Method,
		"-label", fmt.Sprintf("%s/%s/%.0f", w.Name, sys.Name, rate),
	}
	for _, u := range sys.URLs {
		args = append(args, "-url", u)
	}
	if w.BodyBytes > 0 {
		args = append(args, "-body", strconv.Itoa(w.BodyBytes))
	}
	if w.NoKeepAlive {
		args = append(args, "-no-keepalive")
	}
	if plan.MaxConns > 0 {
		args = append(args, "-max-conns", strconv.Itoa(plan.MaxConns))
	}
	return args
}

func (r *Runner) waitReady(ctx context.Context, compose Compose, plan Plan) error {
	for _, sys := range plan.Systems {
		var last error
		ready := false
		for range 20 {
			args := []string{"-rate", "50", "-duration", "1s", "-warmup", "0s", "-timeout", "2s"}
			for _, u := range sys.URLs {
				args = append(args, "-url", u)
			}
			sum, err := compose.Loadgen(ctx, args...)
			if err == nil && sum.Status["200"] == sum.Scheduled && sum.Scheduled > 0 {
				ready = true
				break
			}
			last = err
			if err == nil {
				last = fmt.Errorf("status %v, errors %v", sum.Status, sum.Errors)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
		if !ready {
			return fmt.Errorf("%s never became ready: %v", sys.Name, last)
		}
	}
	return nil
}

func (r *Runner) prime(ctx context.Context, compose Compose, plan Plan) error {
	rate := slices.Min(plan.Rates)
	for _, sys := range plan.Systems {
		r.logf("priming %s at %.0f rps", sys.Name, rate)
		p := plan
		p.Duration, p.Warmup = primeDuration, 0
		if _, err := compose.Loadgen(ctx, loadgenArgs(p, sys, rate)...); err != nil {
			return fmt.Errorf("priming %s: %w", sys.Name, err)
		}
	}
	return nil
}

func versions(ctx context.Context, compose Compose, systems []System) map[string]string {
	probes := map[string]string{
		"stdlib":  "stdproxy -version",
		"nginx":   "nginx -v",
		"haproxy": "haproxy -v",
		"caddy":   "caddy version",
		"envoy":   "envoy --version",
	}
	out := map[string]string{}
	for _, s := range systems {
		probe, ok := probes[s.Service]
		if !ok {
			continue
		}
		v, err := compose.Exec(ctx, s.Service, "sh", "-c", probe+" 2>&1")
		if err != nil {
			v = "unknown: " + err.Error()
		}
		out[s.Name] = firstLine(v)
	}
	return out
}

func commit(ctx context.Context) string {
	out, err := exec.CommandContext(ctx, "git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		if c := os.Getenv("BENCH_COMMIT"); c != "" {
			return c
		}
		return "unknown"
	}
	c := strings.TrimSpace(string(out))
	if dirty, err := exec.CommandContext(ctx, "git", "status", "--porcelain", "--untracked-files=no").Output(); err == nil && len(bytes.TrimSpace(dirty)) > 0 {
		c += "-dirty"
	}
	return c
}

func firstLine(s string) string {
	for line := range strings.Lines(s) {
		if t := strings.TrimSpace(line); t != "" {
			return t
		}
	}
	return ""
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func (r *Runner) logf(format string, args ...any) {
	if r.Log != nil {
		fmt.Fprintf(r.Log, format+"\n", args...)
	}
}
