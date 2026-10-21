package bench

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
)

var registryNodes = []string{"ff1", "ff2", "ff3", "ff4", "ff5"}

type RegistryPlan struct {
	Join     int     `json:"join"`
	Leader   int     `json:"leader"`
	Failover int     `json:"failover"`
	Down     int     `json:"down"`
	Rate     float64 `json:"rate"`
	Label    string  `json:"label"`
}

type RegistryTrial struct {
	Cell          string   `json:"cell"`
	Trial         int      `json:"trial"`
	StartedAt     string   `json:"startedAt"`
	Requests      int      `json:"requests"`
	Lost          int      `json:"lost"`
	Dropped       int      `json:"dropped"`
	Versions      []string `json:"registryVersions"`
	JoinMs        *float64 `json:"joinMs,omitempty"`
	AppliedMs     *float64 `json:"appliedMs,omitempty"`
	GatewaysMs    *float64 `json:"gatewaysMs,omitempty"`
	LeaveMs       *float64 `json:"leaveMs,omitempty"`
	RegisterMs    *float64 `json:"registerMs,omitempty"`
	KillToFirstMs *float64 `json:"killToFirstMs,omitempty"`
	Killed        string   `json:"killed,omitempty"`
	Error         string   `json:"error,omitempty"`
}

type logLine struct {
	Time    time.Time `json:"time"`
	Msg     string    `json:"msg"`
	Version any       `json:"version"`
	Reason  string    `json:"reason"`
}

func (l logLine) configVersion() int64 {
	v, _ := l.Version.(float64)
	return int64(v)
}

func (r *Runner) RunE5(ctx context.Context, plan RegistryPlan) (string, error) {
	if plan.Rate <= 0 || plan.Join+plan.Leader+plan.Failover+plan.Down == 0 {
		return "", errors.New("the plan needs a rate and at least one trial")
	}
	dir, err := filepath.Abs(filepath.Join(r.Out, time.Now().UTC().Format("20060102-150405")+"-"+plan.Label))
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	compose := r.Compose
	r.logf("starting the registry stack")
	if err := compose.Up(ctx); err != nil {
		return "", err
	}
	defer func() {
		if _, err := compose.run(context.Background(), "--profile", "join", "down", "-v", "--remove-orphans"); err != nil {
			r.logf("tearing down: %v", err)
		}
	}()
	if err := writeJSON(filepath.Join(dir, "meta.json"), map[string]any{"plan": plan, "commit": commit(ctx), "startedAt": time.Now().UTC()}); err != nil {
		return dir, err
	}
	f, err := os.Create(filepath.Join(dir, "trials.jsonl"))
	if err != nil {
		return dir, err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	cells := []struct {
		name   string
		trials int
		run    func(context.Context, Compose, float64, int) (RegistryTrial, error)
	}{
		{"join", plan.Join, r.joinTrial},
		{"leader", plan.Leader, r.leaderTrial},
		{"failover", plan.Failover, r.failoverTrial},
		{"down", plan.Down, r.downTrial},
	}
	var all []RegistryTrial
	for _, c := range cells {
		for i := range c.trials {
			if err := r.steady(ctx, compose); err != nil {
				return dir, fmt.Errorf("%s trial %d: %w", c.name, i+1, err)
			}
			t, err := c.run(ctx, compose, plan.Rate, i)
			t.Cell, t.Trial = c.name, i+1
			if err != nil {
				if ctx.Err() != nil {
					return dir, err
				}
				t.Error = err.Error()
			}
			if err := enc.Encode(t); err != nil {
				return dir, err
			}
			all = append(all, t)
			r.logf("%-8s %2d  lost %d of %d  join %s  leave %s  register %s  kill-to-first %s  versions %v %s",
				c.name, i+1, t.Lost, t.Requests, msOrDash(t.JoinMs), msOrDash(t.LeaveMs), msOrDash(t.RegisterMs), msOrDash(t.KillToFirstMs), t.Versions, t.Error)
		}
	}
	return dir, writeJSON(filepath.Join(dir, "summary.json"), SummarizeE5(all))
}

func (c Compose) fetch(ctx context.Context, url string) ([]byte, error) {
	out, err := c.run(ctx, "exec", "-T", "loadgen", "wget", "-q", "-T", "3", "-O", "-", url)
	return out, err
}

func (r *Runner) steady(ctx context.Context, compose Compose) error {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		why := ""
		leaders := map[uint64]bool{}
		for _, n := range registryNodes {
			raw, err := compose.fetch(ctx, "http://"+n+":3171/v1/status")
			var st struct{ Leader uint64 }
			if err == nil {
				err = json.Unmarshal(raw, &st)
			}
			if err != nil || st.Leader == 0 {
				why = n + " has no leader"
				break
			}
			leaders[st.Leader] = true
		}
		if why == "" && len(leaders) != 1 {
			why = "the nodes name different leaders"
		}
		for _, g := range []string{"gateway-1", "gateway-2"} {
			if why != "" {
				break
			}
			raw, err := compose.fetch(ctx, "http://"+g+":9090/state")
			var st struct {
				Upstreams []struct {
					ID      string `json:"id"`
					Healthy bool   `json:"healthy"`
				} `json:"upstreams"`
			}
			if err == nil {
				err = json.Unmarshal(raw, &st)
			}
			healthy := 0
			for _, u := range st.Upstreams {
				if u.Healthy {
					healthy++
				}
			}
			if err != nil || len(st.Upstreams) != 4 || healthy != 4 {
				why = fmt.Sprintf("%s has %d dimensions, %d healthy", g, len(st.Upstreams), healthy)
			}
		}
		if why == "" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("not steady after two minutes: %s", why)
		}
		if !sleepCtx(ctx, time.Second) {
			return ctx.Err()
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

type load struct {
	done chan error
	file string
}

func startLoad(ctx context.Context, compose Compose, rate float64, d time.Duration, seed int) *load {
	l := &load{done: make(chan error, 1), file: fmt.Sprintf("/tmp/e5-%d.bin", seed)}
	go func() {
		_, err := compose.Loadgen(ctx, "-url", "http://gateway-1:8080/", "-url", "http://gateway-2:8080/",
			"-rate", strconv.FormatFloat(rate, 'f', -1, 64), "-arrival", "poisson", "-seed", strconv.Itoa(seed+1),
			"-warmup", "0s", "-duration", d.String(), "-timeout", "5s", "-records", l.file)
		l.done <- err
	}()
	return l
}

func (l *load) wait(ctx context.Context, compose Compose) (*loadgen.Log, error) {
	if err := <-l.done; err != nil {
		return nil, err
	}
	local := filepath.Join(os.TempDir(), filepath.Base(l.file))
	if _, err := compose.run(ctx, "cp", "loadgen:"+l.file, local); err != nil {
		return nil, err
	}
	defer os.Remove(local)
	f, err := os.Open(local)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return loadgen.ReadLog(f)
}

func (c Compose) logLines(ctx context.Context, service string) ([]logLine, error) {
	out, err := c.run(ctx, "--profile", "join", "logs", "--no-color", "--no-log-prefix", service)
	if err != nil {
		return nil, err
	}
	var lines []logLine
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		var l logLine
		if json.Unmarshal(sc.Bytes(), &l) == nil && !l.Time.IsZero() {
			lines = append(lines, l)
		}
	}
	return lines, sc.Err()
}

func first(lines []logLine, ok func(logLine) bool) (logLine, bool) {
	for _, l := range lines {
		if ok(l) {
			return l, true
		}
	}
	return logLine{}, false
}

func ms(from, to time.Time) *float64 {
	v := float64(to.Sub(from)) / float64(time.Millisecond)
	return &v
}

func tally(t *RegistryTrial, lg *loadgen.Log) {
	t.Requests = len(lg.Records)
	for _, rec := range lg.Records {
		switch {
		case rec.Err == loadgen.ErrDropped:
			t.Dropped++
		case rec.Err != loadgen.ErrNone || rec.Status >= 500:
			t.Lost++
		}
	}
}

func served(lg *loadgen.Log, dim string) (firstAt, lastAt time.Time, ok bool) {
	idx := slices.Index(lg.Dimensions, dim)
	if idx < 0 {
		return time.Time{}, time.Time{}, false
	}
	for _, rec := range lg.Records {
		if int(rec.Dimension) != idx+1 || rec.Err != loadgen.ErrNone {
			continue
		}
		at := time.Unix(0, lg.StartUnixNano+rec.Started)
		if !ok || at.Before(firstAt) {
			firstAt = at
		}
		if end := time.Unix(0, lg.StartUnixNano+rec.Ended); end.After(lastAt) {
			lastAt = end
		}
		ok = true
	}
	return firstAt, lastAt, ok
}

func (r *Runner) registryVersions(ctx context.Context, compose Compose, from, to time.Time) ([]logLine, error) {
	lines, err := compose.logLines(ctx, "controller")
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(lines, func(l logLine) bool {
		return l.Msg != "dimensions from the registry applied" || l.Time.Before(from) || l.Time.After(to)
	}), nil
}

func reasons(lines []logLine) []string {
	out := []string{}
	for _, l := range lines {
		out = append(out, fmt.Sprintf("v%d %s", l.configVersion(), l.Reason))
	}
	return out
}

func (r *Runner) appliedAt(ctx context.Context, compose Compose, version int64) (time.Time, error) {
	var last time.Time
	for _, g := range []string{"gateway-1", "gateway-2"} {
		lines, err := compose.logLines(ctx, g)
		if err != nil {
			return last, err
		}
		l, ok := first(lines, func(l logLine) bool { return l.Msg == "cluster config applied" && l.configVersion() == version })
		if !ok {
			return last, fmt.Errorf("%s never applied version %d", g, version)
		}
		last = maxTime(last, l.Time)
	}
	return last, nil
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func (r *Runner) leader(ctx context.Context, compose Compose) (string, error) {
	for _, n := range registryNodes {
		raw, err := compose.fetch(ctx, "http://"+n+":3171/v1/status")
		if err != nil {
			continue
		}
		var st struct {
			ID     uint64 `json:"id"`
			Role   string `json:"role"`
			Leader uint64 `json:"leader"`
		}
		if json.Unmarshal(raw, &st) == nil && st.Leader != 0 {
			return fmt.Sprintf("ff%d", st.Leader), nil
		}
	}
	return "", errors.New("no node names a leader")
}

func (r *Runner) kill(ctx context.Context, compose Compose, service string) (time.Time, error) {
	ids, err := compose.ContainerIDs(ctx, []string{service})
	if err != nil {
		return time.Time{}, err
	}
	if _, err := compose.run(ctx, "kill", "-s", "KILL", service); err != nil {
		return time.Time{}, err
	}
	out, err := exec.CommandContext(ctx, "docker", "inspect", "-f", "{{.State.FinishedAt}}", ids[service]).Output()
	if err != nil {
		return time.Time{}, fmt.Errorf("inspecting %s: %w", service, err)
	}
	return time.Parse(time.RFC3339Nano, strings.TrimSpace(string(out)))
}

func (r *Runner) startAgain(compose Compose, services ...string) {
	if _, err := compose.run(context.Background(), append([]string{"start"}, services...)...); err != nil {
		r.logf("starting %v again: %v", services, err)
	}
}

func (r *Runner) joinTrial(ctx context.Context, compose Compose, rate float64, i int) (RegistryTrial, error) {
	t := RegistryTrial{StartedAt: time.Now().UTC().Format(time.RFC3339)}
	l := startLoad(ctx, compose, rate, 12*time.Second, i)
	sleepCtx(ctx, 2*time.Second)
	if _, err := compose.run(ctx, "--profile", "join", "up", "-d", "--no-deps", "dim-e"); err != nil {
		return t, err
	}
	sleepCtx(ctx, 6*time.Second)
	if _, err := compose.run(ctx, "--profile", "join", "stop", "-t", "10", "dim-e"); err != nil {
		return t, err
	}
	lg, err := l.wait(ctx, compose)
	if err != nil {
		return t, err
	}
	tally(&t, lg)
	defer func() { _, _ = compose.run(context.Background(), "--profile", "join", "rm", "-f", "dim-e") }()
	dim, err := compose.logLines(ctx, "dim-e")
	if err != nil {
		return t, err
	}
	up, ok0 := first(dim, func(l logLine) bool { return l.Msg == "dimension up" })
	registered, ok1 := first(dim, func(l logLine) bool { return l.Msg == "registered" })
	deregistering, ok2 := first(dim, func(l logLine) bool { return l.Msg == "deregistering" })
	if !ok0 || !ok1 || !ok2 {
		return t, errors.New("dim-e never logged its start, registration and deregistration")
	}
	t.RegisterMs = ms(up.Time, registered.Time)
	versions, err := r.registryVersions(ctx, compose, time.Unix(0, lg.StartUnixNano), deregistering.Time.Add(5*time.Second))
	if err != nil {
		return t, err
	}
	t.Versions = reasons(versions)
	firstAt, lastAt, ok := served(lg, "e")
	if !ok {
		return t, errors.New("dim-e served nothing")
	}
	t.JoinMs, t.LeaveMs = ms(registered.Time, firstAt), ms(deregistering.Time, lastAt)
	if v, ok := first(versions, func(l logLine) bool { return strings.Contains(l.Reason, "e joined") }); ok {
		t.AppliedMs = ms(registered.Time, v.Time)
		if at, err := r.appliedAt(ctx, compose, v.configVersion()); err == nil {
			t.GatewaysMs = ms(registered.Time, at)
		}
	}
	return t, nil
}

func (r *Runner) leaderTrial(ctx context.Context, compose Compose, rate float64, i int) (RegistryTrial, error) {
	t := RegistryTrial{StartedAt: time.Now().UTC().Format(time.RFC3339)}
	l := startLoad(ctx, compose, rate, 20*time.Second, 100+i)
	sleepCtx(ctx, 3*time.Second)
	leader, err := r.leader(ctx, compose)
	if err != nil {
		return t, err
	}
	t.Killed = leader
	defer r.startAgain(compose, leader)
	if _, err := r.kill(ctx, compose, leader); err != nil {
		return t, err
	}
	sleepCtx(ctx, 10*time.Second)
	if _, err := compose.run(ctx, "start", leader); err != nil {
		return t, err
	}
	lg, err := l.wait(ctx, compose)
	if err != nil {
		return t, err
	}
	tally(&t, lg)
	versions, err := r.registryVersions(ctx, compose, time.Unix(0, lg.StartUnixNano), time.Unix(0, lg.StartUnixNano).Add(25*time.Second))
	t.Versions = reasons(versions)
	return t, err
}

func (r *Runner) failoverTrial(ctx context.Context, compose Compose, rate float64, i int) (RegistryTrial, error) {
	t := RegistryTrial{StartedAt: time.Now().UTC().Format(time.RFC3339)}
	l := startLoad(ctx, compose, rate, 15*time.Second, 200+i)
	sleepCtx(ctx, 3*time.Second)
	leader, err := r.leader(ctx, compose)
	if err != nil {
		return t, err
	}
	t.Killed = leader
	defer r.startAgain(compose, leader)
	killed, err := r.kill(ctx, compose, leader)
	if err != nil {
		return t, err
	}
	if _, err := compose.run(ctx, "--profile", "join", "up", "-d", "--no-deps", "dim-e"); err != nil {
		return t, err
	}
	defer func() { _, _ = compose.run(context.Background(), "--profile", "join", "rm", "-f", "-s", "dim-e") }()
	lg, err := l.wait(ctx, compose)
	if err != nil {
		return t, err
	}
	tally(&t, lg)
	if _, err := compose.run(ctx, "start", leader); err != nil {
		return t, err
	}
	dim, err := compose.logLines(ctx, "dim-e")
	if err != nil {
		return t, err
	}
	up, ok0 := first(dim, func(l logLine) bool { return l.Msg == "dimension up" })
	registered, ok1 := first(dim, func(l logLine) bool { return l.Msg == "registered" })
	if !ok0 || !ok1 {
		return t, errors.New("dim-e never logged its start and registration")
	}
	t.RegisterMs = ms(up.Time, registered.Time)
	if firstAt, _, ok := served(lg, "e"); ok {
		t.KillToFirstMs, t.JoinMs = ms(killed, firstAt), ms(registered.Time, firstAt)
	}
	versions, err := r.registryVersions(ctx, compose, time.Unix(0, lg.StartUnixNano), time.Unix(0, lg.StartUnixNano).Add(20*time.Second))
	t.Versions = reasons(versions)
	return t, err
}

func (r *Runner) downTrial(ctx context.Context, compose Compose, rate float64, i int) (RegistryTrial, error) {
	t := RegistryTrial{StartedAt: time.Now().UTC().Format(time.RFC3339)}
	l := startLoad(ctx, compose, rate, 45*time.Second, 300+i)
	sleepCtx(ctx, 5*time.Second)
	defer r.startAgain(compose, registryNodes...)
	if _, err := compose.run(ctx, append([]string{"kill", "-s", "KILL"}, registryNodes...)...); err != nil {
		return t, err
	}
	sleepCtx(ctx, 30*time.Second)
	if _, err := compose.run(ctx, append([]string{"start"}, registryNodes...)...); err != nil {
		return t, err
	}
	lg, err := l.wait(ctx, compose)
	if err != nil {
		return t, err
	}
	tally(&t, lg)
	versions, err := r.registryVersions(ctx, compose, time.Unix(0, lg.StartUnixNano), time.Unix(0, lg.StartUnixNano).Add(50*time.Second))
	t.Versions = reasons(versions)
	return t, err
}

type CellSummary struct {
	Trials      int  `json:"trials"`
	Failed      int  `json:"failed"`
	Lost        int  `json:"lost"`
	Requests    int  `json:"requests"`
	Versions    int  `json:"registryVersions"`
	Join        Stat `json:"joinMs"`
	Applied     Stat `json:"appliedMs"`
	Gateways    Stat `json:"gatewaysMs"`
	Leave       Stat `json:"leaveMs"`
	Register    Stat `json:"registerMs"`
	KillToFirst Stat `json:"killToFirstMs"`
}

func SummarizeE5(trials []RegistryTrial) map[string]CellSummary {
	out := map[string]CellSummary{}
	values := map[string]map[string][]float64{}
	for _, t := range trials {
		s := out[t.Cell]
		s.Trials++
		if t.Error != "" {
			s.Failed++
			out[t.Cell] = s
			continue
		}
		s.Lost += t.Lost
		s.Requests += t.Requests
		s.Versions += len(t.Versions)
		out[t.Cell] = s
		if values[t.Cell] == nil {
			values[t.Cell] = map[string][]float64{}
		}
		for name, v := range map[string]*float64{"join": t.JoinMs, "applied": t.AppliedMs, "gateways": t.GatewaysMs, "leave": t.LeaveMs, "register": t.RegisterMs, "killToFirst": t.KillToFirstMs} {
			if v != nil {
				values[t.Cell][name] = append(values[t.Cell][name], *v)
			}
		}
	}
	for cell, s := range out {
		v := values[cell]
		s.Join, s.Applied, s.Gateways = statOf(v["join"]), statOf(v["applied"]), statOf(v["gateways"])
		s.Leave, s.Register, s.KillToFirst = statOf(v["leave"]), statOf(v["register"]), statOf(v["killToFirst"])
		out[cell] = s
	}
	return out
}
