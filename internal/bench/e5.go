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

var (
	registryNodes  = []string{"ff1", "ff2", "ff3", "ff4", "ff5"}
	registeredDims = []string{"dim-a", "dim-b", "dim-c", "dim-d"}
)

type RegistryPlan struct {
	Join     int     `json:"join"`
	Leader   int     `json:"leader"`
	Failover int     `json:"failover"`
	Down     int     `json:"down"`
	Rate     float64 `json:"rate"`
	Label    string  `json:"label"`
	Image    string  `json:"image"`
}

type RegistryTrial struct {
	Cell              string   `json:"cell"`
	Trial             int      `json:"trial"`
	StartedAt         string   `json:"startedAt"`
	Requests          int      `json:"requests"`
	Lost              int      `json:"lost"`
	Dropped           int      `json:"dropped"`
	Invalid           []string `json:"invalid,omitempty"`
	Versions          []string `json:"registryVersions"`
	LostRegistrations int      `json:"lostRegistrations"`
	JoinMs            *float64 `json:"joinMs,omitempty"`
	AppliedMs         *float64 `json:"appliedMs,omitempty"`
	GatewaysMs        *float64 `json:"gatewaysMs,omitempty"`
	LeaveMs           *float64 `json:"leaveMs,omitempty"`
	RegisterMs        *float64 `json:"registerMs,omitempty"`
	KillToFirstMs     *float64 `json:"killToFirstMs,omitempty"`
	Killed            string   `json:"killed,omitempty"`
	Error             string   `json:"error,omitempty"`
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

type nodeStatus struct {
	ID     uint64 `json:"id"`
	Role   string `json:"role"`
	Leader uint64 `json:"leader"`
}

func (r *Runner) RunE5(ctx context.Context, plan RegistryPlan) (string, error) {
	if plan.Rate <= 0 || plan.Join+plan.Leader+plan.Failover+plan.Down == 0 {
		return "", errors.New("the plan needs a rate and at least one trial")
	}
	image, err := exec.CommandContext(ctx, "docker", "image", "inspect", "-f", "{{.Id}}", plan.Image).Output()
	if err != nil {
		return "", fmt.Errorf("no %s image: build FiveFamilies' node image in its repo, or name another one in FF_IMAGE", plan.Image)
	}
	dir, err := filepath.Abs(filepath.Join(r.Out, time.Now().UTC().Format("20060102-150405")+"-"+plan.Label))
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	compose := r.Compose
	compose.Env = append(slices.Clone(compose.Env), "FF_IMAGE="+plan.Image)
	r.logf("starting the registry stack")
	if err := compose.Up(ctx); err != nil {
		return "", err
	}
	defer func() {
		if _, err := compose.run(context.Background(), "--profile", "join", "down", "-v", "--remove-orphans"); err != nil {
			r.logf("tearing down: %v", err)
		}
	}()
	meta := map[string]any{"plan": plan, "commit": commit(ctx), "imageId": strings.TrimSpace(string(image)), "startedAt": time.Now().UTC()}
	if err := writeJSON(filepath.Join(dir, "meta.json"), meta); err != nil {
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
		run    func(context.Context, Compose, float64, int, *RegistryTrial) error
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
			t := RegistryTrial{Cell: c.name, Trial: i + 1, StartedAt: time.Now().UTC().Format(time.RFC3339)}
			if err := c.run(ctx, compose, plan.Rate, i, &t); err != nil {
				if ctx.Err() != nil {
					return dir, err
				}
				t.Error = err.Error()
			}
			if err := enc.Encode(t); err != nil {
				return dir, err
			}
			all = append(all, t)
			r.logf("%-8s %2d  lost %d of %d  join %s  leave %s  register %s  kill-to-first %s  versions %v  lost registrations %d %s",
				c.name, i+1, t.Lost, t.Requests, msOrDash(t.JoinMs), msOrDash(t.LeaveMs), msOrDash(t.RegisterMs), msOrDash(t.KillToFirstMs), t.Versions, t.LostRegistrations, t.Error)
		}
	}
	return dir, writeJSON(filepath.Join(dir, "summary.json"), SummarizeE5(all))
}

func ReportE5(dir string) error {
	f, err := os.Open(filepath.Join(dir, "trials.jsonl"))
	if err != nil {
		return err
	}
	defer f.Close()
	var trials []RegistryTrial
	dec := json.NewDecoder(f)
	for dec.More() {
		var t RegistryTrial
		if err := dec.Decode(&t); err != nil {
			return fmt.Errorf("trials.jsonl: %w", err)
		}
		trials = append(trials, t)
	}
	return writeJSON(filepath.Join(dir, "summary.json"), SummarizeE5(trials))
}

func (c Compose) status(ctx context.Context, node string) (nodeStatus, error) {
	var st nodeStatus
	out, err := c.Exec(ctx, "loadgen", "wget", "-q", "-T", "3", "-O", "-", "http://"+node+":3171/v1/status")
	if err == nil {
		err = json.Unmarshal([]byte(out), &st)
	}
	return st, err
}

func (r *Runner) steady(ctx context.Context, compose Compose) error {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		why := ""
		leaders := map[uint64]bool{}
		for _, n := range registryNodes {
			st, err := compose.status(ctx, n)
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
			out, err := compose.Exec(ctx, "loadgen", "wget", "-q", "-T", "3", "-O", "-", "http://"+g+":9090/state")
			var st struct {
				Upstreams []struct {
					Healthy bool `json:"healthy"`
				} `json:"upstreams"`
			}
			if err == nil {
				err = json.Unmarshal([]byte(out), &st)
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
	done   chan error
	sum    loadgen.Summary
	file   string
	waited bool
}

func startLoad(ctx context.Context, compose Compose, rate float64, d time.Duration, seed int) *load {
	l := &load{done: make(chan error, 1), file: fmt.Sprintf("/tmp/e5-%d.bin", seed)}
	go func() {
		sum, err := compose.Loadgen(ctx, "-url", "http://gateway-1:8080/", "-url", "http://gateway-2:8080/",
			"-rate", strconv.FormatFloat(rate, 'f', -1, 64), "-arrival", "poisson", "-seed", strconv.Itoa(seed+1),
			"-warmup", "0s", "-duration", d.String(), "-timeout", "5s", "-records", l.file)
		l.sum = sum
		l.done <- err
	}()
	return l
}

func (l *load) drain() {
	if !l.waited {
		<-l.done
		l.waited = true
	}
}

func (l *load) wait(ctx context.Context, compose Compose, t *RegistryTrial) (*loadgen.Log, error) {
	l.waited = true
	if err := <-l.done; err != nil {
		return nil, err
	}
	t.Invalid = l.sum.Invalid
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
	lg, err := loadgen.ReadLog(f)
	if err == nil {
		tally(t, lg)
	}
	return lg, err
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

func logged(lines []logLine, msg string) (time.Time, bool) {
	i := slices.IndexFunc(lines, func(l logLine) bool { return l.Msg == msg })
	if i < 0 {
		return time.Time{}, false
	}
	return lines[i].Time, true
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

func servedBy(lg *loadgen.Log, dim string) (firstAt, lastAt time.Time, ok bool) {
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

func within(lines []logLine, msg string, from, to time.Time) []logLine {
	return slices.DeleteFunc(slices.Clone(lines), func(l logLine) bool {
		return l.Msg != msg || l.Time.Before(from) || l.Time.After(to)
	})
}

func (r *Runner) registryAfter(ctx context.Context, compose Compose, t *RegistryTrial, from, to time.Time) ([]logLine, error) {
	lines, err := compose.logLines(ctx, "controller")
	if err != nil {
		return nil, err
	}
	versions := within(lines, "dimensions from the registry applied", from, to)
	t.Versions = []string{}
	for _, l := range versions {
		t.Versions = append(t.Versions, fmt.Sprintf("v%d %s", l.configVersion(), l.Reason))
	}
	for _, d := range registeredDims {
		lines, err := compose.logLines(ctx, d)
		if err != nil {
			return nil, err
		}
		t.LostRegistrations += len(within(lines, "the registration was lost, registering again", from, to))
	}
	return versions, nil
}

func (r *Runner) appliedAt(ctx context.Context, compose Compose, version int64) (time.Time, error) {
	var last time.Time
	for _, g := range []string{"gateway-1", "gateway-2"} {
		lines, err := compose.logLines(ctx, g)
		if err != nil {
			return last, err
		}
		i := slices.IndexFunc(lines, func(l logLine) bool { return l.Msg == "cluster config applied" && l.configVersion() == version })
		if i < 0 {
			return last, fmt.Errorf("%s never applied version %d", g, version)
		}
		if lines[i].Time.After(last) {
			last = lines[i].Time
		}
	}
	return last, nil
}

func (r *Runner) leader(ctx context.Context, compose Compose) (string, error) {
	for _, n := range registryNodes {
		if st, err := compose.status(ctx, n); err == nil && st.Role == "leader" {
			return n, nil
		}
	}
	return "", errors.New("no node says it leads")
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

func (r *Runner) removeDimE(compose Compose) {
	if _, err := compose.run(context.Background(), "--profile", "join", "rm", "-f", "-s", "dim-e"); err != nil {
		r.logf("removing dim-e: %v", err)
	}
}

func (r *Runner) joinTrial(ctx context.Context, compose Compose, rate float64, i int, t *RegistryTrial) error {
	defer r.removeDimE(compose)
	l := startLoad(ctx, compose, rate, 12*time.Second, i)
	defer l.drain()
	sleepCtx(ctx, 2*time.Second)
	if _, err := compose.run(ctx, "--profile", "join", "up", "-d", "--no-deps", "dim-e"); err != nil {
		return err
	}
	sleepCtx(ctx, 6*time.Second)
	if _, err := compose.run(ctx, "--profile", "join", "stop", "-t", "10", "dim-e"); err != nil {
		return err
	}
	lg, err := l.wait(ctx, compose, t)
	if err != nil {
		return err
	}
	dim, err := compose.logLines(ctx, "dim-e")
	if err != nil {
		return err
	}
	up, ok0 := logged(dim, "dimension up")
	registered, ok1 := logged(dim, "registered")
	deregistering, ok2 := logged(dim, "deregistering")
	if !ok0 || !ok1 || !ok2 {
		return errors.New("dim-e never logged its start, registration and deregistration")
	}
	t.RegisterMs = ms(up, registered)
	versions, err := r.registryAfter(ctx, compose, t, time.Unix(0, lg.StartUnixNano), deregistering.Add(5*time.Second))
	if err != nil {
		return err
	}
	firstAt, lastAt, ok := servedBy(lg, "e")
	if !ok {
		return errors.New("dim-e served nothing")
	}
	t.JoinMs, t.LeaveMs = ms(registered, firstAt), ms(deregistering, lastAt)
	if i := slices.IndexFunc(versions, func(l logLine) bool { return strings.Contains(l.Reason, "e joined") }); i >= 0 {
		t.AppliedMs = ms(registered, versions[i].Time)
		if at, err := r.appliedAt(ctx, compose, versions[i].configVersion()); err == nil {
			t.GatewaysMs = ms(registered, at)
		}
	}
	return nil
}

func (r *Runner) leaderTrial(ctx context.Context, compose Compose, rate float64, i int, t *RegistryTrial) error {
	l := startLoad(ctx, compose, rate, 20*time.Second, 100+i)
	defer l.drain()
	sleepCtx(ctx, 3*time.Second)
	leader, err := r.leader(ctx, compose)
	if err != nil {
		return err
	}
	t.Killed = leader
	defer r.startAgain(compose, leader)
	if _, err := r.kill(ctx, compose, leader); err != nil {
		return err
	}
	sleepCtx(ctx, 10*time.Second)
	if _, err := compose.run(ctx, "start", leader); err != nil {
		return err
	}
	lg, err := l.wait(ctx, compose, t)
	if err != nil {
		return err
	}
	start := time.Unix(0, lg.StartUnixNano)
	_, err = r.registryAfter(ctx, compose, t, start, start.Add(25*time.Second))
	return err
}

func (r *Runner) failoverTrial(ctx context.Context, compose Compose, rate float64, i int, t *RegistryTrial) error {
	defer r.removeDimE(compose)
	l := startLoad(ctx, compose, rate, 25*time.Second, 200+i)
	defer l.drain()
	sleepCtx(ctx, 3*time.Second)
	leader, err := r.leader(ctx, compose)
	if err != nil {
		return err
	}
	t.Killed = leader
	defer r.startAgain(compose, leader)
	killed, err := r.kill(ctx, compose, leader)
	if err != nil {
		return err
	}
	if _, err := compose.run(ctx, "--profile", "join", "up", "-d", "--no-deps", "dim-e"); err != nil {
		return err
	}
	lg, err := l.wait(ctx, compose, t)
	if err != nil {
		return err
	}
	dim, err := compose.logLines(ctx, "dim-e")
	if err != nil {
		return err
	}
	up, ok0 := logged(dim, "dimension up")
	registered, ok1 := logged(dim, "registered")
	if !ok0 || !ok1 {
		return errors.New("dim-e never logged its start and registration")
	}
	t.RegisterMs = ms(up, registered)
	start := time.Unix(0, lg.StartUnixNano)
	if _, err := r.registryAfter(ctx, compose, t, start, start.Add(30*time.Second)); err != nil {
		return err
	}
	firstAt, _, ok := servedBy(lg, "e")
	if !ok {
		return errors.New("dim-e served nothing before the load ended")
	}
	t.KillToFirstMs, t.JoinMs = ms(killed, firstAt), ms(registered, firstAt)
	return nil
}

func (r *Runner) downTrial(ctx context.Context, compose Compose, rate float64, i int, t *RegistryTrial) error {
	l := startLoad(ctx, compose, rate, 45*time.Second, 300+i)
	defer l.drain()
	sleepCtx(ctx, 5*time.Second)
	defer r.startAgain(compose, registryNodes...)
	if _, err := compose.run(ctx, append([]string{"kill", "-s", "KILL"}, registryNodes...)...); err != nil {
		return err
	}
	sleepCtx(ctx, 30*time.Second)
	if _, err := compose.run(ctx, append([]string{"start"}, registryNodes...)...); err != nil {
		return err
	}
	lg, err := l.wait(ctx, compose, t)
	if err != nil {
		return err
	}
	start := time.Unix(0, lg.StartUnixNano)
	_, err = r.registryAfter(ctx, compose, t, start, start.Add(50*time.Second))
	return err
}

type CellSummary struct {
	Trials            int   `json:"trials"`
	Failed            int   `json:"failed"`
	Invalid           int   `json:"invalid"`
	Lost              int   `json:"lost"`
	Requests          int   `json:"requests"`
	Versions          int   `json:"registryVersions"`
	LostRegistrations int   `json:"lostRegistrations"`
	Join              *Stat `json:"joinMs,omitempty"`
	Applied           *Stat `json:"appliedMs,omitempty"`
	Gateways          *Stat `json:"gatewaysMs,omitempty"`
	Leave             *Stat `json:"leaveMs,omitempty"`
	Register          *Stat `json:"registerMs,omitempty"`
	KillToFirst       *Stat `json:"killToFirstMs,omitempty"`
}

func SummarizeE5(trials []RegistryTrial) map[string]CellSummary {
	out := map[string]CellSummary{}
	values := map[string]map[string][]float64{}
	for _, t := range trials {
		s := out[t.Cell]
		s.Trials++
		s.Lost += t.Lost
		s.Requests += t.Requests
		s.Versions += len(t.Versions)
		s.LostRegistrations += t.LostRegistrations
		if t.Error != "" {
			s.Failed++
		}
		if len(t.Invalid) > 0 {
			s.Invalid++
		}
		out[t.Cell] = s
		if values[t.Cell] == nil {
			values[t.Cell] = map[string][]float64{}
		}
		for name, v := range map[string]*float64{"join": t.JoinMs, "applied": t.AppliedMs, "gateways": t.GatewaysMs, "leave": t.LeaveMs, "register": t.RegisterMs, "killToFirst": t.KillToFirstMs} {
			if v != nil && t.Error == "" {
				values[t.Cell][name] = append(values[t.Cell][name], *v)
			}
		}
	}
	stat := func(vs []float64) *Stat {
		if len(vs) == 0 {
			return nil
		}
		s := statOf(vs)
		return &s
	}
	for cell, s := range out {
		v := values[cell]
		s.Join, s.Applied, s.Gateways = stat(v["join"]), stat(v["applied"]), stat(v["gateways"])
		s.Leave, s.Register, s.KillToFirst = stat(v["leave"]), stat(v["register"]), stat(v["killToFirst"])
		out[cell] = s
	}
	return out
}
