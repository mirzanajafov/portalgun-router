package bench

import (
	"bufio"
	"bytes"
	"cmp"
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
	e5Gateways     = []string{"gateway-1", "gateway-2"}
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

type RegistryRun struct {
	Cell              string   `json:"cell"`
	Run               int      `json:"run"`
	StartedAt         string   `json:"startedAt"`
	Requests          int      `json:"requests"`
	Lost              int      `json:"lost"`
	Dropped           int      `json:"dropped"`
	Invalid           []string `json:"invalid,omitempty"`
	Versions          []string `json:"registryVersions"`
	Refused           int      `json:"refusedLists"`
	LostRegistrations int      `json:"lostRegistrations"`
	RegisterMs        *float64 `json:"registerMs,omitempty"`
	JoinMs            *float64 `json:"joinMs,omitempty"`
	AppliedMs         *float64 `json:"appliedMs,omitempty"`
	GatewaysMs        *float64 `json:"gatewaysMs,omitempty"`
	LeaveAppliedMs    *float64 `json:"leaveAppliedMs,omitempty"`
	LeaveGatewaysMs   *float64 `json:"leaveGatewaysMs,omitempty"`
	UpAfterKillMs     *float64 `json:"upAfterKillMs,omitempty"`
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

type registryStatus struct {
	Role   string `json:"role"`
	Leader uint64 `json:"leader"`
}

func (r *Runner) RunE5(ctx context.Context, plan RegistryPlan) (string, error) {
	if plan.Rate <= 0 || plan.Join+plan.Leader+plan.Failover+plan.Down == 0 {
		return "", errors.New("the plan needs a rate and at least one run")
	}
	image, err := exec.CommandContext(ctx, "docker", "image", "inspect", "-f", "{{.Id}}", plan.Image).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker image inspect %s: %w: %s (build FiveFamilies' node image in its repo, or name another one in FF_IMAGE)", plan.Image, err, bytes.TrimSpace(image))
	}
	compose := r.Compose
	compose.Env = append(slices.Clone(compose.Env), "FF_IMAGE="+plan.Image)
	defer func() {
		if _, err := compose.run(context.Background(), "--profile", "join", "down", "-v", "--remove-orphans"); err != nil {
			r.logf("tearing down: %v", err)
		}
	}()
	r.logf("starting the registry stack")
	if err := compose.Up(ctx); err != nil {
		return "", err
	}
	dir, err := filepath.Abs(filepath.Join(r.Out, time.Now().UTC().Format("20060102-150405")+"-"+plan.Label))
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	meta := map[string]any{"plan": plan, "commit": commit(ctx), "imageId": strings.TrimSpace(string(image)), "startedAt": time.Now().UTC()}
	if err := writeJSON(filepath.Join(dir, "meta.json"), meta); err != nil {
		return dir, err
	}
	f, err := os.Create(filepath.Join(dir, "runs.jsonl"))
	if err != nil {
		return dir, err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	cells := []struct {
		name string
		runs int
		run  func(context.Context, Compose, float64, int, *RegistryRun) error
	}{
		{"join", plan.Join, r.joinRun},
		{"leader", plan.Leader, r.leaderRun},
		{"failover", plan.Failover, r.failoverRun},
		{"down", plan.Down, r.downRun},
	}
	for _, c := range cells {
		for i := range c.runs {
			if err := steady(ctx, compose); err != nil {
				return dir, fmt.Errorf("%s run %d: %w", c.name, i+1, err)
			}
			run := RegistryRun{Cell: c.name, Run: i + 1, StartedAt: time.Now().UTC().Format(time.RFC3339)}
			if err := c.run(ctx, compose, plan.Rate, i, &run); err != nil {
				if ctx.Err() != nil {
					return dir, err
				}
				run.Error = err.Error()
			}
			if err := enc.Encode(run); err != nil {
				return dir, err
			}
			r.logf("%-8s %2d  lost %d of %d  join %s  applied %s  leave %s  up after kill %s  kill to first %s  versions %v  refused %d  lost registrations %d %s",
				c.name, i+1, run.Lost, run.Requests, msOrDash(run.JoinMs), msOrDash(run.AppliedMs), msOrDash(run.LeaveGatewaysMs),
				msOrDash(run.UpAfterKillMs), msOrDash(run.KillToFirstMs), run.Versions, run.Refused, run.LostRegistrations, run.Error)
		}
	}
	return dir, nil
}

func fetch(ctx context.Context, compose Compose, url string, v any) error {
	out, err := compose.Exec(ctx, "loadgen", "wget", "-q", "-T", "3", "-O", "-", url)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(out), v)
}

func steady(ctx context.Context, compose Compose) error {
	deadline := time.Now().Add(2 * time.Minute)
	last := int64(-1)
	for {
		version, why := settled(ctx, compose)
		if why == "" && version == last {
			return nil
		}
		last = -1
		if why == "" {
			last = version
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("not steady after two minutes: %s", cmp.Or(why, "the gateways' config kept changing"))
		}
		if !sleepCtx(ctx, time.Second) {
			return ctx.Err()
		}
	}
}

func settled(ctx context.Context, compose Compose) (int64, string) {
	leaders := map[uint64]bool{}
	for _, n := range registryNodes {
		var st registryStatus
		if err := fetch(ctx, compose, "http://"+n+":3171/v1/status", &st); err != nil || st.Leader == 0 {
			return 0, n + " has no leader"
		}
		leaders[st.Leader] = true
	}
	if len(leaders) != 1 {
		return 0, "the nodes name different leaders"
	}
	var listed struct {
		Instances []json.RawMessage `json:"instances"`
	}
	if err := fetch(ctx, compose, "http://"+registryNodes[0]+":3171/v1/registry/dimensions", &listed); err != nil || len(listed.Instances) != len(registeredDims) {
		return 0, fmt.Sprintf("the registry lists %d of %d dimensions", len(listed.Instances), len(registeredDims))
	}
	version := int64(-1)
	for k, g := range e5Gateways {
		var st struct {
			Upstreams []struct {
				Healthy bool `json:"healthy"`
			} `json:"upstreams"`
		}
		healthy := 0
		err := fetch(ctx, compose, "http://"+g+":9090/state", &st)
		for _, u := range st.Upstreams {
			if u.Healthy {
				healthy++
			}
		}
		if err != nil || len(st.Upstreams) != len(registeredDims) || healthy != len(registeredDims) {
			return 0, fmt.Sprintf("%s has %d dimensions, %d healthy", g, len(st.Upstreams), healthy)
		}
		lines, err := serviceLog(ctx, compose, g)
		if err != nil {
			return 0, err.Error()
		}
		applied := slices.DeleteFunc(lines, func(l logLine) bool { return l.Msg != "cluster config applied" })
		if len(applied) == 0 {
			return 0, g + " has applied no config"
		}
		v := applied[len(applied)-1].configVersion()
		if k > 0 && v != version {
			return 0, "the gateways run different config versions"
		}
		version = v
	}
	return version, ""
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

func (l *load) wait(ctx context.Context, compose Compose, run *RegistryRun) (*loadgen.Log, error) {
	l.waited = true
	if err := <-l.done; err != nil {
		return nil, err
	}
	run.Invalid = l.sum.Invalid
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
		tally(run, lg)
	}
	return lg, err
}

func serviceLog(ctx context.Context, compose Compose, service string) ([]logLine, error) {
	out, err := compose.run(ctx, "--profile", "join", "logs", "--no-color", "--no-log-prefix", service)
	if err != nil {
		return nil, err
	}
	return parseLog(out)
}

func parseLog(out []byte) ([]logLine, error) {
	var lines []logLine
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(make([]byte, 1<<16), 1<<20)
	for sc.Scan() {
		var l logLine
		if json.Unmarshal(sc.Bytes(), &l) == nil && !l.Time.IsZero() {
			lines = append(lines, l)
		}
	}
	return lines, sc.Err()
}

func firstLogged(lines []logLine, from time.Time, msgs ...string) ([]time.Time, error) {
	at := make([]time.Time, len(msgs))
	for k, msg := range msgs {
		i := slices.IndexFunc(lines, func(l logLine) bool { return l.Msg == msg && !l.Time.Before(from) })
		if i < 0 {
			return nil, fmt.Errorf("no %q in the log", msg)
		}
		at[k] = lines[i].Time
	}
	return at, nil
}

func within(lines []logLine, msg string, from, to time.Time) []logLine {
	return slices.DeleteFunc(slices.Clone(lines), func(l logLine) bool {
		return l.Msg != msg || l.Time.Before(from) || l.Time.After(to)
	})
}

func versionWith(versions []logLine, change string) (logLine, bool) {
	i := slices.IndexFunc(versions, func(l logLine) bool { return slices.Contains(strings.Split(l.Reason, ", "), change) })
	if i < 0 {
		return logLine{}, false
	}
	return versions[i], true
}

func firstApplied(lines []logLine, version int64) (time.Time, bool) {
	i := slices.IndexFunc(lines, func(l logLine) bool { return l.Msg == "cluster config applied" && l.configVersion() >= version })
	if i < 0 {
		return time.Time{}, false
	}
	return lines[i].Time, true
}

func ms(from, to time.Time) *float64 {
	v := float64(to.Sub(from)) / float64(time.Millisecond)
	return &v
}

func tally(run *RegistryRun, lg *loadgen.Log) {
	run.Requests = len(lg.Records)
	for _, rec := range lg.Records {
		switch {
		case rec.Err == loadgen.ErrDropped:
			run.Dropped++
		case rec.Err != loadgen.ErrNone || rec.Status >= 500:
			run.Lost++
		}
	}
}

func firstServedBy(lg *loadgen.Log, dim string) (time.Time, bool) {
	idx := slices.Index(lg.Dimensions, dim)
	var first time.Time
	ok := false
	for _, rec := range lg.Records {
		if idx < 0 || int(rec.Dimension) != idx+1 || rec.Err != loadgen.ErrNone {
			continue
		}
		if at := time.Unix(0, lg.StartUnixNano+rec.Started); !ok || at.Before(first) {
			first, ok = at, true
		}
	}
	return first, ok
}

func registryAfter(ctx context.Context, compose Compose, run *RegistryRun, from, to time.Time) ([]logLine, error) {
	lines, err := serviceLog(ctx, compose, "controller")
	if err != nil {
		return nil, err
	}
	versions := within(lines, "dimensions from the registry applied", from, to)
	run.Versions = []string{}
	for _, l := range versions {
		run.Versions = append(run.Versions, fmt.Sprintf("v%d %s", l.configVersion(), l.Reason))
	}
	run.Refused = len(within(lines, "the registry's list is not applied, keeping the pool as it is", from, to))
	for _, d := range registeredDims {
		lines, err := serviceLog(ctx, compose, d)
		if err != nil {
			return nil, err
		}
		run.LostRegistrations += len(within(lines, "the registration was lost, registering again", from, to))
	}
	return versions, nil
}

func reached(ctx context.Context, compose Compose, versions []logLine, change string, from time.Time) (applied, gateways *float64, err error) {
	v, ok := versionWith(versions, change)
	if !ok {
		return nil, nil, fmt.Errorf("the controller wrote no version where %s", change)
	}
	var last time.Time
	for _, g := range e5Gateways {
		lines, err := serviceLog(ctx, compose, g)
		if err != nil {
			return nil, nil, err
		}
		at, ok := firstApplied(lines, v.configVersion())
		if !ok {
			return nil, nil, fmt.Errorf("%s never applied version %d or a later one", g, v.configVersion())
		}
		if at.After(last) {
			last = at
		}
	}
	return ms(from, v.Time), ms(from, last), nil
}

func leader(ctx context.Context, compose Compose) (string, error) {
	for _, n := range registryNodes {
		var st registryStatus
		if err := fetch(ctx, compose, "http://"+n+":3171/v1/status", &st); err == nil && st.Role == "leader" {
			return n, nil
		}
	}
	return "", errors.New("no node says it leads")
}

func kill(ctx context.Context, compose Compose, service string) (time.Time, error) {
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

func dimESteps(ctx context.Context, compose Compose, from time.Time, msgs ...string) ([]time.Time, error) {
	lines, err := serviceLog(ctx, compose, "dim-e")
	if err != nil {
		return nil, err
	}
	at, err := firstLogged(lines, from, msgs...)
	if err != nil {
		return nil, fmt.Errorf("dim-e: %w", err)
	}
	return at, nil
}

func (r *Runner) joinRun(ctx context.Context, compose Compose, rate float64, i int, run *RegistryRun) error {
	defer r.removeDimE(compose)
	l := startLoad(ctx, compose, rate, 12*time.Second, i)
	defer l.drain()
	sleepCtx(ctx, 2*time.Second)
	if _, err := compose.run(ctx, "--profile", "join", "up", "-d", "--no-deps", "--force-recreate", "dim-e"); err != nil {
		return err
	}
	sleepCtx(ctx, 6*time.Second)
	if _, err := compose.run(ctx, "--profile", "join", "stop", "-t", "10", "dim-e"); err != nil {
		return err
	}
	lg, err := l.wait(ctx, compose, run)
	if err != nil {
		return err
	}
	start := time.Unix(0, lg.StartUnixNano)
	at, err := dimESteps(ctx, compose, start, "dimension up", "registered", "deregistering", "deregistered")
	if err != nil {
		return err
	}
	up, registered, deregistering := at[0], at[1], at[2]
	run.RegisterMs = ms(up, registered)
	versions, err := registryAfter(ctx, compose, run, start, deregistering.Add(5*time.Second))
	if err != nil {
		return err
	}
	firstAt, ok := firstServedBy(lg, "e")
	if !ok {
		return errors.New("dim-e served nothing")
	}
	run.JoinMs = ms(registered, firstAt)
	if run.AppliedMs, run.GatewaysMs, err = reached(ctx, compose, versions, "e joined", registered); err != nil {
		return err
	}
	run.LeaveAppliedMs, run.LeaveGatewaysMs, err = reached(ctx, compose, versions, "e left", deregistering)
	return err
}

func (r *Runner) leaderRun(ctx context.Context, compose Compose, rate float64, i int, run *RegistryRun) error {
	l := startLoad(ctx, compose, rate, 20*time.Second, 100+i)
	defer l.drain()
	sleepCtx(ctx, 3*time.Second)
	node, err := leader(ctx, compose)
	if err != nil {
		return err
	}
	run.Killed = node
	defer r.startAgain(compose, node)
	if _, err := kill(ctx, compose, node); err != nil {
		return err
	}
	sleepCtx(ctx, 10*time.Second)
	if _, err := compose.run(ctx, "start", node); err != nil {
		return err
	}
	lg, err := l.wait(ctx, compose, run)
	if err != nil {
		return err
	}
	start := time.Unix(0, lg.StartUnixNano)
	_, err = registryAfter(ctx, compose, run, start, start.Add(25*time.Second))
	return err
}

func (r *Runner) failoverRun(ctx context.Context, compose Compose, rate float64, i int, run *RegistryRun) error {
	defer r.removeDimE(compose)
	if _, err := compose.run(ctx, "--profile", "join", "up", "--no-start", "--no-deps", "--force-recreate", "dim-e"); err != nil {
		return err
	}
	l := startLoad(ctx, compose, rate, 25*time.Second, 200+i)
	defer l.drain()
	sleepCtx(ctx, 3*time.Second)
	node, err := leader(ctx, compose)
	if err != nil {
		return err
	}
	run.Killed = node
	defer r.startAgain(compose, node)
	killed, err := kill(ctx, compose, node)
	if err != nil {
		return err
	}
	if _, err := compose.run(ctx, "--profile", "join", "start", "dim-e"); err != nil {
		return err
	}
	lg, err := l.wait(ctx, compose, run)
	if err != nil {
		return err
	}
	start := time.Unix(0, lg.StartUnixNano)
	at, err := dimESteps(ctx, compose, start, "dimension up", "registered")
	if err != nil {
		return err
	}
	up, registered := at[0], at[1]
	run.UpAfterKillMs, run.RegisterMs = ms(killed, up), ms(up, registered)
	versions, err := registryAfter(ctx, compose, run, start, start.Add(30*time.Second))
	if err != nil {
		return err
	}
	firstAt, ok := firstServedBy(lg, "e")
	if !ok {
		return errors.New("dim-e served nothing before the load ended")
	}
	run.KillToFirstMs, run.JoinMs = ms(killed, firstAt), ms(registered, firstAt)
	run.AppliedMs, run.GatewaysMs, err = reached(ctx, compose, versions, "e joined", registered)
	return err
}

func (r *Runner) downRun(ctx context.Context, compose Compose, rate float64, i int, run *RegistryRun) error {
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
	lg, err := l.wait(ctx, compose, run)
	if err != nil {
		return err
	}
	start := time.Unix(0, lg.StartUnixNano)
	_, err = registryAfter(ctx, compose, run, start, start.Add(50*time.Second))
	return err
}
