package bench

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
)

var (
	registryNodes  = []string{"ff1", "ff2", "ff3", "ff4", "ff5"}
	registeredDims = []string{"dim-a", "dim-b", "dim-c", "dim-d"}
	registeredIDs  = []string{"a", "b", "c", "d"}
	e5Gateways     = []string{"gateway-1", "gateway-2"}
)

type RegistryPlan struct {
	Join        int     `json:"join"`
	Leader      int     `json:"leader"`
	Failover    int     `json:"failover"`
	Down        int     `json:"down"`
	Rate        float64 `json:"rate"`
	Label       string  `json:"label"`
	Image       string  `json:"image"`
	ImageCommit string  `json:"imageCommit"`
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
	Refusals          int      `json:"refusals"`
	LostRegistrations int      `json:"lostRegistrations"`
	RegisterMs        *float64 `json:"registerMs,omitempty"`
	JoinMs            *float64 `json:"joinMs,omitempty"`
	AppliedMs         *float64 `json:"appliedMs,omitempty"`
	GatewaysMs        *float64 `json:"gatewaysMs,omitempty"`
	LeaveAppliedMs    *float64 `json:"leaveAppliedMs,omitempty"`
	LeaveGatewaysMs   *float64 `json:"leaveGatewaysMs,omitempty"`
	LeftBeforeEndMs   *float64 `json:"leftBeforeEndMs,omitempty"`
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

func (r *Runner) RunE5(ctx context.Context, plan RegistryPlan) (dir string, err error) {
	counts := []int{plan.Join, plan.Leader, plan.Failover, plan.Down}
	if math.IsNaN(plan.Rate) || math.IsInf(plan.Rate, 0) || plan.Rate <= 0 || slices.Min(counts) < 0 || slices.Max(counts) == 0 {
		return "", errors.New("the plan needs a finite positive rate, no negative run counts and at least one run")
	}
	image, err := docker(ctx, "image", "inspect", "-f", "{{.Id}}", plan.Image)
	if err != nil {
		return "", fmt.Errorf("%w (build FiveFamilies' node image in its repo, or name another one in FF_IMAGE)", err)
	}
	if plan.ImageCommit == "" {
		r.Log.Printf("FF_COMMIT is not set, so the results won't say which FiveFamilies commit %s was built from", plan.Image)
	}
	compose := r.Compose
	compose.Env = append(slices.Clone(compose.Env), "FF_IMAGE="+plan.Image)
	down := func(ctx context.Context) error {
		_, err := compose.run(ctx, "--profile", "join", "down", "-v", "--remove-orphans")
		return err
	}
	if err := down(ctx); err != nil {
		return "", fmt.Errorf("clearing what an earlier run left: %w", err)
	}
	defer func() {
		if err := down(context.Background()); err != nil {
			r.Log.Printf("tearing down: %v", err)
		}
	}()
	r.Log.Printf("starting the registry stack")
	if err := compose.Up(ctx); err != nil {
		return "", err
	}
	dir, err = filepath.Abs(filepath.Join(r.Out, time.Now().UTC().Format("20060102-150405")+"-"+plan.Label))
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	meta := map[string]any{"plan": plan, "commit": commit(ctx), "imageId": image, "startedAt": time.Now().UTC()}
	if err := WriteJSON(filepath.Join(dir, "meta.json"), meta); err != nil {
		return dir, err
	}
	f, err := os.Create(filepath.Join(dir, "runs.jsonl"))
	if err != nil {
		return dir, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
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
			r.Log.Printf("%-8s %2d  lost %d of %d  join %s  applied %s  leave %s  up after kill %s  kill to first %s  versions %v  refusals %d  lost registrations %d %s",
				c.name, i+1, run.Lost, run.Requests, msOrDash(run.JoinMs), msOrDash(run.AppliedMs), msOrDash(run.LeaveGatewaysMs),
				msOrDash(run.UpAfterKillMs), msOrDash(run.KillToFirstMs), run.Versions, run.Refusals, run.LostRegistrations, run.Error)
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
		if err := fetch(ctx, compose, "http://"+n+":3171/v1/status", &st); err != nil {
			return 0, fmt.Sprintf("%s: %v", n, err)
		}
		if st.Leader == 0 {
			return 0, n + " has no leader"
		}
		leaders[st.Leader] = true
	}
	if len(leaders) != 1 {
		return 0, "the nodes name different leaders"
	}
	var listed struct {
		Instances []struct {
			Name string `json:"name"`
		} `json:"instances"`
	}
	if err := fetch(ctx, compose, "http://"+registryNodes[0]+":3171/v1/registry/dimensions", &listed); err != nil {
		return 0, fmt.Sprintf("listing the dimensions: %v", err)
	}
	var names []string
	for _, in := range listed.Instances {
		names = append(names, in.Name)
	}
	if !slices.Equal(names, registeredIDs) {
		return 0, fmt.Sprintf("the registry lists %v, want %v", names, registeredIDs)
	}
	version := int64(-1)
	for k, g := range e5Gateways {
		var st gatewayapi.State
		if err := fetch(ctx, compose, "http://"+g+":9090/state", &st); err != nil {
			return 0, fmt.Sprintf("%s: %v", g, err)
		}
		healthy := 0
		for _, u := range st.Upstreams {
			if u.Healthy {
				healthy++
			}
		}
		if len(st.Upstreams) != len(registeredDims) || healthy != len(registeredDims) {
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
	done          chan struct{}
	sum           loadgen.Summary
	err           error
	file          string
	duration      time.Duration
	log           *loadgen.Log
	progress      *log.Logger
	counted, read bool
}

const (
	registryTail = 5 * time.Second
	lostMessage  = "the registration was lost, registering again"
)

func (r *Runner) startLoad(ctx context.Context, compose Compose, rate float64, d time.Duration, seed int) *load {
	l := &load{done: make(chan struct{}), file: fmt.Sprintf("/tmp/e5-%d.bin", seed), duration: d, progress: r.Log}
	go func() {
		defer close(l.done)
		l.sum, l.err = compose.Loadgen(ctx, "-url", "http://gateway-1:8080/", "-url", "http://gateway-2:8080/",
			"-rate", strconv.FormatFloat(rate, 'f', -1, 64), "-arrival", "poisson", "-seed", strconv.Itoa(seed+1),
			"-warmup", "0s", "-duration", d.String(), "-timeout", "5s", "-records", l.file)
	}()
	return l
}

func (l *load) finish(ctx context.Context, compose Compose, run *RegistryRun) error {
	if !l.counted {
		if _, err := l.count(ctx, compose, run); err != nil {
			return err
		}
	}
	if l.log == nil || l.read {
		return nil
	}
	_, err := l.readWindow(ctx, compose, run)
	return err
}

func (l *load) started() time.Time {
	return time.Unix(0, l.log.StartUnixNano)
}

func (l *load) windowEnd() time.Time {
	return l.started().Add(l.duration + registryTail)
}

func (l *load) readWindow(ctx context.Context, compose Compose, run *RegistryRun) ([]logLine, error) {
	l.read = true
	if !sleepCtx(ctx, registryTail) {
		return nil, ctx.Err()
	}
	return registryAfter(ctx, compose, run, l.started(), l.windowEnd())
}

func (l *load) count(ctx context.Context, compose Compose, run *RegistryRun) (*loadgen.Log, error) {
	<-l.done
	l.counted = true
	if l.err != nil {
		return nil, l.err
	}
	run.Invalid = l.sum.Invalid
	local := filepath.Join(os.TempDir(), filepath.Base(l.file))
	if _, err := compose.run(ctx, "cp", "loadgen:"+l.file, local); err != nil {
		return nil, err
	}
	defer func() {
		if err := os.Remove(local); err != nil {
			l.progress.Printf("removing %s: %v", local, err)
		}
	}()
	f, err := os.Open(local)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	lg, err := loadgen.ReadLog(f)
	if err != nil {
		return nil, err
	}
	tally(run, lg)
	l.log = lg
	return lg, nil
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

func changed(versions []logLine, change string) (logLine, error) {
	i := slices.IndexFunc(versions, func(l logLine) bool { return slices.Contains(strings.Split(l.Reason, ", "), change) })
	if i < 0 {
		return logLine{}, fmt.Errorf("the controller wrote no version where %s", change)
	}
	return versions[i], nil
}

func firstApplied(lines []logLine, version, before int64) (time.Time, bool) {
	i := slices.IndexFunc(lines, func(l logLine) bool {
		return l.Msg == "cluster config applied" && l.configVersion() >= version && l.configVersion() < before
	})
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
	run.Refusals = len(within(lines, "the registry's list is not applied, keeping the pool as it is", from, to))
	for _, d := range registeredDims {
		lines, err := serviceLog(ctx, compose, d)
		if err != nil {
			return nil, err
		}
		run.LostRegistrations += len(within(lines, lostMessage, from, to))
	}
	return versions, nil
}

func reached(ctx context.Context, compose Compose, v logLine, before int64, from time.Time) (applied, gateways *float64, err error) {
	var last time.Time
	for _, g := range e5Gateways {
		lines, err := serviceLog(ctx, compose, g)
		if err != nil {
			return nil, nil, err
		}
		at, ok := firstApplied(lines, v.configVersion(), before)
		if !ok {
			return nil, nil, fmt.Errorf("%s applied no version from %d and before %d", g, v.configVersion(), before)
		}
		if at.After(last) {
			last = at
		}
	}
	return ms(from, v.Time), ms(from, last), nil
}

func leader(ctx context.Context, compose Compose) (string, error) {
	errs := []error{errors.New("no node says it leads")}
	for _, n := range registryNodes {
		var st registryStatus
		if err := fetch(ctx, compose, "http://"+n+":3171/v1/status", &st); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", n, err))
		} else if st.Role == "leader" {
			return n, nil
		}
	}
	return "", errors.Join(errs...)
}

func kill(ctx context.Context, compose Compose, service string) (time.Time, error) {
	ids, err := compose.ContainerIDs(ctx, []string{service})
	if err != nil {
		return time.Time{}, err
	}
	if _, err := compose.run(ctx, "kill", "-s", "KILL", service); err != nil {
		return time.Time{}, err
	}
	out, err := docker(ctx, "inspect", "-f", "{{.State.FinishedAt}}", ids[service])
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339Nano, out)
}

func (r *Runner) startAgain(compose Compose, services ...string) {
	if _, err := compose.run(context.Background(), append([]string{"start"}, services...)...); err != nil {
		r.Log.Printf("starting %v again: %v", services, err)
	}
}

func (r *Runner) removeDimE(compose Compose) {
	if _, err := compose.run(context.Background(), "--profile", "join", "rm", "-f", "-s", "dim-e"); err != nil {
		r.Log.Printf("removing dim-e: %v", err)
	}
}

func dimESteps(ctx context.Context, compose Compose, run *RegistryRun, from, to time.Time, msgs ...string) ([]time.Time, error) {
	lines, err := serviceLog(ctx, compose, "dim-e")
	if err != nil {
		return nil, err
	}
	run.LostRegistrations += len(within(lines, lostMessage, from, to))
	at, err := firstLogged(lines, from, msgs...)
	if err != nil {
		return nil, fmt.Errorf("dim-e: %w", err)
	}
	return at, nil
}

func (r *Runner) joinRun(ctx context.Context, compose Compose, rate float64, i int, run *RegistryRun) (err error) {
	defer r.removeDimE(compose)
	l := r.startLoad(ctx, compose, rate, 15*time.Second, i)
	defer func() { err = errors.Join(err, l.finish(ctx, compose, run)) }()
	sleepCtx(ctx, 2*time.Second)
	if _, err := compose.run(ctx, "--profile", "join", "up", "-d", "--no-deps", "--force-recreate", "dim-e"); err != nil {
		return err
	}
	sleepCtx(ctx, 6*time.Second)
	if _, err := compose.run(ctx, "--profile", "join", "stop", "-t", "10", "dim-e"); err != nil {
		return err
	}
	lg, err := l.count(ctx, compose, run)
	if err != nil {
		return err
	}
	versions, err := l.readWindow(ctx, compose, run)
	if err != nil {
		return err
	}
	at, err := dimESteps(ctx, compose, run, l.started(), l.windowEnd(), "dimension up", "registered", "deregistering", "deregistered")
	if err != nil {
		return err
	}
	up, registered, deregistering := at[0], at[1], at[2]
	run.RegisterMs, run.LeftBeforeEndMs = ms(up, registered), ms(at[3], l.started().Add(l.duration))
	firstAt, ok := firstServedBy(lg, "e")
	if !ok {
		return errors.New("dim-e served nothing")
	}
	run.JoinMs = ms(registered, firstAt)
	joined, err := changed(versions, "e joined")
	if err != nil {
		return err
	}
	left, err := changed(versions, "e left")
	if err != nil {
		return err
	}
	if run.AppliedMs, run.GatewaysMs, err = reached(ctx, compose, joined, left.configVersion(), registered); err != nil {
		return err
	}
	run.LeaveAppliedMs, run.LeaveGatewaysMs, err = reached(ctx, compose, left, math.MaxInt64, deregistering)
	return err
}

func (r *Runner) leaderRun(ctx context.Context, compose Compose, rate float64, i int, run *RegistryRun) (err error) {
	l := r.startLoad(ctx, compose, rate, 20*time.Second, 100+i)
	defer func() { err = errors.Join(err, l.finish(ctx, compose, run)) }()
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
	_, err = compose.run(ctx, "start", node)
	return err
}

func (r *Runner) failoverRun(ctx context.Context, compose Compose, rate float64, i int, run *RegistryRun) (err error) {
	defer r.removeDimE(compose)
	if _, err := compose.run(ctx, "--profile", "join", "up", "--no-start", "--no-deps", "--force-recreate", "dim-e"); err != nil {
		return err
	}
	l := r.startLoad(ctx, compose, rate, 25*time.Second, 200+i)
	defer func() { err = errors.Join(err, l.finish(ctx, compose, run)) }()
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
	if _, err := compose.run(ctx, "--profile", "join", "up", "-d", "--no-deps", "dim-e"); err != nil {
		return err
	}
	lg, err := l.count(ctx, compose, run)
	if err != nil {
		return err
	}
	versions, err := l.readWindow(ctx, compose, run)
	if err != nil {
		return err
	}
	at, err := dimESteps(ctx, compose, run, l.started(), l.windowEnd(), "dimension up", "registered")
	if err != nil {
		return err
	}
	up, registered := at[0], at[1]
	run.UpAfterKillMs, run.RegisterMs = ms(killed, up), ms(up, registered)
	firstAt, ok := firstServedBy(lg, "e")
	if !ok {
		return errors.New("dim-e served nothing before the load ended")
	}
	run.KillToFirstMs, run.JoinMs = ms(killed, firstAt), ms(registered, firstAt)
	joined, err := changed(versions, "e joined")
	if err != nil {
		return err
	}
	run.AppliedMs, run.GatewaysMs, err = reached(ctx, compose, joined, math.MaxInt64, registered)
	return err
}

func (r *Runner) downRun(ctx context.Context, compose Compose, rate float64, i int, run *RegistryRun) (err error) {
	l := r.startLoad(ctx, compose, rate, 45*time.Second, 300+i)
	defer func() { err = errors.Join(err, l.finish(ctx, compose, run)) }()
	sleepCtx(ctx, 5*time.Second)
	defer r.startAgain(compose, registryNodes...)
	if _, err := compose.run(ctx, append([]string{"kill", "-s", "KILL"}, registryNodes...)...); err != nil {
		return err
	}
	sleepCtx(ctx, 30*time.Second)
	_, err = compose.run(ctx, append([]string{"start"}, registryNodes...)...)
	return err
}
