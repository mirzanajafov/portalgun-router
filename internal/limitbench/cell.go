package limitbench

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/chaos"
	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
	"github.com/mirzanajafov/portalgun-router/internal/httpx"
	"github.com/mirzanajafov/portalgun-router/internal/limit"
	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
	"github.com/redis/go-redis/v9"
)

const (
	RedisSlow = "slow"
	RedisDown = "down"
	redisName = "redis"
)

type Cell struct {
	Label        string
	Gateways     []string
	Admins       []string
	Toxiproxy    string
	Redis        string
	RedisFault   string
	RedisLatency time.Duration
	Rule         limit.Rule
	Limited      bool
	Load         loadgen.Config
}

type Instance struct {
	Admin         string `json:"admin"`
	Decisions     int    `json:"decisions"`
	Dropped       int64  `json:"dropped"`
	Degraded      int64  `json:"degraded"`
	RoundTrips    int64  `json:"roundTrips"`
	RedisCalls    int64  `json:"redisCalls"`
	RedisSlow     int64  `json:"redisSlow"`
	RedisTimeouts int64  `json:"redisTimeouts"`
	RedisDials    int64  `json:"redisDials"`
	RedisSlowDial int64  `json:"redisSlowDials"`
}

type Redis struct {
	Commands    int64   `json:"commands"`
	RoundTrips  int64   `json:"roundTrips"`
	CPUSeconds  float64 `json:"cpuSeconds"`
	CommandsReq float64 `json:"commandsPerRequest"`
	TripsReq    float64 `json:"roundTripsPerRequest"`
	CPUPerReqUs float64 `json:"cpuPerRequestUs"`
}

const (
	degradedMetric = "portal_rate_limit_degraded_total"
	tripsMetric    = "portal_redis_round_trips_total"
	callsMetric    = "portal_redis_call_seconds"
	dialsMetric    = "portal_redis_dial_seconds"
	slowAfter      = "0.002"

	callsKey    = "calls"
	slowKey     = "slow"
	timeoutsKey = "timeouts"
	dialsKey    = "dials"
	slowDialKey = "slowDials"
)

type Report struct {
	Label     string          `json:"label"`
	Config    json.RawMessage `json:"config,omitempty"`
	Generator loadgen.Summary `json:"generator"`
	Instances []Instance      `json:"instances"`
	Redis     Redis           `json:"redis"`
	Verdict   *Verdict        `json:"verdict,omitempty"`
}

func Requests(decisions int, measured int64, warmup, window time.Duration) float64 {
	if decisions > 0 {
		return float64(decisions)
	}
	if window <= 0 {
		return float64(measured)
	}
	return float64(measured) * float64(warmup+window) / float64(window)
}

func (r *Redis) perRequest(n float64) {
	if n <= 0 {
		return
	}
	r.CommandsReq = float64(r.Commands) / n
	r.TripsReq = float64(r.RoundTrips) / n
	r.CPUPerReqUs = r.CPUSeconds * 1e6 / n
}

func (c Cell) validate() error {
	switch {
	case len(c.Gateways) == 0 || len(c.Gateways) != len(c.Admins):
		return errors.New("needs one admin address per gateway")
	case c.Toxiproxy == "" || c.Redis == "":
		return errors.New("needs the toxiproxy and redis addresses")
	case c.RedisFault != "" && c.RedisFault != RedisSlow && c.RedisFault != RedisDown:
		return fmt.Errorf("unknown redis fault %q", c.RedisFault)
	case c.Limited && (!(c.Rule.Rate > 0) || c.Rule.Burst < 1):
		return errors.New("a limited route needs the rule for the oracle")
	}
	return nil
}

func Run(ctx context.Context, c Cell) (*Report, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 30 * time.Second}
	toxi := chaos.NewToxiproxy(c.Toxiproxy)
	if err := toxi.Reset(ctx); err != nil {
		return nil, err
	}
	defer func() { _ = toxi.Reset(context.WithoutCancel(ctx)) }()
	switch c.RedisFault {
	case RedisSlow:
		err := toxi.AddToxic(ctx, redisName, chaos.Toxic{Name: "slow", Type: "latency", Stream: "downstream", Toxicity: 1, Attributes: map[string]int{"latency": int(c.RedisLatency / time.Millisecond)}})
		if err != nil {
			return nil, err
		}
	case RedisDown:
		if err := toxi.SetEnabled(ctx, redisName, false); err != nil {
			return nil, err
		}
	}
	for _, a := range c.Admins {
		if err := waitUp(ctx, client, a, time.Minute); err != nil {
			return nil, err
		}
		if _, err := drain(ctx, client, a); err != nil {
			return nil, err
		}
	}
	rdb := redis.NewClient(&redis.Options{Addr: c.Redis})
	defer rdb.Close()
	before, err := redisUsage(ctx, rdb)
	if err != nil {
		return nil, err
	}
	countersBefore := make([]map[string]int64, len(c.Admins))
	for i, a := range c.Admins {
		if countersBefore[i], err = counters(ctx, client, a); err != nil {
			return nil, err
		}
	}

	load := c.Load
	load.URLs = c.Gateways
	load.Label = c.Label
	res, err := loadgen.Run(ctx, load)
	if err != nil {
		return nil, err
	}

	rep := &Report{Label: c.Label, Generator: res.Summary}
	var all []gatewayapi.Decision
	for i, a := range c.Admins {
		d, err := drain(ctx, client, a)
		if err != nil {
			return nil, err
		}
		inst := Instance{Admin: a, Decisions: len(d.Decisions), Dropped: d.Dropped}
		now, err := counters(ctx, client, a)
		if err != nil {
			return nil, err
		}
		delta := func(k string) int64 { return now[k] - countersBefore[i][k] }
		inst.Degraded, inst.RoundTrips = delta(degradedMetric), delta(tripsMetric)
		inst.RedisCalls, inst.RedisSlow, inst.RedisTimeouts = delta(callsKey), delta(slowKey), delta(timeoutsKey)
		inst.RedisDials, inst.RedisSlowDial = delta(dialsKey), delta(slowDialKey)
		rep.Redis.RoundTrips += inst.RoundTrips
		rep.Instances = append(rep.Instances, inst)
		all = append(all, d.Decisions...)
	}
	after, err := redisUsage(ctx, rdb)
	if err != nil {
		return nil, err
	}
	rep.Redis.Commands = after.Calls - before.Calls
	rep.Redis.CPUSeconds = after.CPUSeconds - before.CPUSeconds
	rep.Redis.perRequest(Requests(len(all), res.Summary.Scheduled, load.Warmup, load.Duration))
	if c.Limited {
		v := Replay(all, c.Rule)
		rep.Verdict = &v
	}
	if err := httpx.GetJSON(ctx, client, c.Admins[0]+"/config", &rep.Config); err != nil {
		return nil, err
	}
	return rep, nil
}

func drain(ctx context.Context, client *http.Client, admin string) (gatewayapi.Decisions, error) {
	var d gatewayapi.Decisions
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, admin+"/decisions/drain", nil)
	if err != nil {
		return d, err
	}
	if err := httpx.Do(client, req, &d, http.StatusOK); err != nil {
		return d, err
	}
	if !d.Recording {
		return d, fmt.Errorf("%s does not record decisions; start it with -record-decisions", admin)
	}
	return d, nil
}

func counters(ctx context.Context, client *http.Client, admin string) (map[string]int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, admin+"/metrics", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := httpx.CheckStatus(req, resp, http.StatusOK); err != nil {
		return nil, err
	}
	ss, err := Samples(resp.Body)
	if err != nil {
		return nil, &url.Error{Op: "Get", URL: req.URL.Redacted(), Err: err}
	}
	return Counters(ss), nil
}

func Counters(ss []Sample) map[string]int64 {
	sum := func(name string, keep func(Sample) bool) int64 {
		var total float64
		for _, s := range ss {
			if s.Name == name && keep(s) {
				total += s.Value
			}
		}
		return int64(total)
	}
	all := func(Sample) bool { return true }
	within := func(s Sample) bool { return s.Labels["le"] == slowAfter }
	calls := sum(callsMetric+"_count", all)
	dials := sum(dialsMetric+"_count", all)
	return map[string]int64{
		degradedMetric: sum(degradedMetric, all),
		tripsMetric:    sum(tripsMetric, all),
		callsKey:       calls,
		slowKey:        calls - sum(callsMetric+"_bucket", within),
		timeoutsKey:    sum(callsMetric+"_count", func(s Sample) bool { return s.Labels["outcome"] == "timeout" }),
		dialsKey:       dials,
		slowDialKey:    dials - sum(dialsMetric+"_bucket", within),
	}
}

type Sample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

func Samples(r io.Reader) ([]Sample, error) {
	var out []Sample
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] == '#' {
			continue
		}
		cut := strings.LastIndexByte(line, ' ')
		if cut < 0 {
			continue
		}
		v, err := strconv.ParseFloat(line[cut+1:], 64)
		if err != nil {
			return nil, fmt.Errorf("metric line %q: %w", line, err)
		}
		s := Sample{Value: v, Labels: map[string]string{}}
		head := line[:cut]
		name, labels, ok := strings.Cut(head, "{")
		s.Name = name
		if ok {
			for pair := range strings.SplitSeq(strings.TrimSuffix(labels, "}"), ",") {
				k, val, ok := strings.Cut(pair, "=")
				if ok {
					s.Labels[k] = strings.Trim(val, `"`)
				}
			}
		}
		out = append(out, s)
	}
	return out, sc.Err()
}

type usage struct {
	Calls      int64
	CPUSeconds float64
}

func redisUsage(ctx context.Context, rdb *redis.Client) (usage, error) {
	info, err := rdb.Info(ctx, "commandstats", "cpu").Result()
	if err != nil {
		return usage{}, fmt.Errorf("redis info: %w", err)
	}
	return ParseInfo(info)
}

func ParseInfo(info string) (usage, error) {
	var u usage
	for line := range strings.Lines(info) {
		line = strings.TrimSpace(line)
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch {
		case strings.HasPrefix(name, "cmdstat_") && name != "cmdstat_info":
			for f := range strings.SplitSeq(value, ",") {
				if n, ok := strings.CutPrefix(f, "calls="); ok {
					v, err := strconv.ParseInt(n, 10, 64)
					if err != nil {
						return u, fmt.Errorf("redis info %q: %w", line, err)
					}
					u.Calls += v
				}
			}
		case name == "used_cpu_sys" || name == "used_cpu_user":
			v, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return u, fmt.Errorf("redis info %q: %w", line, err)
			}
			u.CPUSeconds += v
		}
	}
	return u, nil
}

func waitUp(ctx context.Context, client *http.Client, admin string, patience time.Duration) error {
	deadline := time.Now().Add(patience)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, admin+"/healthz", nil)
		if err != nil {
			return err
		}
		err = httpx.Do(client, req, nil, http.StatusOK)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("not up after %v: %w", patience, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}
