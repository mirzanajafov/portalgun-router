package limitbench

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/chaos"
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
	Admin      string `json:"admin"`
	Decisions  int    `json:"decisions"`
	Dropped    int64  `json:"dropped"`
	Degraded   int64  `json:"degraded"`
	RoundTrips int64  `json:"roundTrips"`
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
)

type Report struct {
	Label     string          `json:"label"`
	Config    json.RawMessage `json:"config,omitempty"`
	Generator loadgen.Summary `json:"generator"`
	Instances []Instance      `json:"instances"`
	Redis     Redis           `json:"redis"`
	Verdict   *limit.Verdict  `json:"verdict,omitempty"`
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
	var all []limit.Logged
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
		inst.Degraded = now[degradedMetric] - countersBefore[i][degradedMetric]
		inst.RoundTrips = now[tripsMetric] - countersBefore[i][tripsMetric]
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
		v := limit.Replay(all, c.Rule)
		rep.Verdict = &v
	}
	if err := getJSON(ctx, client, c.Admins[0]+"/config", &rep.Config); err != nil {
		return nil, err
	}
	return rep, nil
}

type decisions struct {
	Recording bool           `json:"recording"`
	Dropped   int64          `json:"dropped"`
	Decisions []limit.Logged `json:"decisions"`
}

func drain(ctx context.Context, client *http.Client, admin string) (decisions, error) {
	var d decisions
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, admin+"/decisions/drain", nil)
	if err != nil {
		return d, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return d, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return d, fmt.Errorf("%s: %s", admin, resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
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
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for _, name := range []string{degradedMetric, tripsMetric} {
		n, err := SumMetric(strings.NewReader(string(body)), name)
		if err != nil {
			return nil, err
		}
		out[name] = n
	}
	return out, nil
}

func SumMetric(r io.Reader, name string) (int64, error) {
	var total float64
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := sc.Text()
		rest, ok := strings.CutPrefix(line, name)
		if !ok || (rest != "" && rest[0] != '{' && rest[0] != ' ') {
			continue
		}
		fields := strings.Fields(rest)
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			return 0, fmt.Errorf("metric line %q: %w", line, err)
		}
		total += v
	}
	return int64(total), sc.Err()
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
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			err = errors.New(resp.Status)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s not up after %v: %w", admin, patience, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func getJSON(ctx context.Context, client *http.Client, url string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", url, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}
