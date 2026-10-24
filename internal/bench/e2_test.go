package bench

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/limitbench"
	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
)

func TestEveryLimiterCellBuildsAValidConfig(t *testing.T) {
	cells, err := LimiterCells(LimiterStudyNames())
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for _, c := range cells {
		raw, err := json.Marshal(c.Config())
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := config.Parse(raw)
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		rl := cfg.Routes[0].RateLimit
		switch {
		case !c.Design.HasLimiter() && rl != nil:
			t.Fatalf("%s has a limiter", c.Name)
		case c.Design.HasLimiter() && (rl == nil || rl.Design != c.Design.Kind || rl.Instances != c.Instances || rl.Rate != c.Rule.Rate):
			t.Fatalf("%s: limiter %+v", c.Name, rl)
		case c.Design.Sync > 0 && rl.SyncInterval.D() != c.Design.Sync, c.Design.Lease > 0 && rl.LeaseTarget.D() != c.Design.Lease:
			t.Fatalf("%s: intervals %+v", c.Name, rl)
		}
		if other, dup := files[c.file()]; dup {
			t.Fatalf("%s and %s share a config file", c.Name, other)
		}
		files[c.file()] = c.Name
		if ws := c.weights(); ws != nil && (len(ws) != c.Instances || ws[0] != 0.9) {
			t.Fatalf("%s: weights %v", c.Name, ws)
		}
	}
	accuracy, _ := LimiterCells([]string{"accuracy"})
	if len(accuracy) != len(Designs)*3*2 {
		t.Fatalf("%d accuracy cells", len(accuracy))
	}
}

func TestTheLimiterDefaultConfigIsLocalSyncOverTwoGateways(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "bench", "limiter", "configs", "default.json"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := config.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := limiterCell(Design{Kind: config.LimitLocalSync, Sync: 100 * time.Millisecond}, 2, LoadOver, SplitEven).Config()
	want.ApplyDefaults()
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("default.json\n%+v\nwant\n%+v", *got, want)
	}
}

func limiterRun(cell string, p50, p99 float64, v *limitbench.Verdict) LimiterRun {
	return LimiterRun{Cell: cell, Report: &limitbench.Report{
		Generator: loadgen.Summary{Valid: true, LatencyUs: loadgen.Percentiles{P50: p50, P99: p99}},
		Instances: []limitbench.Instance{{Degraded: 2, RedisCalls: 600, RedisSlow: 30, RedisTimeouts: 6, RedisDials: 4}, {Degraded: 1, RedisCalls: 400, RedisSlow: 20, RedisTimeouts: 4, RedisDials: 3}},
		Redis:     limitbench.Redis{TripsReq: 1.01, CommandsReq: 4.03},
		Verdict:   v,
	}}
}

func TestLimiterSummaryComparesWithTheRunWithoutALimiter(t *testing.T) {
	v := &limitbench.Verdict{Requests: 1000, OverAdmission: 0.12, MaxAhead: 37, WorstRatio: 1.3, FalseAdmits: 50, FalseRejects: 10}
	sums := SummarizeLimiter([]LimiterRun{
		limiterRun("none/over-even-2gw", 300, 900, nil),
		limiterRun("none/over-even-2gw", 320, 1000, nil),
		limiterRun("redis-exact/over-even-2gw", 520, 1500, v),
		limiterRun("redis-exact/over-even-2gw", 500, 1400, v),
		limiterRun("lease-20ms/zipf-10k-2gw", 330, 950, v),
	})
	if len(sums) != 3 {
		t.Fatalf("%+v", sums)
	}
	r := sums[1]
	if r.Baseline != "none/over-even-2gw" || r.AddedP50Us == nil || r.AddedP50Us.Median != 200 || r.AddedP99Us.Median != 500 {
		t.Fatalf("%+v %+v", r, r.AddedP50Us)
	}
	if r.OverAdmission.Median != 0.12 || r.MaxAhead.Median != 37 || r.FalseAdmits.Median != 0.05 || r.Degraded.Median != 3 || r.RedisTrips.Median != 1.01 || r.RedisCommands.Median != 4.03 {
		t.Fatalf("%+v", r)
	}
	if r.RedisSlow.Median != 0.05 || r.RedisTimeouts.Median != 0.01 || r.RedisDials.Median != 7 {
		t.Fatalf("slow %+v, timeouts %+v, dials %+v: shares of all calls across both gateways", r.RedisSlow, r.RedisTimeouts, r.RedisDials)
	}
	if sums[2].AddedP50Us != nil || sums[0].AddedP50Us != nil {
		t.Fatal("a cell without a matching baseline got an added latency")
	}
	var b bytes.Buffer
	if err := WriteLimiterText(&b, sums); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"+200", "+12.0%", "+5.0%"} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("report lacks %q:\n%s", want, b.String())
		}
	}
}

func TestLimiterSummaryLeavesACellWithoutValidRunsBlank(t *testing.T) {
	lagged := limiterRun("lease-5ms/over-even-2gw", 1700, 3_200_000, &limitbench.Verdict{Requests: 1000})
	lagged.Report.Generator.Valid = false
	sums := SummarizeLimiter([]LimiterRun{limiterRun("none/over-even-2gw", 300, 900, nil), lagged})
	if s := sums[1]; s.InvalidRuns != 1 || s.AddedP50Us != nil || s.AddedP99Us != nil {
		t.Fatalf("%+v", s)
	}
	var b bytes.Buffer
	if err := WriteLimiterText(&b, sums); err != nil {
		t.Fatal(err)
	}
	line := strings.Fields(strings.Split(b.String(), "\n")[2])
	if strings.Join(line[1:], " ") != "1 (1 invalid) "+strings.TrimSpace(strings.Repeat("- ", 17)) {
		t.Fatalf("row %q", line)
	}
}
