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
	"github.com/mirzanajafov/portalgun-router/internal/failover"
	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
)

func TestEveryStudyBuildsValidGatewayConfigs(t *testing.T) {
	cells, err := FailoverCells(StudyNames())
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for _, c := range cells {
		raw, err := json.Marshal(c.Variant.Config())
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := config.Parse(raw)
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		p, r := cfg.Pools[0], cfg.Routes[0]
		v := c.Variant
		if p.HealthCheck.Disabled == v.Probes || p.Outlier.Disabled == v.Passive || *r.Retries.Max != v.Retries || r.PerTryTimeout.D() != v.PerTry {
			t.Fatalf("%s: config %+v does not match the variant %+v", c.Name, cfg, v)
		}
		if other, dup := files[c.file()]; dup {
			t.Fatalf("%s and %s share a config file", c.Name, other)
		}
		files[c.file()] = c.Name
	}
}

func TestStudiesShareCellsInsteadOfRepeatingThem(t *testing.T) {
	cells, err := FailoverCells([]string{"detect", "refused", "slow-start", "per-try"})
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]int{}
	for _, c := range cells {
		names[c.Name]++
	}
	for _, want := range []string{"crash/both-retries", "crash/both-retries-refused", "crash/both-retries-warm0s", "blackhole/both-retries-try100ms", "slow/probes", "flapping/passive"} {
		if names[want] != 1 {
			t.Fatalf("cell %s appears %d times in %v", want, names[want], names)
		}
	}
	if len(cells) != 24+1+1+2 {
		t.Fatalf("%d cells", len(cells))
	}
	if _, err := FailoverCells([]string{"nope"}); err == nil {
		t.Fatal("an unknown study was accepted")
	}
}

func run(cell string, valid bool, detect, move *float64, lost int) FailoverRun {
	return FailoverRun{Cell: cell, Report: &failover.Report{
		Generator: loadgen.Summary{Valid: valid},
		Result:    failover.Result{DetectMs: detect, MoveMs: move, Lost: lost, P99BeforeMs: 3, P99FaultMs: 40, AttemptsFault: 1.2},
	}}
}

func TestFailoverSummaryKeepsMissesVisible(t *testing.T) {
	ms := func(v float64) *float64 { return &v }
	sums := SummarizeFailover([]FailoverRun{
		run("crash/both", true, ms(310), ms(400), 120),
		run("crash/both", true, ms(290), nil, 200),
		run("crash/both", true, nil, ms(600), 80),
		run("crash/both", false, ms(1), ms(1), 0),
		run("slow/both", true, nil, nil, 0),
	})
	if len(sums) != 2 || sums[0].Cell != "crash/both" {
		t.Fatalf("%+v", sums)
	}
	c := sums[0]
	if c.Runs != 4 || c.InvalidRuns != 1 || c.Detected != 2 || c.Moved != 2 {
		t.Fatalf("%+v", c)
	}
	if c.DetectMs.Median != 300 || c.Lost.Median != 120 || c.MoveMs.Min != 400 || c.MoveMs.Max != 600 {
		t.Fatalf("detect %+v lost %+v move %+v", c.DetectMs, c.Lost, c.MoveMs)
	}
	var b bytes.Buffer
	if err := WriteFailoverText(&b, sums); err != nil {
		t.Fatal(err)
	}
	text := b.String()
	for _, want := range []string{"4 (1 invalid)", "300 (290-310) [2/3]", "500 (400-600) [2/3]"} {
		if !strings.Contains(text, want) {
			t.Fatalf("report lacks %q:\n%s", want, text)
		}
	}
}

func TestTheDefaultConfigIsTheBaseVariant(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "bench", "failover", "configs", "default.json"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := config.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := baseVariant().Config()
	want.ApplyDefaults()
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("default.json\n%+v\nbase variant\n%+v", *got, want)
	}
}

func TestVariantNames(t *testing.T) {
	v := baseVariant()
	if v.Name() != "both-retries" {
		t.Fatal(v.Name())
	}
	v.Probes, v.Retries, v.PerTry, v.Balancer = false, 0, 250*time.Millisecond, config.BalancerRoundRobin
	if v.Name() != "passive-try250ms-round-robin" {
		t.Fatal(v.Name())
	}
}
