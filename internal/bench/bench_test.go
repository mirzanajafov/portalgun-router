package bench

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
)

func TestWatchArgs(t *testing.T) {
	ids := map[string]string{"dim-a": "1", "dim-b": "2", "dim-c": "3", "nginx": "4"}
	got := strings.Join(watchArgs(ids, Systems[3]), " ")
	want := "-cgroup-root /host/cgroup -watch dim-a=1 -watch dim-b=2 -watch dim-c=3 -watch system=4"
	if Systems[3].Name != "nginx" || got != want {
		t.Fatalf("got %q", got)
	}
	if direct := strings.Join(watchArgs(ids, Systems[0]), " "); strings.Contains(direct, "system=") {
		t.Fatalf("direct watches a system: %q", direct)
	}
	if watchArgs(nil, Systems[3]) != nil {
		t.Fatal("watch args without container ids")
	}
}

func TestRotateVisitsEveryOrder(t *testing.T) {
	s := []string{"a", "b", "c"}
	if got := rotate(s, 1); !reflect.DeepEqual(got, []string{"b", "c", "a"}) {
		t.Fatalf("got %v", got)
	}
	if got := rotate(s, 5); !reflect.DeepEqual(got, []string{"c", "a", "b"}) {
		t.Fatalf("got %v", got)
	}
	if !reflect.DeepEqual(s, []string{"a", "b", "c"}) {
		t.Fatal("rotate changed its input")
	}
}

func TestJudge(t *testing.T) {
	healthy := loadgen.Summary{
		Scheduled: 10000, Completed: 10000, ThroughputRps: 1000,
		LatencyUs: loadgen.Percentiles{P99: 900}, LagUs: loadgen.Percentiles{P99: 100},
		CPU: &loadgen.CPU{Utilization: 0.3},
	}
	p := Point{Rate: 1000, Loadgen: healthy}
	judge(&p)
	if p.Saturated || !p.GeneratorOK || len(p.Reasons) != 0 {
		t.Fatalf("healthy point judged %+v", p)
	}

	slow := healthy
	slow.LatencyUs.P99 = 80_000
	slow.Failed = 50
	slow.ThroughputRps = 900
	p = Point{Rate: 1000, Loadgen: slow}
	judge(&p)
	if !p.Saturated || len(p.Reasons) != 3 {
		t.Fatalf("saturated point judged %+v", p)
	}

	lagging := healthy
	lagging.LagUs.P99 = 3000
	lagging.LatencyUs.P99 = 95_000
	p = Point{Rate: 1000, Loadgen: lagging}
	judge(&p)
	if p.Saturated || p.GeneratorOK {
		t.Fatalf("a lagging generator's tail was blamed on the system: %+v", p)
	}

	lagging.Failed = 500
	p = Point{Rate: 1000, Loadgen: lagging}
	judge(&p)
	if !p.Saturated {
		t.Fatalf("failures ignored because the generator lagged: %+v", p)
	}
}

func TestSummarizeDropsLaggingRunsFromLatency(t *testing.T) {
	lag := point("nginx", 1000, 1, 900, 95000, false)
	lag.GeneratorOK = false
	r := Summarize([]Point{point("nginx", 1000, 0, 300, 1500, false), lag})
	c := r.Cells[0]
	if c.Runs != 2 || c.GeneratorIssues != 1 || c.P99Us.Max != 1500 {
		t.Fatalf("cell %+v", c)
	}
}

func point(system string, rate float64, rep int, p50, p99 float64, saturated bool) Point {
	return Point{
		Workload: "w1", System: system, Rate: rate, Rep: rep, Saturated: saturated, GeneratorOK: true,
		CPUPerRequestUs: 20,
		Loadgen: loadgen.Summary{
			ThroughputRps: rate,
			LatencyUs:     loadgen.Percentiles{P50: p50, P99: p99, P999: p99 * 2},
		},
	}
}

func TestSummarize(t *testing.T) {
	points := []Point{
		point("direct", 1000, 0, 200, 900, false),
		point("direct", 1000, 1, 220, 1100, false),
		point("direct", 1000, 2, 210, 1000, false),
		point("nginx", 1000, 0, 300, 1500, false),
		point("nginx", 1000, 1, 320, 1300, false),
		point("nginx", 1000, 2, 310, 1400, false),
		point("nginx", 5000, 0, 400, 2000, false),
		point("nginx", 5000, 1, 420, 90000, true),
		point("nginx", 5000, 2, 410, 2100, false),
		point("nginx", 9000, 0, 9000, 99000, true),
		point("nginx", 9000, 1, 9000, 99000, true),
	}
	r := Summarize(points)
	if len(r.Cells) != 4 {
		t.Fatalf("got %d cells", len(r.Cells))
	}
	nginx := r.Cells[1]
	if nginx.System != "nginx" || nginx.Rate != 1000 || nginx.P50Us.Median != 310 || nginx.OverheadP50Us != 100 || nginx.OverheadP99Us != 400 {
		t.Fatalf("nginx at 1000: %+v", nginx)
	}
	if nginx.P99Us.Min != 1300 || nginx.P99Us.Max != 1500 {
		t.Fatalf("range %+v", nginx.P99Us)
	}
	if r.MaxRate["nginx"] != 5000 || r.MaxRate["direct"] != 1000 {
		t.Fatalf("max rates %v", r.MaxRate)
	}
	var buf bytes.Buffer
	if err := r.WriteText(&buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "max sustained rate nginx    5000 rps") {
		t.Fatalf("report:\n%s", buf.String())
	}
}

func TestDeath(t *testing.T) {
	for in, want := range map[string]string{
		"true false 0\n":   "",
		"false true 137\n": "OOM-killed",
		"false false 2\n":  "exited with 2",
		"":                 "",
	} {
		if got := death(in); got != want {
			t.Errorf("%q: got %q, want %q", in, got, want)
		}
	}
}

func TestSummarizeCountsDeaths(t *testing.T) {
	gone := Point{Workload: "w1", System: "envoy", Rate: 6000, Rep: 1, Saturated: true, Died: "OOM-killed"}
	hit := point("envoy", 6000, 0, 4000, 900000, true)
	hit.Died, hit.Loadgen.Scheduled, hit.MemoryPeakMB = "OOM-killed", 120000, 255
	r := Summarize([]Point{point("envoy", 5000, 0, 500, 2000, false), hit, gone})
	c := r.Cells[1]
	if c.Runs != 2 || c.Saturated != 2 || c.Died != 2 || c.GeneratorIssues != 0 || c.P50Us.Median != 4000 || c.MemoryPeakMB.Max != 255 {
		t.Fatalf("cell %+v", c)
	}
	if r.MaxRate["envoy"] != 5000 {
		t.Fatalf("max rates %v", r.MaxRate)
	}
}

func TestSummarizeOverload(t *testing.T) {
	shedding := func(rep int, ok, refused int64, p99 float64) Point {
		p := point("portalgun-capped", 12000, rep, 300, 900, true)
		p.MemoryPeakMB, p.SwapPeakMB = 60, 0
		p.Loadgen.WindowSec = 20
		p.Loadgen.Scheduled = ok + refused + 40
		p.Loadgen.Failed = 40
		p.Loadgen.Status = map[string]int64{"200": ok, "503": refused}
		p.Loadgen.OKLatencyUs = loadgen.Percentiles{Count: ok, P50: 2000, P99: p99}
		return p
	}
	queued := point("portalgun", 12000, 0, 90000, 900000, true)
	queued.MemoryPeakMB, queued.SwapPeakMB = 256, 180
	queued.Loadgen.WindowSec, queued.Loadgen.Scheduled = 20, 240000
	queued.Loadgen.Status = map[string]int64{"200": 30000}
	r := Summarize([]Point{shedding(0, 160000, 79960, 40000), shedding(1, 150000, 89960, 60000), shedding(2, 170000, 69960, 50000), queued})
	c := r.Cells[slices.IndexFunc(r.Cells, func(c Cell) bool { return c.System == "portalgun-capped" })]
	if c.GoodputRps.Median != 8000 || c.GoodputRps.Min != 7500 || c.GoodputRps.Max != 8500 {
		t.Fatalf("goodput %+v", c.GoodputRps)
	}
	if c.ShedShare.Median != 79960.0/240000 || c.FailedShare.Median != 40.0/240000 || c.OKP99Us.Median != 50000 || c.OKP50Us.Median != 2000 {
		t.Fatalf("shed %+v failed %+v ok p99 %+v", c.ShedShare, c.FailedShare, c.OKP99Us)
	}
	q := r.Cells[slices.IndexFunc(r.Cells, func(c Cell) bool { return c.System == "portalgun" })]
	if q.GoodputRps.Median != 1500 || q.SwapPeakMB.Max != 180 || q.OKP99Us != (Stat{}) {
		t.Fatalf("queued cell %+v", q)
	}
	var buf bytes.Buffer
	if err := r.WriteOverloadText(&buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "8000 (7500-8500)") || !strings.Contains(buf.String(), "50.0 (40.0-60.0)") {
		t.Fatalf("report:\n%s", buf.String())
	}
}

func TestLoadPoints(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "runs.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	want := []Point{point("direct", 1000, 0, 200, 900, false), point("envoy", 1000, 0, 400, 1900, false)}
	for _, p := range want {
		if err := enc.Encode(p); err != nil {
			t.Fatal(err)
		}
	}
	f.Close()
	got, err := LoadPoints(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
	}
}

func TestLoadgenArgs(t *testing.T) {
	plan := Plan{Workload: Workloads["w4"], Duration: 20 * time.Second, Warmup: 5 * time.Second, MaxConns: 64}
	got := strings.Join(loadgenArgs(plan, Systems[0], 2000), " ")
	for _, want := range []string{"-rate 2000", "-duration 20s", "-warmup 5s", "-method POST", "-body 16384", "-max-conns 64", "-url http://dim-a:8080/ -url http://dim-b:8080/ -url http://dim-c:8080/"} {
		if !strings.Contains(got, want) {
			t.Errorf("args %q lack %q", got, want)
		}
	}
}
