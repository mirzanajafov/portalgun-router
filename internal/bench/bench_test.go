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

	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
)

func TestParseCPUStat(t *testing.T) {
	got, err := parseCPUStat("usage_usec 1500000\nuser_usec 1000000\nsystem_usec 500000\nnr_periods 0\n")
	want := CPUTime{Total: 1500 * time.Millisecond, User: time.Second, System: 500 * time.Millisecond}
	if err != nil || got != want {
		t.Fatalf("got %+v %v", got, err)
	}
	if _, err := parseCPUStat("usage_usec 10\nnr_periods 0\n"); err == nil {
		t.Fatal("missing user and system time accepted")
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
