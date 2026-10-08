package bench

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

func TestParseBenchLine(t *testing.T) {
	row, ok := parseBenchLine("BenchmarkServeHTTP/portalgun-full    \t  200000\t      5030 ns/op\t      5012 cpu-ns/op\t    4000 B/op\t      41 allocs/op")
	want := HotpathRow{Group: "ServeHTTP", System: "portalgun-full", Iterations: 200000, NsPerOp: 5030, CPUNsPerOp: 5012, BytesPerOp: 4000, AllocsPerOp: 41}
	if !ok || row != want {
		t.Fatalf("got %+v %v", row, ok)
	}
	if row, ok := parseBenchLine("BenchmarkLoopback/bare-8  1000  48000 ns/op"); !ok || row.System != "bare" || row.CPUNsPerOp != 0 {
		t.Fatalf("cpu suffix: %+v %v", row, ok)
	}
	for _, line := range []string{"goos: linux", "PASS", "BenchmarkServeHTTP 10 5 ns/op", "BenchmarkServeHTTP/bare x 5 ns/op"} {
		if _, ok := parseBenchLine(line); ok {
			t.Errorf("parsed %q", line)
		}
	}
}

func TestSummarizeHotpathPairsRunsByRepetition(t *testing.T) {
	var rows []HotpathRow
	for rep, drift := range []float64{1, 2} {
		add := func(group, system string, cpu float64) {
			rows = append(rows, HotpathRow{Rep: rep, Group: group, System: system, NsPerOp: cpu * drift, CPUNsPerOp: cpu * drift, AllocsPerOp: 10})
		}
		add("ServeHTTP", "bare", 1000)
		add("ServeHTTP", "portalgun", 2000)
		add("Loopback", "direct", 20000)
		add("Loopback", "bare", 40000)
		add("Loopback", "portalgun", 42000)
	}
	s := SummarizeHotpath(rows)
	if len(s.Ratios) != 1 || s.Ratios[0].System != "portalgun" {
		t.Fatalf("ratios %+v", s.Ratios)
	}
	r := s.Ratios[0]
	if r.Handler.Median != 2 || math.Abs(r.Loopback.Median-1.1) > 1e-9 || r.Loopback.Min != r.Loopback.Max {
		t.Fatalf("a drift that hits every system in a repetition moved the ratios: %+v", r)
	}
	if !s.Passed || !s.CPUMeasured {
		t.Fatalf("summary %+v", s)
	}
	var buf bytes.Buffer
	if err := WriteHotpathText(&buf, s); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "passed") {
		t.Fatal(buf.String())
	}

	if math.Abs(r.Estimate-1.05) > 1e-9 {
		t.Fatalf("estimate %.4f, want 1 + 1500/30000", r.Estimate)
	}

	heavy := []HotpathRow{
		{Group: "ServeHTTP", System: "bare", NsPerOp: 1000},
		{Group: "ServeHTTP", System: "portalgun", NsPerOp: 12000},
		{Group: "Loopback", System: "direct", NsPerOp: 20000},
		{Group: "Loopback", System: "bare", NsPerOp: 40000},
		{Group: "Loopback", System: "portalgun", NsPerOp: 45000},
	}
	s = SummarizeHotpath(heavy)
	if s.CPUMeasured || s.Passed || math.Abs(s.Ratios[0].Estimate-1.55) > 1e-9 || s.Ratios[0].Loopback.Median != 1.25 {
		t.Fatalf("an 11 us handler difference on a 20 us proxy passed: %+v", s)
	}
}
