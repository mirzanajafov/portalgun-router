package bench

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"text/tabwriter"
)

const HotpathGate = 1.5

type HotpathPlan struct {
	Package   string
	Reps      int
	BenchTime string
	Out       string
	Log       *log.Logger
}

type HotpathRow struct {
	Rep         int     `json:"rep"`
	Group       string  `json:"group"`
	System      string  `json:"system"`
	Iterations  int64   `json:"iterations"`
	NsPerOp     float64 `json:"nsPerOp"`
	CPUNsPerOp  float64 `json:"cpuNsPerOp"`
	BytesPerOp  float64 `json:"bytesPerOp"`
	AllocsPerOp float64 `json:"allocsPerOp"`
}

func (r HotpathRow) cost() float64 {
	if r.CPUNsPerOp > 0 {
		return r.CPUNsPerOp
	}
	return r.NsPerOp
}

type hotpathGroup struct {
	name    string
	systems []string
}

var hotpathGroups = []hotpathGroup{
	{"ServeHTTP", []string{"bare", "portalgun", "portalgun-full"}},
	{"Loopback", []string{"direct", "bare", "portalgun", "portalgun-full"}},
}

func parseBenchLine(line string) (HotpathRow, bool) {
	fields := strings.Fields(line)
	if len(fields) < 4 || !strings.HasPrefix(fields[0], "Benchmark") {
		return HotpathRow{}, false
	}
	group, system, ok := strings.Cut(strings.TrimPrefix(fields[0], "Benchmark"), "/")
	if !ok {
		return HotpathRow{}, false
	}
	if i := strings.LastIndexByte(system, '-'); i >= 0 {
		if _, err := strconv.Atoi(system[i+1:]); err == nil {
			system = system[:i]
		}
	}
	n, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return HotpathRow{}, false
	}
	row := HotpathRow{Group: group, System: system, Iterations: n}
	for i := 2; i+1 < len(fields); i += 2 {
		v, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			return HotpathRow{}, false
		}
		switch fields[i+1] {
		case "ns/op":
			row.NsPerOp = v
		case "cpu-ns/op":
			row.CPUNsPerOp = v
		case "B/op":
			row.BytesPerOp = v
		case "allocs/op":
			row.AllocsPerOp = v
		}
	}
	return row, row.NsPerOp > 0
}

func RunHotpath(ctx context.Context, plan HotpathPlan) (dir string, err error) {
	dir, err = runDir(plan.Out, "")
	if err != nil {
		return "", err
	}
	tmp, err := os.MkdirTemp("", "hotpath")
	if err != nil {
		return "", err
	}
	defer func() {
		if err := os.RemoveAll(tmp); err != nil {
			plan.Log.Printf("removing %s: %v", tmp, err)
		}
	}()
	bin := filepath.Join(tmp, "proxy.test")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	build := exec.CommandContext(ctx, "go", "test", "-c", "-o", bin, plan.Package)
	build.Stdout, build.Stderr = plan.Log.Writer(), plan.Log.Writer()
	if err := build.Run(); err != nil {
		return "", fmt.Errorf("building the benchmarks: %w", err)
	}
	raw, err := os.Create(filepath.Join(dir, "raw.txt"))
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, raw.Close()) }()
	runs, err := os.Create(filepath.Join(dir, "runs.jsonl"))
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, runs.Close()) }()
	enc := json.NewEncoder(runs)
	for rep := range plan.Reps {
		for _, g := range hotpathGroups {
			for _, sys := range rotate(g.systems, rep) {
				if err := plan.benchOnce(ctx, bin, g.name, sys, rep, raw, enc); err != nil {
					return dir, err
				}
			}
		}
	}
	return dir, nil
}

func (plan HotpathPlan) benchOnce(ctx context.Context, bin, group, sys string, rep int, raw io.Writer, rows *json.Encoder) error {
	pattern := fmt.Sprintf("^Benchmark%s$/^%s$", group, sys)
	cmd := exec.CommandContext(ctx, bin, "-test.run", "^$", "-test.bench", pattern,
		"-test.benchtime", plan.BenchTime, "-test.cpu", "1", "-test.benchmem")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w\n%s", pattern, err, out.String())
	}
	found := false
	sc := bufio.NewScanner(&out)
	for sc.Scan() {
		line := sc.Text()
		if line == "PASS" || strings.HasPrefix(line, "ok ") {
			continue
		}
		if _, err := fmt.Fprintln(raw, line); err != nil {
			return err
		}
		row, ok := parseBenchLine(line)
		if !ok {
			continue
		}
		row.Rep = rep
		found = true
		if err := rows.Encode(row); err != nil {
			return err
		}
		plan.Log.Printf("rep %d  %-10s %-15s %8.0f ns/op %8.0f cpu-ns/op %4.0f allocs/op",
			rep+1, row.Group, row.System, row.NsPerOp, row.CPUNsPerOp, row.AllocsPerOp)
	}
	if !found {
		return fmt.Errorf("%s: no benchmark result in the output", pattern)
	}
	return nil
}

func LoadHotpathRows(dir string) ([]HotpathRow, error) { return loadRuns[HotpathRow](dir) }

type HotpathCell struct {
	Group       string  `json:"group"`
	System      string  `json:"system"`
	Runs        int     `json:"runs"`
	CPUNs       Stat    `json:"cpuNsPerOp"`
	WallNs      Stat    `json:"nsPerOp"`
	BytesPerOp  float64 `json:"bytesPerOp"`
	AllocsPerOp float64 `json:"allocsPerOp"`
}

type HotpathRatio struct {
	System   string  `json:"system"`
	Handler  Stat    `json:"handler"`
	Loopback Stat    `json:"loopback"`
	Estimate float64 `json:"estimate"`
}

type HotpathSummary struct {
	CPUMeasured bool           `json:"cpuMeasured"`
	Cells       []HotpathCell  `json:"cells"`
	Ratios      []HotpathRatio `json:"ratios"`
	Gate        float64        `json:"gate"`
	Passed      bool           `json:"passed"`
}

func SummarizeHotpath(rows []HotpathRow) HotpathSummary {
	s := HotpathSummary{Gate: HotpathGate, CPUMeasured: len(rows) > 0}
	type key struct{ group, system string }
	cost := map[int]map[key]float64{}
	groups := map[key][]HotpathRow{}
	for _, r := range rows {
		k := key{r.Group, r.System}
		groups[k] = append(groups[k], r)
		if cost[r.Rep] == nil {
			cost[r.Rep] = map[key]float64{}
		}
		cost[r.Rep][k] = r.cost()
		s.CPUMeasured = s.CPUMeasured && r.CPUNsPerOp > 0
	}
	for _, g := range hotpathGroups {
		for _, sys := range g.systems {
			rs := groups[key{g.name, sys}]
			if len(rs) == 0 {
				continue
			}
			var c, w, b, a []float64
			for _, r := range rs {
				c, w, b, a = append(c, r.cost()), append(w, r.NsPerOp), append(b, r.BytesPerOp), append(a, r.AllocsPerOp)
			}
			s.Cells = append(s.Cells, HotpathCell{Group: g.name, System: sys, Runs: len(rs), CPUNs: statOf(c), WallNs: statOf(w),
				BytesPerOp: statOf(b).Median, AllocsPerOp: statOf(a).Median})
		}
	}
	median := func(group, system string) (float64, bool) {
		for _, c := range s.Cells {
			if c.Group == group && c.System == system {
				return c.CPUNs.Median, true
			}
		}
		return 0, false
	}
	s.Passed = true
	for _, sys := range []string{"portalgun", "portalgun-full"} {
		var handler, loop []float64
		for _, c := range cost {
			bare, ok1 := c[key{"ServeHTTP", "bare"}]
			pg, ok2 := c[key{"ServeHTTP", sys}]
			if ok1 && ok2 && bare > 0 {
				handler = append(handler, pg/bare)
			}
			direct, ok1 := c[key{"Loopback", "direct"}]
			bare, ok2 = c[key{"Loopback", "bare"}]
			pg, ok3 := c[key{"Loopback", sys}]
			if ok1 && ok2 && ok3 && bare > direct {
				loop = append(loop, (pg-direct)/(bare-direct))
			}
		}
		if len(handler) == 0 && len(loop) == 0 {
			continue
		}
		r := HotpathRatio{System: sys, Handler: statOf(handler), Loopback: statOf(loop)}
		hb, ok1 := median("ServeHTTP", "bare")
		hp, ok2 := median("ServeHTTP", sys)
		ld, ok3 := median("Loopback", "direct")
		lb, ok4 := median("Loopback", "bare")
		if ok1 && ok2 && ok3 && ok4 && lb > ld {
			r.Estimate = 1 + (hp-hb)/(lb-ld)
		}
		s.Ratios = append(s.Ratios, r)
		if r.Estimate == 0 || r.Estimate > HotpathGate {
			s.Passed = false
		}
	}
	if len(s.Ratios) == 0 {
		s.Passed = false
	}
	return s
}

func WriteHotpathText(w io.Writer, s HotpathSummary) error {
	unit := "cpu ns/op"
	if !s.CPUMeasured {
		unit = "wall ns/op (no CPU clock here)"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "benchmark\tsystem\truns\t%s (min-max)\tB/op\tallocs/op\t\n", unit)
	for _, c := range s.Cells {
		fmt.Fprintf(&b, "%s\t%s\t%d\t%.0f (%.0f-%.0f)\t%.0f\t%.0f\t\n", c.Group, c.System, c.Runs,
			c.CPUNs.Median, c.CPUNs.Min, c.CPUNs.Max, c.BytesPerOp, c.AllocsPerOp)
	}
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "against the bare ReverseProxy, per repetition (median, min-max):")
	for _, r := range s.Ratios {
		fmt.Fprintf(&b, "  %-15s handler only %.2f (%.2f-%.2f)   whole proxy over loopback, direct path subtracted %.2f (%.2f-%.2f)\n",
			r.System, r.Handler.Median, r.Handler.Min, r.Handler.Max, r.Loopback.Median, r.Loopback.Min, r.Loopback.Max)
	}
	fmt.Fprintln(&b, "whole proxy, estimated as 1 + handler difference / bare proxy's whole cost (medians):")
	for _, r := range s.Ratios {
		fmt.Fprintf(&b, "  %-15s %.2f\n", r.System, r.Estimate)
	}
	verdict := "passed"
	if !s.Passed {
		verdict = "FAILED"
	}
	fmt.Fprintf(&b, "gate: whole-proxy estimate at most %.1fx the bare proxy: %s\n", s.Gate, verdict)
	return writeTable(w, b.String(), tabwriter.AlignRight)
}
