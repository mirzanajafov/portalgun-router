package bench

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"text/tabwriter"
)

type Stat struct {
	Median float64 `json:"median"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
}

func statOf(vs []float64) Stat {
	if len(vs) == 0 {
		return Stat{}
	}
	s := slices.Clone(vs)
	slices.Sort(s)
	m := s[len(s)/2]
	if len(s)%2 == 0 {
		m = (s[len(s)/2-1] + s[len(s)/2]) / 2
	}
	return Stat{Median: m, Min: s[0], Max: s[len(s)-1]}
}

type Cell struct {
	System           string  `json:"system"`
	Rate             float64 `json:"rate"`
	Runs             int     `json:"runs"`
	Saturated        int     `json:"saturated"`
	GeneratorIssues  int     `json:"generatorIssues"`
	P50Us            Stat    `json:"p50Us"`
	P99Us            Stat    `json:"p99Us"`
	P999Us           Stat    `json:"p999Us"`
	ThroughputRps    Stat    `json:"throughputRps"`
	CPUPerRequestUs  Stat    `json:"cpuPerRequestUs"`
	UserPerRequestUs Stat    `json:"userPerRequestUs"`
	OverheadP50Us    float64 `json:"overheadP50Us"`
	OverheadP99Us    float64 `json:"overheadP99Us"`
}

type Report struct {
	Workload string             `json:"workload"`
	Cells    []Cell             `json:"cells"`
	MaxRate  map[string]float64 `json:"maxRate"`
}

func LoadPoints(dir string) ([]Point, error) {
	f, err := os.Open(filepath.Join(dir, "runs.jsonl"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var points []Point
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var p Point
		if err := json.Unmarshal(sc.Bytes(), &p); err != nil {
			return nil, err
		}
		points = append(points, p)
	}
	return points, sc.Err()
}

func Summarize(points []Point) Report {
	type key struct {
		system string
		rate   float64
	}
	groups := map[key][]Point{}
	var order []string
	var workload string
	for _, p := range points {
		workload = p.Workload
		k := key{p.System, p.Rate}
		groups[k] = append(groups[k], p)
		if !slices.Contains(order, p.System) {
			order = append(order, p.System)
		}
	}
	rank := func(name string) int {
		if i := slices.IndexFunc(Systems, func(s System) bool { return s.Name == name }); i >= 0 {
			return i
		}
		return len(Systems) + slices.Index(order, name)
	}

	rep := Report{Workload: workload, MaxRate: map[string]float64{}}
	for k, ps := range groups {
		c := Cell{System: k.system, Rate: k.rate, Runs: len(ps)}
		var p50, p99, p999, tput, cpu, user []float64
		for _, p := range ps {
			if p.Saturated {
				c.Saturated++
			}
			if !p.GeneratorOK {
				c.GeneratorIssues++
			}
			p50 = append(p50, p.Loadgen.LatencyUs.P50)
			p99 = append(p99, p.Loadgen.LatencyUs.P99)
			p999 = append(p999, p.Loadgen.LatencyUs.P999)
			tput = append(tput, p.Loadgen.ThroughputRps)
			if p.CPUPerRequestUs > 0 {
				cpu = append(cpu, p.CPUPerRequestUs)
				user = append(user, p.UserPerRequestUs)
			}
		}
		c.P50Us, c.P99Us, c.P999Us = statOf(p50), statOf(p99), statOf(p999)
		c.ThroughputRps, c.CPUPerRequestUs, c.UserPerRequestUs = statOf(tput), statOf(cpu), statOf(user)
		rep.Cells = append(rep.Cells, c)
	}
	direct := map[float64]Cell{}
	for _, c := range rep.Cells {
		if c.System == "direct" {
			direct[c.Rate] = c
		}
	}
	for i, c := range rep.Cells {
		if d, ok := direct[c.Rate]; ok && c.System != "direct" {
			rep.Cells[i].OverheadP50Us = c.P50Us.Median - d.P50Us.Median
			rep.Cells[i].OverheadP99Us = c.P99Us.Median - d.P99Us.Median
		}
	}
	slices.SortFunc(rep.Cells, func(a, b Cell) int {
		if a.Rate != b.Rate {
			if a.Rate < b.Rate {
				return -1
			}
			return 1
		}
		return rank(a.System) - rank(b.System)
	})
	for _, name := range order {
		for _, c := range rep.Cells {
			if c.System != name {
				continue
			}
			if c.Saturated*2 >= c.Runs {
				break
			}
			rep.MaxRate[name] = c.Rate
		}
	}
	return rep
}

func (r Report) WriteText(w io.Writer) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintf(tw, "rate\tsystem\truns\tsat\tp50 us\t+p50\tp99 us (min-max)\t+p99\tp99.9 us\tcpu us/req\tuser\t\n")
	for _, c := range r.Cells {
		fmt.Fprintf(tw, "%.0f\t%s\t%d\t%d\t%.1f\t%+.1f\t%.0f (%.0f-%.0f)\t%+.0f\t%.0f\t%.1f\t%.1f\t\n",
			c.Rate, c.System, c.Runs, c.Saturated,
			c.P50Us.Median, c.OverheadP50Us,
			c.P99Us.Median, c.P99Us.Min, c.P99Us.Max, c.OverheadP99Us,
			c.P999Us.Median, c.CPUPerRequestUs.Median, c.UserPerRequestUs.Median)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	fmt.Fprintln(w)
	names := make([]string, 0, len(r.MaxRate))
	for n := range r.MaxRate {
		names = append(names, n)
	}
	slices.Sort(names)
	for _, n := range names {
		fmt.Fprintf(w, "max sustained rate %-8s %.0f rps\n", n, r.MaxRate[n])
	}
	return nil
}
