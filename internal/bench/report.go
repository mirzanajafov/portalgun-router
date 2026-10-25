package bench

import (
	"fmt"
	"io"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
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
	Died             int     `json:"died,omitempty"`
	Collapsed        int     `json:"collapsed,omitempty"`
	GeneratorIssues  int     `json:"generatorIssues"`
	P50Us            Stat    `json:"p50Us"`
	P99Us            Stat    `json:"p99Us"`
	P999Us           Stat    `json:"p999Us"`
	ThroughputRps    Stat    `json:"throughputRps"`
	CPUPerRequestUs  Stat    `json:"cpuPerRequestUs"`
	UserPerRequestUs Stat    `json:"userPerRequestUs"`
	MemoryPeakMB     Stat    `json:"memoryPeakMb"`
	SwapPeakMB       Stat    `json:"swapPeakMb,omitzero"`
	AnonPeakMB       Stat    `json:"anonPeakMb,omitzero"`
	SockPeakMB       Stat    `json:"sockPeakMb,omitzero"`
	OverheadP50Us    float64 `json:"overheadP50Us"`
	OverheadP99Us    float64 `json:"overheadP99Us"`
	GoodputRps       Stat    `json:"goodputRps,omitzero"`
	OKP50Us          Stat    `json:"okP50Us,omitzero"`
	OKP99Us          Stat    `json:"okP99Us,omitzero"`
	ShedShare        Stat    `json:"shedShare,omitzero"`
	FailedShare      Stat    `json:"failedShare,omitzero"`
}

const maxHealthyFailures = 0.01

func collapsed(p Point) bool {
	s := p.Loadgen
	return p.Died != "" || s.Scheduled == 0 || float64(s.Failed+s.Dropped)/float64(s.Scheduled) > maxHealthyFailures
}

func goodput(s loadgen.Summary) float64 {
	if s.WindowSec <= 0 {
		return 0
	}
	var ok int64
	for code, n := range s.Status {
		if len(code) == 3 && code[0] == '2' {
			ok += n
		}
	}
	return float64(ok) / s.WindowSec
}

type Report struct {
	Workload string             `json:"workload"`
	Cells    []Cell             `json:"cells"`
	MaxRate  map[string]float64 `json:"maxRate"`
}

func LoadPoints(dir string) ([]Point, error) { return loadRuns[Point](dir) }

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
		clean := slices.ContainsFunc(ps, func(p Point) bool { return p.GeneratorOK })
		var p50, p99, p999, tput, cpu, user, mem, swap, anon, sock, good, okP50, okP99, shed, failed []float64
		for _, p := range ps {
			if p.Saturated {
				c.Saturated++
			}
			if p.MemoryPeakMB > 0 {
				mem = append(mem, p.MemoryPeakMB)
				swap = append(swap, p.SwapPeakMB)
				anon = append(anon, p.AnonPeakMB)
				sock = append(sock, p.SockPeakMB)
			}
			if collapsed(p) {
				c.Collapsed++
			}
			if p.Died != "" {
				c.Died++
				if p.Loadgen.Scheduled == 0 {
					continue
				}
			}
			good = append(good, goodput(p.Loadgen))
			if n := p.Loadgen.Scheduled; n > 0 {
				shed = append(shed, float64(p.Loadgen.Status["503"])/float64(n))
				failed = append(failed, float64(p.Loadgen.Failed+p.Loadgen.Dropped)/float64(n))
			}
			if p.Loadgen.OKLatencyUs.Count > 0 {
				okP50 = append(okP50, p.Loadgen.OKLatencyUs.P50)
				okP99 = append(okP99, p.Loadgen.OKLatencyUs.P99)
			}
			if !p.GeneratorOK {
				c.GeneratorIssues++
				if clean {
					continue
				}
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
		c.MemoryPeakMB, c.SwapPeakMB = statOf(mem), statOf(swap)
		c.AnonPeakMB, c.SockPeakMB = statOf(anon), statOf(sock)
		c.GoodputRps, c.ShedShare, c.FailedShare = statOf(good), statOf(shed), statOf(failed)
		c.OKP50Us, c.OKP99Us = statOf(okP50), statOf(okP99)
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

func WriteReportText(w io.Writer, r Report) error {
	var b strings.Builder
	fmt.Fprintf(&b, "rate\tsystem\truns\tsat\tdied\tp50 us\t+p50\tp99 us (min-max)\t+p99\tp99.9 us\tcpu us/req\tuser\tmem MB\t\n")
	for _, c := range r.Cells {
		fmt.Fprintf(&b, "%.0f\t%s\t%d\t%d\t%d\t%.1f\t%+.1f\t%.0f (%.0f-%.0f)\t%+.0f\t%.0f\t%.1f\t%.1f\t%.0f\t\n",
			c.Rate, c.System, c.Runs, c.Saturated, c.Died,
			c.P50Us.Median, c.OverheadP50Us,
			c.P99Us.Median, c.P99Us.Min, c.P99Us.Max, c.OverheadP99Us,
			c.P999Us.Median, c.CPUPerRequestUs.Median, c.UserPerRequestUs.Median, c.MemoryPeakMB.Max)
	}
	fmt.Fprintln(&b)
	names := make([]string, 0, len(r.MaxRate))
	for n := range r.MaxRate {
		names = append(names, n)
	}
	slices.Sort(names)
	for _, n := range names {
		fmt.Fprintf(&b, "max sustained rate %-8s %.0f rps\n", n, r.MaxRate[n])
	}
	return writeTable(w, b.String(), tabwriter.AlignRight)
}

func WriteOverloadText(w io.Writer, r Report) error {
	var b strings.Builder
	fmt.Fprintf(&b, "rate\tsystem\truns\tcollapsed\tgoodput rps (min-max)\t503 %%\tfailed %%\tok p50 ms\tok p99 ms (min-max)\tcpu us/req\tmem MB\tanon MB\tsock MB\tswap MB\t\n")
	for _, c := range r.Cells {
		fmt.Fprintf(&b, "%.0f\t%s\t%d\t%d\t%.0f (%.0f-%.0f)\t%.1f\t%.2f\t%.1f\t%.1f (%.1f-%.1f)\t%.1f\t%.0f\t%.0f\t%.0f\t%.0f\t\n",
			c.Rate, c.System, c.Runs, c.Collapsed,
			c.GoodputRps.Median, c.GoodputRps.Min, c.GoodputRps.Max,
			c.ShedShare.Median*100, c.FailedShare.Median*100,
			c.OKP50Us.Median/1000, c.OKP99Us.Median/1000, c.OKP99Us.Min/1000, c.OKP99Us.Max/1000,
			c.CPUPerRequestUs.Median, c.MemoryPeakMB.Max, c.AnonPeakMB.Max, c.SockPeakMB.Max, c.SwapPeakMB.Max)
	}
	return writeTable(w, b.String(), tabwriter.AlignRight)
}
