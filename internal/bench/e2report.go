package bench

import (
	"fmt"
	"io"
	"slices"
	"strings"
)

type LimiterSummary struct {
	Cell          string `json:"cell"`
	Baseline      string `json:"baseline,omitempty"`
	Runs          int    `json:"runs"`
	InvalidRuns   int    `json:"invalidRuns"`
	P50Us         Stat   `json:"p50Us"`
	P99Us         Stat   `json:"p99Us"`
	AddedP50Us    *Stat  `json:"addedP50Us,omitempty"`
	AddedP99Us    *Stat  `json:"addedP99Us,omitempty"`
	OverAdmission Stat   `json:"overAdmission"`
	MaxAhead      Stat   `json:"maxAhead"`
	MaxBehind     Stat   `json:"maxBehind"`
	WorstRatio    Stat   `json:"worstRatio"`
	FalseAdmits   Stat   `json:"falseAdmits"`
	FalseRejects  Stat   `json:"falseRejects"`
	RedisTrips    Stat   `json:"redisRoundTripsPerRequest"`
	RedisCommands Stat   `json:"redisCommandsPerRequest"`
	RedisCPUUs    Stat   `json:"redisCpuPerRequestUs"`
	Degraded      Stat   `json:"degraded"`
	RedisSlow     Stat   `json:"redisSlowShare"`
	RedisTimeouts Stat   `json:"redisTimeoutShare"`
	RedisDials    Stat   `json:"redisDials"`
	DroppedLogs   int64  `json:"droppedDecisions"`
}

func LoadLimiterRuns(dir string) ([]LimiterRun, error) {
	runs, err := loadRuns[LimiterRun](dir)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(runs, func(r LimiterRun) bool { return r.Report == nil }), nil
}

func baselineOf(cell string) string {
	_, rest, ok := strings.Cut(cell, "/")
	if !ok {
		return ""
	}
	return NoLimiter + "/" + rest
}

func SummarizeLimiter(runs []LimiterRun) []LimiterSummary {
	var order []string
	by := map[string][]LimiterRun{}
	for _, r := range runs {
		if _, ok := by[r.Cell]; !ok {
			order = append(order, r.Cell)
		}
		by[r.Cell] = append(by[r.Cell], r)
	}
	out := make([]LimiterSummary, 0, len(order))
	index := map[string]int{}
	for _, cell := range order {
		s := LimiterSummary{Cell: cell}
		var p50, p99, over, ahead, behind, worst, fa, fr, trips, cmds, cpu, deg, slow, timeouts, dials []float64
		for _, r := range by[cell] {
			s.Runs++
			g := r.Report.Generator
			if !g.Valid {
				s.InvalidRuns++
				continue
			}
			p50 = append(p50, g.LatencyUs.P50)
			p99 = append(p99, g.LatencyUs.P99)
			trips = append(trips, r.Report.Redis.TripsReq)
			cmds = append(cmds, r.Report.Redis.CommandsReq)
			cpu = append(cpu, r.Report.Redis.CPUPerReqUs)
			var d, calls, late, out, opened int64
			for _, in := range r.Report.Instances {
				d += in.Degraded
				s.DroppedLogs += in.Dropped
				calls, late, out, opened = calls+in.RedisCalls, late+in.RedisSlow, out+in.RedisTimeouts, opened+in.RedisDials
			}
			deg = append(deg, float64(d))
			if calls > 0 {
				slow = append(slow, float64(late)/float64(calls))
				timeouts = append(timeouts, float64(out)/float64(calls))
				dials = append(dials, float64(opened))
			}
			if v := r.Report.Verdict; v != nil && v.Requests > 0 {
				over = append(over, v.OverAdmission)
				ahead = append(ahead, float64(v.MaxAhead))
				behind = append(behind, float64(v.MaxBehind))
				worst = append(worst, v.WorstRatio)
				fa = append(fa, float64(v.FalseAdmits)/float64(v.Requests))
				fr = append(fr, float64(v.FalseRejects)/float64(v.Requests))
			}
		}
		s.P50Us, s.P99Us = statOf(p50), statOf(p99)
		s.OverAdmission, s.WorstRatio = statOf(over), statOf(worst)
		s.MaxAhead, s.MaxBehind = statOf(ahead), statOf(behind)
		s.FalseAdmits, s.FalseRejects = statOf(fa), statOf(fr)
		s.RedisTrips, s.RedisCommands = statOf(trips), statOf(cmds)
		s.RedisCPUUs, s.Degraded = statOf(cpu), statOf(deg)
		s.RedisSlow, s.RedisTimeouts, s.RedisDials = statOf(slow), statOf(timeouts), statOf(dials)
		index[cell] = len(out)
		out = append(out, s)
	}
	for i := range out {
		base := baselineOf(out[i].Cell)
		j, ok := index[base]
		if !ok || base == out[i].Cell || out[j].Runs == out[j].InvalidRuns || out[i].Runs == out[i].InvalidRuns {
			continue
		}
		out[i].Baseline = base
		b := out[j]
		out[i].AddedP50Us = &Stat{Median: out[i].P50Us.Median - b.P50Us.Median, Min: out[i].P50Us.Min - b.P50Us.Max, Max: out[i].P50Us.Max - b.P50Us.Min}
		out[i].AddedP99Us = &Stat{Median: out[i].P99Us.Median - b.P99Us.Median, Min: out[i].P99Us.Min - b.P99Us.Max, Max: out[i].P99Us.Max - b.P99Us.Min}
	}
	return out
}

func pctOf(s Stat) string {
	v := fmt.Sprintf("%+.1f%%", s.Median*100)
	if s.Min != s.Max {
		v += fmt.Sprintf(" (%+.1f to %+.1f)", s.Min*100, s.Max*100)
	}
	return v
}

func WriteLimiterText(w io.Writer, sums []LimiterSummary) error {
	var b strings.Builder
	fmt.Fprintln(&b, "cell\truns\tp50 us\tp99 us\tadded p50\tadded p99\tover-admission\tmost ahead\tmost behind\tworst 1 s\tfalse admits\tfalse rejects\tredis trips/req\tredis cmds/req\tredis cpu us/req\tdegraded\tcalls over 2 ms\ttimeouts\tdials")
	for _, s := range sums {
		valid := s.Runs - s.InvalidRuns
		runs := fmt.Sprint(s.Runs)
		if s.InvalidRuns > 0 {
			runs += fmt.Sprintf(" (%d invalid)", s.InvalidRuns)
		}
		added50, added99 := "-", "-"
		if s.AddedP50Us != nil {
			added50, added99 = fmt.Sprintf("%+.0f", s.AddedP50Us.Median), fmt.Sprintf("%+.0f", s.AddedP99Us.Median)
		}
		over, ahead, behind, worst, fa, fr := "-", "-", "-", "-", "-", "-"
		if !strings.HasPrefix(s.Cell, NoLimiter+"/") && valid > 0 {
			over, worst = pctOf(s.OverAdmission), rangeOf(s.WorstRatio, valid, valid, 2)
			ahead, behind = rangeOf(s.MaxAhead, valid, valid, 0), rangeOf(s.MaxBehind, valid, valid, 0)
			fa, fr = pctOf(s.FalseAdmits), pctOf(s.FalseRejects)
		}
		late, out, dials := "-", "-", "-"
		if s.RedisSlow.Max > 0 || s.RedisDials.Max > 0 {
			late, out, dials = pctOf(s.RedisSlow), pctOf(s.RedisTimeouts), rangeOf(s.RedisDials, valid, valid, 0)
		}
		fmt.Fprintf(&b, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", s.Cell, runs,
			rangeOf(s.P50Us, valid, valid, 0), rangeOf(s.P99Us, valid, valid, 0), added50, added99,
			over, ahead, behind, worst, fa, fr, rangeOf(s.RedisTrips, valid, valid, 2), rangeOf(s.RedisCommands, valid, valid, 2),
			rangeOf(s.RedisCPUUs, valid, valid, 1), rangeOf(s.Degraded, valid, valid, 0), late, out, dials)
	}
	return writeTable(w, b.String(), 0)
}
