package bench

import (
	"fmt"
	"io"
	"strings"
)

type RegistrySummary struct {
	Cell              string `json:"cell"`
	Runs              int    `json:"runs"`
	Failed            int    `json:"failed"`
	Invalid           int    `json:"invalid"`
	Requests          int    `json:"requests"`
	Lost              int    `json:"lost"`
	Dropped           int    `json:"dropped"`
	Versions          int    `json:"registryVersions"`
	Refusals          int    `json:"refusals"`
	LostRegistrations int    `json:"lostRegistrations"`
	Register          *Stat  `json:"registerMs,omitempty"`
	Join              *Stat  `json:"joinMs,omitempty"`
	Applied           *Stat  `json:"appliedMs,omitempty"`
	Gateways          *Stat  `json:"gatewaysMs,omitempty"`
	LeaveApplied      *Stat  `json:"leaveAppliedMs,omitempty"`
	LeaveGateways     *Stat  `json:"leaveGatewaysMs,omitempty"`
	LeftBeforeEnd     *Stat  `json:"leftBeforeEndMs,omitempty"`
	UpAfterKill       *Stat  `json:"upAfterKillMs,omitempty"`
	KillToFirst       *Stat  `json:"killToFirstMs,omitempty"`
}

func LoadRegistryRuns(dir string) ([]RegistryRun, error) { return loadRuns[RegistryRun](dir) }

func SummarizeRegistry(runs []RegistryRun) []RegistrySummary {
	var order []string
	by := map[string][]RegistryRun{}
	for _, r := range runs {
		if _, ok := by[r.Cell]; !ok {
			order = append(order, r.Cell)
		}
		by[r.Cell] = append(by[r.Cell], r)
	}
	out := make([]RegistrySummary, 0, len(order))
	for _, cell := range order {
		s := RegistrySummary{Cell: cell}
		var register, join, applied, gateways, leaveApplied, leaveGateways, leftBeforeEnd, upAfterKill, killToFirst []float64
		for _, r := range by[cell] {
			s.Runs++
			s.Requests += r.Requests
			s.Lost += r.Lost
			s.Dropped += r.Dropped
			s.Versions += len(r.Versions)
			s.Refusals += r.Refusals
			s.LostRegistrations += r.LostRegistrations
			switch {
			case r.Error != "":
				s.Failed++
				continue
			case len(r.Invalid) > 0:
				s.Invalid++
				continue
			}
			for _, m := range []struct {
				into *[]float64
				v    *float64
			}{
				{&register, r.RegisterMs}, {&join, r.JoinMs}, {&applied, r.AppliedMs}, {&gateways, r.GatewaysMs},
				{&leaveApplied, r.LeaveAppliedMs}, {&leaveGateways, r.LeaveGatewaysMs}, {&leftBeforeEnd, r.LeftBeforeEndMs},
				{&upAfterKill, r.UpAfterKillMs}, {&killToFirst, r.KillToFirstMs},
			} {
				if m.v != nil {
					*m.into = append(*m.into, *m.v)
				}
			}
		}
		s.Register, s.Join, s.Applied, s.Gateways = statOrNil(register), statOrNil(join), statOrNil(applied), statOrNil(gateways)
		s.LeaveApplied, s.LeaveGateways, s.LeftBeforeEnd = statOrNil(leaveApplied), statOrNil(leaveGateways), statOrNil(leftBeforeEnd)
		s.UpAfterKill, s.KillToFirst = statOrNil(upAfterKill), statOrNil(killToFirst)
		out = append(out, s)
	}
	return out
}

func statOrNil(vs []float64) *Stat {
	if len(vs) == 0 {
		return nil
	}
	s := statOf(vs)
	return &s
}

func statText(s *Stat) string {
	if s == nil {
		return "-"
	}
	return rangeOf(*s, 1, 1, 0)
}

func WriteRegistryText(w io.Writer, sums []RegistrySummary) error {
	var b strings.Builder
	fmt.Fprintln(&b, "cell\truns\tlost\tversions\trefusals\tlost registrations\tregister ms\tjoin ms\tapplied ms\tgateways ms\tleave applied ms\tleave gateways ms\tleft before end ms\tup after kill ms\tkill to first ms")
	for _, s := range sums {
		runs := fmt.Sprint(s.Runs)
		if s.Invalid > 0 {
			runs += fmt.Sprintf(" (%d invalid)", s.Invalid)
		}
		if s.Failed > 0 {
			runs += fmt.Sprintf(" (%d failed)", s.Failed)
		}
		lost := fmt.Sprintf("%d of %d", s.Lost, s.Requests)
		if s.Dropped > 0 {
			lost += fmt.Sprintf(" (%d not sent)", s.Dropped)
		}
		fmt.Fprintf(&b, "%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", s.Cell, runs, lost, s.Versions, s.Refusals, s.LostRegistrations,
			statText(s.Register), statText(s.Join), statText(s.Applied), statText(s.Gateways),
			statText(s.LeaveApplied), statText(s.LeaveGateways), statText(s.LeftBeforeEnd), statText(s.UpAfterKill), statText(s.KillToFirst))
	}
	return writeTable(w, b.String(), 0)
}
