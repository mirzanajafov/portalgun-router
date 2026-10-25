package bench

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type FailoverSummary struct {
	Cell           string `json:"cell"`
	Runs           int    `json:"runs"`
	InvalidRuns    int    `json:"invalidRuns"`
	Failed         int    `json:"failed"`
	Detected       int    `json:"detected"`
	Moved          int    `json:"moved"`
	Recovered      int    `json:"recovered"`
	DetectMs       Stat   `json:"detectMs"`
	MoveMs         Stat   `json:"moveMs"`
	Lost           Stat   `json:"lost"`
	LostAfter      Stat   `json:"lostAfter"`
	P99BeforeMs    Stat   `json:"p99BeforeMs"`
	P99FaultMs     Stat   `json:"p99FaultMs"`
	AttemptsFault  Stat   `json:"attemptsFault"`
	RecoverMs      Stat   `json:"recoverMs"`
	FalseEjections Stat   `json:"falseEjections"`
}

func LoadFailoverRuns(dir string) ([]FailoverRun, error) {
	data, err := os.ReadFile(filepath.Join(dir, "runs.jsonl"))
	if err != nil {
		return nil, err
	}
	var runs []FailoverRun
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		var r FailoverRun
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return nil, err
		}
		if r.Report != nil || r.Error != "" {
			runs = append(runs, r)
		}
	}
	return runs, sc.Err()
}

func SummarizeFailover(runs []FailoverRun) []FailoverSummary {
	var order []string
	by := map[string][]FailoverRun{}
	for _, r := range runs {
		if _, ok := by[r.Cell]; !ok {
			order = append(order, r.Cell)
		}
		by[r.Cell] = append(by[r.Cell], r)
	}
	out := make([]FailoverSummary, 0, len(order))
	for _, cell := range order {
		s := FailoverSummary{Cell: cell}
		var detect, move, lost, after, before, fault, attempts, recovery, falseEj []float64
		for _, r := range by[cell] {
			if r.Report == nil {
				s.Failed++
				continue
			}
			s.Runs++
			if !r.Report.Generator.Valid {
				s.InvalidRuns++
				continue
			}
			res := r.Report.Result
			if res.DetectMs != nil {
				s.Detected++
				detect = append(detect, *res.DetectMs)
			}
			if res.MoveMs != nil {
				s.Moved++
				move = append(move, *res.MoveMs)
			}
			if res.RecoverMs != nil {
				s.Recovered++
				recovery = append(recovery, *res.RecoverMs)
			}
			lost = append(lost, float64(res.Lost))
			after = append(after, float64(res.LostAfter))
			before = append(before, res.P99BeforeMs)
			fault = append(fault, res.P99FaultMs)
			attempts = append(attempts, res.AttemptsFault)
			falseEj = append(falseEj, float64(res.FalseEjections))
		}
		s.DetectMs, s.MoveMs, s.RecoverMs = statOf(detect), statOf(move), statOf(recovery)
		s.Lost, s.LostAfter = statOf(lost), statOf(after)
		s.P99BeforeMs, s.P99FaultMs = statOf(before), statOf(fault)
		s.AttemptsFault, s.FalseEjections = statOf(attempts), statOf(falseEj)
		out = append(out, s)
	}
	return out
}

func rangeOf(s Stat, n, of int, digits int) string {
	if n == 0 {
		return "-"
	}
	v := fmt.Sprintf("%.*f", digits, s.Median)
	if s.Min != s.Max {
		v += fmt.Sprintf(" (%.*f-%.*f)", digits, s.Min, digits, s.Max)
	}
	if n < of {
		v += fmt.Sprintf(" [%d/%d]", n, of)
	}
	return v
}

func WriteFailoverText(w io.Writer, sums []FailoverSummary) error {
	var b strings.Builder
	fmt.Fprintln(&b, "cell\truns\tdetect ms\tmove ms\tlost\tlost after\tp99 before\tp99 fault\tattempts\trecover ms\tfalse ejections")
	for _, s := range sums {
		valid := s.Runs - s.InvalidRuns
		runs := fmt.Sprint(s.Runs)
		if s.InvalidRuns > 0 {
			runs += fmt.Sprintf(" (%d invalid)", s.InvalidRuns)
		}
		if s.Failed > 0 {
			runs += fmt.Sprintf(" (%d failed)", s.Failed)
		}
		fmt.Fprintf(&b, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", s.Cell, runs,
			rangeOf(s.DetectMs, s.Detected, valid, 0), rangeOf(s.MoveMs, s.Moved, valid, 0),
			rangeOf(s.Lost, valid, valid, 0), rangeOf(s.LostAfter, valid, valid, 0),
			rangeOf(s.P99BeforeMs, valid, valid, 1), rangeOf(s.P99FaultMs, valid, valid, 1),
			rangeOf(s.AttemptsFault, valid, valid, 3), rangeOf(s.RecoverMs, s.Recovered, valid, 0),
			rangeOf(s.FalseEjections, valid, valid, 0))
	}
	return writeTable(w, b.String(), 0)
}
