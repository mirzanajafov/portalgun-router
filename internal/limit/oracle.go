package limit

import (
	"cmp"
	"slices"
	"time"
)

type Logged struct {
	UnixNano int64  `json:"t"`
	Key      string `json:"k"`
	Allowed  bool   `json:"a"`
}

type Window struct {
	Key      string  `json:"key"`
	Start    int64   `json:"start"`
	Admitted int     `json:"admitted"`
	Allowed  float64 `json:"allowed"`
}

type Verdict struct {
	Requests       int     `json:"requests"`
	Admitted       int     `json:"admitted"`
	OracleAdmitted int     `json:"oracleAdmitted"`
	FalseAdmits    int     `json:"falseAdmits"`
	FalseRejects   int     `json:"falseRejects"`
	OverAdmission  float64 `json:"overAdmission"`
	MaxAhead       int     `json:"maxAhead"`
	MaxBehind      int     `json:"maxBehind"`
	Worst          Window  `json:"worst"`
	WorstRatio     float64 `json:"worstRatio"`
}

func Replay(ds []Logged, rule Rule) Verdict {
	sorted := slices.Clone(ds)
	slices.SortStableFunc(sorted, func(a, b Logged) int { return cmp.Compare(a.UnixNano, b.UnixNano) })
	perfect := NewBuckets(rule)
	admits := map[string][]int64{}
	var v Verdict
	for _, d := range sorted {
		v.Requests++
		ok, _ := perfect.Take(d.Key, d.UnixNano)
		if ok {
			v.OracleAdmitted++
		}
		if d.Allowed {
			v.Admitted++
			admits[d.Key] = append(admits[d.Key], d.UnixNano)
		}
		v.MaxAhead = max(v.MaxAhead, v.Admitted-v.OracleAdmitted)
		v.MaxBehind = max(v.MaxBehind, v.OracleAdmitted-v.Admitted)
		switch {
		case d.Allowed && !ok:
			v.FalseAdmits++
		case !d.Allowed && ok:
			v.FalseRejects++
		}
	}
	if v.OracleAdmitted > 0 {
		v.OverAdmission = float64(v.Admitted-v.OracleAdmitted) / float64(v.OracleAdmitted)
	}
	allowed := rule.Burst + rule.Rate
	v.Worst = Window{Allowed: allowed}
	keys := make([]string, 0, len(admits))
	for k := range admits {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		ts := admits[k]
		j := 0
		for i := range ts {
			for j < len(ts) && ts[j] < ts[i]+int64(time.Second) {
				j++
			}
			if n := j - i; n > v.Worst.Admitted {
				v.Worst = Window{Key: k, Start: ts[i], Admitted: n, Allowed: allowed}
			}
		}
	}
	v.WorstRatio = float64(v.Worst.Admitted) / allowed
	return v
}
