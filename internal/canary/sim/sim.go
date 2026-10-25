package sim

import (
	"math"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/canary"
)

const (
	stableSlow = 0.05
	blipLength = 30
	blipRate   = 0.2
)

type Cell struct {
	Group        string  `json:"group"`
	BaseError    float64 `json:"baseError"`
	Share        float64 `json:"share"`
	Rho          float64 `json:"rho"`
	TotalFailure bool    `json:"totalFailure,omitempty"`
	CanarySlow   float64 `json:"canarySlow"`
	Blip         bool    `json:"blip,omitempty"`
	RPS          float64 `json:"rps"`
	Trials       int     `json:"trials"`
}

type Config struct {
	Step    time.Duration
	MinStep time.Duration
	Seed    uint64
	Alpha   float64
	Beta    float64
	Rho     float64
}

func (c Config) Policies(cell Cell) []canary.Policy {
	return []canary.Policy{
		canary.NewSequential(c.Alpha, c.Beta, c.Rho, c.MinStep),
		newFixedAbsolute(2*cell.BaseError, 60, 100),
		newFixedRelative(2, 60, 100),
		newPeeking(0.05, 100),
		canary.NewFixedHorizon(c.Alpha),
		canary.NewHybrid(c.Alpha, c.Beta, c.Rho, c.MinStep),
	}
}

type Outcome struct {
	Verdict      canary.Verdict
	At           int
	CanaryErrors int64
	Truncated    bool
}

type PolicyResult struct {
	Policy           string  `json:"policy"`
	Trials           int     `json:"trials"`
	Rollbacks        int     `json:"rollbacks"`
	RollbackRate     float64 `json:"rollbackRate"`
	CILow            float64 `json:"ciLow"`
	CIHigh           float64 `json:"ciHigh"`
	MedianRollbackS  float64 `json:"medianRollbackSec"`
	P90RollbackS     float64 `json:"p90RollbackSec"`
	MedianPromoteS   float64 `json:"medianPromoteSec"`
	MeanCanaryErrors float64 `json:"meanCanaryErrors"`
	Truncated        int     `json:"truncated"`
}

type CellResult struct {
	Cell     Cell           `json:"cell"`
	Policies []PolicyResult `json:"policies"`
}

func Run(cells []Cell, cfg Config) []CellResult {
	out := make([]CellResult, len(cells))
	var wg sync.WaitGroup
	sem := make(chan struct{}, runtime.GOMAXPROCS(0))
	for i, cell := range cells {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			out[i] = runCell(cell, cfg, uint64(i))
		})
	}
	wg.Wait()
	return out
}

func runCell(cell Cell, cfg Config, index uint64) CellResult {
	names := []string{}
	for _, p := range cfg.Policies(cell) {
		names = append(names, p.Name())
	}
	all := make([][]Outcome, len(names))
	for trial := range cell.Trials {
		rng := rand.New(rand.NewPCG(cfg.Seed, index<<32|uint64(trial)))
		for i, o := range Trial(cell, cfg, rng) {
			all[i] = append(all[i], o)
		}
	}
	res := CellResult{Cell: cell}
	for i, name := range names {
		res.Policies = append(res.Policies, summarize(name, all[i]))
	}
	return res
}

func Trial(cell Cell, cfg Config, rng *rand.Rand) []Outcome {
	pols := cfg.Policies(cell)
	steps := int(cfg.Step / time.Second)
	blipStart := -1
	if cell.Blip {
		blipStart = rng.IntN(max(1, steps-blipLength))
	}
	pStable := cell.BaseError
	pCanary := min(1, cell.BaseError*cell.Rho)
	if cell.TotalFailure {
		pCanary = 1
	}
	slow := cell.CanarySlow
	if slow == 0 {
		slow = stableSlow
	}

	out := make([]Outcome, len(pols))
	decided := make([]bool, len(pols))
	left := len(pols)
	var canaryErrors int64
	for sec := 0; sec < steps && left > 0; sec++ {
		n := poisson(rng, cell.RPS)
		n2 := binomial(rng, n, cell.Share)
		n1 := n - n2
		ps, pc := pStable, pCanary
		if blipStart >= 0 && sec >= blipStart && sec < blipStart+blipLength {
			ps, pc = min(1, ps+blipRate), min(1, pc+blipRate)
		}
		b := canary.Bin{
			Requests: [2]int64{n1, n2},
			Errors:   [2]int64{binomial(rng, n1, ps), binomial(rng, n2, pc)},
			Slow:     [2]int64{binomial(rng, n1, stableSlow), binomial(rng, n2, slow)},
		}
		canaryErrors += b.Errors[1]
		for i, p := range pols {
			if decided[i] {
				continue
			}
			if d := p.Observe(b, time.Duration(sec+1)*time.Second); d.Verdict != canary.Continue {
				out[i] = Outcome{Verdict: d.Verdict, At: sec + 1, CanaryErrors: canaryErrors}
				decided[i] = true
				left--
			}
		}
	}
	for i, p := range pols {
		if !decided[i] {
			out[i] = Outcome{Verdict: p.Finish().Verdict, At: steps, CanaryErrors: canaryErrors, Truncated: true}
		}
	}
	return out
}

func summarize(name string, outs []Outcome) PolicyResult {
	r := PolicyResult{Policy: name, Trials: len(outs)}
	var rollbackAt, promoteAt []float64
	var errs float64
	for _, o := range outs {
		errs += float64(o.CanaryErrors)
		if o.Truncated && name == "sprt" {
			r.Truncated++
		}
		if o.Verdict == canary.Rollback {
			r.Rollbacks++
			rollbackAt = append(rollbackAt, float64(o.At))
		} else {
			promoteAt = append(promoteAt, float64(o.At))
		}
	}
	r.RollbackRate = float64(r.Rollbacks) / float64(r.Trials)
	r.CILow, r.CIHigh = wilson(r.Rollbacks, r.Trials)
	r.MedianRollbackS = quantile(rollbackAt, 0.5)
	r.P90RollbackS = quantile(rollbackAt, 0.9)
	r.MedianPromoteS = quantile(promoteAt, 0.5)
	r.MeanCanaryErrors = errs / float64(len(outs))
	return r
}

func quantile(v []float64, q float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := slices.Clone(v)
	slices.Sort(s)
	return s[min(len(s)-1, int(q*float64(len(s))))]
}

func wilson(k, n int) (float64, float64) {
	if n == 0 {
		return 0, 0
	}
	const z = 1.96
	p := float64(k) / float64(n)
	nn := float64(n)
	center := (p + z*z/(2*nn)) / (1 + z*z/nn)
	half := z * math.Sqrt(p*(1-p)/nn+z*z/(4*nn*nn)) / (1 + z*z/nn)
	return max(0, center-half), min(1, center+half)
}

func poisson(rng *rand.Rand, lambda float64) int64 {
	if lambda <= 0 {
		return 0
	}
	if lambda < 30 {
		l := math.Exp(-lambda)
		k, p := int64(0), 1.0
		for {
			p *= rng.Float64()
			if p <= l {
				return k
			}
			k++
		}
	}
	return max(0, int64(math.Round(lambda+math.Sqrt(lambda)*rng.NormFloat64())))
}

func binomial(rng *rand.Rand, n int64, p float64) int64 {
	switch {
	case n <= 0 || p <= 0:
		return 0
	case p >= 1:
		return n
	case n < 64:
		k := int64(0)
		for range n {
			if rng.Float64() < p {
				k++
			}
		}
		return k
	case float64(n)*p < 30:
		return min(n, poisson(rng, float64(n)*p))
	case float64(n)*(1-p) < 30:
		return n - min(n, poisson(rng, float64(n)*(1-p)))
	}
	mean := float64(n) * p
	k := math.Round(mean + math.Sqrt(mean*(1-p))*rng.NormFloat64())
	return int64(min(float64(n), max(0, k)))
}
