package sim

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/canary"
)

func TestSamplersHaveTheRightMeans(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for _, tc := range []struct {
		n int64
		p float64
	}{{20, 0.3}, {1000, 0.001}, {1000, 0.999}, {5000, 0.4}} {
		var sum float64
		const draws = 20000
		for range draws {
			sum += float64(binomial(rng, tc.n, tc.p))
		}
		want := float64(tc.n) * tc.p
		if got := sum / draws; math.Abs(got-want) > 0.02*want+0.05 {
			t.Errorf("binomial(%d, %v): mean %.3f, want %.3f", tc.n, tc.p, got, want)
		}
	}
	for _, lambda := range []float64{0.5, 12, 2000} {
		var sum float64
		for range 20000 {
			sum += float64(poisson(rng, lambda))
		}
		if got := sum / 20000; math.Abs(got-lambda) > 0.02*lambda+0.02 {
			t.Errorf("poisson(%v): mean %.3f", lambda, got)
		}
	}
}

func TestTrialsAreReproducible(t *testing.T) {
	cfg := Config{Step: time.Minute, MinStep: 10 * time.Second, Seed: 3, Alpha: 0.01, Beta: 0.05, Rho: 2}
	cell := Cell{BaseError: 0.01, Share: 0.05, Rho: 2, RPS: 500}
	a := Trial(cell, cfg, rand.New(rand.NewPCG(9, 9)))
	b := Trial(cell, cfg, rand.New(rand.NewPCG(9, 9)))
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("policy %d: %+v vs %+v", i, a[i], b[i])
		}
	}
}

func TestATotalFailureIsCaughtFast(t *testing.T) {
	cfg := Config{Step: time.Minute, MinStep: 10 * time.Second, Seed: 3, Alpha: 0.01, Beta: 0.05, Rho: 2}
	cell := Cell{BaseError: 0.005, Share: 0.05, TotalFailure: true, Rho: 1, RPS: 2000}
	out := Trial(cell, cfg, rand.New(rand.NewPCG(1, 1)))
	if out[0].Verdict != canary.Rollback || out[0].At > 2 {
		t.Fatalf("sprt outcome %+v", out[0])
	}
}

func TestSPRTKeepsItsPromiseOnAA(t *testing.T) {
	cfg := Config{Step: 5 * time.Minute, MinStep: 30 * time.Second, Seed: 11, Alpha: 0.01, Beta: 0.05, Rho: 2}
	res := Run([]Cell{{BaseError: 0.005, Share: 0.05, Rho: 1, RPS: 2000, Trials: 600}}, cfg)
	sprt := res[0].Policies[0]
	if sprt.Policy != "sprt" || sprt.RollbackRate > 0.02 {
		t.Fatalf("sprt false rollback rate %.3f on A/A (CI %.3f-%.3f)", sprt.RollbackRate, sprt.CILow, sprt.CIHigh)
	}
}

func TestWilson(t *testing.T) {
	lo, hi := wilson(10, 100)
	if math.Abs(lo-0.0552) > 0.001 || math.Abs(hi-0.1744) > 0.001 {
		t.Fatalf("wilson(10, 100) = %.4f-%.4f", lo, hi)
	}
}
