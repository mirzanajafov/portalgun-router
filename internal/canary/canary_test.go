package canary

import (
	"math"
	"testing"
	"time"
)

func bin(n1, n2, e1, e2 int64) Bin {
	return Bin{Stable: Counts{Requests: n1, Errors: e1}, Canary: Counts{Requests: n2, Errors: e2}}
}

func TestSPRTMatchesHandComputedValues(t *testing.T) {
	s := NewSPRT(0.01, 0.05, 2)
	lower, upper := s.Bounds()
	if math.Abs(upper-math.Log(95)) > 1e-12 || math.Abs(lower-math.Log(0.05/0.99)) > 1e-12 {
		t.Fatalf("bounds %v %v", lower, upper)
	}
	s.Update(950, 50, 0, 7)
	if want := 7 * math.Log((2*0.05/(2*0.05+0.95))/0.05); math.Abs(s.LLR()-want) > 1e-9 || s.Verdict() != Continue {
		t.Fatalf("after 7 canary errors: llr %.4f want %.4f, verdict %v", s.LLR(), want, s.Verdict())
	}
	s.Update(950, 50, 0, 1)
	if s.Verdict() != Rollback {
		t.Fatalf("after 8 canary errors: llr %.4f, verdict %v", s.LLR(), s.Verdict())
	}
}

func TestSPRTAcceptsWhenErrorsFollowTheTrafficShare(t *testing.T) {
	s := NewSPRT(0.01, 0.05, 2)
	for range 100 {
		s.Update(950, 50, 19, 1)
	}
	if s.Verdict() != Promote {
		t.Fatalf("llr %.3f after errors exactly in proportion to traffic", s.LLR())
	}
}

func TestSPRTAcceptanceSticksButRejectionStillWins(t *testing.T) {
	s := NewSPRT(0.01, 0.05, 2)
	for range 11 {
		s.Update(950, 50, 19, 1)
	}
	if s.Verdict() != Promote {
		t.Fatalf("not accepted at llr %.2f", s.LLR())
	}
	s.Update(950, 50, 0, 3)
	if s.LLR() <= s.lower || s.Verdict() != Promote {
		t.Fatalf("llr %.2f back above the lower bound, verdict %v; acceptance should stick", s.LLR(), s.Verdict())
	}
	s.Update(950, 50, 0, 20)
	if s.Verdict() != Rollback {
		t.Fatalf("llr %.2f, verdict %v; crossing the upper bound must still roll back", s.LLR(), s.Verdict())
	}
}

func TestSPRTIgnoresBinsWithoutInformation(t *testing.T) {
	s := NewSPRT(0.01, 0.05, 2)
	s.Update(0, 100, 0, 50)
	s.Update(100, 0, 50, 0)
	s.Update(100, 100, 0, 0)
	if s.LLR() != 0 {
		t.Fatalf("llr %v from bins where one version had no traffic or nothing went wrong", s.LLR())
	}
}

func TestSequentialRespectsTheMinimumStep(t *testing.T) {
	s := NewSequential(0.01, 0.05, 2, 30*time.Second)
	var d Decision
	for i := range 40 {
		b := bin(950, 50, 19, 1)
		b.Stable.Slow, b.Canary.Slow = 48, 2
		d = s.Observe(b, time.Duration(i+1)*time.Second)
		if d.Verdict == Promote && i+1 < 30 {
			t.Fatalf("promoted after %ds", i+1)
		}
	}
	if d.Verdict != Promote {
		t.Fatalf("not promoted after 40s: %+v", d)
	}
}

func TestSilenceIsEnoughWhenNothingFails(t *testing.T) {
	s := NewSequential(0.01, 0.05, 2, 30*time.Second)
	var d Decision
	sec := 0
	for ; sec < 600 && d.Verdict == Continue; sec++ {
		b := bin(198, 2, 0, 0)
		b.Stable.Slow, b.Canary.Slow = 10, 0
		d = s.Observe(b, time.Duration(sec+1)*time.Second)
	}
	if d.Verdict != Promote || sec != SilentRequests/2 {
		t.Fatalf("decision %+v after %ds, want a promotion once the canary served %d requests", d, sec, SilentRequests)
	}
	broken := NewSequential(0.01, 0.05, 2, 30*time.Second)
	for i := range 200 {
		b := bin(198, 2, 0, 0)
		if i == 100 {
			b.Stable.Errors, b.Canary.Errors = 0, 1
		}
		b.Stable.Slow, b.Canary.Slow = 10, 0
		if d = broken.Observe(b, time.Duration(i+1)*time.Second); d.Verdict == Promote {
			t.Fatalf("promoted at %ds although the canary failed once and the stable never did", i+1)
		}
	}
}

func TestSequentialRollsBackOnSlowness(t *testing.T) {
	s := NewSequential(0.01, 0.05, 2, 0)
	var d Decision
	for i := range 60 {
		b := bin(950, 50, 0, 0)
		b.Stable.Slow, b.Canary.Slow = 47, 10
		if d = s.Observe(b, time.Duration(i)*time.Second); d.Verdict != Continue {
			break
		}
	}
	if d.Verdict != Rollback {
		t.Fatalf("decision %+v", d)
	}
}

func TestFixedHorizonDecidesOnlyAtTheEnd(t *testing.T) {
	f := NewFixedHorizon(0.01)
	for range 60 {
		if d := f.Observe(bin(950, 50, 5, 2), 0); d.Verdict != Continue {
			t.Fatalf("decided early: %+v", d)
		}
	}
	if d := f.Finish(); d.Verdict != Rollback {
		t.Fatalf("canary at 7.6x over a minute: %+v", d)
	}
	same := NewFixedHorizon(0.01)
	same.Observe(bin(9500, 500, 50, 3), 0)
	if d := same.Finish(); d.Verdict != Promote {
		t.Fatalf("canary in line with stable: %+v", d)
	}
}

func TestSequentialPromotesWhenUndecided(t *testing.T) {
	s := NewSequential(0.01, 0.05, 2, 0)
	s.Observe(bin(950, 50, 0, 3), time.Second)
	if d := s.Finish(); d.Verdict != Promote {
		t.Fatalf("undecided step rolled back: %+v", d)
	}
}

func TestBinomialTail(t *testing.T) {
	for _, tc := range []struct {
		k, n int64
		p    float64
		want float64
	}{
		{0, 10, 0.3, 1},
		{11, 10, 0.3, 0},
		{10, 10, 0.5, 1.0 / 1024},
		{3, 10, 0.5, 0.9453125},
		{8, 100, 0.05, 0.12796047862037888},
		{60, 1000, 0.05, 0.08673217838486096},
	} {
		if got := BinomialTail(tc.k, tc.n, tc.p); math.Abs(got-tc.want) > 1e-3*max(tc.want, 1e-3) {
			t.Errorf("P(X >= %d | %d, %v) = %.6f, want %.6f", tc.k, tc.n, tc.p, got, tc.want)
		}
	}
}

func TestHybridCatchesWhatTheSequentialTestLeftOpen(t *testing.T) {
	h := NewHybrid(0.01, 0.05, 2, 0)
	for i := range 300 {
		canaryErrors := int64(1)
		if i%4 == 3 {
			canaryErrors = 0
		}
		if d := h.Observe(bin(1900, 100, 10, canaryErrors), time.Duration(i)*time.Second); d.Verdict != Continue {
			t.Fatalf("a 1.43x regression decided the sequential test at %ds: %+v", i, d)
		}
	}
	if d := h.Finish(); d.Verdict != Rollback {
		t.Fatalf("1.43x over five minutes: %+v", d)
	}
}
