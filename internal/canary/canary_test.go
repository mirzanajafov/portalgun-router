package canary

import (
	"math"
	"testing"
	"time"
)

func bin(n1, n2, e1, e2 int64) Bin {
	return Bin{Requests: [2]int64{n1, n2}, Errors: [2]int64{e1, e2}}
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
		b.Slow = [2]int64{48, 2}
		d = s.Observe(b, time.Duration(i+1)*time.Second)
		if d.Verdict == Promote && i+1 < 30 {
			t.Fatalf("promoted after %ds", i+1)
		}
	}
	if d.Verdict != Promote {
		t.Fatalf("not promoted after 40s: %+v", d)
	}
}

func TestSequentialRollsBackOnSlowness(t *testing.T) {
	s := NewSequential(0.01, 0.05, 2, 0)
	var d Decision
	for i := range 60 {
		b := bin(950, 50, 0, 0)
		b.Slow = [2]int64{47, 10}
		if d = s.Observe(b, time.Duration(i)*time.Second); d.Verdict != Continue {
			break
		}
	}
	if d.Verdict != Rollback {
		t.Fatalf("decision %+v", d)
	}
}

func TestFixedAbsolute(t *testing.T) {
	f := NewFixedAbsolute(0.01, 60, 100)
	if d := f.Observe(bin(950, 50, 0, 5), 0); d.Verdict != Continue {
		t.Fatal("decided below the minimum sample")
	}
	if d := f.Observe(bin(950, 50, 0, 0), 0); d.Verdict != Rollback {
		t.Fatalf("5 errors in 100 canary requests: %+v", d)
	}
}

func TestFixedAbsoluteWindowSlides(t *testing.T) {
	f := NewFixedAbsolute(0.01, 3, 10)
	f.Observe(bin(0, 100, 0, 5), 0)
	for range 3 {
		f.Observe(bin(0, 100, 0, 0), 0)
	}
	if d := f.Observe(bin(0, 100, 0, 1), 0); d.Verdict != Continue {
		t.Fatalf("old errors still counted: %+v", d)
	}
}

func TestFixedRelative(t *testing.T) {
	f := NewFixedRelative(2, 60, 100)
	if d := f.Observe(bin(1000, 100, 10, 2), 0); d.Verdict != Continue {
		t.Fatalf("2x exactly: %+v", d)
	}
	if d := f.Observe(bin(1000, 100, 10, 3), 0); d.Verdict != Rollback {
		t.Fatalf("above 2x: %+v", d)
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

func TestPeekingZ(t *testing.T) {
	p := NewPeeking(0.05, 100)
	if math.Abs(p.z-1.6449) > 1e-3 {
		t.Fatalf("z %v", p.z)
	}
	if d := p.Observe(bin(10000, 1000, 50, 5), 0); d.Verdict != Continue {
		t.Fatalf("equal rates: %+v", d)
	}
	if d := p.Observe(bin(10000, 1000, 50, 20), 0); d.Verdict != Rollback {
		t.Fatalf("canary at 5x: %+v", d)
	}
}
