package sim

import (
	"math"
	"testing"

	"github.com/mirzanajafov/portalgun-router/internal/canary"
)

func bin(n1, n2, e1, e2 int64) canary.Bin {
	return canary.Bin{Stable: canary.Counts{Requests: n1, Errors: e1}, Canary: canary.Counts{Requests: n2, Errors: e2}}
}

func TestFixedAbsolute(t *testing.T) {
	f := newFixedAbsolute(0.01, 60, 100)
	if d := f.Observe(bin(950, 50, 0, 5), 0); d.Verdict != canary.Continue {
		t.Fatal("decided below the minimum sample")
	}
	if d := f.Observe(bin(950, 50, 0, 0), 0); d.Verdict != canary.Rollback {
		t.Fatalf("5 errors in 100 canary requests: %+v", d)
	}
}

func TestFixedAbsoluteWindowSlides(t *testing.T) {
	f := newFixedAbsolute(0.01, 3, 10)
	f.Observe(bin(0, 100, 0, 5), 0)
	for range 3 {
		f.Observe(bin(0, 100, 0, 0), 0)
	}
	if d := f.Observe(bin(0, 100, 0, 1), 0); d.Verdict != canary.Continue {
		t.Fatalf("old errors still counted: %+v", d)
	}
}

func TestFixedRelative(t *testing.T) {
	f := newFixedRelative(2, 60, 100)
	if d := f.Observe(bin(1000, 100, 10, 2), 0); d.Verdict != canary.Continue {
		t.Fatalf("2x exactly: %+v", d)
	}
	if d := f.Observe(bin(1000, 100, 10, 3), 0); d.Verdict != canary.Rollback {
		t.Fatalf("above 2x: %+v", d)
	}
}

func TestPeekingZ(t *testing.T) {
	p := newPeeking(0.05, 100)
	if math.Abs(p.z-1.6449) > 1e-3 {
		t.Fatalf("z %v", p.z)
	}
	if d := p.Observe(bin(10000, 1000, 50, 5), 0); d.Verdict != canary.Continue {
		t.Fatalf("equal rates: %+v", d)
	}
	if d := p.Observe(bin(10000, 1000, 50, 20), 0); d.Verdict != canary.Rollback {
		t.Fatalf("canary at 5x: %+v", d)
	}
}
