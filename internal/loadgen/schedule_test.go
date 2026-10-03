package loadgen

import (
	"math"
	"testing"
)

func TestConstantScheduleIsEvenlySpaced(t *testing.T) {
	s, err := NewSchedule(1000, Constant, 1)
	if err != nil {
		t.Fatal(err)
	}
	for i := range int64(10000) {
		if got := s.Next(); got != i*1_000_000 {
			t.Fatalf("arrival %d at %d, want %d", i, got, i*1_000_000)
		}
	}
}

func TestPoissonScheduleHasTheRateAndShape(t *testing.T) {
	const n = 50000
	s, err := NewSchedule(1000, Poisson, 7)
	if err != nil {
		t.Fatal(err)
	}
	var prev, sum, sumSq float64
	var last int64
	for range n {
		last = s.Next()
		gap := float64(last) - prev
		prev = float64(last)
		sum += gap
		sumSq += gap * gap
	}
	if seconds := float64(last) / 1e9; math.Abs(seconds-50) > 1 {
		t.Fatalf("%d arrivals took %.2fs, want about 50s", n, seconds)
	}
	mean := sum / n
	cv := math.Sqrt(sumSq/n-mean*mean) / mean
	if math.Abs(cv-1) > 0.05 {
		t.Fatalf("coefficient of variation %.3f, want about 1 for exponential gaps", cv)
	}
}

func TestPoissonScheduleIsReproducible(t *testing.T) {
	a, _ := NewSchedule(500, Poisson, 3)
	b, _ := NewSchedule(500, Poisson, 3)
	c, _ := NewSchedule(500, Poisson, 4)
	same, differs := true, false
	for range 1000 {
		x, y, z := a.Next(), b.Next(), c.Next()
		same = same && x == y
		differs = differs || x != z
	}
	if !same || !differs {
		t.Fatalf("same seed equal: %v, other seed differs: %v", same, differs)
	}
}

func TestScheduleRejectsBadInput(t *testing.T) {
	for _, rate := range []float64{0, -1, math.NaN(), math.Inf(1)} {
		if _, err := NewSchedule(rate, Constant, 1); err == nil {
			t.Errorf("rate %v accepted", rate)
		}
	}
	if _, err := NewSchedule(10, "bursty", 1); err == nil {
		t.Error("unknown arrival accepted")
	}
}
