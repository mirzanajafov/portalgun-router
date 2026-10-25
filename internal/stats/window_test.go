package stats

import (
	"slices"
	"testing"
	"time"
)

func TestOutcomesCountOnlyTheLastN(t *testing.T) {
	o := NewOutcomes(3)
	for _, failed := range []bool{true, true, false} {
		o.Add(failed)
	}
	if !o.Full() || o.Len() != 3 || o.Failures() != 2 {
		t.Fatalf("after three: len %d, failures %d", o.Len(), o.Failures())
	}
	o.Add(false)
	if o.Failures() != 1 {
		t.Fatalf("the oldest failure should have dropped out, failures %d", o.Failures())
	}
	o.Reset()
	if o.Full() || o.Len() != 0 || o.Failures() != 0 {
		t.Fatalf("after a reset: len %d, failures %d", o.Len(), o.Failures())
	}
}

func TestSlotsKeepOnlyTheWindow(t *testing.T) {
	s := NewSlots[int](3, time.Second)
	sec := func(n int64) int64 { return n * int64(time.Second) }
	*s.At(sec(10))++
	*s.At(sec(11)) += 2
	*s.At(sec(12)) += 4
	if got := slices.Collect(s.Recent(sec(12))); sum(got) != 7 {
		t.Fatalf("all three seconds in the window: %v", got)
	}
	if got := slices.Collect(s.Recent(sec(13))); sum(got) != 6 {
		t.Fatalf("the oldest second should have left the window: %v", got)
	}
	*s.At(sec(13)) += 8
	if got := slices.Collect(s.Recent(sec(13))); sum(got) != 14 {
		t.Fatalf("a slot reused for a later second should start from zero: %v", got)
	}
	*s.At(sec(14)) += 16
	if got := slices.Collect(s.Recent(sec(13))); sum(got) != 12 {
		t.Fatalf("a second after the one asked about should not count: %v", got)
	}
	s.Reset()
	if got := slices.Collect(s.Recent(sec(13))); sum(got) != 0 {
		t.Fatalf("after a reset: %v", got)
	}
}

func sum(values []int) int {
	total := 0
	for _, v := range values {
		total += v
	}
	return total
}
