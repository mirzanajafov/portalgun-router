package stats

import (
	"iter"
	"time"
)

type Outcomes struct {
	ring     []bool
	next     int
	n        int
	failures int
}

func NewOutcomes(size int) Outcomes { return Outcomes{ring: make([]bool, size)} }

func (o *Outcomes) Add(failed bool) {
	if o.n == len(o.ring) {
		if o.ring[o.next] {
			o.failures--
		}
	} else {
		o.n++
	}
	o.ring[o.next] = failed
	o.next = (o.next + 1) % len(o.ring)
	if failed {
		o.failures++
	}
}

func (o *Outcomes) Len() int { return o.n }

func (o *Outcomes) Full() bool { return o.n == len(o.ring) }

func (o *Outcomes) Failures() int { return o.failures }

func (o *Outcomes) Reset() {
	clear(o.ring)
	o.next, o.n, o.failures = 0, 0, 0
}

type Slots[T any] struct {
	width int64
	slots []slot[T]
}

type slot[T any] struct {
	at    int64
	value T
}

func NewSlots[T any](n int, width time.Duration) Slots[T] {
	return Slots[T]{width: int64(width), slots: make([]slot[T], n)}
}

func (s *Slots[T]) At(now int64) *T {
	at := now / s.width
	sl := &s.slots[at%int64(len(s.slots))]
	if sl.at != at {
		*sl = slot[T]{at: at}
	}
	return &sl.value
}

func (s *Slots[T]) Reset() { clear(s.slots) }

func (s *Slots[T]) Recent(now int64) iter.Seq[T] {
	at := now / s.width
	return func(yield func(T) bool) {
		for _, sl := range s.slots {
			if at-sl.at < int64(len(s.slots)) && sl.at <= at && !yield(sl.value) {
				return
			}
		}
	}
}
