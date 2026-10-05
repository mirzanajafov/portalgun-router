package balance

import (
	"fmt"
	"math/rand/v2"
	"sync/atomic"
)

type Target interface {
	Load() *Load
}

type Balancer[T Target] interface {
	Pick(items []T, now int64) (T, bool)
}

func New[T Target](name string) (Balancer[T], error) {
	switch name {
	case "p2c-ewma":
		return P2C[T]{}, nil
	case "round-robin":
		return &RoundRobin[T]{}, nil
	case "least-outstanding":
		return LeastOutstanding[T]{}, nil
	}
	return nil, fmt.Errorf("unknown balancer %q", name)
}

func available[T Target](items []T, buf []int) []int {
	for i, it := range items {
		if it.Load().Available() {
			buf = append(buf, i)
		}
	}
	return buf
}

type P2C[T Target] struct{}

func (P2C[T]) Pick(items []T, now int64) (T, bool) {
	var zero T
	var buf [16]int
	idx := available(items, buf[:0])
	switch len(idx) {
	case 0:
		return zero, false
	case 1:
		return items[idx[0]], true
	}
	a := rand.IntN(len(idx))
	b := rand.IntN(len(idx) - 1)
	if b >= a {
		b++
	}
	x, y := items[idx[a]], items[idx[b]]
	if y.Load().Cost(now) < x.Load().Cost(now) {
		return y, true
	}
	return x, true
}

type RoundRobin[T Target] struct {
	next atomic.Uint64
}

func (r *RoundRobin[T]) Pick(items []T, _ int64) (T, bool) {
	var zero T
	n := uint64(len(items))
	if n == 0 {
		return zero, false
	}
	start := r.next.Add(1)
	for i := range n {
		it := items[(start+i)%n]
		if it.Load().Available() {
			return it, true
		}
	}
	return zero, false
}

type LeastOutstanding[T Target] struct{}

func (LeastOutstanding[T]) Pick(items []T, _ int64) (T, bool) {
	var zero T
	n := len(items)
	if n == 0 {
		return zero, false
	}
	best, found := zero, false
	bestInflight := int64(0)
	offset := rand.IntN(n)
	for i := range n {
		it := items[(offset+i)%n]
		l := it.Load()
		if !l.Available() {
			continue
		}
		if v := l.Inflight(); !found || v < bestInflight {
			best, bestInflight, found = it, v, true
		}
	}
	return best, found
}
