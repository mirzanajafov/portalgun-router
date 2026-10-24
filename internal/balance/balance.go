package balance

import (
	"math/rand/v2"
	"sync/atomic"

	"github.com/mirzanajafov/portalgun-router/internal/config"
)

type Target interface {
	Load() *Load
}

type Filter[T Target] func(item T, now int64) bool

type Balancer[T Target] interface {
	Pick(items []T, now int64, ok Filter[T]) (T, bool)
}

func New[T Target](kind config.Balancer) Balancer[T] {
	switch kind {
	case config.BalancerRoundRobin:
		return &RoundRobin[T]{}
	case config.BalancerLeastOutstanding:
		return LeastOutstanding[T]{}
	}
	return P2C[T]{}
}

func usable[T Target](it T, now int64, ok Filter[T]) bool {
	return it.Load().Available() && (ok == nil || ok(it, now))
}

type P2C[T Target] struct{}

func (P2C[T]) Pick(items []T, now int64, ok Filter[T]) (T, bool) {
	var zero T
	var buf [16]int
	idx := buf[:0]
	for i, it := range items {
		if usable(it, now, ok) {
			idx = append(idx, i)
		}
	}
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

func (r *RoundRobin[T]) Pick(items []T, now int64, ok Filter[T]) (T, bool) {
	var zero T
	n := uint64(len(items))
	if n == 0 {
		return zero, false
	}
	start := r.next.Add(1)
	for i := range n {
		it := items[(start+i)%n]
		if usable(it, now, ok) {
			return it, true
		}
	}
	return zero, false
}

type LeastOutstanding[T Target] struct{}

func (LeastOutstanding[T]) Pick(items []T, now int64, ok Filter[T]) (T, bool) {
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
		if !usable(it, now, ok) {
			continue
		}
		if v := it.Load().Inflight(); !found || v < bestInflight {
			best, bestInflight, found = it, v, true
		}
	}
	return best, found
}
