package sim

import (
	"fmt"
	"math"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/canary"
)

type window struct {
	size  int
	bins  []canary.Bin
	total canary.Bin
}

func (w *window) push(b canary.Bin) {
	w.bins = append(w.bins, b)
	w.total = w.total.Plus(b)
	if len(w.bins) > w.size {
		w.total = w.total.Minus(w.bins[0])
		w.bins = w.bins[1:]
	}
}

func rate(events, requests int64) float64 {
	if requests == 0 {
		return 0
	}
	return float64(events) / float64(requests)
}

type fixedAbsolute struct {
	w           window
	maxRate     float64
	minRequests int64
}

func newFixedAbsolute(maxRate float64, span int, minRequests int64) *fixedAbsolute {
	return &fixedAbsolute{w: window{size: span}, maxRate: maxRate, minRequests: minRequests}
}

func (f *fixedAbsolute) Name() string { return "fixed-absolute" }

func (f *fixedAbsolute) Observe(b canary.Bin, _ time.Duration) canary.Decision {
	f.w.push(b)
	t := f.w.total
	if t.Requests[1] >= f.minRequests && rate(t.Errors[1], t.Requests[1]) > f.maxRate {
		return canary.Decision{Verdict: canary.Rollback, Reason: fmt.Sprintf("canary error rate %.2f%% above %.2f%%", 100*rate(t.Errors[1], t.Requests[1]), 100*f.maxRate)}
	}
	return canary.Decision{}
}

func (f *fixedAbsolute) Finish() canary.Decision { return canary.Decision{Verdict: canary.Promote} }

type fixedRelative struct {
	w           window
	factor      float64
	minRequests int64
}

func newFixedRelative(factor float64, span int, minRequests int64) *fixedRelative {
	return &fixedRelative{w: window{size: span}, factor: factor, minRequests: minRequests}
}

func (f *fixedRelative) Name() string { return "fixed-relative" }

func (f *fixedRelative) Observe(b canary.Bin, _ time.Duration) canary.Decision {
	f.w.push(b)
	t := f.w.total
	stable, canaryRate := rate(t.Errors[0], t.Requests[0]), rate(t.Errors[1], t.Requests[1])
	if t.Requests[1] >= f.minRequests && t.Errors[1] > 0 && canaryRate > f.factor*stable {
		return canary.Decision{Verdict: canary.Rollback, Reason: fmt.Sprintf("canary error rate %.3f%% is more than %.1fx the stable %.3f%%", 100*canaryRate, f.factor, 100*stable)}
	}
	return canary.Decision{}
}

func (f *fixedRelative) Finish() canary.Decision { return canary.Decision{Verdict: canary.Promote} }

type peeking struct {
	total       canary.Bin
	z           float64
	minRequests int64
}

func newPeeking(alpha float64, minRequests int64) *peeking {
	return &peeking{z: normalQuantile(1 - alpha), minRequests: minRequests}
}

func (p *peeking) Name() string { return "peeking-z" }

func (p *peeking) Observe(b canary.Bin, _ time.Duration) canary.Decision {
	p.total = p.total.Plus(b)
	t := p.total
	n1, n2 := float64(t.Requests[0]), float64(t.Requests[1])
	if t.Requests[1] < p.minRequests || n1 == 0 {
		return canary.Decision{}
	}
	p1, p2 := float64(t.Errors[0])/n1, float64(t.Errors[1])/n2
	pooled := float64(t.Errors[0]+t.Errors[1]) / (n1 + n2)
	se := math.Sqrt(pooled * (1 - pooled) * (1/n1 + 1/n2))
	if se > 0 && (p2-p1)/se > p.z {
		return canary.Decision{Verdict: canary.Rollback, Reason: fmt.Sprintf("z = %.2f above %.2f", (p2-p1)/se, p.z)}
	}
	return canary.Decision{}
}

func (p *peeking) Finish() canary.Decision { return canary.Decision{Verdict: canary.Promote} }

func normalQuantile(p float64) float64 {
	return math.Sqrt2 * math.Erfinv(2*p-1)
}
