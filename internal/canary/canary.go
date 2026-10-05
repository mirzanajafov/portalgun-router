package canary

import (
	"fmt"
	"math"
	"time"
)

type Verdict int

const (
	Continue Verdict = iota
	Promote
	Rollback
)

func (v Verdict) String() string {
	switch v {
	case Promote:
		return "promote"
	case Rollback:
		return "rollback"
	}
	return "continue"
}

type Bin struct {
	Requests [2]int64
	Errors   [2]int64
	Slow     [2]int64
}

type Decision struct {
	Verdict Verdict
	Reason  string
}

type Policy interface {
	Name() string
	Observe(b Bin, elapsed time.Duration) Decision
	Finish() Decision
}

type SPRT struct {
	upper float64
	lower float64
	rho   float64
	llr   float64
	seen  int64
}

func NewSPRT(alpha, beta, rho float64) *SPRT {
	return &SPRT{upper: math.Log((1 - beta) / alpha), lower: math.Log(beta / (1 - alpha)), rho: rho}
}

func (s *SPRT) Update(stable, canary, stableEvents, canaryEvents int64) {
	if stable <= 0 || canary <= 0 || stableEvents+canaryEvents == 0 {
		return
	}
	share := float64(canary) / float64(stable+canary)
	q1 := s.rho * share / (s.rho*share + 1 - share)
	s.llr += float64(canaryEvents)*math.Log(q1/share) + float64(stableEvents)*math.Log((1-q1)/(1-share))
	s.seen += stableEvents + canaryEvents
}

func (s *SPRT) LLR() float64 { return s.llr }

func (s *SPRT) Bounds() (lower, upper float64) { return s.lower, s.upper }

func (s *SPRT) Verdict() Verdict {
	switch {
	case s.llr >= s.upper:
		return Rollback
	case s.llr <= s.lower:
		return Promote
	}
	return Continue
}

type Sequential struct {
	errors  *SPRT
	slow    *SPRT
	minStep time.Duration
	accept  bool
}

func NewSequential(alpha, beta, rho float64, minStep time.Duration) *Sequential {
	return &Sequential{errors: NewSPRT(alpha/2, beta, rho), slow: NewSPRT(alpha/2, beta, rho), minStep: minStep}
}

func (s *Sequential) Name() string { return "sprt" }

func (s *Sequential) Errors() *SPRT { return s.errors }

func (s *Sequential) Slow() *SPRT { return s.slow }

func (s *Sequential) Observe(b Bin, elapsed time.Duration) Decision {
	s.errors.Update(b.Requests[0], b.Requests[1], b.Errors[0], b.Errors[1])
	s.slow.Update(b.Requests[0], b.Requests[1], b.Slow[0], b.Slow[1])
	switch {
	case s.errors.Verdict() == Rollback:
		return Decision{Rollback, fmt.Sprintf("errors: log-likelihood ratio %.2f crossed %.2f", s.errors.llr, s.errors.upper)}
	case s.slow.Verdict() == Rollback:
		return Decision{Rollback, fmt.Sprintf("latency: log-likelihood ratio %.2f crossed %.2f", s.slow.llr, s.slow.upper)}
	case s.errors.Verdict() == Promote && s.slow.Verdict() == Promote && elapsed >= s.minStep:
		return Decision{Promote, "both tests accepted the canary"}
	}
	return Decision{}
}

func (s *Sequential) Finish() Decision {
	return Decision{Promote, "undecided when the step ended: not enough evidence that the canary is worse"}
}

type window struct {
	size  int
	bins  []Bin
	total Bin
}

func (w *window) push(b Bin) {
	w.bins = append(w.bins, b)
	add(&w.total, b, 1)
	if len(w.bins) > w.size {
		add(&w.total, w.bins[0], -1)
		w.bins = w.bins[1:]
	}
}

func add(dst *Bin, b Bin, sign int64) {
	for i := range 2 {
		dst.Requests[i] += sign * b.Requests[i]
		dst.Errors[i] += sign * b.Errors[i]
		dst.Slow[i] += sign * b.Slow[i]
	}
}

func rate(events, requests int64) float64 {
	if requests == 0 {
		return 0
	}
	return float64(events) / float64(requests)
}

type FixedAbsolute struct {
	w           window
	maxRate     float64
	minRequests int64
}

func NewFixedAbsolute(maxRate float64, span int, minRequests int64) *FixedAbsolute {
	return &FixedAbsolute{w: window{size: span}, maxRate: maxRate, minRequests: minRequests}
}

func (f *FixedAbsolute) Name() string { return "fixed-absolute" }

func (f *FixedAbsolute) Observe(b Bin, _ time.Duration) Decision {
	f.w.push(b)
	t := f.w.total
	if t.Requests[1] >= f.minRequests && rate(t.Errors[1], t.Requests[1]) > f.maxRate {
		return Decision{Rollback, fmt.Sprintf("canary error rate %.2f%% above %.2f%%", 100*rate(t.Errors[1], t.Requests[1]), 100*f.maxRate)}
	}
	return Decision{}
}

func (f *FixedAbsolute) Finish() Decision { return Decision{Verdict: Promote} }

type FixedRelative struct {
	w           window
	factor      float64
	minRequests int64
}

func NewFixedRelative(factor float64, span int, minRequests int64) *FixedRelative {
	return &FixedRelative{w: window{size: span}, factor: factor, minRequests: minRequests}
}

func (f *FixedRelative) Name() string { return "fixed-relative" }

func (f *FixedRelative) Observe(b Bin, _ time.Duration) Decision {
	f.w.push(b)
	t := f.w.total
	stable, canary := rate(t.Errors[0], t.Requests[0]), rate(t.Errors[1], t.Requests[1])
	if t.Requests[1] >= f.minRequests && t.Errors[1] > 0 && canary > f.factor*stable {
		return Decision{Rollback, fmt.Sprintf("canary error rate %.3f%% is more than %.1fx the stable %.3f%%", 100*canary, f.factor, 100*stable)}
	}
	return Decision{}
}

func (f *FixedRelative) Finish() Decision { return Decision{Verdict: Promote} }

type Peeking struct {
	total       Bin
	z           float64
	minRequests int64
}

func NewPeeking(alpha float64, minRequests int64) *Peeking {
	return &Peeking{z: normalQuantile(1 - alpha), minRequests: minRequests}
}

func (p *Peeking) Name() string { return "peeking-z" }

func (p *Peeking) Observe(b Bin, _ time.Duration) Decision {
	add(&p.total, b, 1)
	t := p.total
	n1, n2 := float64(t.Requests[0]), float64(t.Requests[1])
	if t.Requests[1] < p.minRequests || n1 == 0 {
		return Decision{}
	}
	p1, p2 := float64(t.Errors[0])/n1, float64(t.Errors[1])/n2
	pooled := float64(t.Errors[0]+t.Errors[1]) / (n1 + n2)
	se := math.Sqrt(pooled * (1 - pooled) * (1/n1 + 1/n2))
	if se > 0 && (p2-p1)/se > p.z {
		return Decision{Rollback, fmt.Sprintf("z = %.2f above %.2f", (p2-p1)/se, p.z)}
	}
	return Decision{}
}

func (p *Peeking) Finish() Decision { return Decision{Verdict: Promote} }

type FixedHorizon struct {
	total Bin
	alpha float64
	slow  bool
}

func NewFixedHorizon(alpha float64) *FixedHorizon {
	return &FixedHorizon{alpha: alpha}
}

func (f *FixedHorizon) Name() string { return "fixed-horizon" }

func (f *FixedHorizon) Observe(b Bin, _ time.Duration) Decision {
	if f.slow {
		b.Errors = b.Slow
	}
	add(&f.total, b, 1)
	return Decision{}
}

type Hybrid struct {
	seq  *Sequential
	errs *FixedHorizon
	slow *FixedHorizon
}

func NewHybrid(alpha, beta, rho float64, minStep time.Duration) *Hybrid {
	return &Hybrid{
		seq:  NewSequential(alpha/2, beta, rho, minStep),
		errs: NewFixedHorizon(alpha / 4),
		slow: &FixedHorizon{alpha: alpha / 4, slow: true},
	}
}

func (h *Hybrid) Name() string { return "sprt+terminal" }

func (h *Hybrid) Sequential() *Sequential { return h.seq }

func (h *Hybrid) Observe(b Bin, elapsed time.Duration) Decision {
	h.errs.Observe(b, elapsed)
	h.slow.Observe(b, elapsed)
	return h.seq.Observe(b, elapsed)
}

func (h *Hybrid) Finish() Decision {
	if d := h.errs.Finish(); d.Verdict == Rollback {
		return Decision{Rollback, "errors, terminal test: " + d.Reason}
	}
	if d := h.slow.Finish(); d.Verdict == Rollback {
		return Decision{Rollback, "latency, terminal test: " + d.Reason}
	}
	return Decision{Promote, "undecided by the sequential test and not worse at the end of the step"}
}

func (f *FixedHorizon) Finish() Decision {
	t := f.total
	if t.Requests[0] == 0 || t.Requests[1] == 0 {
		return Decision{Verdict: Promote}
	}
	share := float64(t.Requests[1]) / float64(t.Requests[0]+t.Requests[1])
	events := t.Errors[0] + t.Errors[1]
	if p := BinomialTail(t.Errors[1], events, share); p < f.alpha {
		return Decision{Rollback, fmt.Sprintf("%d of %d events on a canary with %.1f%% of the traffic, p = %.2g", t.Errors[1], events, 100*share, p)}
	}
	return Decision{Verdict: Promote}
}

func BinomialTail(k, n int64, p float64) float64 {
	if k <= 0 {
		return 1
	}
	if k > n {
		return 0
	}
	lp, lq := math.Log(p), math.Log1p(-p)
	ln, _ := math.Lgamma(float64(n + 1))
	var sum float64
	for x := k; x <= n; x++ {
		lx, _ := math.Lgamma(float64(x + 1))
		lnx, _ := math.Lgamma(float64(n - x + 1))
		term := math.Exp(ln - lx - lnx + float64(x)*lp + float64(n-x)*lq)
		sum += term
		if term < sum*1e-15 && float64(x) > float64(n)*p {
			break
		}
	}
	return min(1, sum)
}

func normalQuantile(p float64) float64 {
	return math.Sqrt2 * math.Erfinv(2*p-1)
}
