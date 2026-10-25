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

func (b Bin) Plus(o Bin) Bin { return b.add(o, 1) }

func (b Bin) Minus(o Bin) Bin { return b.add(o, -1) }

func (b Bin) add(o Bin, sign int64) Bin {
	for i := range 2 {
		b.Requests[i] += sign * o.Requests[i]
		b.Errors[i] += sign * o.Errors[i]
		b.Slow[i] += sign * o.Slow[i]
	}
	return b
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
	upper    float64
	lower    float64
	rho      float64
	llr      float64
	seen     int64
	accepted bool
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
	if s.llr <= s.lower {
		s.accepted = true
	}
}

func (s *SPRT) LLR() float64 { return s.llr }

func (s *SPRT) Bounds() (lower, upper float64) { return s.lower, s.upper }

func (s *SPRT) Verdict() Verdict {
	switch {
	case s.llr >= s.upper:
		return Rollback
	case s.accepted:
		return Promote
	}
	return Continue
}

type Sequential struct {
	errors      *SPRT
	slow        *SPRT
	minStep     time.Duration
	canary      int64
	errorEvents int64
}

func NewSequential(alpha, beta, rho float64, minStep time.Duration) *Sequential {
	return &Sequential{errors: NewSPRT(alpha/2, beta, rho), slow: NewSPRT(alpha/2, beta, rho), minStep: minStep}
}

func (s *Sequential) Name() string { return "sprt" }

func (s *Sequential) Errors() *SPRT { return s.errors }

func (s *Sequential) Slow() *SPRT { return s.slow }

const SilentRequests = 300

func (s *Sequential) Observe(b Bin, elapsed time.Duration) Decision {
	s.errors.Update(b.Requests[0], b.Requests[1], b.Errors[0], b.Errors[1])
	s.slow.Update(b.Requests[0], b.Requests[1], b.Slow[0], b.Slow[1])
	s.canary += b.Requests[1]
	s.errorEvents += b.Errors[0] + b.Errors[1]
	silent := s.errorEvents == 0 && s.canary >= SilentRequests
	switch {
	case s.errors.Verdict() == Rollback:
		return Decision{Rollback, fmt.Sprintf("errors: log-likelihood ratio %.2f crossed %.2f", s.errors.llr, s.errors.upper)}
	case s.slow.Verdict() == Rollback:
		return Decision{Rollback, fmt.Sprintf("latency: log-likelihood ratio %.2f crossed %.2f", s.slow.llr, s.slow.upper)}
	case s.slow.Verdict() == Promote && elapsed >= s.minStep && s.errors.Verdict() == Promote:
		return Decision{Promote, "both tests accepted the canary"}
	case s.slow.Verdict() == Promote && elapsed >= s.minStep && silent:
		return Decision{Promote, fmt.Sprintf("latency accepted, and %d canary requests without a single error on either version", s.canary)}
	}
	return Decision{}
}

func (s *Sequential) Finish() Decision {
	return Decision{Promote, "undecided when the step ended: not enough evidence that the canary is worse"}
}

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
	f.total = f.total.Plus(b)
	return Decision{}
}

type Hybrid struct {
	seq  *Sequential
	errs *FixedHorizon
	slow *FixedHorizon
}

const (
	DefaultAlpha   = 0.01
	DefaultBeta    = 0.05
	DefaultRho     = 2
	DefaultMinStep = 30 * time.Second
	DefaultMaxStep = 5 * time.Minute
)

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
