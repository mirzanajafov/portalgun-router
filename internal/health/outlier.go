package health

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type State int32

const (
	Closed State = iota
	Ejected
	HalfOpen
	Disabled
)

var states = [...]string{Closed: "closed", Ejected: "ejected", HalfOpen: "half-open", Disabled: "disabled"}

func (s State) String() string {
	if s >= 0 && int(s) < len(states) {
		return states[s]
	}
	return "unknown"
}

func (s State) MarshalText() ([]byte, error) {
	if s < 0 || int(s) >= len(states) {
		return nil, fmt.Errorf("outlier state %d has no name", s)
	}
	return []byte(states[s]), nil
}

func (s *State) UnmarshalText(b []byte) error {
	i := slices.Index(states[:], string(b))
	if i < 0 {
		return fmt.Errorf("unknown outlier state %q", b)
	}
	*s = State(i)
	return nil
}

const ejectionMemory = int64(5 * time.Minute)

type OutlierConfig struct {
	Consecutive    int
	Window         int
	MinRequests    int
	FailureRatio   float64
	BaseEjection   time.Duration
	MaxEjection    time.Duration
	HalfOpenTrials int
	EjectOnRefused bool
}

type Transition struct {
	From, To State
	At       int64
	Until    int64
	Reason   string
}

type Outlier struct {
	cfg      OutlierConfig
	onChange func(Transition)

	state   atomic.Int32
	until   atomic.Int64
	probing atomic.Int32

	mu          sync.Mutex
	ring        []bool
	idx         int
	n           int
	failures    int
	consecutive int
	successes   int
	ejections   []int64
}

func NewOutlier(cfg OutlierConfig, onChange func(Transition)) *Outlier {
	return &Outlier{cfg: cfg, onChange: onChange, ring: make([]bool, cfg.Window)}
}

func (o *Outlier) State() State { return State(o.state.Load()) }

func (o *Outlier) StateAt(now int64) State {
	s := State(o.state.Load())
	if s == Ejected && now >= o.until.Load() {
		return HalfOpen
	}
	return s
}

func (o *Outlier) Until() int64 { return o.until.Load() }

func (o *Outlier) Eligible(now int64) bool {
	switch State(o.state.Load()) {
	case Closed:
		return true
	case Ejected:
		if now < o.until.Load() {
			return false
		}
		o.enterHalfOpen(now)
	}
	return o.probing.Load() < int32(o.cfg.HalfOpenTrials)
}

func (o *Outlier) Begin() bool {
	if State(o.state.Load()) != HalfOpen {
		return false
	}
	o.probing.Add(1)
	return true
}

func (o *Outlier) Record(now int64, failed, refused, trial bool) {
	var t *Transition
	o.mu.Lock()
	if trial {
		o.probing.Add(-1)
	}
	switch State(o.state.Load()) {
	case HalfOpen:
		switch {
		case failed:
			t = o.ejectLocked(now, "failed a half-open trial")
		case trial:
			o.successes++
			if o.successes >= o.cfg.HalfOpenTrials {
				t = o.closeLocked(now)
			}
		}
	case Closed:
		t = o.observeLocked(now, failed, refused)
	}
	o.mu.Unlock()
	if t != nil && o.onChange != nil {
		o.onChange(*t)
	}
}

func (o *Outlier) observeLocked(now int64, failed, refused bool) *Transition {
	if o.n == len(o.ring) {
		if o.ring[o.idx] {
			o.failures--
		}
	} else {
		o.n++
	}
	o.ring[o.idx] = failed
	o.idx = (o.idx + 1) % len(o.ring)
	if failed {
		o.failures++
		o.consecutive++
	} else {
		o.consecutive = 0
	}
	switch {
	case refused && o.cfg.EjectOnRefused:
		return o.ejectLocked(now, "connection refused")
	case o.consecutive >= o.cfg.Consecutive:
		return o.ejectLocked(now, "consecutive failures")
	case o.n >= o.cfg.MinRequests && float64(o.failures) >= o.cfg.FailureRatio*float64(o.n):
		return o.ejectLocked(now, "failure ratio")
	}
	return nil
}

func (o *Outlier) ejectLocked(now int64, reason string) *Transition {
	recent := o.ejections[:0]
	for _, at := range o.ejections {
		if now-at < ejectionMemory {
			recent = append(recent, at)
		}
	}
	o.ejections = append(recent, now)
	d := o.cfg.BaseEjection << (len(o.ejections) - 1)
	if d > o.cfg.MaxEjection || d <= 0 {
		d = o.cfg.MaxEjection
	}
	from := State(o.state.Load())
	until := now + int64(d)
	o.until.Store(until)
	o.state.Store(int32(Ejected))
	o.resetLocked()
	return &Transition{From: from, To: Ejected, At: now, Until: until, Reason: reason}
}

func (o *Outlier) closeLocked(now int64) *Transition {
	o.state.Store(int32(Closed))
	o.resetLocked()
	return &Transition{From: HalfOpen, To: Closed, At: now, Reason: "half-open trials passed"}
}

func (o *Outlier) resetLocked() {
	clear(o.ring)
	o.idx, o.n, o.failures, o.consecutive, o.successes = 0, 0, 0, 0, 0
}

func (o *Outlier) enterHalfOpen(now int64) {
	o.mu.Lock()
	var t *Transition
	if State(o.state.Load()) == Ejected && now >= o.until.Load() {
		o.probing.Store(0)
		o.successes = 0
		o.state.Store(int32(HalfOpen))
		t = &Transition{From: Ejected, To: HalfOpen, At: now, Reason: "ejection over"}
	}
	o.mu.Unlock()
	if t != nil && o.onChange != nil {
		o.onChange(*t)
	}
}
