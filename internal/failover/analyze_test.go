package failover

import (
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
)

const (
	t0   = int64(1_000_000_000_000)
	step = int64(500 * time.Microsecond)
)

func sec(s float64) int64 { return t0 + int64(s*1e9) }

func synthetic(seconds float64, route func(i int, at int64) (dim uint8, status uint16)) *loadgen.Log {
	lg := &loadgen.Log{StartUnixNano: t0, Dimensions: []string{"a", "b", "c"}}
	for i := 0; int64(i)*step < int64(seconds*1e9); i++ {
		at := int64(i) * step
		d, status := route(i, t0+at)
		lg.Records = append(lg.Records, loadgen.Record{Scheduled: at, Started: at, Ended: at + int64(2*time.Millisecond), Status: status, Dimension: d, Attempts: 1})
	}
	return lg
}

func TestAnalyzeACrash(t *testing.T) {
	lg := synthetic(40, func(i int, at int64) (uint8, uint16) {
		d := uint8(i%3) + 1
		switch {
		case d == 1 && at >= sec(20) && at < sec(20.35):
			return 0, 502
		case d == 1 && at >= sec(20.35) && at < sec(32):
			return uint8(i%2) + 2, 200
		}
		return d, 200
	})
	w := Window{Mode: "crash", Target: "a", InjectAt: sec(20), InjectAck: sec(20.001), ClearAt: sec(30), ClearAck: sec(30.001)}
	events := []gatewayapi.DimensionEvent{
		{UnixNano: sec(1), Dimension: "a", State: "ejected", Reason: "before the settle window"},
		{UnixNano: sec(12), Dimension: "b", State: "ejected", Reason: "noise"},
		{UnixNano: sec(20.3), Dimension: "a", State: "ejected", Reason: "connection refused"},
		{UnixNano: sec(21.1), Dimension: "a", State: "unhealthy", Reason: "probes"},
		{UnixNano: sec(26), Dimension: "a", State: "ejected", Reason: "failed a half-open trial"},
	}
	r := Analyze(lg, w, events, DefaultOptions())

	if r.DetectMs == nil || *r.DetectMs != 300 || r.DetectedBy != "ejected: connection refused" {
		t.Fatalf("detect %v by %q", r.DetectMs, r.DetectedBy)
	}
	if r.MoveMs == nil || *r.MoveMs != 400 {
		t.Fatalf("move %v, want 400 ms", r.MoveMs)
	}
	if r.Lost < 233 || r.Lost > 234 || r.LostAfter != 0 || r.BaselineFailed != 0 {
		t.Fatalf("lost %d, after %d, baseline %d", r.Lost, r.LostAfter, r.BaselineFailed)
	}
	if r.TargetEjections != 2 || r.FalseEjections != 1 || r.FalseUnhealthy != 0 {
		t.Fatalf("ejections %d, false %d/%d", r.TargetEjections, r.FalseEjections, r.FalseUnhealthy)
	}
	if r.BaselineShare < 0.33 || r.BaselineShare > 0.34 {
		t.Fatalf("baseline share %v", r.BaselineShare)
	}
	if r.RecoverMs == nil || *r.RecoverMs < 1000 || *r.RecoverMs > 2000 {
		t.Fatalf("recover %v, want within the second before a returned at +2 s", r.RecoverMs)
	}
	if r.InjectUncertaintyMs != 1 {
		t.Fatalf("uncertainty %v", r.InjectUncertaintyMs)
	}
}

func TestAnalyzeTrafficThatNeverSettles(t *testing.T) {
	lg := synthetic(40, func(i int, at int64) (uint8, uint16) {
		d := uint8(i%3) + 1
		flap := (at-sec(20))/int64(2*time.Second)%2 == 0
		if d == 1 && at >= sec(20) && at < sec(30) && flap {
			return 0, 502
		}
		return d, 200
	})
	w := Window{Mode: "flapping", Target: "a", InjectAt: sec(20), ClearAt: sec(30)}
	r := Analyze(lg, w, nil, DefaultOptions())
	if r.MoveMs != nil || r.DetectMs != nil {
		t.Fatalf("move %v detect %v, want neither", r.MoveMs, r.DetectMs)
	}
	if r.RecoverMs == nil || *r.RecoverMs != 0 {
		t.Fatalf("recover %v, want 0: a never left", r.RecoverMs)
	}
}

func TestHalfOpenTrialsDoNotUndoTheMove(t *testing.T) {
	lg := synthetic(40, func(i int, at int64) (uint8, uint16) {
		d := uint8(i%3) + 1
		if d == 1 && at >= sec(20) && at < sec(30) {
			if (at-sec(25)) >= 0 && (at-sec(25)) < 3*step {
				return 1, 503
			}
			return uint8(i%2) + 2, 200
		}
		return d, 200
	})
	w := Window{Mode: "partial", Target: "a", InjectAt: sec(20), ClearAt: sec(30)}
	r := Analyze(lg, w, nil, DefaultOptions())
	if r.MoveMs == nil || *r.MoveMs != 0 {
		t.Fatalf("move %v, want 0: three trial requests are not traffic coming back", r.MoveMs)
	}
	if r.Lost != 1 {
		t.Fatalf("lost %d", r.Lost)
	}
}

func TestLatencyAndAttemptsByWindow(t *testing.T) {
	lg := synthetic(40, func(int, int64) (uint8, uint16) { return 2, 200 })
	for i := range lg.Records {
		at := lg.StartUnixNano + lg.Records[i].Scheduled
		if at >= sec(20) && at < sec(30) {
			lg.Records[i].Ended += int64(500 * time.Millisecond)
			lg.Records[i].Attempts = 2
		}
	}
	r := Analyze(lg, Window{Target: "a", InjectAt: sec(20), ClearAt: sec(30)}, nil, DefaultOptions())
	if r.P99BeforeMs != 2 || r.P99FaultMs != 502 || r.P99AfterMs != 2 {
		t.Fatalf("p99 %v / %v / %v", r.P99BeforeMs, r.P99FaultMs, r.P99AfterMs)
	}
	if r.AttemptsBefore != 1 || r.AttemptsFault != 2 {
		t.Fatalf("attempts %v / %v", r.AttemptsBefore, r.AttemptsFault)
	}
	if r.RecoverMs != nil {
		t.Fatalf("recover %v for a dimension that never had traffic", r.RecoverMs)
	}
}

func TestRetriedRequestsHaveNotMovedYet(t *testing.T) {
	lg := synthetic(40, func(i int, at int64) (uint8, uint16) {
		d := uint8(i%3) + 1
		if d == 1 && at >= sec(20) && at < sec(30) {
			return uint8(i%2) + 2, 200
		}
		return d, 200
	})
	for i := range lg.Records {
		at := lg.StartUnixNano + lg.Records[i].Scheduled
		if i%3 == 0 && at >= sec(20) && at < sec(20.5) {
			lg.Records[i].Attempts = 2
		}
	}
	opt := DefaultOptions()
	opt.KeepBins = true
	r := Analyze(lg, Window{Mode: "blackhole", Target: "a", InjectAt: sec(20) + 30_000_000, ClearAt: sec(30)}, nil, opt)
	if r.MoveMs == nil || *r.MoveMs != 500 || r.Lost != 0 {
		t.Fatalf("move %v lost %d: answers from b and c after a timed-out try on a are not a move", deref(r.MoveMs), r.Lost)
	}
	var zero *Bin
	for i := range r.Bins {
		if r.Bins[i].AtMs == 0 {
			zero = &r.Bins[i]
		}
	}
	if zero == nil || zero.Retried == 0 {
		t.Fatal("the series has no bin starting at the fault")
	}
}
func deref(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}
