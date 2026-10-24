package failover

import (
	"cmp"
	"math"
	"slices"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
	"github.com/mirzanajafov/portalgun-router/internal/health"
	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
)

type Window struct {
	Mode      string `json:"mode"`
	Target    string `json:"target"`
	InjectAt  int64  `json:"injectAt"`
	InjectAck int64  `json:"injectAck"`
	ClearAt   int64  `json:"clearAt"`
	ClearAck  int64  `json:"clearAck"`
}

type Options struct {
	Settle       time.Duration
	Bin          time.Duration
	MoveWindow   int
	MovedShare   float64
	RecoverShare float64
	ErrorsAfter  time.Duration
	KeepBins     bool
}

func DefaultOptions() Options {
	return Options{
		Settle:       5 * time.Second,
		Bin:          100 * time.Millisecond,
		MoveWindow:   10,
		MovedShare:   0.01,
		RecoverShare: 0.9,
		ErrorsAfter:  10 * time.Second,
	}
}

type Bin struct {
	AtMs     float64 `json:"atMs"`
	Requests int     `json:"requests"`
	Target   int     `json:"target"`
	Failed   int     `json:"failed"`
	Retried  int     `json:"retried"`
}

type Result struct {
	Requests            int      `json:"requests"`
	BaselineShare       float64  `json:"baselineShare"`
	BaselineFailed      int      `json:"baselineFailed"`
	DetectMs            *float64 `json:"detectMs"`
	DetectedBy          string   `json:"detectedBy,omitempty"`
	MoveMs              *float64 `json:"moveMs"`
	Lost                int      `json:"lost"`
	LostAfter           int      `json:"lostAfter"`
	P99BeforeMs         float64  `json:"p99BeforeMs"`
	P99FaultMs          float64  `json:"p99FaultMs"`
	P99AfterMs          float64  `json:"p99AfterMs"`
	AttemptsBefore      float64  `json:"attemptsBefore"`
	AttemptsFault       float64  `json:"attemptsFault"`
	RecoverMs           *float64 `json:"recoverMs"`
	TargetEjections     int      `json:"targetEjections"`
	FalseEjections      int      `json:"falseEjections"`
	FalseUnhealthy      int      `json:"falseUnhealthy"`
	InjectUncertaintyMs float64  `json:"injectUncertaintyMs"`
	Bins                []Bin    `json:"bins,omitempty"`
}

func Failed(r loadgen.Record) bool {
	return r.Err != loadgen.ErrNone || r.Status == 0 || r.Status >= 500
}

type request struct {
	at       int64
	latency  float64
	target   bool
	failed   bool
	attempts int
}

func requests(lg *loadgen.Log, target string) []request {
	idx := uint8(0)
	for i, d := range lg.Dimensions {
		if d == target {
			idx = uint8(i + 1)
		}
	}
	out := make([]request, 0, len(lg.Records))
	for _, r := range lg.Records {
		out = append(out, request{
			at:       lg.StartUnixNano + r.Scheduled,
			latency:  float64(r.Latency()) / 1e6,
			target:   idx != 0 && r.Dimension == idx,
			failed:   Failed(r),
			attempts: max(int(r.Attempts), 1),
		})
	}
	slices.SortFunc(out, func(a, b request) int { return cmp.Compare(a.at, b.at) })
	return out
}

func between(rs []request, from, to int64) []request {
	lo, _ := slices.BinarySearchFunc(rs, from, func(r request, t int64) int { return cmp.Compare(r.at, t) })
	hi, _ := slices.BinarySearchFunc(rs, to, func(r request, t int64) int { return cmp.Compare(r.at, t) })
	return rs[lo:hi]
}

func bins(rs []request, origin, end int64, width time.Duration) []Bin {
	w := int64(width)
	n := int((end - origin + w - 1) / w)
	if n <= 0 {
		return nil
	}
	out := make([]Bin, n)
	for i := range out {
		out[i].AtMs = float64(int64(i)*w) / 1e6
	}
	for _, r := range between(rs, origin, end) {
		b := &out[(r.at-origin)/w]
		b.Requests++
		if r.target {
			b.Target++
		}
		if r.failed {
			b.Failed++
		}
		if r.attempts > 1 {
			b.Retried++
		}
	}
	return out
}

func share(bs []Bin, count func(Bin) int) float64 {
	var n, k int
	for _, b := range bs {
		n += b.Requests
		k += count(b)
	}
	if n == 0 {
		return 0
	}
	return float64(k) / float64(n)
}

func p99(rs []request) float64 {
	if len(rs) == 0 {
		return 0
	}
	ls := make([]float64, len(rs))
	for i, r := range rs {
		ls[i] = r.latency
	}
	slices.Sort(ls)
	return ls[int(math.Ceil(0.99*float64(len(ls))))-1]
}

func meanAttempts(rs []request) float64 {
	if len(rs) == 0 {
		return 0
	}
	var n int
	for _, r := range rs {
		n += r.attempts
	}
	return float64(n) / float64(len(rs))
}

func countFailed(rs []request) int {
	var n int
	for _, r := range rs {
		if r.failed {
			n++
		}
	}
	return n
}

func ms(d int64) *float64 {
	v := math.Round(float64(d)/1e3) / 1e3
	return &v
}

func Analyze(lg *loadgen.Log, w Window, events []gatewayapi.DimensionEvent, opt Options) Result {
	rs := requests(lg, w.Target)
	end := lg.StartUnixNano
	if len(rs) > 0 {
		end = rs[len(rs)-1].at + 1
	}
	start := lg.StartUnixNano + int64(opt.Settle)
	before := between(rs, start, w.InjectAt)
	fault := between(rs, w.InjectAt, w.ClearAt)
	after := between(rs, w.ClearAt, end)

	res := Result{
		Requests:            len(rs),
		BaselineFailed:      countFailed(before),
		Lost:                countFailed(fault),
		LostAfter:           countFailed(between(rs, w.ClearAt, w.ClearAt+int64(opt.ErrorsAfter))),
		P99BeforeMs:         p99(before),
		P99FaultMs:          p99(fault),
		P99AfterMs:          p99(after),
		AttemptsBefore:      meanAttempts(before),
		AttemptsFault:       meanAttempts(fault),
		InjectUncertaintyMs: float64(w.InjectAck-w.InjectAt) / 1e6,
	}
	baseline := bins(rs, start, w.InjectAt, opt.Bin)
	res.BaselineShare = share(baseline, func(b Bin) int { return b.Target })

	ejected := gatewayapi.OutlierEvent(health.Ejected)
	for _, e := range events {
		if e.UnixNano < start || (e.State != ejected && e.State != gatewayapi.EventUnhealthy) {
			continue
		}
		hit := e.Dimension == w.Target && e.UnixNano >= w.InjectAt
		if hit && res.DetectMs == nil {
			res.DetectMs = ms(e.UnixNano - w.InjectAt)
			res.DetectedBy = string(e.State)
			if e.Reason != "" {
				res.DetectedBy += ": " + e.Reason
			}
		}
		switch {
		case hit && e.State == ejected:
			res.TargetEjections++
		case hit:
		case e.State == ejected:
			res.FalseEjections++
		default:
			res.FalseUnhealthy++
		}
	}

	during := bins(rs, w.InjectAt, w.ClearAt, opt.Bin)
	touched := func(b Bin) int { return b.Target + b.Failed + b.Retried }
	if k, ok := settledFrom(during, opt.MoveWindow, func(win []Bin) bool { return share(win, touched) < opt.MovedShare }); ok {
		res.MoveMs = ms(int64(k) * int64(opt.Bin))
	}
	recovery := bins(rs, w.ClearAt, end, opt.Bin)
	want := opt.RecoverShare * res.BaselineShare
	for k := 0; k+opt.MoveWindow <= len(recovery); k++ {
		if res.BaselineShare > 0 && share(recovery[k:k+opt.MoveWindow], func(b Bin) int { return b.Target }) >= want {
			res.RecoverMs = ms(int64(k) * int64(opt.Bin))
			break
		}
	}
	if opt.KeepBins {
		width := int64(opt.Bin)
		lead := (w.InjectAt - lg.StartUnixNano + width - 1) / width
		res.Bins = bins(rs, w.InjectAt-lead*width, end, opt.Bin)
		for i := range res.Bins {
			res.Bins[i].AtMs -= float64(lead*width) / 1e6
		}
	}
	return res
}

func settledFrom(bs []Bin, window int, ok func([]Bin) bool) (int, bool) {
	last := len(bs) - window
	if last < 0 {
		return 0, false
	}
	k := -1
	for j := last; j >= 0; j-- {
		if !ok(bs[j : j+window]) {
			break
		}
		k = j
	}
	if k < 0 {
		return 0, false
	}
	return k, true
}
