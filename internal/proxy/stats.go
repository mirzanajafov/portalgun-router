package proxy

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
	"github.com/mirzanajafov/portalgun-router/internal/stats"
)

const (
	stableSlot   = 0
	canarySlot   = 1
	brakeWindow  = 20
	brakeFailing = 10
	slowWindow   = 60
	slowMinCount = 100
)

type versionCounters struct {
	attempts atomic.Int64
	errors   atomic.Int64
	slow     atomic.Int64
}

type routeStats struct {
	requests  atomic.Int64
	limited   atomic.Int64
	failed    atomic.Int64
	versions  [2]versionCounters
	stable    stats.Histogram
	threshold atomic.Int64
	rolling   *stats.Rolling

	brakeOn  atomic.Bool
	brakeMu  sync.Mutex
	rollout  string
	ring     [brakeWindow]bool
	ringIdx  int
	ringLen  int
	ringFail int
}

func newRouteStats() *routeStats {
	return &routeStats{rolling: stats.NewRolling(slowWindow)}
}

func (rs *routeStats) observe(slot int, failed bool, latency time.Duration) {
	v := &rs.versions[slot]
	v.attempts.Add(1)
	if failed {
		v.errors.Add(1)
	}
	if t := rs.threshold.Load(); t > 0 && int64(latency) > t {
		v.slow.Add(1)
	}
	if slot == stableSlot && !failed {
		rs.stable.Observe(latency)
	}
	if slot == canarySlot {
		rs.brake(failed)
	}
}

func (rs *routeStats) brake(failed bool) {
	rs.brakeMu.Lock()
	defer rs.brakeMu.Unlock()
	if rs.ringLen == brakeWindow {
		if rs.ring[rs.ringIdx] {
			rs.ringFail--
		}
	} else {
		rs.ringLen++
	}
	rs.ring[rs.ringIdx] = failed
	rs.ringIdx = (rs.ringIdx + 1) % brakeWindow
	if failed {
		rs.ringFail++
	}
	if rs.ringLen == brakeWindow && rs.ringFail >= brakeFailing {
		rs.brakeOn.Store(true)
	}
}

func (rs *routeStats) resetBrake(rollout string) {
	rs.brakeMu.Lock()
	defer rs.brakeMu.Unlock()
	if rs.rollout == rollout {
		return
	}
	rs.rollout = rollout
	rs.ring = [brakeWindow]bool{}
	rs.ringIdx, rs.ringLen, rs.ringFail = 0, 0, 0
	rs.brakeOn.Store(false)
}

func (rs *routeStats) tick() gatewayapi.RouteSnapshot {
	snap := gatewayapi.RouteSnapshot{
		Requests: rs.requests.Swap(0),
		Limited:  rs.limited.Swap(0),
		Failed:   rs.failed.Swap(0),
		Brake:    rs.brakeOn.Load(),
	}
	for i := range rs.versions {
		v := &rs.versions[i]
		snap.Versions[i] = gatewayapi.VersionSnapshot{Attempts: v.attempts.Swap(0), Errors: v.errors.Swap(0), Slow: v.slow.Swap(0)}
	}
	rs.rolling.Push(rs.stable.Drain())
	sum := rs.rolling.Sum()
	var threshold time.Duration
	if sum.Total() >= slowMinCount {
		threshold = sum.Quantile(0.95)
	}
	rs.threshold.Store(int64(threshold))
	snap.SlowThresholdMs = float64(threshold.Microseconds()) / 1000
	return snap
}

func sticky(rollout, key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(rollout))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(key))
	return h.Sum64() % 10000
}

type upstreamCounters struct {
	attempts atomic.Int64
	errors   atomic.Int64
	latency  stats.Histogram
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
