package proxy

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

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

func (rs *routeStats) tick() RouteSnapshot {
	snap := RouteSnapshot{
		Requests: rs.requests.Swap(0),
		Limited:  rs.limited.Swap(0),
		Brake:    rs.brakeOn.Load(),
	}
	for i := range rs.versions {
		v := &rs.versions[i]
		snap.Versions[i] = VersionSnapshot{Attempts: v.attempts.Swap(0), Errors: v.errors.Swap(0), Slow: v.slow.Swap(0)}
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

type VersionSnapshot struct {
	Attempts int64 `json:"attempts"`
	Errors   int64 `json:"errors"`
	Slow     int64 `json:"slow"`
}

type RouteSnapshot struct {
	Requests        int64              `json:"requests"`
	Limited         int64              `json:"limited"`
	Stable          string             `json:"stable,omitempty"`
	Canary          string             `json:"canary,omitempty"`
	Weight          float64            `json:"weight"`
	Rollout         string             `json:"rollout,omitempty"`
	Versions        [2]VersionSnapshot `json:"versions"`
	SlowThresholdMs float64            `json:"slowThresholdMs"`
	Brake           bool               `json:"brake,omitempty"`
}

type DimensionSnapshot struct {
	ID        string  `json:"id"`
	Version   string  `json:"version"`
	Attempts  int64   `json:"attempts"`
	Errors    int64   `json:"errors"`
	P50Ms     float64 `json:"p50Ms"`
	P99Ms     float64 `json:"p99Ms"`
	Inflight  int64   `json:"inflight"`
	Healthy   bool    `json:"healthy"`
	Outlier   string  `json:"outlier"`
	EjectedMs float64 `json:"ejectedForMs,omitempty"`
}

type Snapshot struct {
	Instance      string                   `json:"instance"`
	Second        int64                    `json:"second"`
	ConfigVersion int64                    `json:"configVersion"`
	ClusterConfig int64                    `json:"clusterConfig"`
	Routes        map[string]RouteSnapshot `json:"routes"`
	Dimensions    []DimensionSnapshot      `json:"dimensions"`
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
