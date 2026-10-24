package controller

import (
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/health"
	"github.com/mirzanajafov/portalgun-router/internal/proxy"
)

const (
	grace       = 1500 * time.Millisecond
	instanceTTL = 10
)

type Merged struct {
	Second         int64                          `json:"second"`
	Instances      []string                       `json:"instances"`
	ConfigVersions map[string]int64               `json:"configVersions"`
	Routes         map[string]proxy.RouteSnapshot `json:"routes"`
	Dimensions     []proxy.DimensionSnapshot      `json:"dimensions"`
}

func (m Merged) MinConfig() int64 {
	var low int64 = -1
	for _, v := range m.ConfigVersions {
		if low < 0 || v < low {
			low = v
		}
	}
	return max(low, 0)
}

type Aggregator struct {
	mu      sync.Mutex
	pending map[int64]map[string]proxy.Snapshot
	seen    map[string]int64
	emitted int64
	out     func(Merged)
}

func NewAggregator(out func(Merged)) *Aggregator {
	return &Aggregator{pending: map[int64]map[string]proxy.Snapshot{}, seen: map[string]int64{}, out: out}
}

func (a *Aggregator) Add(s proxy.Snapshot) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if s.Second <= a.emitted {
		return
	}
	a.seen[s.Instance] = max(a.seen[s.Instance], s.Second)
	bin, ok := a.pending[s.Second]
	if !ok {
		bin = map[string]proxy.Snapshot{}
		a.pending[s.Second] = bin
	}
	bin[s.Instance] = s
}

func (a *Aggregator) Flush(now time.Time) {
	a.mu.Lock()
	var ready []Merged
	seconds := make([]int64, 0, len(a.pending))
	for sec := range a.pending {
		seconds = append(seconds, sec)
	}
	slices.Sort(seconds)
	for _, sec := range seconds {
		bin := a.pending[sec]
		end := time.Unix(sec+1, 0)
		if !a.complete(sec, bin) && now.Before(end.Add(grace)) {
			break
		}
		ready = append(ready, merge(sec, bin))
		delete(a.pending, sec)
		a.emitted = sec
	}
	for inst, last := range a.seen {
		if a.emitted-last > instanceTTL {
			delete(a.seen, inst)
		}
	}
	a.mu.Unlock()
	for _, m := range ready {
		a.out(m)
	}
}

func (a *Aggregator) complete(sec int64, bin map[string]proxy.Snapshot) bool {
	for inst, last := range a.seen {
		if sec-last > instanceTTL {
			continue
		}
		if _, ok := bin[inst]; !ok {
			return false
		}
	}
	return true
}

func severity(s health.State) int {
	switch s {
	case health.Ejected:
		return 3
	case health.HalfOpen:
		return 2
	case health.Closed:
		return 1
	}
	return 0
}

func merge(sec int64, bin map[string]proxy.Snapshot) Merged {
	m := Merged{Second: sec, ConfigVersions: map[string]int64{}, Routes: map[string]proxy.RouteSnapshot{}}
	dims := map[string]*proxy.DimensionSnapshot{}
	for inst, s := range bin {
		m.Instances = append(m.Instances, inst)
		m.ConfigVersions[inst] = s.ClusterConfig
		for name, r := range s.Routes {
			acc, ok := m.Routes[name]
			if !ok {
				acc = proxy.RouteSnapshot{Stable: r.Stable, Canary: r.Canary, Weight: r.Weight, Rollout: r.Rollout}
			}
			acc.Requests += r.Requests
			acc.Limited += r.Limited
			acc.Failed += r.Failed
			for i := range acc.Versions {
				acc.Versions[i].Attempts += r.Versions[i].Attempts
				acc.Versions[i].Errors += r.Versions[i].Errors
				acc.Versions[i].Slow += r.Versions[i].Slow
			}
			acc.SlowThresholdMs = max(acc.SlowThresholdMs, r.SlowThresholdMs)
			acc.Brake = acc.Brake || r.Brake
			m.Routes[name] = acc
		}
		for _, d := range s.Dimensions {
			acc, ok := dims[d.ID]
			if !ok {
				c := d
				c.Attempts, c.Errors, c.Inflight = 0, 0, 0
				acc = &c
				dims[d.ID] = acc
			}
			acc.Attempts += d.Attempts
			acc.Errors += d.Errors
			acc.Inflight += d.Inflight
			acc.P50Ms = max(acc.P50Ms, d.P50Ms)
			acc.P99Ms = max(acc.P99Ms, d.P99Ms)
			acc.Healthy = acc.Healthy && d.Healthy
			if severity(d.Outlier) > severity(acc.Outlier) {
				acc.Outlier = d.Outlier
			}
			acc.EjectedMs = max(acc.EjectedMs, d.EjectedMs)
		}
	}
	slices.Sort(m.Instances)
	for _, d := range dims {
		m.Dimensions = append(m.Dimensions, *d)
	}
	slices.SortFunc(m.Dimensions, func(a, b proxy.DimensionSnapshot) int { return strings.Compare(a.ID, b.ID) })
	return m
}
