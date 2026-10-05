package controller

import (
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/proxy"
)

func snap(inst string, sec int64, attempts, errors int64, dim proxy.DimensionSnapshot) proxy.Snapshot {
	return proxy.Snapshot{
		Instance:      inst,
		Second:        sec,
		ClusterConfig: 7,
		Routes: map[string]proxy.RouteSnapshot{"api": {
			Requests: attempts,
			Versions: [2]proxy.VersionSnapshot{{Attempts: attempts, Errors: errors}, {Attempts: 1}},
		}},
		Dimensions: []proxy.DimensionSnapshot{dim},
	}
}

func collect() (*Aggregator, *[]Merged) {
	var out []Merged
	return NewAggregator(func(m Merged) { out = append(out, m) }), &out
}

func TestMergeSumsAcrossInstances(t *testing.T) {
	agg, out := collect()
	agg.Add(snap("gw-1", 100, 10, 1, proxy.DimensionSnapshot{ID: "a", Attempts: 10, P99Ms: 3, Healthy: true, Outlier: "closed"}))
	agg.Add(snap("gw-2", 100, 20, 2, proxy.DimensionSnapshot{ID: "a", Attempts: 20, P99Ms: 9, Healthy: false, Outlier: "ejected"}))
	agg.Flush(time.Unix(100, 0))
	if len(*out) != 1 {
		t.Fatalf("emitted %d seconds", len(*out))
	}
	m := (*out)[0]
	r := m.Routes["api"]
	if r.Requests != 30 || r.Versions[0].Errors != 3 || r.Versions[1].Attempts != 2 {
		t.Fatalf("route %+v", r)
	}
	d := m.Dimensions[0]
	if d.Attempts != 30 || d.P99Ms != 9 || d.Healthy || d.Outlier != "ejected" {
		t.Fatalf("dimension %+v", d)
	}
	if len(m.Instances) != 2 || m.MinConfig() != 7 {
		t.Fatalf("instances %v config %d", m.Instances, m.MinConfig())
	}
}

func TestWaitsForEveryKnownInstanceOrTheGrace(t *testing.T) {
	agg, out := collect()
	dim := proxy.DimensionSnapshot{ID: "a", Healthy: true}
	agg.Add(snap("gw-1", 100, 1, 0, dim))
	agg.Add(snap("gw-2", 100, 1, 0, dim))
	agg.Flush(time.Unix(101, 0))
	agg.Add(snap("gw-1", 101, 1, 0, dim))
	agg.Flush(time.Unix(102, 0))
	if len(*out) != 1 {
		t.Fatalf("emitted %d seconds while gw-2 had not reported second 101", len(*out))
	}
	agg.Flush(time.Unix(102, 0).Add(grace))
	if len(*out) != 2 || (*out)[1].Routes["api"].Requests != 1 {
		t.Fatalf("after the grace: %d seconds", len(*out))
	}
	agg.Add(snap("gw-2", 101, 5, 0, dim))
	agg.Flush(time.Unix(110, 0))
	if len(*out) != 2 {
		t.Fatal("a late snapshot changed a second that was already out")
	}
}

func TestSecondsComeOutInOrder(t *testing.T) {
	agg, out := collect()
	dim := proxy.DimensionSnapshot{ID: "a", Healthy: true}
	agg.Add(snap("gw-1", 102, 1, 0, dim))
	agg.Add(snap("gw-1", 101, 1, 0, dim))
	agg.Flush(time.Unix(200, 0))
	if len(*out) != 2 || (*out)[0].Second != 101 || (*out)[1].Second != 102 {
		t.Fatalf("order %v", *out)
	}
}
