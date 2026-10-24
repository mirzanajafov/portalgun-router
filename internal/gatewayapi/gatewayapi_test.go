package gatewayapi

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/mirzanajafov/portalgun-router/internal/health"
)

func TestTheWireNamesStayWhatTheDashboardAndTheResultsRead(t *testing.T) {
	values := map[string]any{
		"snapshot": Snapshot{Instance: "gw-1", Second: 1, ConfigVersion: 2, ClusterConfig: 3,
			Routes:     map[string]RouteSnapshot{"api": {Requests: 4, Limited: 5, Failed: 6, Stable: "v1", Canary: "v2", Weight: 0.5, Rollout: "ro-1", Versions: [2]VersionSnapshot{{Attempts: 7, Errors: 8, Slow: 9}, {}}, SlowThresholdMs: 10, Brake: true}},
			Dimensions: []DimensionSnapshot{{ID: "a", Version: "v1", Attempts: 11, Errors: 12, P50Ms: 13, P99Ms: 14, Inflight: 15, Healthy: true, Outlier: health.Ejected, EjectedMs: 16}}},
		"state":     State{Version: 1, Inflight: 2, MaxInflight: 3, Upstreams: []Upstream{{ID: "a", Version: "v1", Address: "dim-a:8080", Inflight: 4, MaxInflight: 5, LatencyMs: 6, Healthy: true, Outlier: health.HalfOpen, EjectedMs: 7}}},
		"event":     DimensionEvent{UnixNano: 1, Dimension: "a", State: "ejected", Reason: "5 failures in a row"},
		"decisions": Decisions{Recording: true, Dropped: 1, Decisions: []Decision{{UnixNano: 2, Key: "ip:1.2.3.4", Allowed: true}}},
		"headers":   []string{DimensionHeader, AttemptsHeader},
	}
	got, err := json.MarshalIndent(values, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("testdata/wire.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, bytes.TrimSpace(want)) {
		t.Fatalf("the wire format changed, and the dashboard, Redis and the committed results read the old one:\n%s", got)
	}
}
