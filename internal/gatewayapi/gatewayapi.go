package gatewayapi

import "github.com/mirzanajafov/portalgun-router/internal/health"

const (
	DimensionHeader = "X-Portal-Dimension"
	AttemptsHeader  = "X-Portal-Attempts"
)

const (
	PathHealthz = "/healthz"
	PathMetrics = "/metrics"
	PathState   = "/state"
	PathEvents  = "/events"
	PathDrain   = "/decisions/drain"
	PathConfig  = "/config"
)

type VersionSnapshot struct {
	Attempts int64 `json:"attempts"`
	Errors   int64 `json:"errors"`
	Slow     int64 `json:"slow"`
}

const (
	StableSlot = 0
	CanarySlot = 1
)

type RouteSnapshot struct {
	Requests        int64              `json:"requests"`
	Limited         int64              `json:"limited"`
	Failed          int64              `json:"failed"`
	Stable          string             `json:"stable,omitempty"`
	Canary          string             `json:"canary,omitempty"`
	Weight          float64            `json:"weight"`
	Rollout         string             `json:"rollout,omitempty"`
	Versions        [2]VersionSnapshot `json:"versions"`
	SlowThresholdMs float64            `json:"slowThresholdMs"`
	Brake           bool               `json:"brake,omitempty"`
}

type DimensionSnapshot struct {
	ID        string       `json:"id"`
	Version   string       `json:"version"`
	Attempts  int64        `json:"attempts"`
	Errors    int64        `json:"errors"`
	P50Ms     float64      `json:"p50Ms"`
	P99Ms     float64      `json:"p99Ms"`
	Inflight  int64        `json:"inflight"`
	Healthy   bool         `json:"healthy"`
	Outlier   health.State `json:"outlier"`
	EjectedMs float64      `json:"ejectedForMs,omitempty"`
}

type Snapshot struct {
	Instance      string                   `json:"instance"`
	Second        int64                    `json:"second"`
	ConfigVersion int64                    `json:"configVersion"`
	ClusterConfig int64                    `json:"clusterConfig"`
	Routes        map[string]RouteSnapshot `json:"routes"`
	Dimensions    []DimensionSnapshot      `json:"dimensions"`
}

type Upstream struct {
	ID          string       `json:"id"`
	Version     string       `json:"version"`
	Address     string       `json:"address"`
	Inflight    int64        `json:"inflight"`
	MaxInflight int64        `json:"maxInflight"`
	LatencyMs   float64      `json:"latencyMs"`
	Healthy     bool         `json:"healthy"`
	Outlier     health.State `json:"outlier"`
	EjectedMs   float64      `json:"ejectedForMs,omitempty"`
}

type State struct {
	Version     int64      `json:"version"`
	Inflight    int64      `json:"inflight"`
	MaxInflight int64      `json:"maxInflight"`
	Upstreams   []Upstream `json:"upstreams"`
}

type EventState string

const (
	EventHealthy   EventState = "healthy"
	EventUnhealthy EventState = "unhealthy"
)

func OutlierEvent(s health.State) EventState { return EventState(s.String()) }

type DimensionEvent struct {
	UnixNano  int64      `json:"unixNano"`
	Dimension string     `json:"dimension"`
	State     EventState `json:"state"`
	Reason    string     `json:"reason,omitempty"`
}

type Decision struct {
	UnixNano int64  `json:"t"`
	Key      string `json:"k"`
	Allowed  bool   `json:"a"`
}

type Decisions struct {
	Recording bool       `json:"recording"`
	Dropped   int64      `json:"dropped"`
	Decisions []Decision `json:"decisions"`
}
