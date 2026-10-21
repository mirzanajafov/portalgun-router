package config

import (
	"strings"
	"testing"
	"time"
)

const valid = `{
  "routes": [{"name": "all", "host": "Portal.Example", "pathPrefix": "/", "pool": "main", "timeout": "5s"}],
  "pools": [{"name": "main", "dimensions": [{"id": "c-137", "address": "dim-a:8080"}]}]
}`

func TestParseAppliesDefaults(t *testing.T) {
	c, err := Parse([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	r, p, d := c.Routes[0], c.Pools[0], c.Pools[0].Dimensions[0]
	if r.Host != "portal.example" || r.Timeout.D() != 5*time.Second || r.PerTryTimeout.D() != 5*time.Second {
		t.Fatalf("route %+v", r)
	}
	if p.Balancer != BalancerP2C || d.Version != DefaultVersion || d.MaxInflight != DefaultMaxInflight || *c.MaxInflight != DefaultGatewayCap {
		t.Fatalf("pool %+v dimension %+v", p, d)
	}
	if *r.Retries.Max != 2 || r.Retries.BudgetRatio != 0.2 || *p.PanicThreshold != 0.5 || p.SlowStart.D() != 10*time.Second {
		t.Fatalf("resilience defaults: retries %+v panic %v slow start %v", r.Retries, *p.PanicThreshold, *p.SlowStart)
	}
	if p.HealthCheck.Path != "/healthz" || p.Outlier.Consecutive != 5 || p.Outlier.BaseEjection.D() != 5*time.Second || p.EwmaDecay.D() != 10*time.Second {
		t.Fatalf("health %+v outlier %+v", p.HealthCheck, p.Outlier)
	}
}

func TestRateLimitDefaults(t *testing.T) {
	c, err := Parse([]byte(`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p", "rateLimit": {"design": "local-sync", "rate": 1000, "burst": 100}}],
		"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:1"}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	l := c.Routes[0].RateLimit
	if l.Key != "ip" || l.Instances != 1 || l.SyncInterval.D() != 100*time.Millisecond || l.LeaseTarget.D() != 20*time.Millisecond || l.RedisTimeout.D() != 2*time.Millisecond || !l.UsesRedis() {
		t.Fatalf("defaults %+v", l)
	}
}

func TestZeroIsKeptWhereItMeansOff(t *testing.T) {
	c, err := Parse([]byte(`{"maxInflight": 0, "routes": [{"name": "r", "pathPrefix": "/", "pool": "p", "retries": {"max": 0}}],
		"pools": [{"name": "p", "panicThreshold": 0, "slowStart": "0s", "healthCheck": {"disabled": true},
		"dimensions": [{"id": "a", "address": "a:1"}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if *c.MaxInflight != 0 || *c.Routes[0].Retries.Max != 0 || *c.Pools[0].PanicThreshold != 0 || *c.Pools[0].SlowStart != 0 || !c.Pools[0].HealthCheck.Disabled {
		t.Fatalf("explicit zeros were replaced by defaults: %+v %+v", c.Routes[0].Retries, c.Pools[0])
	}
}

func TestParseRejectsBadConfigs(t *testing.T) {
	for name, tc := range map[string]struct{ json, want string }{
		"unknown field": {`{"routes": [], "pools": [], "colour": 1}`, "unknown field"},
		"no routes":     {`{"routes": [], "pools": []}`, "no routes"},
		"unknown pool": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "x"}],
			"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:1"}]}]}`, `unknown pool "x"`},
		"bad prefix": {`{"routes": [{"name": "r", "pathPrefix": "api", "pool": "p"}],
			"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:1"}]}]}`, "must start with /"},
		"bad address": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p"}],
			"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "nohost"}]}]}`, "not host:port"},
		"port zero": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p"}],
			"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:0"}]}]}`, "not host:port"},
		"port over 65535": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p"}],
			"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:65536"}]}]}`, "not host:port"},
		"named port": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p"}],
			"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:http"}]}]}`, "not host:port"},
		"duplicate dimension": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p"}],
			"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:1"}]},
			          {"name": "q", "dimensions": [{"id": "a", "address": "b:1"}]}]}`, `appears in pools`},
		"unknown balancer": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p"}],
			"pools": [{"name": "p", "balancer": "dice", "dimensions": [{"id": "a", "address": "a:1"}]}]}`, "unknown balancer"},
		"per-try over total": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p", "timeout": "1s", "perTryTimeout": "2s"}],
			"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:1"}]}]}`, "perTryTimeout"},
		"bad duration": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p", "timeout": 5}],
			"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:1"}]}]}`, "duration"},
		"too many retries": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p", "retries": {"max": 9}}],
			"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:1"}]}]}`, "retries.max"},
		"panic above one": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p"}],
			"pools": [{"name": "p", "panicThreshold": 1.5, "dimensions": [{"id": "a", "address": "a:1"}]}]}`, "panicThreshold"},
		"probe slower than interval": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p"}],
			"pools": [{"name": "p", "healthCheck": {"interval": "1s", "timeout": "2s"}, "dimensions": [{"id": "a", "address": "a:1"}]}]}`, "healthCheck"},
		"unknown limiter": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p", "rateLimit": {"design": "vibes", "rate": 1, "burst": 1}}],
			"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:1"}]}]}`, "unknown rate limit design"},
		"limiter burst": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p", "rateLimit": {"design": "lease", "rate": 10, "burst": 0.5}}],
			"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:1"}]}]}`, "burst of at least 1"},
		"limiter key": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p", "rateLimit": {"design": "lease", "rate": 10, "burst": 5, "key": "cookie:id"}}],
			"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:1"}]}]}`, "key must be"},
		"trusted hops": {`{"trustedHops": -1, "routes": [{"name": "r", "pathPrefix": "/", "pool": "p"}],
			"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:1"}]}]}`, "trustedHops"},
		"negative cap": {`{"maxInflight": -1, "routes": [{"name": "r", "pathPrefix": "/", "pool": "p"}],
			"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:1"}]}]}`, "maxInflight"},
		"tiny decay": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p"}],
			"pools": [{"name": "p", "ewmaDecay": "10ms", "dimensions": [{"id": "a", "address": "a:1"}]}]}`, "ewmaDecay"},
		"outlier ratio": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p"}],
			"pools": [{"name": "p", "outlier": {"failureRatio": 2}, "dimensions": [{"id": "a", "address": "a:1"}]}]}`, "failureRatio"},
	} {
		_, err := Parse([]byte(tc.json))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want an error mentioning %q", name, err, tc.want)
		}
	}
}
