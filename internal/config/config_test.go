package config

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

type edit func(cfg, route, pool, dim map[string]any)

func configWith(t *testing.T, change edit) []byte {
	t.Helper()
	dim := map[string]any{"id": "a", "address": "a:1"}
	pool := map[string]any{"name": "p", "dimensions": []any{dim}}
	route := map[string]any{"name": "r", "pathPrefix": "/", "pool": "p"}
	cfg := map[string]any{"routes": []any{route}, "pools": []any{pool}}
	change(cfg, route, pool, dim)
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestParseAppliesDefaults(t *testing.T) {
	c, err := Parse(configWith(t, func(_, r, _, _ map[string]any) { r["host"], r["timeout"] = "Portal.Example", "5s" }))
	if err != nil {
		t.Fatal(err)
	}
	r, p, d := c.Routes[0], c.Pools[0], c.Pools[0].Dimensions[0]
	if r.Host != "portal.example" || r.Timeout.D() != 5*time.Second || r.PerTryTimeout.D() != 5*time.Second {
		t.Fatalf("route %+v", r)
	}
	if p.Balancer != BalancerP2C || d.Version != DefaultVersion || d.MaxInflight != DefaultMaxInflight || *c.MaxInflight != 256 {
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
	c, err := Parse(configWith(t, func(_, r, _, _ map[string]any) {
		r["rateLimit"] = map[string]any{"design": "local-sync", "rate": 1000, "burst": 100}
	}))
	if err != nil {
		t.Fatal(err)
	}
	l := c.Routes[0].RateLimit
	if l.Key != "ip" || l.Instances != 1 || l.SyncInterval.D() != 100*time.Millisecond || l.LeaseTarget.D() != 20*time.Millisecond || l.RedisTimeout.D() != 2*time.Millisecond || !l.UsesRedis() {
		t.Fatalf("defaults %+v", l)
	}
}

func TestZeroIsKeptWhereItMeansOff(t *testing.T) {
	c, err := Parse(configWith(t, func(cfg, r, p, _ map[string]any) {
		cfg["maxInflight"] = 0
		r["retries"] = map[string]any{"max": 0}
		p["panicThreshold"], p["slowStart"], p["healthCheck"] = 0, "0s", map[string]any{"disabled": true}
	}))
	if err != nil {
		t.Fatal(err)
	}
	if *c.MaxInflight != 0 || *c.Routes[0].Retries.Max != 0 || *c.Pools[0].PanicThreshold != 0 || *c.Pools[0].SlowStart != 0 || !c.Pools[0].HealthCheck.Disabled {
		t.Fatalf("explicit zeros were replaced by defaults: %+v %+v", c.Routes[0].Retries, c.Pools[0])
	}
}

func TestParseRejectsBadConfigs(t *testing.T) {
	address := func(a string) edit { return func(_, _, _, d map[string]any) { d["address"] = a } }
	limiter := func(l map[string]any) edit { return func(_, r, _, _ map[string]any) { r["rateLimit"] = l } }
	for name, tc := range map[string]struct {
		change edit
		want   string
	}{
		"unknown field":    {func(c, _, _, _ map[string]any) { c["colour"] = 1 }, "unknown field"},
		"no routes":        {func(c, _, _, _ map[string]any) { c["routes"] = []any{} }, "no routes"},
		"unknown pool":     {func(_, r, _, _ map[string]any) { r["pool"] = "x" }, `unknown pool "x"`},
		"bad prefix":       {func(_, r, _, _ map[string]any) { r["pathPrefix"] = "api" }, "must start with /"},
		"bad address":      {address("nohost"), "not host:port"},
		"port zero":        {address("a:0"), "not host:port"},
		"port over 65535":  {address("a:65536"), "not host:port"},
		"named port":       {address("a:http"), "not host:port"},
		"NUL in a version": {func(_, _, _, d map[string]any) { d["version"] = "v\x00" }, "NUL"},
		"duplicate dimension": {func(c, _, _, _ map[string]any) {
			c["pools"] = append(c["pools"].([]any), map[string]any{"name": "q", "dimensions": []any{map[string]any{"id": "a", "address": "b:1"}}})
		}, "appears in pools"},
		"unknown balancer":           {func(_, _, p, _ map[string]any) { p["balancer"] = "dice" }, "unknown balancer"},
		"per-try over total":         {func(_, r, _, _ map[string]any) { r["timeout"], r["perTryTimeout"] = "1s", "2s" }, "perTryTimeout"},
		"bad duration":               {func(_, r, _, _ map[string]any) { r["timeout"] = 5 }, "duration"},
		"too many retries":           {func(_, r, _, _ map[string]any) { r["retries"] = map[string]any{"max": 9} }, "retries.max"},
		"panic above one":            {func(_, _, p, _ map[string]any) { p["panicThreshold"] = 1.5 }, "panicThreshold"},
		"probe slower than interval": {func(_, _, p, _ map[string]any) { p["healthCheck"] = map[string]any{"interval": "1s", "timeout": "2s"} }, "healthCheck"},
		"unknown limiter":            {limiter(map[string]any{"design": "vibes", "rate": 1, "burst": 1}), "unknown rate limit design"},
		"limiter burst":              {limiter(map[string]any{"design": "lease", "rate": 10, "burst": 0.5}), "burst of at least 1"},
		"limiter key":                {limiter(map[string]any{"design": "lease", "rate": 10, "burst": 5, "key": "cookie:id"}), "key must be"},
		"trusted hops":               {func(c, _, _, _ map[string]any) { c["trustedHops"] = -1 }, "trustedHops"},
		"negative cap":               {func(c, _, _, _ map[string]any) { c["maxInflight"] = -1 }, "maxInflight"},
		"tiny decay":                 {func(_, _, p, _ map[string]any) { p["ewmaDecay"] = "10ms" }, "ewmaDecay"},
		"outlier ratio":              {func(_, _, p, _ map[string]any) { p["outlier"] = map[string]any{"failureRatio": 2} }, "failureRatio"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse(configWith(t, tc.change)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error mentioning %q", err, tc.want)
			}
		})
	}
}
