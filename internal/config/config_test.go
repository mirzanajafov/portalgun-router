package config

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

type base struct{ cfg, route, pool, dim map[string]any }

type edit func(b base)

func configWith(t *testing.T, change edit) []byte {
	t.Helper()
	dim := map[string]any{"id": "c-137", "address": "dim-a:8080"}
	pool := map[string]any{"name": "p", "dimensions": []any{dim}}
	route := map[string]any{"name": "r", "pathPrefix": "/", "pool": "p"}
	cfg := map[string]any{"routes": []any{route}, "pools": []any{pool}}
	change(base{cfg, route, pool, dim})
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestParseAppliesDefaults(t *testing.T) {
	c, err := Parse(configWith(t, func(b base) { b.route["host"], b.route["timeout"] = "Portal.Example", "5s" }))
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
	c, err := Parse(configWith(t, func(b base) {
		b.route["rateLimit"] = map[string]any{"design": "local-sync", "rate": 1000, "burst": 100}
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
	c, err := Parse(configWith(t, func(b base) {
		b.cfg["maxInflight"] = 0
		b.route["retries"] = map[string]any{"max": 0}
		b.pool["panicThreshold"], b.pool["slowStart"], b.pool["healthCheck"] = 0, "0s", map[string]any{"disabled": true}
	}))
	if err != nil {
		t.Fatal(err)
	}
	if *c.MaxInflight != 0 || *c.Routes[0].Retries.Max != 0 || *c.Pools[0].PanicThreshold != 0 || *c.Pools[0].SlowStart != 0 || !c.Pools[0].HealthCheck.Disabled {
		t.Fatalf("explicit zeros were replaced by defaults: %+v %+v", c.Routes[0].Retries, c.Pools[0])
	}
}

func TestParseRejectsBadConfigs(t *testing.T) {
	address := func(a string) edit { return func(b base) { b.dim["address"] = a } }
	limiter := func(l map[string]any) edit { return func(b base) { b.route["rateLimit"] = l } }
	for name, tc := range map[string]struct {
		change edit
		want   string
	}{
		"unknown field":    {func(b base) { b.cfg["colour"] = 1 }, "unknown field"},
		"no routes":        {func(b base) { b.cfg["routes"] = []any{} }, "no routes"},
		"unknown pool":     {func(b base) { b.route["pool"] = "x" }, `unknown pool "x"`},
		"bad prefix":       {func(b base) { b.route["pathPrefix"] = "api" }, "must start with /"},
		"bad address":      {address("nohost"), "not host:port"},
		"port zero":        {address("dim-a:0"), "not host:port"},
		"port over 65535":  {address("dim-a:65536"), "not host:port"},
		"named port":       {address("dim-a:http"), "not host:port"},
		"NUL in a version": {func(b base) { b.dim["version"] = "v\x00" }, "NUL"},
		"duplicate dimension": {func(b base) {
			b.cfg["pools"] = append(b.cfg["pools"].([]any), map[string]any{"name": "q", "dimensions": []any{map[string]any{"id": "c-137", "address": "dim-b:8080"}}})
		}, "appears in pools"},
		"unknown balancer":           {func(b base) { b.pool["balancer"] = "dice" }, "unknown balancer"},
		"per-try over total":         {func(b base) { b.route["timeout"], b.route["perTryTimeout"] = "1s", "2s" }, "perTryTimeout"},
		"bad duration":               {func(b base) { b.route["timeout"] = 5 }, "duration"},
		"too many retries":           {func(b base) { b.route["retries"] = map[string]any{"max": 9} }, "retries.max"},
		"panic above one":            {func(b base) { b.pool["panicThreshold"] = 1.5 }, "panicThreshold"},
		"probe slower than interval": {func(b base) { b.pool["healthCheck"] = map[string]any{"interval": "1s", "timeout": "2s"} }, "healthCheck"},
		"unknown limiter":            {limiter(map[string]any{"design": "vibes", "rate": 1, "burst": 1}), "unknown rate limit design"},
		"limiter burst":              {limiter(map[string]any{"design": "lease", "rate": 10, "burst": 0.5}), "burst of at least 1"},
		"limiter key":                {limiter(map[string]any{"design": "lease", "rate": 10, "burst": 5, "key": "cookie:id"}), "key must be"},
		"trusted hops":               {func(b base) { b.cfg["trustedHops"] = -1 }, "trustedHops"},
		"negative cap":               {func(b base) { b.cfg["maxInflight"] = -1 }, "maxInflight"},
		"tiny decay":                 {func(b base) { b.pool["ewmaDecay"] = "10ms" }, "ewmaDecay"},
		"outlier ratio":              {func(b base) { b.pool["outlier"] = map[string]any{"failureRatio": 2} }, "failureRatio"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Parse(configWith(t, tc.change))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error mentioning %q", err, tc.want)
			}
			if tc.want != "unknown field" && strings.Contains(err.Error(), "unknown field") {
				t.Fatalf("the change went into the wrong object: %v", err)
			}
		})
	}
}
