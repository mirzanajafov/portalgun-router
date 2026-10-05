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
	if p.Balancer != BalancerP2C || d.Version != DefaultVersion || d.MaxInflight != DefaultMaxInflight {
		t.Fatalf("pool %+v dimension %+v", p, d)
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
		"duplicate dimension": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p"}],
			"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:1"}]},
			          {"name": "q", "dimensions": [{"id": "a", "address": "b:1"}]}]}`, `appears in pools`},
		"unknown balancer": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p"}],
			"pools": [{"name": "p", "balancer": "dice", "dimensions": [{"id": "a", "address": "a:1"}]}]}`, "unknown balancer"},
		"per-try over total": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p", "timeout": "1s", "perTryTimeout": "2s"}],
			"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:1"}]}]}`, "perTryTimeout"},
		"bad duration": {`{"routes": [{"name": "r", "pathPrefix": "/", "pool": "p", "timeout": 5}],
			"pools": [{"name": "p", "dimensions": [{"id": "a", "address": "a:1"}]}]}`, "duration"},
	} {
		_, err := Parse([]byte(tc.json))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want an error mentioning %q", name, err, tc.want)
		}
	}
}
