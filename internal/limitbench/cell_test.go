package limitbench

import (
	"strings"
	"testing"
	"time"
)

func TestParseInfoSumsCommandsButNotItself(t *testing.T) {
	info := "# Commandstats\r\n" +
		"cmdstat_evalsha:calls=1200,usec=3400,usec_per_call=2.83,rejected_calls=0,failed_calls=0\r\n" +
		"cmdstat_info:calls=7,usec=900,usec_per_call=128.57,rejected_calls=0,failed_calls=0\r\n" +
		"cmdstat_publish:calls=30,usec=60,usec_per_call=2.00,rejected_calls=0,failed_calls=0\r\n" +
		"\r\n# CPU\r\nused_cpu_sys:1.250000\r\nused_cpu_user:0.750000\r\nused_cpu_sys_children:9.0\r\n"
	u, err := ParseInfo(info)
	if err != nil {
		t.Fatal(err)
	}
	if u.Calls != 1230 || u.CPUSeconds != 2 {
		t.Fatalf("%+v", u)
	}
}

func TestCountersSumAcrossLabels(t *testing.T) {
	text := `# HELP portal_rate_limit_degraded_total x
# TYPE portal_rate_limit_degraded_total counter
portal_rate_limit_degraded_total{route="fluid"} 12
portal_rate_limit_degraded_total{route="other"} 3
portal_rate_limit_degraded_totalish 99
portal_rate_limited_total{route="fluid"} 40
portal_redis_round_trips_total 7
`
	ss, err := Samples(strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	c := Counters(ss)
	if c[degradedMetric] != 15 || c[tripsMetric] != 7 {
		t.Fatalf("%v", c)
	}
}

func TestCountersSplitRedisCallsAtTheTimeout(t *testing.T) {
	text := `portal_redis_call_seconds_bucket{outcome="ok",le="0.0015"} 900
portal_redis_call_seconds_bucket{outcome="ok",le="0.002"} 950
portal_redis_call_seconds_bucket{outcome="ok",le="+Inf"} 960
portal_redis_call_seconds_count{outcome="ok"} 960
portal_redis_call_seconds_bucket{outcome="timeout",le="0.002"} 0
portal_redis_call_seconds_bucket{outcome="timeout",le="+Inf"} 30
portal_redis_call_seconds_count{outcome="timeout"} 30
portal_redis_call_seconds_sum{outcome="timeout"} 0.07
portal_redis_dial_seconds_bucket{outcome="ok",le="0.002"} 20
portal_redis_dial_seconds_count{outcome="ok"} 26
`
	ss, err := Samples(strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	c := Counters(ss)
	if c[callsKey] != 990 || c[slowKey] != 40 || c[timeoutsKey] != 30 || c[dialsKey] != 26 || c[slowDialKey] != 6 {
		t.Fatalf("%v", c)
	}
}

func TestRedisCostIsPerRequestTheLimiterSaw(t *testing.T) {
	r := Redis{Commands: 191607, RoundTrips: 52500, CPUSeconds: 7.1}
	r.perRequest(Requests(52166, 44803, 5*time.Second, 30*time.Second))
	if r.TripsReq < 1.0 || r.TripsReq > 1.01 {
		t.Fatalf("%.3f round trips per request: the warm-up's decisions count too, so divide by them, not by the measured window", r.TripsReq)
	}
	if n := Requests(0, 30000, 5*time.Second, 30*time.Second); n != 35000 {
		t.Fatalf("without a limiter the whole run is %v requests, want 35000", n)
	}
}
