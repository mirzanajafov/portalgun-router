package proxy

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func gathered(t *testing.T, m *Metrics, name string) float64 {
	t.Helper()
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return f.GetMetric()[0].GetCounter().GetValue()
		}
	}
	t.Fatalf("no metric %s", name)
	return 0
}

func TestRedisHookCountsRoundTrips(t *testing.T) {
	m := NewMetrics()
	h := m.RedisHook()
	one := h.ProcessHook(func(context.Context, redis.Cmder) error { return nil })
	many := h.ProcessPipelineHook(func(context.Context, []redis.Cmder) error { return nil })
	ctx := context.Background()
	_ = one(ctx, redis.NewStatusCmd(ctx, "ping"))
	_ = one(ctx, redis.NewStatusCmd(ctx, "ping"))
	_ = many(ctx, []redis.Cmder{redis.NewStatusCmd(ctx, "ping"), redis.NewStatusCmd(ctx, "ping"), redis.NewStatusCmd(ctx, "ping")})
	if n := gathered(t, m, "portal_redis_round_trips_total"); n != 3 {
		t.Fatalf("%v round trips, want 3: two commands and one pipeline", n)
	}
}
