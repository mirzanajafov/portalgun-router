package proxy

import (
	"fmt"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/limit"
)

type routeLimiter struct {
	limit.Limiter
	config config.RateLimit
}

func (g *Gateway) newLimiter(route string, c config.RateLimit) (limit.Limiter, error) {
	rule := limit.Rule{Rate: c.Rate, Burst: c.Burst}
	prefix := "pf:" + route + ":"
	switch c.Design {
	case config.LimitStaticSplit:
		return limit.NewStatic(rule, c.Instances, g.now), nil
	case config.LimitRedisExact:
		return limit.NewRedisExact(g.redis, prefix, rule, c.Instances, c.RedisTimeout.D(), g.now)
	case config.LimitLocalSync:
		return limit.NewLocalSync(g.redis, prefix, rule, c.Instances, g.instance, c.SyncInterval.D(), c.RedisTimeout.D(), g.now)
	case config.LimitLease:
		return limit.NewLease(g.redis, prefix, rule, c.Instances, c.LeaseTarget.D(), c.LeaseTTL.D(), c.RedisTimeout.D(), g.now)
	}
	return nil, fmt.Errorf("unknown rate limit design %q", c.Design)
}

func (g *Gateway) buildLimiters(cfg *config.Config) (map[string]*routeLimiter, error) {
	out := map[string]*routeLimiter{}
	for _, rc := range cfg.Routes {
		c := rc.RateLimit
		if c == nil {
			continue
		}
		if c.UsesRedis() && g.redis == nil {
			closeReplaced(out, g.limiters)
			return nil, fmt.Errorf("route %q: the %s rate limiter needs Redis", rc.Name, c.Design)
		}
		if old, ok := g.limiters[rc.Name]; ok && old.config == *c {
			out[rc.Name] = old
			continue
		}
		l, err := g.newLimiter(rc.Name, *c)
		if err != nil {
			closeReplaced(out, g.limiters)
			return nil, err
		}
		out[rc.Name] = &routeLimiter{Limiter: l, config: *c}
	}
	return out, nil
}

func closeReplaced(from, keep map[string]*routeLimiter) {
	for name, l := range from {
		if keep[name] != l {
			l.Close()
		}
	}
}
