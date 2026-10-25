package limit

import (
	"context"
	"fmt"
	"math"
	"time"
)

type Rule struct {
	Rate  float64
	Burst float64
}

type Decision struct {
	Allowed    bool
	RetryAfter time.Duration
	Degraded   bool
}

type Limiter interface {
	Allow(ctx context.Context, key string) Decision
	Close()
}

type Clock func() int64

type Options struct {
	Rule         Rule
	Instances    int
	Now          Clock
	Redis        Pipeliner
	Prefix       string
	InstanceName string
	RedisTimeout time.Duration
	SyncInterval time.Duration
	LeaseTarget  time.Duration
	LeaseTTL     time.Duration
}

func split(rule Rule, instances int) Rule {
	n := float64(max(instances, 1))
	return Rule{Rate: rule.Rate / n, Burst: max(1, rule.Burst/n)}
}

func usPerToken(rate float64) float64 { return 1e6 / rate }

func ttlMillis(rule Rule) int64 {
	return int64(math.Ceil(rule.Burst/rule.Rate*1000)) + 1000
}

func validate(rule Rule) error {
	if !(rule.Rate > 0) || !(rule.Burst >= 1) || math.IsInf(rule.Rate, 0) || math.IsInf(rule.Burst, 0) {
		return fmt.Errorf("rate must be positive and burst at least 1, got %v/%v", rule.Rate, rule.Burst)
	}
	return nil
}
