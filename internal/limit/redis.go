package limit

import (
	"time"

	"github.com/redis/go-redis/v9"
)

type Pipeliner interface {
	redis.Scripter
	Pipeline() redis.Pipeliner
}

func ClientOptions(addr string) *redis.Options {
	return &redis.Options{
		Addr:                  addr,
		MaxRetries:            -1,
		DialTimeout:           time.Second,
		ReadTimeout:           time.Second,
		WriteTimeout:          time.Second,
		PoolSize:              64,
		MinIdleConns:          16,
		ContextTimeoutEnabled: true,
	}
}
