package limit

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

type Redis interface {
	redis.Scripter
}

type Pipeliner interface {
	Redis
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

func TimedOut(err error) bool {
	var ne net.Error
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout())
}
