package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type Counter interface {
	Hit(ctx context.Context, key string, window time.Duration) (int64, error)
}

type RedisCounter struct{ RDB *redis.Client }

func (r RedisCounter) Hit(ctx context.Context, key string, window time.Duration) (int64, error) {
	var incr *redis.IntCmd
	_, err := r.RDB.TxPipelined(ctx, func(p redis.Pipeliner) error {
		incr = p.Incr(ctx, key)
		p.ExpireNX(ctx, key, window)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return incr.Val(), nil
}

var ErrLimitUnchecked = errors.New("the visitor limits cannot be checked right now, so try again in a minute")

const recountAfter = time.Minute

type visitorCounter struct {
	counter Counter
	logger  *slog.Logger
	down    atomic.Bool
}

func (v *visitorCounter) overHourly(ctx context.Context, key string, perHour int64) (bool, error) {
	n, err := v.counter.Hit(ctx, key, time.Hour)
	switch {
	case errors.Is(err, context.Canceled):
		return false, err
	case err != nil:
		if !v.down.Swap(true) {
			v.logger.Error("refusing visitors until they can be counted again", "key", key, "err", err)
		}
		return false, ErrLimitUnchecked
	}
	if v.down.Swap(false) {
		v.logger.Info("counting visitors again")
	}
	return n > perHour, nil
}

func uncounted(w http.ResponseWriter) {
	w.Header().Set("Retry-After", fmt.Sprint(int(recountAfter.Seconds())))
	writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": ErrLimitUnchecked.Error(), "retryAfterSec": recountAfter.Seconds()})
}
