package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
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

type visitorLimit struct {
	name    string
	perHour int64
	counter Counter
	logger  *slog.Logger

	mu     sync.Mutex
	logged time.Time
}

func (l *visitorLimit) exceeded(ctx context.Context, client string) (bool, error) {
	n, err := l.counter.Hit(ctx, l.name+":ip:"+client, time.Hour)
	if err != nil {
		if ctx.Err() == nil {
			l.logUnchecked(err)
		}
		return false, ErrLimitUnchecked
	}
	return n > l.perHour, nil
}

func (l *visitorLimit) logUnchecked(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if time.Since(l.logged) < recountAfter {
		return
	}
	l.logged = time.Now()
	l.logger.Error("refusing visitors that cannot be counted", "limit", l.name, "err", err)
}

func uncounted(w http.ResponseWriter) {
	w.Header().Set("Retry-After", fmt.Sprint(int(recountAfter.Seconds())))
	writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": ErrLimitUnchecked.Error(), "retryAfterSec": recountAfter.Seconds()})
}
