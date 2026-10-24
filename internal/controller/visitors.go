package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"sync"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/proxy"
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

type TrustedProxy struct {
	Host   string
	Hops   int
	Logger *slog.Logger
}

func (p TrustedProxy) Client(r *http.Request) string {
	if p.Host != "" && p.sent(r) {
		return proxy.ClientIP(r, p.Hops)
	}
	return proxy.ClientIP(r, 0)
}

func (p TrustedProxy) sent(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	peer, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	addrs, err := net.DefaultResolver.LookupNetIP(r.Context(), "ip", p.Host)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			p.Logger.Warn("ignoring X-Forwarded-For, the trusted proxy does not resolve", "host", p.Host, "err", err)
		}
		return false
	}
	return slices.ContainsFunc(addrs, func(a netip.Addr) bool { return a.Unmap() == peer.Unmap() })
}

var ErrLimitUnchecked = errors.New("the visitor limits cannot be checked right now, so try again in a minute")

const recountAfter = time.Minute

type visitorLimit struct {
	name    string
	perHour int64
	counter Counter
	logger  *slog.Logger
	now     func() time.Time

	mu       sync.Mutex
	failedAt time.Time
}

func newVisitorLimit(name string, perHour int64, counter Counter, logger *slog.Logger) *visitorLimit {
	return &visitorLimit{name: name, perHour: perHour, counter: counter, logger: logger, now: time.Now}
}

func (l *visitorLimit) exceeded(ctx context.Context, client string) (bool, error) {
	if l.pausedAfterFailure() {
		return false, ErrLimitUnchecked
	}
	n, err := l.counter.Hit(ctx, l.name+":ip:"+client, time.Hour)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			l.pause(err)
		}
		return false, fmt.Errorf("%w: %w", ErrLimitUnchecked, err)
	}
	return n > l.perHour, nil
}

func (l *visitorLimit) pausedAfterFailure() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.now().Sub(l.failedAt) < recountAfter
}

func (l *visitorLimit) pause(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if now.Sub(l.failedAt) >= recountAfter {
		l.logger.Error("refusing visitors that cannot be counted", "limit", l.name, "err", err)
	}
	l.failedAt = now
}
