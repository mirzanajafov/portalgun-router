package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/netip"
	"sync"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/httpx"
)

type Counter interface {
	Hit(ctx context.Context, key string, window time.Duration) (int64, error)
}

const proxyTokenHeader = "X-Portal-Proxy-Token"

var ErrLimitUnchecked = errors.New("the visitor limits cannot be checked right now, so try again in a minute")

const recountAfter = time.Minute

type visitorAddress struct {
	limit       string
	proxyToken  string
	logger      *slog.Logger
	now         func() time.Time
	mismatchLog everyMinute
}

func newVisitorAddress(limit, proxyToken string, logger *slog.Logger) *visitorAddress {
	if proxyToken == "" {
		logger.Warn("no proxy token, so the limit ignores X-Forwarded-For and counts every caller by its own address", "limit", limit)
	}
	return &visitorAddress{limit: limit, proxyToken: proxyToken, logger: logger, now: time.Now}
}

func (v *visitorAddress) of(r *http.Request) string {
	if httpx.Carries(r, proxyTokenHeader, v.proxyToken) {
		if addr, err := netip.ParseAddr(httpx.ClientIP(r, 1)); err == nil {
			return addr.Unmap().String()
		}
	} else if r.Header.Get(proxyTokenHeader) != "" {
		v.mismatch()
	}
	return httpx.ClientIP(r, 0)
}

func (v *visitorAddress) mismatch() {
	if v.mismatchLog.allow(v.now()) {
		v.logger.Warn("a request carried a proxy token that doesn't match, so it was counted by its peer", "limit", v.limit)
	}
}

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
