package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/mirzanajafov/portalgun-router/internal/httpx"
	"github.com/mirzanajafov/portalgun-router/internal/store"
)

type History interface {
	ConfigHistory(ctx context.Context, limit int) ([]store.ConfigVersion, error)
	Rollouts(ctx context.Context, limit int) ([]store.Rollout, error)
}

type APIOptions struct {
	Configs                *ConfigManager
	Engine                 *Engine
	History                History
	Hub                    *Hub
	Chaos                  *ChaosService
	Counter                Counter
	VisitorRolloutsPerHour int64
	Token                  string
	ProxyToken             string
	Logger                 *slog.Logger
}

type API struct {
	cfgs           *ConfigManager
	engine         *Engine
	history        History
	hub            *Hub
	chaos          *ChaosService
	visitors       *visitorLimit
	rolloutAddress *visitorAddress
	chaosAddress   *visitorAddress
	token          string
	latest         atomic.Pointer[Merged]
	logger         *slog.Logger
	historyLog     everyMinute
	stateLog       everyMinute
}

var ErrRolloutLimit = errors.New("that is enough rollouts from this address for an hour")

func NewAPI(o APIOptions) *API {
	a := &API{cfgs: o.Configs, engine: o.Engine, history: o.History, hub: o.Hub, chaos: o.Chaos, token: o.Token, logger: o.Logger}
	if o.VisitorRolloutsPerHour > 0 {
		a.visitors = newVisitorLimit("rollout", o.VisitorRolloutsPerHour, o.Counter, o.Logger)
		a.rolloutAddress = newVisitorAddress("rollout", o.ProxyToken, o.Logger)
	}
	if o.Chaos != nil {
		a.chaosAddress = newVisitorAddress("chaos", o.ProxyToken, o.Logger)
	}
	return a
}

type State struct {
	ConfigVersion int64   `json:"configVersion"`
	Latest        *Merged `json:"latest"`
	Rollout       Status  `json:"rollout"`
	Viewers       int     `json:"viewers"`
	Events        []Event `json:"events"`
}

func (a *API) State() State {
	_, v := a.cfgs.Raw()
	return State{ConfigVersion: v, Latest: a.latest.Load(), Rollout: a.engine.Status(), Viewers: a.hub.Viewers(), Events: a.hub.Events()}
}

type Tick struct {
	Latest  Merged     `json:"latest"`
	Rollout Status     `json:"rollout"`
	Viewers int        `json:"viewers"`
	Chaos   *ChaosView `json:"chaos,omitempty"`
}

func (a *API) Observe(ctx context.Context, m Merged) {
	a.latest.Store(&m)
	a.engine.Observe(ctx, m)
	tick := Tick{Latest: m, Rollout: a.engine.Status(), Viewers: a.hub.Viewers()}
	if a.chaos != nil {
		a.chaos.Observe(m)
		v := a.chaos.View()
		tick.Chaos = &v
	}
	a.hub.Broadcast("tick", tick)
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, _ *http.Request) { httpx.WriteJSON(w, http.StatusOK, a.State()) })
	mux.HandleFunc("GET /api/events", a.events)
	mux.HandleFunc("GET /api/config", func(w http.ResponseWriter, _ *http.Request) {
		raw, v := a.cfgs.Raw()
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"version": v, "config": raw})
	})
	mux.HandleFunc("PUT /api/config", a.admin(a.putConfig))
	mux.HandleFunc("GET /api/config/history", func(w http.ResponseWriter, r *http.Request) {
		h, err := a.history.ConfigHistory(r.Context(), 50)
		a.respond(w, r, h, err)
	})
	mux.HandleFunc("GET /api/rollouts", func(w http.ResponseWriter, r *http.Request) {
		h, err := a.history.Rollouts(r.Context(), 20)
		a.respond(w, r, h, err)
	})
	mux.HandleFunc("GET /api/admin", a.admin(func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]bool{"admin": true})
	}))
	mux.HandleFunc("POST /api/rollouts", a.startRollout)
	mux.HandleFunc("POST /api/rollouts/abort", a.admin(a.abortRollout))
	if a.chaos != nil {
		mux.HandleFunc("GET /api/chaos", func(w http.ResponseWriter, _ *http.Request) { httpx.WriteJSON(w, http.StatusOK, a.chaos.View()) })
		mux.HandleFunc("POST /api/chaos", a.startChaos)
		mux.HandleFunc("DELETE /api/chaos", a.admin(a.stopChaos))
	}
	return mux
}

const adminOnly = "this needs the admin token"

const maxReason = 200

var badReason = fmt.Sprintf("a reason is at most %d bytes of UTF-8 text with no control characters", maxReason)

func validReason(reason string) bool {
	return len(reason) <= maxReason && utf8.ValidString(reason) && !strings.ContainsFunc(reason, unicode.IsControl)
}

func (a *API) admin(next http.HandlerFunc) http.HandlerFunc {
	return httpx.RequireBearer(a.token, adminOnly, next)
}

func retryLater(w http.ResponseWriter, status int, err error, wait time.Duration) {
	w.Header().Set("Retry-After", strconv.Itoa(max(1, int(wait.Seconds()+0.5))))
	httpx.WriteJSON(w, status, map[string]any{"error": err.Error(), "retryAfterSec": wait.Seconds()})
}

func (a *API) putConfig(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	reason := r.Header.Get("X-Reason")
	if !validReason(reason) {
		httpx.WriteError(w, http.StatusBadRequest, badReason)
		return
	}
	if reason == "" {
		reason = "edited by an admin"
	}
	v, err := a.cfgs.Replace(r.Context(), store.ActorAdmin, reason, body)
	switch {
	case errors.Is(err, ErrConfigNotSaved):
		unavailable(w, a.logger, true, ErrConfigNotSaved, err)
	case err != nil:
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
	default:
		httpx.WriteJSON(w, http.StatusOK, map[string]int64{"version": v})
	}
}

func (a *API) startRollout(w http.ResponseWriter, r *http.Request) {
	author := store.ActorAdmin
	if !httpx.Authorized(r, a.token) {
		if a.visitors == nil {
			httpx.WriteError(w, http.StatusUnauthorized, adminOnly)
			return
		}
		author = store.ActorVisitor
	}
	var req struct {
		Route  string `json:"route"`
		Canary string `json:"canary"`
	}
	if err := decode(w, r, &req); err != nil {
		return
	}
	if author == store.ActorVisitor {
		if a.engine.Busy() {
			httpx.WriteError(w, http.StatusConflict, ErrBusy.Error())
			return
		}
		exceeded, err := a.visitors.exceeded(r.Context(), a.rolloutAddress.of(r))
		if err != nil {
			retryLater(w, http.StatusServiceUnavailable, ErrLimitUnchecked, recountAfter)
			return
		}
		if exceeded {
			retryLater(w, http.StatusTooManyRequests, ErrRolloutLimit, time.Hour)
			return
		}
	}
	ro, err := a.engine.Start(r.Context(), req.Route, req.Canary, author)
	switch {
	case errors.Is(err, ErrBusy):
		httpx.WriteError(w, http.StatusConflict, err.Error())
	case errors.Is(err, ErrRolloutFailed):
		unavailable(w, a.logger, author == store.ActorAdmin, ErrRolloutFailed, err)
	case err != nil:
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
	default:
		httpx.WriteJSON(w, http.StatusCreated, ro)
	}
}

func (a *API) abortRollout(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Reason string `json:"reason"`
	}
	if err := decode(w, r, &req); err != nil {
		return
	}
	if !validReason(req.Reason) {
		httpx.WriteError(w, http.StatusBadRequest, badReason)
		return
	}
	if req.Reason == "" {
		req.Reason = "aborted by an admin"
	}
	err := a.engine.Abort(r.Context(), req.Reason)
	switch {
	case errors.Is(err, ErrNoRollout):
		httpx.WriteError(w, http.StatusConflict, err.Error())
	case err != nil:
		unavailable(w, a.logger, true, ErrEndNotSaved, err)
	default:
		httpx.WriteJSON(w, http.StatusOK, a.engine.Status())
	}
}

func (a *API) startChaos(w http.ResponseWriter, r *http.Request) {
	var req ChaosRequest
	if err := decode(w, r, &req); err != nil {
		return
	}
	authorized, client := httpx.Authorized(r, a.token), ""
	if !authorized {
		client = a.chaosAddress.of(r)
	}
	run, wait, err := a.chaos.Start(r.Context(), req, authorized, client)
	switch {
	case errors.Is(err, ErrChaosBusy):
		retryLater(w, http.StatusConflict, err, wait)
	case errors.Is(err, ErrChaosCooldown), errors.Is(err, ErrChaosLimit):
		retryLater(w, http.StatusTooManyRequests, err, wait)
	case errors.Is(err, ErrLimitUnchecked):
		retryLater(w, http.StatusServiceUnavailable, ErrLimitUnchecked, recountAfter)
	case errors.Is(err, ErrChaosFailed):
		unavailable(w, a.logger, authorized, ErrChaosFailed, err, "mode", req.Mode, "target", req.Target)
	case err != nil:
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
	default:
		httpx.WriteJSON(w, http.StatusCreated, run)
	}
}

func (a *API) stopChaos(w http.ResponseWriter, r *http.Request) {
	if err := a.chaos.Stop(r.Context()); err != nil {
		httpx.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, a.chaos.View())
}

func (a *API) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		httpx.WriteError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if first, err := frame("state", a.State()); err != nil {
		if a.stateLog.allow(time.Now()) {
			a.logger.Error("the state for a new dashboard could not be sent", "err", err)
		}
	} else {
		_, _ = w.Write(first)
	}
	flusher.Flush()
	ch, done := a.hub.Subscribe()
	defer done()
	heartbeat := time.NewTicker(25 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-a.hub.Closed():
			return
		case f := <-ch:
			if _, err := w.Write(f); err != nil {
				return
			}
			flusher.Flush()
		case <-heartbeat.C:
			if _, err := io.WriteString(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func decode(w http.ResponseWriter, r *http.Request, v any) error {
	if err := httpx.ReadJSON(w, r, v); err != nil && !errors.Is(err, io.EOF) {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return err
	}
	return nil
}

var errHistory = errors.New("the history could not be read")

func (a *API) respond(w http.ResponseWriter, r *http.Request, v any, err error) {
	if err != nil {
		if !errors.Is(err, context.Canceled) && a.historyLog.allow(time.Now()) {
			a.logger.Error(errHistory.Error(), "err", err)
		}
		refuse(w, httpx.Authorized(r, a.token), errHistory, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, v)
}

func unavailable(w http.ResponseWriter, logger *slog.Logger, admin bool, public, err error, attrs ...any) {
	level := slog.LevelError
	if errors.Is(err, context.Canceled) {
		level = slog.LevelInfo
	}
	logger.Log(context.Background(), level, public.Error(), append(attrs, "err", err)...)
	refuse(w, admin, public, err)
}

func refuse(w http.ResponseWriter, admin bool, public, err error) {
	msg := public.Error()
	if admin {
		msg = err.Error()
	}
	httpx.WriteError(w, http.StatusServiceUnavailable, msg)
}
