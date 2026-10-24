package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/httpx"
	"github.com/mirzanajafov/portalgun-router/internal/store"
)

type History interface {
	ConfigHistory(ctx context.Context, limit int) ([]store.ConfigVersion, error)
	Rollouts(ctx context.Context, limit int) ([]store.Rollout, error)
}

type API struct {
	cfgs     *ConfigManager
	engine   *Engine
	history  History
	hub      *Hub
	token    string
	latest   atomic.Pointer[Merged]
	extra    func(mux *http.ServeMux, admin func(http.HandlerFunc) http.HandlerFunc)
	visitors *visitorLimit
}

type RolloutLimits struct {
	PerHour int64
}

func DefaultRolloutLimits() RolloutLimits {
	return RolloutLimits{PerHour: 3}
}

var ErrRolloutLimit = errors.New("that is enough rollouts from this address for an hour")

func NewAPI(cfgs *ConfigManager, engine *Engine, history History, hub *Hub, token string) *API {
	return &API{cfgs: cfgs, engine: engine, history: history, hub: hub, token: token}
}

func (a *API) LetVisitorsRollOut(counter Counter, limits RolloutLimits, proxyToken string, logger *slog.Logger) {
	a.visitors = newVisitorLimit("rollout", limits.PerHour, counter, proxyToken, logger)
}

func (a *API) SetLatest(m Merged) { a.latest.Store(&m) }

func (a *API) Extend(fn func(mux *http.ServeMux, admin func(http.HandlerFunc) http.HandlerFunc)) {
	a.extra = fn
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
		respond(w, h, err)
	})
	mux.HandleFunc("GET /api/rollouts", func(w http.ResponseWriter, r *http.Request) {
		h, err := a.history.Rollouts(r.Context(), 20)
		respond(w, h, err)
	})
	mux.HandleFunc("GET /api/admin", a.admin(func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]bool{"admin": true})
	}))
	mux.HandleFunc("POST /api/rollouts", a.startRollout)
	mux.HandleFunc("POST /api/rollouts/abort", a.admin(a.abortRollout))
	if a.extra != nil {
		a.extra(mux, a.admin)
	}
	return mux
}

const adminOnly = "this needs the admin token"

func (a *API) admin(next http.HandlerFunc) http.HandlerFunc {
	return httpx.RequireBearer(a.token, adminOnly, next)
}

func uncounted(w http.ResponseWriter) {
	w.Header().Set("Retry-After", fmt.Sprint(int(recountAfter.Seconds())))
	httpx.WriteJSON(w, http.StatusServiceUnavailable, map[string]any{"error": ErrLimitUnchecked.Error(), "retryAfterSec": recountAfter.Seconds()})
}

func (a *API) putConfig(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	reason := r.Header.Get("X-Reason")
	if reason == "" {
		reason = "edited by an admin"
	}
	v, err := a.cfgs.Replace(r.Context(), store.ActorAdmin, reason, body)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]int64{"version": v})
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
		if a.engine.Status().Active {
			httpx.WriteError(w, http.StatusConflict, ErrBusy.Error())
			return
		}
		exceeded, err := a.visitors.exceeded(r.Context(), a.visitors.client(r))
		if err != nil {
			uncounted(w)
			return
		}
		if exceeded {
			w.Header().Set("Retry-After", fmt.Sprint(int(time.Hour.Seconds())))
			httpx.WriteJSON(w, http.StatusTooManyRequests, map[string]any{"error": ErrRolloutLimit.Error(), "retryAfterSec": time.Hour.Seconds()})
			return
		}
	}
	ro, err := a.engine.Start(r.Context(), req.Route, req.Canary, author)
	switch {
	case errors.Is(err, ErrBusy):
		httpx.WriteError(w, http.StatusConflict, err.Error())
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
	if req.Reason == "" {
		req.Reason = "aborted by an admin"
	}
	if err := a.engine.Abort(r.Context(), req.Reason); err != nil {
		httpx.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, a.engine.Status())
}

func (a *API) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(frame("state", a.State()))
	flusher.Flush()
	ch, done := a.hub.Subscribe()
	defer done()
	heartbeat := time.NewTicker(25 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
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

func respond(w http.ResponseWriter, v any, err error) {
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, v)
}
