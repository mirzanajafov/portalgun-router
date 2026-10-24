package proxy

import (
	"io"
	"net/http"
	"strconv"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
	"github.com/mirzanajafov/portalgun-router/internal/httpx"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func AdminHandler(g *Gateway, token string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+gatewayapi.PathHealthz, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.Handle("GET "+gatewayapi.PathMetrics, promhttp.HandlerFor(g.metrics.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET "+gatewayapi.PathState, func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, g.State())
	})
	mux.HandleFunc("GET "+gatewayapi.PathEvents, func(w http.ResponseWriter, r *http.Request) {
		var since int64
		if v := r.URL.Query().Get("since"); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				httpx.WriteError(w, http.StatusBadRequest, "since must be unix nanoseconds")
				return
			}
			since = n
		}
		httpx.WriteJSON(w, http.StatusOK, g.Events(since))
	})
	mux.HandleFunc("POST "+gatewayapi.PathDrain, func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, g.DrainDecisions())
	})
	mux.HandleFunc("GET "+gatewayapi.PathConfig, func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, g.Config())
	})
	mux.HandleFunc("PUT "+gatewayapi.PathConfig, httpx.RequireBearer(token, "config writes need the admin token", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		cfg, err := config.Parse(body)
		if err == nil {
			err = g.Apply(cfg)
		}
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]int64{"version": g.Version()})
	}))
	return mux
}
