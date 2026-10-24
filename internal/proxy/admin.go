package proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/httpx"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func AdminHandler(g *Gateway, token string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(g.metrics.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /state", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, g.State())
	})
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		var since int64
		if v := r.URL.Query().Get("since"); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "since must be unix nanoseconds"})
				return
			}
			since = n
		}
		writeJSON(w, http.StatusOK, g.Events(since))
	})
	mux.HandleFunc("POST /decisions/drain", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, g.DrainDecisions())
	})
	mux.HandleFunc("GET /config", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, g.Config())
	})
	mux.HandleFunc("PUT /config", func(w http.ResponseWriter, r *http.Request) {
		if !httpx.Authorized(r, token) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "config writes need the admin token"})
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		cfg, err := config.Parse(body)
		if err == nil {
			err = g.Apply(cfg)
		}
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]int64{"version": g.Version()})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
