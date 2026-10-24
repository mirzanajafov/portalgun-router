package dimension

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/httpx"
)

const (
	IDHeader      = "X-Dimension-Id"
	VersionHeader = "X-Dimension-Version"

	maxBodyBytes = 16 << 20
	maxDelay     = time.Minute
)

var (
	octetStream = []string{"application/octet-stream"}
	textPlain   = []string{"text/plain; charset=utf-8"}
)

type Behavior struct {
	BodyBytes   int     `json:"bodyBytes"`
	DelayMs     float64 `json:"delayMs"`
	DelayDist   string  `json:"delayDist"`
	DelaySigma  float64 `json:"delaySigma"`
	ErrorRate   float64 `json:"errorRate"`
	ErrorStatus int     `json:"errorStatus"`
	Hang        bool    `json:"hang"`
	Healthy     bool    `json:"healthy"`
}

func DefaultBehavior() Behavior {
	return Behavior{
		BodyBytes:   1024,
		DelayDist:   "fixed",
		DelaySigma:  0.5,
		ErrorStatus: http.StatusServiceUnavailable,
		Healthy:     true,
	}
}

func (b Behavior) Validate() error {
	switch {
	case b.BodyBytes < 0 || b.BodyBytes > maxBodyBytes:
		return fmt.Errorf("bodyBytes must be between 0 and %d", maxBodyBytes)
	case !(b.DelayMs >= 0 && b.DelayMs <= float64(maxDelay/time.Millisecond)):
		return fmt.Errorf("delayMs must be between 0 and %d", maxDelay/time.Millisecond)
	case b.DelayDist != "fixed" && b.DelayDist != "lognormal":
		return errors.New("delayDist must be fixed or lognormal")
	case !(b.DelaySigma >= 0 && b.DelaySigma <= 3):
		return errors.New("delaySigma must be between 0 and 3")
	case !(b.ErrorRate >= 0 && b.ErrorRate <= 1):
		return errors.New("errorRate must be between 0 and 1")
	case b.ErrorStatus < 400 || b.ErrorStatus > 599:
		return errors.New("errorStatus must be between 400 and 599")
	}
	return nil
}

type Patch struct {
	BodyBytes   *int     `json:"bodyBytes"`
	DelayMs     *float64 `json:"delayMs"`
	DelayDist   *string  `json:"delayDist"`
	DelaySigma  *float64 `json:"delaySigma"`
	ErrorRate   *float64 `json:"errorRate"`
	ErrorStatus *int     `json:"errorStatus"`
	Hang        *bool    `json:"hang"`
	Healthy     *bool    `json:"healthy"`
	Listening   *bool    `json:"listening"`
}

func (p Patch) apply(b Behavior) Behavior {
	if p.BodyBytes != nil {
		b.BodyBytes = *p.BodyBytes
	}
	if p.DelayMs != nil {
		b.DelayMs = *p.DelayMs
	}
	if p.DelayDist != nil {
		b.DelayDist = *p.DelayDist
	}
	if p.DelaySigma != nil {
		b.DelaySigma = *p.DelaySigma
	}
	if p.ErrorRate != nil {
		b.ErrorRate = *p.ErrorRate
	}
	if p.ErrorStatus != nil {
		b.ErrorStatus = *p.ErrorStatus
	}
	if p.Hang != nil {
		b.Hang = *p.Hang
	}
	if p.Healthy != nil {
		b.Healthy = *p.Healthy
	}
	return b
}

type View struct {
	ID        string `json:"id"`
	Version   string `json:"version"`
	Listening bool   `json:"listening"`
	Behavior
}

type state struct {
	b             Behavior
	delay         time.Duration
	body          []byte
	contentLength []string
}

func newState(b Behavior) *state {
	body := make([]byte, b.BodyBytes)
	for i := range body {
		body[i] = 'a' + byte(i%26)
	}
	return &state{
		b:             b,
		delay:         time.Duration(b.DelayMs * float64(time.Millisecond)),
		body:          body,
		contentLength: []string{strconv.Itoa(b.BodyBytes)},
	}
}

func (st *state) sampleDelay() time.Duration {
	if st.delay == 0 || st.b.DelayDist == "fixed" {
		return st.delay
	}
	d := float64(st.delay) * math.Exp(st.b.DelaySigma*rand.NormFloat64())
	return time.Duration(min(d, float64(maxDelay)))
}

type Server struct {
	id            string
	version       string
	idHeader      []string
	versionHeader []string
	initial       Behavior
	state         atomic.Pointer[state]
	awaitingLease atomic.Bool

	mu   sync.Mutex
	addr string
	data *http.Server
}

func New(id, version string, b Behavior) (*Server, error) {
	if id == "" {
		return nil, errors.New("dimension id is required")
	}
	if version == "" {
		return nil, errors.New("dimension version is required")
	}
	if err := b.Validate(); err != nil {
		return nil, err
	}
	s := &Server{
		id:            id,
		version:       version,
		idHeader:      []string{id},
		versionHeader: []string{version},
		initial:       b,
	}
	s.state.Store(newState(b))
	return s, nil
}

func (s *Server) Listen(addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listenLocked(addr)
}

func (s *Server) listenLocked(addr string) error {
	if s.data != nil {
		return nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.addr = ln.Addr().String()
	srv := &http.Server{
		Handler:           http.HandlerFunc(s.serve),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	s.data = srv
	go func() { _ = srv.Serve(ln) }()
	return nil
}

func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		return nil
	}
	srv := s.data
	s.data = nil
	if err := srv.Shutdown(ctx); err != nil {
		return srv.Close()
	}
	return nil
}

func (s *Server) SetLeased(held bool) { s.awaitingLease.Store(!held) }

func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.serve)
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	st := s.state.Load()
	h := w.Header()
	h[IDHeader] = s.idHeader
	h[VersionHeader] = s.versionHeader
	_, _ = io.Copy(io.Discard, r.Body)
	if st.b.Hang {
		<-r.Context().Done()
		return
	}
	if r.URL.Path == "/healthz" {
		h["Content-Type"] = textPlain
		if !st.b.Healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "unhealthy\n")
			return
		}
		_, _ = io.WriteString(w, "ok\n")
		return
	}
	if d := st.sampleDelay(); d > 0 && !sleep(r.Context(), d) {
		return
	}
	if st.b.ErrorRate > 0 && rand.Float64() < st.b.ErrorRate {
		h["Content-Type"] = textPlain
		w.WriteHeader(st.b.ErrorStatus)
		_, _ = io.WriteString(w, "injected error\n")
		return
	}
	h["Content-Type"] = octetStream
	h["Content-Length"] = st.contentLength
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(st.body)
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *Server) ControlHandler(token string) http.Handler {
	control := func(next http.HandlerFunc) http.HandlerFunc {
		return httpx.RequireBearer(token, "this needs the control token", next)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ready", s.ready)
	mux.HandleFunc("GET /state", control(s.getState))
	mux.HandleFunc("PATCH /state", control(s.patchState))
	mux.HandleFunc("POST /reset", control(s.reset))
	return mux
}

func (s *Server) view() View {
	return View{
		ID:        s.id,
		Version:   s.version,
		Listening: s.data != nil,
		Behavior:  s.state.Load().b,
	}
}

func (s *Server) ready(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	listening := s.data != nil
	s.mu.Unlock()
	switch {
	case !listening:
		http.Error(w, "not listening", http.StatusServiceUnavailable)
	case s.awaitingLease.Load():
		http.Error(w, "not registered", http.StatusServiceUnavailable)
	default:
		_, _ = io.WriteString(w, "ready\n")
	}
}

func (s *Server) getState(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	v := s.view()
	s.mu.Unlock()
	httpx.WriteJSON(w, http.StatusOK, v)
}

func (s *Server) patchState(w http.ResponseWriter, r *http.Request) {
	var p Patch
	if err := httpx.ReadJSON(w, r, &p); err != nil {
		httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b := p.apply(s.state.Load().b)
	if err := b.Validate(); err != nil {
		httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if p.Listening != nil {
		if err := s.setListeningLocked(*p.Listening); err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
	}
	s.state.Store(newState(b))
	httpx.WriteJSON(w, http.StatusOK, s.view())
}

func (s *Server) reset(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Store(newState(s.initial))
	if err := s.setListeningLocked(true); err != nil {
		httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	httpx.WriteJSON(w, http.StatusOK, s.view())
}

func (s *Server) setListeningLocked(on bool) error {
	if on {
		return s.listenLocked(s.addr)
	}
	if s.data == nil {
		return nil
	}
	err := s.data.Close()
	s.data = nil
	return err
}
