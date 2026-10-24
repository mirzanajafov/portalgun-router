package controller

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const recentEvents = 50

type Hub struct {
	logger    *slog.Logger
	brokenLog everyMinute
	closed    chan struct{}
	closeOnce sync.Once

	mu      sync.Mutex
	subs    map[chan []byte]struct{}
	events  []Event
	backlog [][]byte
}

func NewHub(logger *slog.Logger) *Hub {
	return &Hub{logger: logger, closed: make(chan struct{}), subs: map[chan []byte]struct{}{}}
}

func frame(kind string, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encoding a %s frame: %w", kind, err)
	}
	return fmt.Appendf(nil, "event: %s\ndata: %s\n\n", kind, b), nil
}

func (h *Hub) Close() { h.closeOnce.Do(func() { close(h.closed) }) }

func (h *Hub) Closed() <-chan struct{} { return h.closed }

func (h *Hub) Subscribe() (<-chan []byte, func()) {
	ch := make(chan []byte, 16)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	backlog := append([][]byte(nil), h.backlog...)
	h.mu.Unlock()
	for _, f := range backlog {
		select {
		case ch <- f:
		default:
		}
	}
	return ch, func() {
		h.mu.Lock()
		delete(h.subs, ch)
		h.mu.Unlock()
	}
}

func (h *Hub) Viewers() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

func (h *Hub) Broadcast(kind string, v any) {
	f, err := frame(kind, v)
	if err != nil {
		h.broken(err)
		return
	}
	h.send(f)
}

func (h *Hub) broken(err error) {
	if h.brokenLog.allow(time.Now()) {
		h.logger.Error("an update for the dashboards could not be sent", "err", err)
	}
}

func (h *Hub) send(f []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- f:
		default:
		}
	}
}

func (h *Hub) Event(e Event) {
	if e.At.IsZero() {
		e.At = time.Now().UTC()
	}
	f, err := frame("event", e)
	if err != nil {
		h.broken(err)
		return
	}
	h.mu.Lock()
	h.events = append(h.events, e)
	h.backlog = append(h.backlog, f)
	if len(h.events) > recentEvents {
		h.events = h.events[len(h.events)-recentEvents:]
		h.backlog = h.backlog[len(h.backlog)-recentEvents:]
	}
	h.mu.Unlock()
	h.send(f)
}

func (h *Hub) Events() []Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]Event(nil), h.events...)
}
