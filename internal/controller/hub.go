package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

const recentEvents = 50

type Hub struct {
	mu     sync.Mutex
	subs   map[chan []byte]struct{}
	events []Event
}

func NewHub() *Hub {
	return &Hub{subs: map[chan []byte]struct{}{}}
}

func frame(kind string, v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "event: %s\ndata: %s\n\n", kind, b)
	return buf.Bytes()
}

func (h *Hub) Subscribe() (<-chan []byte, func()) {
	ch := make(chan []byte, 16)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	backlog := append([]Event(nil), h.events...)
	h.mu.Unlock()
	for _, e := range backlog {
		select {
		case ch <- frame("event", e):
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
	f := frame(kind, v)
	if f == nil {
		return
	}
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
	h.mu.Lock()
	h.events = append(h.events, e)
	if len(h.events) > recentEvents {
		h.events = h.events[len(h.events)-recentEvents:]
	}
	h.mu.Unlock()
	h.Broadcast("event", e)
}

func (h *Hub) Events() []Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]Event(nil), h.events...)
}
