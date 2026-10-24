package controller

import (
	"sync"
	"time"
)

type everyMinute struct {
	mu   sync.Mutex
	last time.Time
}

func (g *everyMinute) allow(now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.last.IsZero() && now.Sub(g.last) < time.Minute {
		return false
	}
	g.last = now
	return true
}
