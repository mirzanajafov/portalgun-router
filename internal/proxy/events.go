package proxy

import (
	"cmp"
	"slices"
	"sync"

	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
)

const eventLogSize = 4096

type eventLog struct {
	mu     sync.Mutex
	events []gatewayapi.DimensionEvent
}

func (l *eventLog) add(e gatewayapi.DimensionEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.events) >= eventLogSize {
		l.events = slices.Delete(l.events, 0, len(l.events)-eventLogSize+1)
	}
	l.events = append(l.events, e)
}

func (l *eventLog) since(unixNano int64) []gatewayapi.DimensionEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := []gatewayapi.DimensionEvent{}
	for _, e := range l.events {
		if e.UnixNano >= unixNano {
			out = append(out, e)
		}
	}
	slices.SortStableFunc(out, func(a, b gatewayapi.DimensionEvent) int { return cmp.Compare(a.UnixNano, b.UnixNano) })
	return out
}
