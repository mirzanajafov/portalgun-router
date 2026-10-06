package limit

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type Pipeliner interface {
	Redis
	Pipeline() redis.Pipeliner
}

type syncKey struct {
	mu     sync.Mutex
	budget float64
	spent  float64
	demand float64
	idle   int
}

type localSync struct {
	rdb       Pipeliner
	prefix    string
	rule      Rule
	instances int
	instance  string
	interval  time.Duration
	timeout   time.Duration
	fallback  *Buckets
	now       Clock

	mu       sync.RWMutex
	keys     map[string]*syncKey
	degraded atomic.Bool
	syncs    atomic.Int64
	cancel   context.CancelFunc
	done     chan struct{}
}

func NewLocalSync(rdb Pipeliner, prefix string, rule Rule, instances int, instance string, interval, timeout time.Duration, now Clock) (Limiter, error) {
	if err := validate(rule); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &localSync{
		rdb:       rdb,
		prefix:    prefix,
		rule:      rule,
		instances: max(instances, 1),
		instance:  instance,
		interval:  interval,
		timeout:   max(timeout, interval/2),
		fallback:  NewBuckets(split(rule, instances)),
		now:       now,
		keys:      map[string]*syncKey{},
		cancel:    cancel,
		done:      make(chan struct{}),
	}
	go s.loop(ctx)
	return s, nil
}

func (s *localSync) key(k string) *syncKey {
	s.mu.RLock()
	st, ok := s.keys[k]
	s.mu.RUnlock()
	if ok {
		return st
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok = s.keys[k]; !ok {
		st = &syncKey{budget: s.rule.Burst / float64(s.instances)}
		s.keys[k] = st
	}
	return st
}

func (s *localSync) Allow(_ context.Context, key string) Decision {
	st := s.key(key)
	if s.degraded.Load() {
		ok, after := s.fallback.Take(key, s.now())
		st.mu.Lock()
		st.demand++
		if ok {
			st.spent++
		}
		st.mu.Unlock()
		return Decision{Allowed: ok, RetryAfter: after, Degraded: true}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.demand++
	if st.budget-st.spent >= 1 {
		st.spent++
		return Decision{Allowed: true}
	}
	return Decision{RetryAfter: s.interval}
}

func (s *localSync) Close() {
	s.cancel()
	<-s.done
}

func (s *localSync) Syncs() int64 { return s.syncs.Load() }

func (s *localSync) loop(ctx context.Context) {
	defer close(s.done)
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.sync(ctx)
		}
	}
}

type pending struct {
	key    string
	st     *syncKey
	spent  float64
	demand float64
	cmd    *redis.Cmd
}

func (s *localSync) sync(ctx context.Context) {
	s.mu.Lock()
	var batch []pending
	for k, st := range s.keys {
		st.mu.Lock()
		if st.spent == 0 && st.demand == 0 {
			st.idle++
			if st.idle > 10 {
				delete(s.keys, k)
			}
			st.mu.Unlock()
			continue
		}
		st.idle = 0
		batch = append(batch, pending{key: k, st: st, spent: st.spent, demand: st.demand})
		st.mu.Unlock()
	}
	s.mu.Unlock()
	if len(batch) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	s.send(ctx, batch)
	if slices.ContainsFunc(batch, func(p pending) bool { return redis.HasErrorPrefix(p.cmd.Err(), "NOSCRIPT") }) {
		if err := syncScript.Load(ctx, s.rdb).Err(); err == nil {
			s.send(ctx, batch)
		}
	}

	failed := false
	refill := s.rule.Rate * s.interval.Seconds()
	for _, p := range batch {
		vals, err := p.cmd.StringSlice()
		if err != nil || len(vals) != 2 {
			failed = true
			continue
		}
		level, err1 := strconv.ParseFloat(vals[0], 64)
		total, err2 := strconv.ParseFloat(vals[1], 64)
		if err1 != nil || err2 != nil {
			failed = true
			continue
		}
		share := 1 / float64(s.instances)
		if total > 0 {
			share = p.demand / total
		}
		p.st.mu.Lock()
		p.st.spent -= p.spent
		p.st.demand -= p.demand
		p.st.budget = share * max(0, level+refill)
		p.st.mu.Unlock()
	}
	s.degraded.Store(failed)
	if !failed {
		s.syncs.Add(1)
	}
}

func (s *localSync) send(ctx context.Context, batch []pending) {
	window := strconv.FormatFloat(float64(3*s.interval/time.Microsecond), 'f', 0, 64)
	ttl := ttlMillis(s.rule)
	pipe := s.rdb.Pipeline()
	for i := range batch {
		p := &batch[i]
		keys := []string{s.prefix + p.key, s.prefix + p.key + ":demand"}
		p.cmd = syncScript.EvalSha(ctx, pipe, keys, s.rule.Rate/1e6, s.rule.Burst, ttl, p.spent, s.instance, p.demand, window)
	}
	_, _ = pipe.Exec(ctx)
}
