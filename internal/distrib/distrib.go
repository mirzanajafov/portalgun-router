package distrib

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
	"github.com/redis/go-redis/v9"
)

const (
	ConfigKey      = "portal:config"
	ConfigChannel  = "portal:config:changed"
	SnapshotStream = "portal:snapshots"
	streamLength   = 7200
)

type Envelope struct {
	Version int64           `json:"version"`
	Config  json.RawMessage `json:"config"`
	Author  string          `json:"author,omitempty"`
	Reason  string          `json:"reason,omitempty"`
}

type Applier interface {
	Apply(*config.Config) error
	SetClusterVersion(int64)
	ClusterVersion() int64
}

func PublishConfig(ctx context.Context, rdb redis.UniversalClient, env Envelope) error {
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	_, err = rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, ConfigKey, b, 0)
		p.Publish(ctx, ConfigChannel, env.Version)
		return nil
	})
	return err
}

func FetchConfig(ctx context.Context, rdb redis.UniversalClient) (*Envelope, error) {
	b, err := rdb.Get(ctx, ConfigKey).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var env Envelope
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("config envelope: %w", err)
	}
	return &env, nil
}

type Watcher struct {
	rdb    *redis.Client
	target Applier
	logger *slog.Logger
	poll   time.Duration
}

func NewWatcher(rdb *redis.Client, target Applier, logger *slog.Logger, poll time.Duration) *Watcher {
	return &Watcher{rdb: rdb, target: target, logger: logger, poll: poll}
}

func (w *Watcher) Sync(ctx context.Context) error {
	env, err := FetchConfig(ctx, w.rdb)
	if err != nil || env == nil || env.Version <= w.target.ClusterVersion() {
		return err
	}
	cfg, err := config.Parse(env.Config)
	if err == nil {
		err = w.target.Apply(cfg)
	}
	if err != nil {
		w.logger.Error("cluster config rejected", "version", env.Version, "err", err)
		w.target.SetClusterVersion(env.Version)
		return err
	}
	w.target.SetClusterVersion(env.Version)
	w.logger.Info("cluster config applied", "version", env.Version, "author", env.Author, "reason", env.Reason)
	return nil
}

func (w *Watcher) Run(ctx context.Context) {
	sub := w.rdb.Subscribe(ctx, ConfigChannel)
	defer sub.Close()
	changes := sub.Channel()
	t := time.NewTicker(w.poll)
	defer t.Stop()
	_ = w.Sync(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-changes:
		case <-t.C:
		}
		_ = w.Sync(ctx)
	}
}

type Publisher struct {
	rdb    *redis.Client
	logger *slog.Logger
	queue  chan gatewayapi.Snapshot
}

func NewPublisher(rdb *redis.Client, logger *slog.Logger) *Publisher {
	return &Publisher{rdb: rdb, logger: logger, queue: make(chan gatewayapi.Snapshot, 8)}
}

func (p *Publisher) Offer(s gatewayapi.Snapshot) {
	select {
	case p.queue <- s:
	default:
	}
}

func (p *Publisher) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case s := <-p.queue:
			b, err := json.Marshal(s)
			if err != nil {
				continue
			}
			wctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			err = p.rdb.XAdd(wctx, &redis.XAddArgs{Stream: SnapshotStream, MaxLen: streamLength, Approx: true, Values: map[string]any{"s": b}}).Err()
			cancel()
			if err != nil {
				p.logger.Debug("snapshot not published", "err", err)
			}
		}
	}
}

func DecodeSnapshot(msg redis.XMessage) (gatewayapi.Snapshot, error) {
	var s gatewayapi.Snapshot
	raw, ok := msg.Values["s"].(string)
	if !ok {
		return s, errors.New("snapshot entry without a payload")
	}
	err := json.Unmarshal([]byte(raw), &s)
	return s, err
}
