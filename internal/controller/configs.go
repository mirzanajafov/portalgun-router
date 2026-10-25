package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/distrib"
	"github.com/mirzanajafov/portalgun-router/internal/store"
)

var ErrConfigNotSaved = errors.New("the config could not be saved")

type ConfigStore interface {
	LatestConfig(ctx context.Context) (store.ConfigVersion, error)
	SaveConfig(ctx context.Context, c store.ConfigChange) (store.ConfigVersion, error)
}

type saver func(context.Context, store.ConfigChange) (store.ConfigVersion, error)

type savedConfig struct {
	cfg     *config.Config
	raw     json.RawMessage
	version int64
	author  store.Actor
	reason  string
}

type ConfigManager struct {
	store   ConfigStore
	publish func(context.Context, distrib.Envelope) error
	notify  func(Event)
	logger  *slog.Logger

	mu    sync.Mutex
	saved atomic.Pointer[savedConfig]
}

func NewConfigManager(s ConfigStore, publish func(context.Context, distrib.Envelope) error, notify func(Event), logger *slog.Logger) *ConfigManager {
	m := &ConfigManager{store: s, publish: publish, notify: notify, logger: logger}
	m.saved.Store(&savedConfig{})
	return m
}

func (m *ConfigManager) Load(ctx context.Context, bootstrap []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	latest, err := m.store.LatestConfig(ctx)
	if errors.Is(err, store.ErrNoConfig) {
		if bootstrap == nil {
			return errors.New("no config stored and no bootstrap config given")
		}
		return m.saveLocked(ctx, store.ConfigChange{Author: store.ActorBootstrap, Reason: "first config", Body: bootstrap}, m.store.SaveConfig)
	}
	if err != nil {
		return err
	}
	cfg, err := config.Parse(latest.Body)
	if err != nil {
		return fmt.Errorf("stored config %d: %w", latest.Version, err)
	}
	m.saved.Store(&savedConfig{cfg: cfg, raw: latest.Body, version: latest.Version, author: latest.Author, reason: latest.Reason})
	if err := m.publishLocked(ctx); err != nil {
		m.logger.Warn("the stored config is not published yet, the next republish sends it", "err", err)
	}
	return nil
}

func (m *ConfigManager) saveLocked(ctx context.Context, c store.ConfigChange, save saver) error {
	ctx = context.WithoutCancel(ctx)
	cfg, err := config.Parse(c.Body)
	if err != nil {
		return err
	}
	if c.Body, err = json.Marshal(cfg); err != nil {
		return err
	}
	saved, err := save(ctx, c)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrConfigNotSaved, err)
	}
	m.saved.Store(&savedConfig{cfg: cfg, raw: c.Body, version: saved.Version, author: c.Author, reason: c.Reason})
	if err := m.publishLocked(ctx); err != nil {
		m.logger.Warn("the config is saved but not published yet, the next republish sends it", "err", err)
	}
	if m.notify != nil {
		m.notify(Event{Type: "config", Message: fmt.Sprintf("config v%d by %s: %s", saved.Version, c.Author, c.Reason)})
	}
	return nil
}

func (m *ConfigManager) publishLocked(ctx context.Context) error {
	s := m.saved.Load()
	if err := m.publish(ctx, distrib.Envelope{Version: s.version, Config: s.raw, Author: string(s.author), Reason: s.reason}); err != nil {
		return fmt.Errorf("publishing config %d: %w", s.version, err)
	}
	return nil
}

func (m *ConfigManager) Replace(ctx context.Context, author store.Actor, reason string, raw []byte) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.saveLocked(ctx, store.ConfigChange{Author: author, Reason: reason, Body: raw}, m.store.SaveConfig); err != nil {
		return 0, err
	}
	return m.saved.Load().version, nil
}

func (m *ConfigManager) Update(ctx context.Context, author store.Actor, reason string, change func(*config.Config) error) (int64, error) {
	return m.commit(ctx, author, reason, change, m.store.SaveConfig)
}

func (m *ConfigManager) commit(ctx context.Context, author store.Actor, reason string, change func(*config.Config) error, save saver) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	next, err := clone(m.saved.Load().cfg)
	if err != nil {
		return 0, err
	}
	if err := change(next); err != nil {
		return 0, err
	}
	raw, err := json.Marshal(next)
	if err != nil {
		return 0, err
	}
	if err := m.saveLocked(ctx, store.ConfigChange{Author: author, Reason: reason, Body: raw}, save); err != nil {
		return 0, err
	}
	return m.saved.Load().version, nil
}

func (m *ConfigManager) Current() (*config.Config, int64) {
	s := m.saved.Load()
	c, _ := clone(s.cfg)
	return c, s.version
}

func (m *ConfigManager) Raw() (json.RawMessage, int64) {
	s := m.saved.Load()
	return s.raw, s.version
}

func (m *ConfigManager) Republish(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saved.Load().cfg == nil {
		return nil
	}
	return m.publishLocked(ctx)
}

func clone(c *config.Config) (*config.Config, error) {
	if c == nil {
		return nil, errors.New("no config loaded")
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	return config.Parse(raw)
}

func findRoute(c *config.Config, name string) (*config.Route, *config.Pool, error) {
	for i := range c.Routes {
		if c.Routes[i].Name != name {
			continue
		}
		for j := range c.Pools {
			if c.Pools[j].Name == c.Routes[i].Pool {
				return &c.Routes[i], &c.Pools[j], nil
			}
		}
	}
	return nil, nil, fmt.Errorf("no route %q", name)
}
