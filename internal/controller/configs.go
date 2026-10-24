package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/distrib"
	"github.com/mirzanajafov/portalgun-router/internal/store"
)

var ErrConfigNotSaved = errors.New("the config could not be saved")

type ConfigStore interface {
	LatestConfig(ctx context.Context) (store.ConfigVersion, error)
	SaveConfig(ctx context.Context, author store.Actor, reason string, body json.RawMessage) (store.ConfigVersion, error)
}

type ConfigManager struct {
	store   ConfigStore
	publish func(context.Context, distrib.Envelope) error
	notify  func(Event)
	logger  *slog.Logger

	mu      sync.Mutex
	current *config.Config
	raw     json.RawMessage
	version int64
	author  store.Actor
	reason  string
}

func NewConfigManager(s ConfigStore, publish func(context.Context, distrib.Envelope) error, notify func(Event), logger *slog.Logger) *ConfigManager {
	return &ConfigManager{store: s, publish: publish, notify: notify, logger: logger}
}

func (m *ConfigManager) Load(ctx context.Context, bootstrap []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	latest, err := m.store.LatestConfig(ctx)
	if errors.Is(err, store.ErrNoConfig) {
		if bootstrap == nil {
			return errors.New("no config stored and no bootstrap config given")
		}
		return m.saveLocked(ctx, store.ActorBootstrap, "first config", bootstrap)
	}
	if err != nil {
		return err
	}
	cfg, err := config.Parse(latest.Body)
	if err != nil {
		return fmt.Errorf("stored config %d: %w", latest.Version, err)
	}
	m.current, m.raw, m.version, m.author, m.reason = cfg, latest.Body, latest.Version, latest.Author, latest.Reason
	if err := m.publishLocked(ctx); err != nil {
		m.logger.Warn("the stored config is not published yet, the next republish sends it", "err", err)
	}
	return nil
}

func (m *ConfigManager) saveLocked(ctx context.Context, author store.Actor, reason string, raw []byte) error {
	cfg, err := config.Parse(raw)
	if err != nil {
		return err
	}
	normalized, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	saved, err := m.store.SaveConfig(ctx, author, reason, normalized)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrConfigNotSaved, err)
	}
	m.current, m.raw, m.version, m.author, m.reason = cfg, normalized, saved.Version, author, reason
	if err := m.publishLocked(ctx); err != nil {
		m.logger.Warn("the config is saved but not published yet, the next republish sends it", "err", err)
	}
	if m.notify != nil {
		m.notify(Event{Type: "config", Message: fmt.Sprintf("config v%d by %s: %s", saved.Version, author, reason)})
	}
	return nil
}

func (m *ConfigManager) publishLocked(ctx context.Context) error {
	if err := m.publish(ctx, distrib.Envelope{Version: m.version, Config: m.raw, Author: string(m.author), Reason: m.reason}); err != nil {
		return fmt.Errorf("publishing config %d: %w", m.version, err)
	}
	return nil
}

func (m *ConfigManager) Replace(ctx context.Context, author store.Actor, reason string, raw []byte) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.saveLocked(ctx, author, reason, raw); err != nil {
		return 0, err
	}
	return m.version, nil
}

func (m *ConfigManager) Update(ctx context.Context, author store.Actor, reason string, change func(*config.Config) error) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, err := m.changedLocked(change)
	if err != nil {
		return 0, err
	}
	if err := m.saveLocked(ctx, author, reason, raw); err != nil {
		return 0, err
	}
	return m.version, nil
}

func (m *ConfigManager) Check(change func(*config.Config) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, err := m.changedLocked(change)
	if err != nil {
		return err
	}
	_, err = config.Parse(raw)
	return err
}

func (m *ConfigManager) changedLocked(change func(*config.Config) error) ([]byte, error) {
	next, err := clone(m.current)
	if err != nil {
		return nil, err
	}
	if err := change(next); err != nil {
		return nil, err
	}
	return json.Marshal(next)
}

func (m *ConfigManager) Current() (*config.Config, int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, _ := clone(m.current)
	return c, m.version
}

func (m *ConfigManager) Raw() (json.RawMessage, int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.raw, m.version
}

func (m *ConfigManager) Republish(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil {
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
