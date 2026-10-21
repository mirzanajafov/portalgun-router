package controller

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/registry"
)

const reconcileEvery = 10 * time.Second

type Discovery struct {
	pool    string
	cfgs    *ConfigManager
	logger  *slog.Logger
	refused string
	skipped string
}

func NewDiscovery(pool string, cfgs *ConfigManager, logger *slog.Logger) (*Discovery, error) {
	cfg, _ := cfgs.Current()
	if poolNamed(cfg, pool) == nil {
		return nil, fmt.Errorf("-registry-pool %q names no pool in the config", pool)
	}
	return &Discovery{pool: pool, cfgs: cfgs, logger: logger}, nil
}

func (d *Discovery) Run(ctx context.Context, lists <-chan []registry.Instance) {
	t := time.NewTicker(reconcileEvery)
	defer t.Stop()
	var want []registry.Instance
	heard := false
	for {
		select {
		case <-ctx.Done():
			return
		case list, ok := <-lists:
			if !ok {
				return
			}
			want, heard = list, true
		case <-t.C:
		}
		if heard {
			d.reconcile(ctx, want)
		}
	}
}

func usable(in registry.Instance) bool {
	host, port, err := net.SplitHostPort(in.Addr)
	return in.Name != "" && err == nil && host != "" && port != ""
}

func dimensionsFrom(list []registry.Instance, current []config.Dimension) []config.Dimension {
	inflight := map[string]int{}
	for _, d := range current {
		inflight[d.ID] = d.MaxInflight
	}
	out := make([]config.Dimension, 0, len(list))
	for _, in := range list {
		if !usable(in) {
			continue
		}
		announced, _ := strconv.Atoi(in.Meta["maxInflight"])
		out = append(out, config.Dimension{
			ID:          in.Name,
			Address:     in.Addr,
			Version:     cmp.Or(in.Meta["version"], config.DefaultVersion),
			MaxInflight: cmp.Or(max(announced, 0), inflight[in.Name], config.DefaultMaxInflight),
		})
	}
	return out
}

func byID(a, b config.Dimension) int { return strings.Compare(a.ID, b.ID) }

func sameDimensions(a, b []config.Dimension) bool {
	return slices.Equal(slices.SortedFunc(slices.Values(a), byID), slices.SortedFunc(slices.Values(b), byID))
}

func describe(before, after []config.Dimension) string {
	old := map[string]config.Dimension{}
	for _, d := range before {
		old[d.ID] = d
	}
	var changes []string
	for _, d := range after {
		was, ok := old[d.ID]
		switch {
		case !ok:
			changes = append(changes, d.ID+" joined")
		case was != d:
			changes = append(changes, d.ID+" changed")
		}
		delete(old, d.ID)
	}
	for _, d := range slices.SortedFunc(maps.Values(old), byID) {
		changes = append(changes, d.ID+" left")
	}
	return strings.Join(changes, ", ")
}

func poolNamed(c *config.Config, name string) *config.Pool {
	for i := range c.Pools {
		if c.Pools[i].Name == name {
			return &c.Pools[i]
		}
	}
	return nil
}

func (d *Discovery) reconcile(ctx context.Context, list []registry.Instance) {
	d.reportSkipped(list)
	cfg, _ := d.cfgs.Current()
	pool := poolNamed(cfg, d.pool)
	if pool == nil {
		d.refuse(fmt.Sprintf("the config has no pool %q", d.pool), list)
		return
	}
	next := dimensionsFrom(list, pool.Dimensions)
	if sameDimensions(next, pool.Dimensions) {
		d.refused = ""
		return
	}
	reason := describe(pool.Dimensions, next)
	version, err := d.cfgs.Update(ctx, "registry", reason, func(c *config.Config) error {
		p := poolNamed(c, d.pool)
		if p == nil {
			return fmt.Errorf("the config has no pool %q", d.pool)
		}
		p.Dimensions = dimensionsFrom(list, p.Dimensions)
		return nil
	})
	if err != nil {
		if now, _ := d.cfgs.Current(); poolNamed(now, d.pool) != nil && sameDimensions(poolNamed(now, d.pool).Dimensions, next) {
			d.refused = ""
			d.logger.Warn("dimensions from the registry saved, but publishing them failed", "pool", d.pool, "reason", reason, "err", err)
			return
		}
		d.refuse(err.Error(), list)
		return
	}
	d.refused = ""
	d.logger.Info("dimensions from the registry applied", "pool", d.pool, "version", version, "reason", reason)
}

func (d *Discovery) reportSkipped(list []registry.Instance) {
	var bad []string
	for _, in := range list {
		if !usable(in) {
			bad = append(bad, fmt.Sprintf("%s=%q", in.Name, in.Addr))
		}
	}
	if s := strings.Join(bad, " "); s != d.skipped {
		d.skipped = s
		if s != "" {
			d.logger.Warn("registered instances without a host:port address are left out", "pool", d.pool, "instances", s)
		}
	}
}

func (d *Discovery) refuse(why string, list []registry.Instance) {
	if why == d.refused {
		return
	}
	d.refused = why
	d.logger.Warn("the registry's list is not applied, keeping the pool as it is", "pool", d.pool, "instances", len(list), "err", why)
}
