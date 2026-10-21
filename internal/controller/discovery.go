package controller

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
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
	retry   time.Duration
	refused string
	skipped string
}

func NewDiscovery(pool string, cfgs *ConfigManager, logger *slog.Logger) *Discovery {
	return &Discovery{pool: pool, cfgs: cfgs, logger: logger, retry: reconcileEvery}
}

func (d *Discovery) Run(ctx context.Context, lists <-chan []registry.Instance) {
	t := time.NewTicker(d.retry)
	defer t.Stop()
	var want []registry.Instance
	for {
		select {
		case <-ctx.Done():
			return
		case list, ok := <-lists:
			if !ok {
				return
			}
			want = list
		case <-t.C:
			if d.refused == "" {
				continue
			}
		}
		if ctx.Err() == nil {
			d.reconcile(ctx, want)
		}
	}
}

func dimensionOf(in registry.Instance, kept int) config.Dimension {
	announced, err := strconv.Atoi(in.Meta["maxInflight"])
	if err != nil || announced < 0 {
		announced = 0
	}
	return config.Dimension{
		ID:          in.Name,
		Address:     in.Addr,
		Version:     cmp.Or(in.Meta["version"], config.DefaultVersion),
		MaxInflight: cmp.Or(announced, kept, config.DefaultMaxInflight),
	}
}

func usable(in registry.Instance) bool {
	return registry.CheckName(in.Name) == nil && dimensionOf(in, 0).Check() == nil
}

func dimensionsFrom(list []registry.Instance, current []config.Dimension) []config.Dimension {
	kept := map[string]int{}
	for _, d := range current {
		kept[d.ID] = d.MaxInflight
	}
	out := make([]config.Dimension, 0, len(list))
	for _, in := range list {
		if usable(in) {
			out = append(out, dimensionOf(in, kept[in.Name]))
		}
	}
	return out
}

func byDimensionID(a, b config.Dimension) int { return strings.Compare(a.ID, b.ID) }

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
	for _, d := range slices.SortedFunc(maps.Values(old), byDimensionID) {
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
	reason := describe(pool.Dimensions, dimensionsFrom(list, pool.Dimensions))
	if reason == "" {
		d.refused = ""
		return
	}
	version, err := d.cfgs.Update(ctx, "registry", reason, func(c *config.Config) error {
		p := poolNamed(c, d.pool)
		if p == nil {
			return fmt.Errorf("the config has no pool %q", d.pool)
		}
		p.Dimensions = dimensionsFrom(list, p.Dimensions)
		return nil
	})
	if err != nil {
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
			d.logger.Warn("registered instances the pool cannot take are left out", "pool", d.pool, "instances", s)
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
