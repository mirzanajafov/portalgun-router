package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/controller"
	"github.com/mirzanajafov/portalgun-router/internal/distrib"
	"github.com/mirzanajafov/portalgun-router/internal/store"
	"github.com/redis/go-redis/v9"
)

func main() {
	addr := flag.String("addr", ":9100", "API listener, reached only by the dashboard's server")
	postgres := flag.String("postgres", os.Getenv("PORTAL_POSTGRES"), "Postgres URL")
	redisAddr := flag.String("redis", os.Getenv("PORTAL_REDIS"), "Redis address")
	bootstrap := flag.String("bootstrap", "", "config used when Postgres has none yet")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, logger, *addr, *postgres, *redisAddr, *bootstrap, os.Getenv("PORTAL_ADMIN_TOKEN")); err != nil {
		logger.Error("controller stopped", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger, addr, postgres, redisAddr, bootstrapPath, token string) error {
	if postgres == "" || redisAddr == "" {
		return errors.New("-postgres and -redis are required")
	}
	st, err := store.Open(ctx, postgres)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer st.Close()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	var boot []byte
	if bootstrapPath != "" {
		if boot, err = os.ReadFile(bootstrapPath); err != nil {
			return err
		}
	}
	hub := controller.NewHub()
	publish := func(ctx context.Context, env distrib.Envelope) error { return distrib.PublishConfig(ctx, rdb, env) }
	cfgs := controller.NewConfigManager(st, publish, hub.Event)
	if err := cfgs.Load(ctx, boot); err != nil {
		return err
	}
	if err := recoverRollouts(ctx, st, cfgs, logger); err != nil {
		return err
	}

	engine := controller.NewEngine(controller.DefaultEngineOptions(), cfgs, st, logger, hub.Event)
	api := controller.NewAPI(cfgs, engine, st, hub, token)
	agg := controller.NewAggregator(func(m controller.Merged) {
		api.SetLatest(m)
		engine.Observe(ctx, m)
		hub.Broadcast("tick", map[string]any{"latest": m, "rollout": engine.Status(), "viewers": hub.Viewers()})
	})
	go controller.Consume(ctx, rdb, agg, logger)
	go controller.FlushLoop(ctx, agg)
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := cfgs.Republish(ctx); err != nil {
					logger.Warn("republishing config", "err", err)
				}
			}
		}
	}()

	srv := &http.Server{Addr: addr, Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	_, version := cfgs.Raw()
	logger.Info("controller up", "addr", addr, "config", version, "admin", token != "")
	select {
	case <-ctx.Done():
	case err := <-errc:
		return err
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func recoverRollouts(ctx context.Context, st *store.Store, cfgs *controller.ConfigManager, logger *slog.Logger) error {
	abandoned, err := st.AbandonActive(ctx, "the controller restarted during the rollout", time.Now().UTC())
	if err != nil {
		return err
	}
	cfg, _ := cfgs.Current()
	live := false
	for _, r := range cfg.Routes {
		live = live || (r.Versions.Canary != "" && r.Versions.Weight > 0)
	}
	if len(abandoned) == 0 && !live {
		return nil
	}
	logger.Warn("taking traffic off canaries left from before a restart", "rollouts", abandoned)
	_, err = cfgs.Update(ctx, "controller", "restarted during a rollout: canary traffic set to zero", func(c *config.Config) error {
		for i := range c.Routes {
			v := &c.Routes[i].Versions
			if v.Canary != "" {
				v.Canary, v.Weight = "", 0
			}
		}
		return nil
	})
	return err
}
