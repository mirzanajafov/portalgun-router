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
	"strings"
	"syscall"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/chaos"
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
	toxiproxy := flag.String("toxiproxy", "", "Toxiproxy API URL for the chaos controls")
	targets := flag.String("targets", "", "comma-separated name=controlURL for every dimension chaos may touch")
	hops := flag.Int("trusted-hops", 1, "proxies in front of the controller whose X-Forwarded-For entries are trusted")
	visitorRollouts := flag.Bool("visitor-rollouts", false, "let anyone start a rollout, a few an hour per address; aborting still needs the admin token")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	o := options{addr: *addr, postgres: *postgres, redis: *redisAddr, bootstrap: *bootstrap, toxiproxy: *toxiproxy, targets: *targets, hops: *hops, visitorRollouts: *visitorRollouts, token: os.Getenv("PORTAL_ADMIN_TOKEN")}
	if err := run(ctx, logger, o); err != nil {
		logger.Error("controller stopped", "err", err)
		os.Exit(1)
	}
}

type options struct {
	addr, postgres, redis, bootstrap, toxiproxy, targets, token string
	hops                                                        int
	visitorRollouts                                             bool
}

func chaosTargets(spec string) ([]string, map[string]*chaos.DimensionControl, error) {
	var names []string
	controls := map[string]*chaos.DimensionControl{}
	for item := range strings.SplitSeq(spec, ",") {
		if item = strings.TrimSpace(item); item == "" {
			continue
		}
		name, url, ok := strings.Cut(item, "=")
		if !ok || name == "" || url == "" {
			return nil, nil, fmt.Errorf("target %q is not name=controlURL", item)
		}
		names = append(names, name)
		controls[name] = &chaos.DimensionControl{BaseURL: url, Client: &http.Client{Timeout: 2 * time.Second}}
	}
	return names, controls, nil
}

func run(ctx context.Context, logger *slog.Logger, o options) error {
	addr, postgres, redisAddr, bootstrapPath, token := o.addr, o.postgres, o.redis, o.bootstrap, o.token
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
	if o.visitorRollouts {
		api.LetVisitorsRollOut(controller.RedisCounter{RDB: rdb}, controller.DefaultRolloutLimits(), o.hops)
	}
	var chaosSvc *controller.ChaosService
	if o.toxiproxy != "" {
		names, controls, err := chaosTargets(o.targets)
		if err != nil {
			return err
		}
		inj := chaos.NewInjector(chaos.NewToxiproxy(o.toxiproxy), controls)
		chaosSvc = controller.NewChaosService(inj, st, controller.RedisCounter{RDB: rdb}, hub, controller.DefaultChaosLimits(), names, o.hops, logger)
		if err := chaosSvc.Reset(ctx); err != nil {
			logger.Warn("clearing faults left from before", "err", err)
		}
		api.Extend(func(mux *http.ServeMux, admin func(http.HandlerFunc) http.HandlerFunc) {
			chaosSvc.Routes(mux, admin, token)
		})
	}
	agg := controller.NewAggregator(func(m controller.Merged) {
		api.SetLatest(m)
		engine.Observe(ctx, m)
		tick := map[string]any{"latest": m, "rollout": engine.Status(), "viewers": hub.Viewers()}
		if chaosSvc != nil {
			chaosSvc.Observe(m)
			tick["chaos"] = chaosSvc.View()
		}
		hub.Broadcast("tick", tick)
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
