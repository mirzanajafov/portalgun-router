package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
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
	"github.com/mirzanajafov/portalgun-router/internal/registry"
	"github.com/mirzanajafov/portalgun-router/internal/store"
	"github.com/redis/go-redis/v9"
)

const registryWait = 10 * time.Second

func main() {
	addr := flag.String("addr", ":9100", "API listener")
	postgres := flag.String("postgres", os.Getenv("PORTAL_POSTGRES"), "Postgres URL")
	redisAddr := flag.String("redis", os.Getenv("PORTAL_REDIS"), "Redis address")
	bootstrap := flag.String("bootstrap", "", "config used when Postgres has none yet")
	toxiproxy := flag.String("toxiproxy", "", "Toxiproxy API URL for the chaos controls")
	targets := flag.String("targets", "", "comma-separated name=controlURL for every dimension chaos may touch")
	visitorRollouts := flag.Bool("visitor-rollouts", false, "let anyone start a rollout, a few an hour per address, while aborting still needs the admin token")
	registryURLs := flag.String("registry", os.Getenv("PORTAL_REGISTRY"), "comma-separated FiveFamilies HTTP endpoints to take a pool's dimensions from (off by default, when the config is the whole truth)")
	registryPool := flag.String("registry-pool", "dimensions", "the pool fed from the registry service of the same name")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	o := options{addr: *addr, postgres: *postgres, redis: *redisAddr, bootstrap: *bootstrap, toxiproxy: *toxiproxy, targets: *targets, visitorRollouts: *visitorRollouts, registry: *registryURLs, registryPool: *registryPool, token: os.Getenv("PORTAL_ADMIN_TOKEN"), proxyToken: os.Getenv("PORTAL_PROXY_TOKEN"), controlToken: os.Getenv("PORTAL_CONTROL_TOKEN")}
	if err := run(ctx, logger, o); err != nil {
		logger.Error("controller stopped", "err", err)
		os.Exit(1)
	}
}

type options struct {
	addr, postgres, redis, bootstrap, toxiproxy, targets, token string
	registry, registryPool, proxyToken, controlToken            string
	visitorRollouts                                             bool
}

func splitList(spec string) []string {
	var items []string
	for item := range strings.SplitSeq(spec, ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	return items
}

func chaosTargets(spec, token string) ([]string, map[string]*chaos.DimensionControl, error) {
	items := splitList(spec)
	if len(items) > 0 && token == "" {
		return nil, nil, errors.New("-targets needs PORTAL_CONTROL_TOKEN, which the dimensions' control ports ask for")
	}
	var names []string
	controls := map[string]*chaos.DimensionControl{}
	for _, item := range items {
		name, url, ok := strings.Cut(item, "=")
		if !ok || name == "" || url == "" {
			return nil, nil, fmt.Errorf("target %q is not name=controlURL", item)
		}
		names = append(names, name)
		controls[name] = &chaos.DimensionControl{BaseURL: url, Token: token, Client: &http.Client{Timeout: 2 * time.Second}}
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

	var rc *registry.Client
	if o.registry != "" {
		if err := registry.CheckName(o.registryPool); err != nil {
			return fmt.Errorf("-registry-pool: %w", err)
		}
		if rc, err = registry.New(splitList(o.registry)); err != nil {
			return err
		}
	}

	engine := controller.NewEngine(controller.DefaultEngineOptions(), cfgs, st, logger, hub.Event)
	api := controller.NewAPI(cfgs, engine, st, hub, token)
	if o.visitorRollouts {
		api.LetVisitorsRollOut(controller.RedisCounter{RDB: rdb}, controller.DefaultRolloutLimits(), o.proxyToken, logger)
	}
	var chaosSvc *controller.ChaosService
	if o.toxiproxy != "" {
		names, controls, err := chaosTargets(o.targets, o.controlToken)
		if err != nil {
			return err
		}
		inj := chaos.NewInjector(chaos.NewToxiproxy(o.toxiproxy), controls)
		chaosSvc = controller.NewChaosService(inj, st, controller.RedisCounter{RDB: rdb}, hub, controller.DefaultChaosLimits(), names, o.proxyToken, logger)
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
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	if rc != nil {
		go controller.NewDiscovery(o.registryPool, cfgs, logger).Run(ctx, rc.Watch(ctx, o.registryPool, registryWait, logger))
	}
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

	srv := &http.Server{Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	_, version := cfgs.Raw()
	logger.Info("controller up", "addr", ln.Addr().String(), "config", version, "admin", token != "", "registry", o.registry != "")
	select {
	case <-ctx.Done():
	case err := <-errc:
		return err
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
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
	_, err = cfgs.Update(ctx, store.ActorController, "restarted during a rollout: canary traffic set to zero", func(c *config.Config) error {
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
