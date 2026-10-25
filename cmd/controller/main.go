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
	"github.com/mirzanajafov/portalgun-router/internal/controller"
	"github.com/mirzanajafov/portalgun-router/internal/distrib"
	"github.com/mirzanajafov/portalgun-router/internal/registry"
	"github.com/mirzanajafov/portalgun-router/internal/store"
	"github.com/redis/go-redis/v9"
)

const (
	registryWait           = 10 * time.Second
	visitorRolloutsPerHour = 3
)

func main() {
	addr := flag.String("addr", ":9100", "API listener")
	postgres := flag.String("postgres", os.Getenv("PORTAL_POSTGRES"), "Postgres URL")
	redisAddr := flag.String("redis", os.Getenv("PORTAL_REDIS"), "Redis address")
	bootstrap := flag.String("bootstrap", "", "config used when Postgres has none yet")
	toxiproxy := flag.String("toxiproxy", "", "Toxiproxy API URL for the chaos controls")
	targets := flag.String("targets", "", "comma-separated name=controlURL for every dimension chaos may touch, with PORTAL_CONTROL_TOKEN set")
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

func chaosInjector(ctx context.Context, o options, logger *slog.Logger) (*chaos.Injector, []string, error) {
	if o.toxiproxy == "" {
		return nil, nil, nil
	}
	names, controls, err := chaosTargets(o.targets, o.controlToken)
	if err != nil {
		return nil, nil, err
	}
	inj := chaos.NewInjector(chaos.NewToxiproxy(o.toxiproxy), controls)
	err = inj.ClearAll(ctx)
	if errors.Is(err, chaos.ErrControlToken) {
		return nil, nil, err
	}
	if err != nil {
		logger.Warn("clearing faults left from before", "err", err)
	}
	return inj, names, nil
}

func run(ctx context.Context, logger *slog.Logger, o options) error {
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	addr, postgres, redisAddr, bootstrapPath, token := o.addr, o.postgres, o.redis, o.bootstrap, o.token
	if postgres == "" || redisAddr == "" {
		return errors.New("-postgres and -redis are required")
	}
	inj, names, err := chaosInjector(ctx, o, logger)
	if err != nil {
		return err
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
	hub := controller.NewHub(logger)
	publish := func(ctx context.Context, env distrib.Envelope) error { return distrib.PublishConfig(ctx, rdb, env) }
	cfgs := controller.NewConfigManager(st, publish, hub.Event, logger)
	if err := cfgs.Load(ctx, boot); err != nil {
		return err
	}
	engine := controller.NewEngine(controller.DefaultEngineOptions(), cfgs, st, logger, hub.Event)
	if err := engine.Recover(ctx); err != nil {
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

	settled := make(chan struct{})
	go func() {
		defer close(settled)
		engine.Run(ctx)
	}()
	counter := controller.RedisCounter{RDB: rdb}
	apiOpts := controller.APIOptions{Configs: cfgs, Engine: engine, History: st, Hub: hub, Counter: counter, Token: token, ProxyToken: o.proxyToken, Logger: logger}
	if o.visitorRollouts {
		apiOpts.VisitorRolloutsPerHour = visitorRolloutsPerHour
	}
	if inj != nil {
		apiOpts.Chaos = controller.NewChaosService(controller.ChaosOptions{Injector: inj, Log: st, Counter: counter, Limits: controller.DefaultChaosLimits(), Targets: names, ProxyToken: o.proxyToken, Notify: hub.Event, Logger: logger})
	}
	api := controller.NewAPI(apiOpts)
	agg := controller.NewAggregator(func(m controller.Merged) {
		api.SetLatest(m)
		engine.Observe(ctx, m)
		tick := map[string]any{"latest": m, "rollout": engine.Status(), "viewers": hub.Viewers()}
		if apiOpts.Chaos != nil {
			apiOpts.Chaos.Observe(m)
			tick["chaos"] = apiOpts.Chaos.View()
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
	srv.RegisterOnShutdown(hub.Close)
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	_, version := cfgs.Raw()
	logger.Info("controller up", "addr", ln.Addr().String(), "config", version, "admin", token != "", "registry", o.registry != "")
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-errc:
		stop()
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = errors.Join(serveErr, srv.Shutdown(shutdownCtx))
	<-settled
	return err
}
