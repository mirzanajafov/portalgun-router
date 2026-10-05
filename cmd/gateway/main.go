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
	"github.com/mirzanajafov/portalgun-router/internal/distrib"
	"github.com/mirzanajafov/portalgun-router/internal/proxy"
	"github.com/redis/go-redis/v9"
)

func main() {
	configPath := flag.String("config", "/etc/portalgun/config.json", "route and pool config")
	addr := flag.String("addr", ":8080", "traffic listener")
	adminAddr := flag.String("admin-addr", ":9090", "admin listener, never published")
	redisAddr := flag.String("redis", os.Getenv("PORTAL_REDIS"), "Redis address for the shared rate limiters")
	instance := flag.String("instance", "", "name of this gateway instance, defaults to the hostname")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	redis.SetLogger(redisLog{logger})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	opts := options{config: *configPath, addr: *addr, admin: *adminAddr, token: os.Getenv("PORTAL_ADMIN_TOKEN"), redis: *redisAddr, instance: *instance}
	if err := run(ctx, logger, opts); err != nil {
		logger.Error("gateway stopped", "err", err)
		os.Exit(1)
	}
}

type redisLog struct{ l *slog.Logger }

func (r redisLog) Printf(_ context.Context, format string, v ...any) {
	r.l.Debug(fmt.Sprintf(format, v...), "component", "redis")
}

type options struct {
	config, addr, admin, token, redis, instance string
}

func run(ctx context.Context, logger *slog.Logger, o options) error {
	raw, err := os.ReadFile(o.config)
	if err != nil {
		return err
	}
	cfg, err := config.Parse(raw)
	if err != nil {
		return err
	}
	popts := proxy.Options{Logger: logger, Instance: o.instance}
	var rdb *redis.Client
	if o.redis != "" {
		rdb = redis.NewClient(&redis.Options{
			Addr:         o.redis,
			MaxRetries:   -1,
			DialTimeout:  time.Second,
			ReadTimeout:  time.Second,
			WriteTimeout: time.Second,
			PoolSize:     64,
		})
		defer rdb.Close()
		popts.Redis = rdb
	}
	g, err := proxy.New(cfg, popts)
	if err != nil {
		return err
	}
	defer g.Close()
	if rdb != nil {
		pub := distrib.NewPublisher(rdb, logger)
		g.Subscribe(pub.Offer)
		go pub.Run(ctx)
		go distrib.NewWatcher(rdb, g, logger, 10*time.Second).Run(ctx)
	}
	addr, adminAddr, token := o.addr, o.admin, o.token
	traffic := &http.Server{
		Addr:              addr,
		Handler:           g,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    64 << 10,
	}
	admin := &http.Server{
		Addr:              adminAddr,
		Handler:           proxy.AdminHandler(g, token),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errc := make(chan error, 2)
	go func() { errc <- traffic.ListenAndServe() }()
	go func() { errc <- admin.ListenAndServe() }()
	logger.Info("gateway up", "addr", addr, "admin", adminAddr, "routes", len(cfg.Routes), "config_writes", token != "", "redis", o.redis != "")

	select {
	case <-ctx.Done():
	case err := <-errc:
		return err
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = errors.Join(traffic.Shutdown(shutdownCtx), admin.Shutdown(shutdownCtx))
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
