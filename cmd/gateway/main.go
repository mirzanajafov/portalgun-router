package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/proxy"
)

func main() {
	configPath := flag.String("config", "/etc/portalgun/config.json", "route and pool config")
	addr := flag.String("addr", ":8080", "traffic listener")
	adminAddr := flag.String("admin-addr", ":9090", "admin listener, never published")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, logger, *configPath, *addr, *adminAddr, os.Getenv("PORTAL_ADMIN_TOKEN")); err != nil {
		logger.Error("gateway stopped", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger, configPath, addr, adminAddr, token string) error {
	raw, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}
	cfg, err := config.Parse(raw)
	if err != nil {
		return err
	}
	g, err := proxy.New(cfg, proxy.Options{})
	if err != nil {
		return err
	}
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
	logger.Info("gateway up", "addr", addr, "admin", adminAddr, "routes", len(cfg.Routes), "config_writes", token != "")

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
