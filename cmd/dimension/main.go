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

	"github.com/mirzanajafov/portalgun-router/internal/dimension"
)

func main() {
	b := dimension.DefaultBehavior()
	id := flag.String("id", "", "dimension id, sent back in X-Dimension-Id")
	version := flag.String("version", "v1", "dimension version, sent back in X-Dimension-Version")
	addr := flag.String("addr", ":8080", "traffic listener")
	controlAddr := flag.String("control-addr", ":8081", "control listener")
	flag.IntVar(&b.BodyBytes, "body", b.BodyBytes, "response body size in bytes")
	flag.Float64Var(&b.DelayMs, "delay-ms", b.DelayMs, "service time in milliseconds (median for lognormal)")
	flag.StringVar(&b.DelayDist, "delay-dist", b.DelayDist, "fixed or lognormal")
	flag.Float64Var(&b.DelaySigma, "delay-sigma", b.DelaySigma, "sigma of the lognormal service time")
	flag.Float64Var(&b.ErrorRate, "error-rate", b.ErrorRate, "share of requests answered with error-status")
	flag.IntVar(&b.ErrorStatus, "error-status", b.ErrorStatus, "status code for injected errors")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, logger, *id, *version, *addr, *controlAddr, b); err != nil {
		logger.Error("dimension stopped", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger, id, version, addr, controlAddr string, b dimension.Behavior) error {
	s, err := dimension.New(id, version, b)
	if err != nil {
		return err
	}
	if err := s.Listen(addr); err != nil {
		return err
	}
	control := &http.Server{
		Addr:              controlAddr,
		Handler:           s.ControlHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- control.ListenAndServe() }()
	logger.Info("dimension up", "id", id, "version", version, "addr", s.Addr(), "control", controlAddr)

	select {
	case <-ctx.Done():
	case err := <-errc:
		_ = s.Shutdown(context.Background())
		return err
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = errors.Join(s.Shutdown(shutdownCtx), control.Shutdown(shutdownCtx))
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
