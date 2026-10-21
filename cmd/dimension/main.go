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

	"github.com/mirzanajafov/portalgun-router/internal/dimension"
	"github.com/mirzanajafov/portalgun-router/internal/registry"
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
	endpoints := flag.String("registry", os.Getenv("PORTAL_REGISTRY"), "comma-separated FiveFamilies HTTP endpoints to register with once listening (off by default)")
	service := flag.String("registry-service", "dimensions", "service to register under")
	advertise := flag.String("advertise", "", "address the gateways reach this dimension at, registered with -registry")
	ttl := flag.Duration("registry-ttl", 10*time.Second, "TTL of the registration's lease")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var keep func(context.Context)
	if *endpoints != "" {
		c, err := registration(*endpoints, *service, *id, *advertise, *ttl)
		if err != nil {
			logger.Error("dimension not started", "err", err)
			os.Exit(2)
		}
		inst := registry.Instance{Name: *id, Addr: *advertise, Meta: map[string]string{"version": *version}}
		keep = func(ctx context.Context) { c.Keep(ctx, *service, inst, *ttl, logger) }
	}
	if err := run(ctx, logger, *id, *version, *addr, *controlAddr, b, keep); err != nil {
		logger.Error("dimension stopped", "err", err)
		os.Exit(1)
	}
}

func registration(endpoints, service, id, advertise string, ttl time.Duration) (*registry.Client, error) {
	var urls []string
	for e := range strings.SplitSeq(endpoints, ",") {
		if e = strings.TrimSpace(e); e != "" {
			urls = append(urls, e)
		}
	}
	for _, name := range []string{service, id} {
		if name == "" || strings.Contains(name, "/") {
			return nil, fmt.Errorf("-registry needs a -registry-service and an -id without a slash, got %q and %q", service, id)
		}
	}
	if host, port, err := net.SplitHostPort(advertise); err != nil || host == "" || port == "" {
		return nil, fmt.Errorf("-registry needs -advertise as host:port, got %q", advertise)
	}
	if ttl < time.Millisecond || ttl > 24*time.Hour {
		return nil, fmt.Errorf("-registry-ttl must be between 1ms and 24h, got %v", ttl)
	}
	return registry.New(urls)
}

func run(ctx context.Context, logger *slog.Logger, id, version, addr, controlAddr string, b dimension.Behavior, keep func(context.Context)) error {
	s, err := dimension.New(id, version, b)
	if err != nil {
		return err
	}
	if err := s.Listen(addr); err != nil {
		return err
	}
	cl, err := net.Listen("tcp", controlAddr)
	if err != nil {
		_ = s.Shutdown(context.Background())
		return err
	}
	kctx, stopKeeping := context.WithCancel(ctx)
	defer stopKeeping()
	kept := make(chan struct{})
	if keep == nil {
		close(kept)
	} else {
		go func() {
			keep(kctx)
			close(kept)
		}()
	}
	control := &http.Server{
		Handler:           s.ControlHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- control.Serve(cl) }()
	logger.Info("dimension up", "id", id, "version", version, "addr", s.Addr(), "control", controlAddr)

	select {
	case <-ctx.Done():
	case err := <-errc:
		stopKeeping()
		<-kept
		_ = s.Shutdown(context.Background())
		return err
	}
	<-kept
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = errors.Join(s.Shutdown(shutdownCtx), control.Shutdown(shutdownCtx))
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
