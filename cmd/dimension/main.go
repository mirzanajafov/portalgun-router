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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/config"
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
	advertise := flag.String("advertise", os.Getenv("PORTAL_ADVERTISE"), "address the gateways reach this dimension at, registered with -registry")
	ttl := flag.Duration("registry-ttl", 10*time.Second, "TTL of the registration's lease")
	maxInflight := flag.String("max-inflight", os.Getenv("PORTAL_MAX_INFLIGHT"), "requests in flight the gateways may send this dimension, registered with -registry (empty announces none)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var keep func(context.Context, func(bool))
	if *endpoints != "" {
		c, inst, err := registration(registrationFlags{endpoints: *endpoints, service: *service, id: *id, version: *version, advertise: *advertise, maxInflight: *maxInflight, ttl: *ttl})
		if err != nil {
			logger.Error("dimension not started", "err", err)
			os.Exit(2)
		}
		keep = func(ctx context.Context, held func(bool)) { c.Keep(ctx, *service, inst, *ttl, logger, held) }
	}
	control, err := net.Listen("tcp", *controlAddr)
	if err != nil {
		logger.Error("dimension not started", "err", err)
		os.Exit(1)
	}
	if err := run(ctx, logger, *id, *version, *addr, control, b, keep); err != nil {
		logger.Error("dimension stopped", "err", err)
		os.Exit(1)
	}
}

type registrationFlags struct {
	endpoints, service, id, version, advertise, maxInflight string
	ttl                                                     time.Duration
}

func registration(f registrationFlags) (*registry.Client, registry.Instance, error) {
	var urls []string
	for e := range strings.SplitSeq(f.endpoints, ",") {
		if e = strings.TrimSpace(e); e != "" {
			urls = append(urls, e)
		}
	}
	for _, name := range []string{f.service, f.id} {
		if err := registry.CheckName(name); err != nil {
			return nil, registry.Instance{}, fmt.Errorf("-registry-service and -id: %w", err)
		}
	}
	if !config.IsHostPort(f.advertise) {
		return nil, registry.Instance{}, fmt.Errorf("-registry needs -advertise as host:port, got %q", f.advertise)
	}
	if f.ttl < time.Millisecond || f.ttl > 24*time.Hour {
		return nil, registry.Instance{}, fmt.Errorf("-registry-ttl must be between 1ms and 24h, got %v", f.ttl)
	}
	inst := registry.Instance{Name: f.id, Addr: f.advertise, Meta: map[string]string{"version": f.version}}
	if f.maxInflight != "" {
		n, err := strconv.Atoi(f.maxInflight)
		if err != nil || n < 1 {
			return nil, registry.Instance{}, fmt.Errorf("-max-inflight must be a positive number, got %q", f.maxInflight)
		}
		inst.Meta["maxInflight"] = strconv.Itoa(n)
	}
	if err := registry.CheckSize(inst, f.ttl); err != nil {
		return nil, registry.Instance{}, err
	}
	c, err := registry.New(urls)
	return c, inst, err
}

const leaveDrain = time.Second

func run(ctx context.Context, logger *slog.Logger, id, version, addr string, cl net.Listener, b dimension.Behavior, keep func(context.Context, func(bool))) error {
	s, err := dimension.New(id, version, b)
	if err != nil {
		_ = cl.Close()
		return err
	}
	if err := s.Listen(addr); err != nil {
		_ = cl.Close()
		return err
	}
	kctx, stopKeeping := context.WithCancel(ctx)
	defer stopKeeping()
	kept := make(chan struct{})
	if keep == nil {
		close(kept)
	} else {
		s.SetLeased(false)
		go func() {
			keep(kctx, s.SetLeased)
			close(kept)
		}()
	}
	control := &http.Server{
		Handler:           s.ControlHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- control.Serve(cl) }()
	logger.Info("dimension up", "id", id, "version", version, "addr", s.Addr(), "control", cl.Addr().String())

	select {
	case <-ctx.Done():
	case err := <-errc:
		stopKeeping()
		<-kept
		_ = s.Shutdown(context.Background())
		return err
	}
	<-kept
	if keep != nil {
		time.Sleep(leaveDrain)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = errors.Join(s.Shutdown(shutdownCtx), control.Shutdown(shutdownCtx))
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
