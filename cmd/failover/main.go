package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/chaos"
	"github.com/mirzanajafov/portalgun-router/internal/failover"
)

func main() {
	c := failover.Cell{Controls: map[string]string{}, Token: os.Getenv("PORTAL_CONTROL_TOKEN"), Options: failover.DefaultOptions()}
	flag.StringVar(&c.Label, "label", "", "label copied into the report")
	flag.Func("url", "gateway traffic URL, repeatable", func(v string) error {
		c.Gateways = append(c.Gateways, v)
		return nil
	})
	flag.Func("admin", "gateway admin URL, repeatable", func(v string) error {
		c.Admins = append(c.Admins, strings.TrimSuffix(v, "/"))
		return nil
	})
	flag.StringVar(&c.Toxiproxy, "toxiproxy", "http://toxiproxy:8474", "toxiproxy API")
	flag.Func("control", "name=URL of a dimension's control API, repeatable", func(v string) error {
		name, u, ok := strings.Cut(v, "=")
		if !ok || name == "" || u == "" {
			return errors.New("expected name=URL")
		}
		c.Controls[name] = strings.TrimSuffix(u, "/")
		return nil
	})
	mode := flag.String("mode", "crash", "none, crash, reset, blackhole, slow, partial or flapping")
	flag.StringVar(&c.Fault.Target, "target", "a", "dimension to break, or all")
	flag.DurationVar(&c.Fault.Latency, "latency", 500*time.Millisecond, "added latency in slow mode")
	flag.Float64Var(&c.Fault.ErrorRate, "error-rate", 0.3, "error share in partial mode")
	flag.DurationVar(&c.Fault.Period, "period", 2*time.Second, "half cycle in flapping mode")
	background := flag.String("background", "", "JSON behaviour applied to every dimension before the run")
	flag.Float64Var(&c.Rate, "rate", 2000, "requests per second, Poisson")
	flag.Uint64Var(&c.Seed, "seed", 1, "arrival seed")
	flag.DurationVar(&c.Settle, "settle", 5*time.Second, "load before the baseline window")
	flag.DurationVar(&c.Before, "before", 30*time.Second, "steady window before the fault")
	flag.DurationVar(&c.During, "fault", 30*time.Second, "fault duration")
	flag.DurationVar(&c.After, "after", 30*time.Second, "window after the fault")
	flag.DurationVar(&c.Ready, "ready", time.Minute, "how long to wait for every dimension to be healthy")
	flag.StringVar(&c.Records, "records", "", "write every request to this file")
	flag.BoolVar(&c.Options.KeepBins, "bins", false, "keep the 100 ms series in the report")
	flag.Parse()
	c.Fault.Mode = chaos.Mode(*mode)
	c.Options.Settle = c.Settle
	if *background != "" {
		if err := json.Unmarshal([]byte(*background), &c.Background); err != nil {
			fmt.Fprintln(os.Stderr, "failover: -background:", err)
			os.Exit(2)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	rep, err := failover.Run(ctx, c)
	if err != nil {
		fmt.Fprintln(os.Stderr, "failover:", err)
		os.Exit(1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rep); err != nil {
		fmt.Fprintln(os.Stderr, "failover:", err)
		os.Exit(1)
	}
}
