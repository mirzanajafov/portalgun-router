package failover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/chaos"
	"github.com/mirzanajafov/portalgun-router/internal/health"
	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
)

const NoFault = "none"

type Cell struct {
	Label      string
	Gateways   []string
	Admins     []string
	Toxiproxy  string
	Controls   map[string]string
	Token      string
	Fault      chaos.Fault
	Background map[string]any
	Rate       float64
	Seed       uint64
	Settle     time.Duration
	Before     time.Duration
	During     time.Duration
	After      time.Duration
	Ready      time.Duration
	Records    string
	Options    Options
}

type Report struct {
	Label     string          `json:"label"`
	Window    Window          `json:"window"`
	Config    json.RawMessage `json:"config,omitempty"`
	Generator loadgen.Summary `json:"generator"`
	Events    []GatewayEvent  `json:"events"`
	Result    Result          `json:"result"`
}

func (c Cell) validate() error {
	switch {
	case len(c.Gateways) == 0 || len(c.Admins) == 0:
		return errors.New("needs at least one gateway and its admin address")
	case c.Toxiproxy == "":
		return errors.New("needs the toxiproxy address")
	case len(c.Controls) > 0 && c.Token == "":
		return errors.New("needs PORTAL_CONTROL_TOKEN for the dimensions' control ports")
	case c.Rate <= 0 || c.Before <= 0 || c.After <= 0 || c.During < 0 || c.Settle < 0:
		return errors.New("rate and windows must be positive")
	}
	if c.Fault.Mode == NoFault {
		return nil
	}
	return c.Fault.Validate()
}

func Run(ctx context.Context, c Cell) (*Report, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	dims := map[string]*chaos.DimensionControl{}
	for name, u := range c.Controls {
		dims[name] = &chaos.DimensionControl{BaseURL: u, Token: c.Token, Client: client}
	}
	inj := chaos.NewInjector(chaos.NewToxiproxy(c.Toxiproxy), dims)
	reset := func(ctx context.Context) error {
		errs := []error{inj.ClearAll(ctx)}
		for _, d := range dims {
			errs = append(errs, d.Reset(ctx))
		}
		return errors.Join(errs...)
	}
	if err := reset(ctx); err != nil {
		return nil, fmt.Errorf("reset faults: %w", err)
	}
	defer func() { _ = reset(context.WithoutCancel(ctx)) }()
	if len(c.Background) > 0 {
		for name, d := range dims {
			if err := d.Patch(ctx, c.Background); err != nil {
				return nil, fmt.Errorf("background on %s: %w", name, err)
			}
		}
	}
	if err := waitReady(ctx, client, c.Admins, c.Ready); err != nil {
		return nil, err
	}

	cfg := loadgen.Config{
		URLs:        c.Gateways,
		Rate:        c.Rate,
		Arrival:     loadgen.Poisson,
		Seed:        c.Seed,
		Duration:    c.Settle + c.Before + c.During + c.After,
		Timeout:     30 * time.Second,
		KeepRecords: true,
		Label:       c.Label,
	}
	type outcome struct {
		res *loadgen.Result
		err error
	}
	done := make(chan outcome, 1)
	start := time.Now()
	go func() {
		res, err := loadgen.Run(ctx, cfg)
		done <- outcome{res, err}
	}()

	w := Window{Mode: string(c.Fault.Mode), Target: c.Fault.Target}
	faultErr := func() error {
		if !sleepUntil(ctx, start.Add(c.Settle+c.Before)) {
			return ctx.Err()
		}
		w.InjectAt = time.Now().UnixNano()
		if c.Fault.Mode != NoFault {
			if err := c.inject(ctx, inj); err != nil {
				return fmt.Errorf("inject: %w", err)
			}
		}
		w.InjectAck = time.Now().UnixNano()
		if !sleepUntil(ctx, start.Add(c.Settle+c.Before+c.During)) {
			return ctx.Err()
		}
		w.ClearAt = time.Now().UnixNano()
		if c.Fault.Mode != NoFault {
			if err := c.clear(ctx, inj); err != nil {
				return fmt.Errorf("clear: %w", err)
			}
		}
		w.ClearAck = time.Now().UnixNano()
		return nil
	}()
	out := <-done
	if err := errors.Join(faultErr, out.err); err != nil {
		return nil, err
	}

	rep := &Report{Label: c.Label, Window: w, Generator: out.res.Summary}
	for _, admin := range c.Admins {
		var evs []GatewayEvent
		if err := getJSON(ctx, client, fmt.Sprintf("%s/events?since=%d", admin, start.UnixNano()), &evs); err != nil {
			return nil, fmt.Errorf("gateway events: %w", err)
		}
		rep.Events = append(rep.Events, evs...)
	}
	if err := getJSON(ctx, client, c.Admins[0]+"/config", &rep.Config); err != nil {
		return nil, fmt.Errorf("gateway config: %w", err)
	}
	rep.Result = Analyze(out.res.Log, w, rep.Events, c.Options)
	if c.Records != "" {
		if err := writeRecords(c.Records, out.res.Log); err != nil {
			return nil, err
		}
	}
	return rep, nil
}

func (c Cell) targets() []chaos.Fault {
	if c.Fault.Target != "all" {
		return []chaos.Fault{c.Fault}
	}
	var fs []chaos.Fault
	for name := range c.Controls {
		f := c.Fault
		f.Target = name
		fs = append(fs, f)
	}
	return fs
}

func (c Cell) inject(ctx context.Context, inj *chaos.Injector) error {
	for _, f := range c.targets() {
		if err := inj.Inject(ctx, f); err != nil {
			return err
		}
	}
	return nil
}

func (c Cell) clear(ctx context.Context, inj *chaos.Injector) error {
	var errs []error
	for _, f := range c.targets() {
		errs = append(errs, inj.Clear(ctx, f))
	}
	return errors.Join(errs...)
}

func sleepUntil(ctx context.Context, at time.Time) bool {
	t := time.NewTimer(time.Until(at))
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

type upstreamState struct {
	Upstreams []struct {
		ID      string       `json:"id"`
		Healthy bool         `json:"healthy"`
		Outlier health.State `json:"outlier"`
	} `json:"upstreams"`
}

func waitReady(ctx context.Context, client *http.Client, admins []string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for {
		ready := true
		var why string
		for _, admin := range admins {
			var st upstreamState
			if err := getJSON(ctx, client, admin+"/state", &st); err != nil {
				ready, why = false, err.Error()
				break
			}
			for _, u := range st.Upstreams {
				if !u.Healthy || (u.Outlier != health.Closed && u.Outlier != health.Disabled) {
					ready, why = false, fmt.Sprintf("%s is %s, healthy=%v", u.ID, u.Outlier, u.Healthy)
				}
			}
		}
		if ready {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("gateway not ready after %v: %s", limit, why)
		}
		if !sleepUntil(ctx, time.Now().Add(250*time.Millisecond)) {
			return ctx.Err()
		}
	}
}

func getJSON(ctx context.Context, client *http.Client, url string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<10))
		return fmt.Errorf("%s: %s: %s", url, resp.Status, msg)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

func writeRecords(path string, l *loadgen.Log) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := loadgen.WriteLog(f, l); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
