package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type Mode string

const (
	Crash     Mode = "crash"
	Reset     Mode = "reset"
	Blackhole Mode = "blackhole"
	Slow      Mode = "slow"
	Partial   Mode = "partial"
	Flapping  Mode = "flapping"
)

type Fault struct {
	Mode      Mode          `json:"mode"`
	Target    string        `json:"target"`
	Latency   time.Duration `json:"latency,omitempty"`
	ErrorRate float64       `json:"errorRate,omitempty"`
	Period    time.Duration `json:"period,omitempty"`
}

func (f Fault) Validate() error {
	if f.Target == "" {
		return errors.New("fault needs a target")
	}
	switch f.Mode {
	case Crash, Reset, Blackhole:
	case Slow:
		if f.Latency <= 0 || f.Latency > time.Minute {
			return errors.New("slow needs a latency between 0 and 1m")
		}
	case Partial:
		if !(f.ErrorRate > 0 && f.ErrorRate <= 1) {
			return errors.New("partial needs an error rate in (0, 1]")
		}
	case Flapping:
		if f.Period < 100*time.Millisecond {
			return errors.New("flapping needs a period of at least 100ms")
		}
	default:
		return fmt.Errorf("unknown fault mode %q", f.Mode)
	}
	return nil
}

var ErrControlToken = errors.New("the dimension refused the control token")

type DimensionControl struct {
	BaseURL string
	Token   string
	Client  *http.Client
}

func (d *DimensionControl) Patch(ctx context.Context, fields map[string]any) error {
	b, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	return d.send(ctx, http.MethodPatch, "/state", b)
}

func (d *DimensionControl) Reset(ctx context.Context) error {
	return d.send(ctx, http.MethodPost, "/reset", nil)
}

func (d *DimensionControl) send(ctx context.Context, method, path string, b []byte) error {
	req, err := http.NewRequestWithContext(ctx, method, d.BaseURL+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+d.Token)
	client := d.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("dimension control %s: %w", d.BaseURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("dimension control %s: %w", d.BaseURL, ErrControlToken)
	}
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("dimension control %s: %s: %s", d.BaseURL, resp.Status, bytes.TrimSpace(msg))
	}
	return nil
}

type Injector struct {
	toxi *Toxiproxy
	dims map[string]*DimensionControl

	mu    sync.Mutex
	flaps map[string]func()
}

func NewInjector(toxi *Toxiproxy, dims map[string]*DimensionControl) *Injector {
	return &Injector{toxi: toxi, dims: dims, flaps: map[string]func(){}}
}

func toxicName(f Fault, stream string) string {
	return string(f.Mode) + "-" + stream
}

func (in *Injector) Inject(ctx context.Context, f Fault) error {
	if err := f.Validate(); err != nil {
		return err
	}
	var err error
	switch f.Mode {
	case Crash:
		err = in.toxi.SetEnabled(ctx, f.Target, false)
	case Reset:
		err = in.toxi.AddToxic(ctx, f.Target, Toxic{Name: toxicName(f, "downstream"), Type: "reset_peer", Stream: "downstream", Toxicity: 1, Attributes: map[string]int{"timeout": 0}})
	case Blackhole:
		for _, s := range []string{"upstream", "downstream"} {
			if err = in.toxi.AddToxic(ctx, f.Target, Toxic{Name: toxicName(f, s), Type: "timeout", Stream: s, Toxicity: 1, Attributes: map[string]int{"timeout": 0}}); err != nil {
				break
			}
		}
	case Slow:
		err = in.toxi.AddToxic(ctx, f.Target, Toxic{Name: toxicName(f, "downstream"), Type: "latency", Stream: "downstream", Toxicity: 1, Attributes: map[string]int{"latency": int(f.Latency / time.Millisecond)}})
	case Partial:
		var d *DimensionControl
		if d, err = in.dimension(f.Target); err == nil {
			err = d.Patch(ctx, map[string]any{"errorRate": f.ErrorRate})
		}
	case Flapping:
		err = in.startFlapping(f)
	}
	return err
}

func (in *Injector) Clear(ctx context.Context, f Fault) error {
	if err := f.Validate(); err != nil {
		return err
	}
	var err error
	switch f.Mode {
	case Crash:
		err = in.toxi.SetEnabled(ctx, f.Target, true)
	case Reset, Slow:
		err = in.toxi.RemoveToxic(ctx, f.Target, toxicName(f, "downstream"))
	case Blackhole:
		err = errors.Join(
			in.toxi.RemoveToxic(ctx, f.Target, toxicName(f, "upstream")),
			in.toxi.RemoveToxic(ctx, f.Target, toxicName(f, "downstream")),
		)
	case Partial:
		var d *DimensionControl
		if d, err = in.dimension(f.Target); err == nil {
			err = d.Patch(ctx, map[string]any{"errorRate": 0})
		}
	case Flapping:
		in.stopFlapping(f.Target)
		err = in.toxi.SetEnabled(ctx, f.Target, true)
	}
	return err
}

func (in *Injector) dimension(target string) (*DimensionControl, error) {
	d, ok := in.dims[target]
	if !ok {
		return nil, fmt.Errorf("no control endpoint for %q", target)
	}
	return d, nil
}

func (in *Injector) startFlapping(f Fault) error {
	in.mu.Lock()
	if _, busy := in.flaps[f.Target]; busy {
		in.mu.Unlock()
		return fmt.Errorf("%s is already flapping", f.Target)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	in.flaps[f.Target] = func() {
		cancel()
		<-done
	}
	in.mu.Unlock()

	go func() {
		defer close(done)
		t := time.NewTicker(f.Period)
		defer t.Stop()
		up := false
		for {
			_ = in.toxi.SetEnabled(ctx, f.Target, up)
			up = !up
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	return nil
}

func (in *Injector) stopFlapping(target string) {
	in.mu.Lock()
	stop, ok := in.flaps[target]
	delete(in.flaps, target)
	in.mu.Unlock()
	if ok {
		stop()
	}
}

func (in *Injector) ClearAll(ctx context.Context) error {
	in.mu.Lock()
	targets := make([]string, 0, len(in.flaps))
	for t := range in.flaps {
		targets = append(targets, t)
	}
	in.mu.Unlock()
	for _, t := range targets {
		in.stopFlapping(t)
	}
	errs := []error{in.toxi.Reset(ctx)}
	for _, d := range in.dims {
		errs = append(errs, d.Patch(ctx, map[string]any{"errorRate": 0, "hang": false, "listening": true}))
	}
	return errors.Join(errs...)
}
