package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"250ms\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) D() time.Duration { return time.Duration(d) }

const (
	BalancerP2C              = "p2c-ewma"
	BalancerRoundRobin       = "round-robin"
	BalancerLeastOutstanding = "least-outstanding"

	DefaultVersion     = "v1"
	DefaultMaxInflight = 1024
	DefaultTimeout     = 30 * time.Second
	DefaultPerTry      = 10 * time.Second
	DefaultRetries     = 2
	DefaultPanic       = 0.5
	DefaultSlowStart   = 10 * time.Second
	DefaultEwmaDecay   = 10 * time.Second
)

type Config struct {
	Routes []Route `json:"routes"`
	Pools  []Pool  `json:"pools"`
}

type Route struct {
	Name          string   `json:"name"`
	Host          string   `json:"host,omitempty"`
	PathPrefix    string   `json:"pathPrefix"`
	Pool          string   `json:"pool"`
	Timeout       Duration `json:"timeout,omitempty"`
	PerTryTimeout Duration `json:"perTryTimeout,omitempty"`
	Retries       Retries  `json:"retries,omitzero"`
}

type Retries struct {
	Max          *int    `json:"max,omitempty"`
	BudgetRatio  float64 `json:"budgetRatio,omitempty"`
	MinPerSecond float64 `json:"minPerSecond,omitempty"`
}

type Pool struct {
	Name           string      `json:"name"`
	Balancer       string      `json:"balancer,omitempty"`
	HealthCheck    HealthCheck `json:"healthCheck,omitzero"`
	Outlier        Outlier     `json:"outlier,omitzero"`
	PanicThreshold *float64    `json:"panicThreshold,omitempty"`
	SlowStart      *Duration   `json:"slowStart,omitempty"`
	EwmaDecay      Duration    `json:"ewmaDecay,omitempty"`
	Dimensions     []Dimension `json:"dimensions"`
}

type HealthCheck struct {
	Disabled       bool     `json:"disabled,omitempty"`
	Path           string   `json:"path,omitempty"`
	Interval       Duration `json:"interval,omitempty"`
	Timeout        Duration `json:"timeout,omitempty"`
	UnhealthyAfter int      `json:"unhealthyAfter,omitempty"`
	HealthyAfter   int      `json:"healthyAfter,omitempty"`
}

type Outlier struct {
	Disabled       bool     `json:"disabled,omitempty"`
	Consecutive    int      `json:"consecutive,omitempty"`
	Window         int      `json:"window,omitempty"`
	MinRequests    int      `json:"minRequests,omitempty"`
	FailureRatio   float64  `json:"failureRatio,omitempty"`
	BaseEjection   Duration `json:"baseEjection,omitempty"`
	MaxEjection    Duration `json:"maxEjection,omitempty"`
	HalfOpenTrials int      `json:"halfOpenTrials,omitempty"`
	EjectOnRefused bool     `json:"ejectOnRefused,omitempty"`
}

func orDefault[T comparable](v *T, def T) {
	var zero T
	if *v == zero {
		*v = def
	}
}

func (h *HealthCheck) applyDefaults() {
	orDefault(&h.Path, "/healthz")
	orDefault(&h.Interval, Duration(time.Second))
	orDefault(&h.Timeout, Duration(250*time.Millisecond))
	orDefault(&h.UnhealthyAfter, 2)
	orDefault(&h.HealthyAfter, 2)
}

func (o *Outlier) applyDefaults() {
	orDefault(&o.Consecutive, 5)
	orDefault(&o.Window, 50)
	orDefault(&o.MinRequests, 20)
	orDefault(&o.FailureRatio, 0.5)
	orDefault(&o.BaseEjection, Duration(5*time.Second))
	orDefault(&o.MaxEjection, Duration(time.Minute))
	orDefault(&o.HalfOpenTrials, 3)
}

func (r *Retries) applyDefaults() {
	if r.Max == nil {
		r.Max = new(DefaultRetries)
	}
	orDefault(&r.BudgetRatio, 0.2)
	orDefault(&r.MinPerSecond, 10)
}

type Dimension struct {
	ID          string `json:"id"`
	Address     string `json:"address"`
	Version     string `json:"version,omitempty"`
	MaxInflight int    `json:"maxInflight,omitempty"`
}

func Parse(data []byte) (*Config, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	c.ApplyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) ApplyDefaults() {
	for i := range c.Routes {
		r := &c.Routes[i]
		r.Host = strings.ToLower(r.Host)
		if r.Timeout == 0 {
			r.Timeout = Duration(DefaultTimeout)
		}
		if r.PerTryTimeout == 0 {
			r.PerTryTimeout = Duration(min(DefaultPerTry, r.Timeout.D()))
		}
		r.Retries.applyDefaults()
	}
	for i := range c.Pools {
		p := &c.Pools[i]
		if p.Balancer == "" {
			p.Balancer = BalancerP2C
		}
		p.HealthCheck.applyDefaults()
		p.Outlier.applyDefaults()
		if p.PanicThreshold == nil {
			p.PanicThreshold = new(float64(DefaultPanic))
		}
		if p.SlowStart == nil {
			p.SlowStart = new(Duration(DefaultSlowStart))
		}
		orDefault(&p.EwmaDecay, Duration(DefaultEwmaDecay))
		for j := range p.Dimensions {
			d := &p.Dimensions[j]
			if d.Version == "" {
				d.Version = DefaultVersion
			}
			if d.MaxInflight == 0 {
				d.MaxInflight = DefaultMaxInflight
			}
		}
	}
}

func (c *Config) Validate() error {
	var errs []error
	pools := map[string]bool{}
	dims := map[string]string{}
	for _, p := range c.Pools {
		switch {
		case p.Name == "":
			errs = append(errs, errors.New("pool without a name"))
		case pools[p.Name]:
			errs = append(errs, fmt.Errorf("pool %q defined twice", p.Name))
		}
		pools[p.Name] = true
		switch p.Balancer {
		case BalancerP2C, BalancerRoundRobin, BalancerLeastOutstanding:
		default:
			errs = append(errs, fmt.Errorf("pool %q: unknown balancer %q", p.Name, p.Balancer))
		}
		if len(p.Dimensions) == 0 {
			errs = append(errs, fmt.Errorf("pool %q has no dimensions", p.Name))
		}
		errs = append(errs, p.validateResilience()...)
		for _, d := range p.Dimensions {
			if d.ID == "" {
				errs = append(errs, fmt.Errorf("pool %q: dimension without an id", p.Name))
				continue
			}
			if other, dup := dims[d.ID]; dup {
				errs = append(errs, fmt.Errorf("dimension %q appears in pools %q and %q", d.ID, other, p.Name))
			}
			dims[d.ID] = p.Name
			if host, port, err := net.SplitHostPort(d.Address); err != nil || host == "" || port == "" {
				errs = append(errs, fmt.Errorf("dimension %q: address %q is not host:port", d.ID, d.Address))
			}
			if d.MaxInflight < 1 {
				errs = append(errs, fmt.Errorf("dimension %q: maxInflight must be positive", d.ID))
			}
		}
	}
	routes := map[string]bool{}
	for _, r := range c.Routes {
		switch {
		case r.Name == "":
			errs = append(errs, errors.New("route without a name"))
		case routes[r.Name]:
			errs = append(errs, fmt.Errorf("route %q defined twice", r.Name))
		}
		routes[r.Name] = true
		if !strings.HasPrefix(r.PathPrefix, "/") {
			errs = append(errs, fmt.Errorf("route %q: pathPrefix must start with /", r.Name))
		}
		if !pools[r.Pool] {
			errs = append(errs, fmt.Errorf("route %q: unknown pool %q", r.Name, r.Pool))
		}
		if r.Timeout <= 0 || r.PerTryTimeout <= 0 || r.PerTryTimeout > r.Timeout {
			errs = append(errs, fmt.Errorf("route %q: timeouts must be positive and perTryTimeout no longer than timeout", r.Name))
		}
		rt := r.Retries
		switch {
		case rt.Max == nil:
			errs = append(errs, fmt.Errorf("route %q: retries have no defaults applied", r.Name))
		case *rt.Max < 0 || *rt.Max > 5:
			errs = append(errs, fmt.Errorf("route %q: retries.max must be between 0 and 5", r.Name))
		}
		if !(rt.BudgetRatio >= 0 && rt.BudgetRatio <= 1) || !(rt.MinPerSecond >= 0) {
			errs = append(errs, fmt.Errorf("route %q: retry budget ratio must be in [0, 1] and minPerSecond not negative", r.Name))
		}
	}
	if len(c.Routes) == 0 {
		errs = append(errs, errors.New("config has no routes"))
	}
	return errors.Join(errs...)
}

func (p Pool) validateResilience() []error {
	var errs []error
	bad := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf("pool %q: "+format, append([]any{p.Name}, args...)...))
	}
	if p.PanicThreshold == nil || p.SlowStart == nil {
		bad("panicThreshold and slowStart have no defaults applied")
		return errs
	}
	if !(*p.PanicThreshold >= 0 && *p.PanicThreshold <= 1) {
		bad("panicThreshold must be between 0 and 1")
	}
	if *p.SlowStart < 0 {
		bad("slowStart must not be negative")
	}
	if p.EwmaDecay.D() < 100*time.Millisecond {
		bad("ewmaDecay must be at least 100ms")
	}
	h := p.HealthCheck
	if !h.Disabled {
		if !strings.HasPrefix(h.Path, "/") {
			bad("healthCheck.path must start with /")
		}
		if h.Interval.D() < 100*time.Millisecond || h.Timeout <= 0 || h.Timeout > h.Interval {
			bad("healthCheck needs an interval of at least 100ms and a positive timeout no longer than it")
		}
		if h.UnhealthyAfter < 1 || h.HealthyAfter < 1 {
			bad("healthCheck thresholds must be at least 1")
		}
	}
	o := p.Outlier
	if !o.Disabled {
		if o.Consecutive < 1 || o.Window < 1 || o.MinRequests < 1 || o.MinRequests > o.Window || o.HalfOpenTrials < 1 {
			bad("outlier counts must be positive and minRequests no larger than window")
		}
		if !(o.FailureRatio > 0 && o.FailureRatio <= 1) {
			bad("outlier.failureRatio must be in (0, 1]")
		}
		if o.BaseEjection <= 0 || o.MaxEjection < o.BaseEjection {
			bad("outlier ejection times must be positive with maxEjection at least baseEjection")
		}
	}
	return errs
}
