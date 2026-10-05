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
}

type Pool struct {
	Name       string      `json:"name"`
	Balancer   string      `json:"balancer,omitempty"`
	Dimensions []Dimension `json:"dimensions"`
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
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	for i := range c.Routes {
		r := &c.Routes[i]
		r.Host = strings.ToLower(r.Host)
		if r.Timeout == 0 {
			r.Timeout = Duration(DefaultTimeout)
		}
		if r.PerTryTimeout == 0 {
			r.PerTryTimeout = Duration(min(DefaultPerTry, r.Timeout.D()))
		}
	}
	for i := range c.Pools {
		p := &c.Pools[i]
		if p.Balancer == "" {
			p.Balancer = BalancerP2C
		}
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
	}
	if len(c.Routes) == 0 {
		errs = append(errs, errors.New("config has no routes"))
	}
	return errors.Join(errs...)
}
