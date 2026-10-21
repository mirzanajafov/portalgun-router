package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	callTimeout  = 10 * time.Second
	revokeTime   = 3 * time.Second
	lookupSlack  = 10 * time.Second
	maxBackoff   = 5 * time.Second
	firstBackoff = 100 * time.Millisecond
)

var ErrLeaseNotFound = errors.New("registry: the lease does not exist or has expired")

type Instance struct {
	Name string            `json:"name"`
	Addr string            `json:"addr"`
	Meta map[string]string `json:"meta,omitempty"`
}

type Lease struct {
	ID  uint64
	TTL time.Duration
}

type Client struct {
	endpoints []string
	http      *http.Client
	at        atomic.Uint64
}

func New(endpoints []string) (*Client, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("registry: no endpoints")
	}
	for _, e := range endpoints {
		if u, err := url.Parse(e); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, fmt.Errorf("registry: endpoint %q is not an http URL", e)
		}
	}
	c := &Client{endpoints: slices.Clone(endpoints), http: &http.Client{}}
	c.at.Store(rand.Uint64N(uint64(len(endpoints))))
	return c, nil
}

type failure struct {
	Error  string `json:"error"`
	Reason string `json:"reason"`
}

func (c *Client) send(ctx context.Context, endpoint, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(endpoint, "/")+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("registry %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("registry %s %s: %w", method, path, err)
	}
	if resp.StatusCode >= 300 {
		var f failure
		if json.Unmarshal(raw, &f) != nil || f.Error == "" {
			f.Error = string(bytes.TrimSpace(raw[:min(len(raw), 4<<10)]))
		}
		if resp.StatusCode == http.StatusNotFound && f.Reason == "LEASE_NOT_FOUND" {
			return ErrLeaseNotFound
		}
		return fmt.Errorf("registry %s %s: %s: %s", method, path, resp.Status, f.Error)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("registry %s %s: %w", method, path, err)
	}
	return nil
}

func (c *Client) do(ctx context.Context, timeout time.Duration, method, path string, in, out any) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	at := c.at.Load()
	err := c.send(ctx, c.endpoints[at%uint64(len(c.endpoints))], method, path, in, out)
	if err != nil && !errors.Is(err, ErrLeaseNotFound) && !errors.Is(err, context.Canceled) {
		c.at.CompareAndSwap(at, at+1)
	}
	return err
}

type duration time.Duration

func (d *duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	*d = duration(v)
	return err
}

func (c *Client) Lookup(ctx context.Context, service string, index uint64, wait time.Duration) ([]Instance, uint64, error) {
	path := "/v1/registry/" + url.PathEscape(service)
	timeout := callTimeout
	if index > 0 {
		path += "?" + url.Values{"index": {strconv.FormatUint(index, 10)}, "wait": {wait.String()}}.Encode()
		timeout = wait + lookupSlack
	}
	var out struct {
		Index     uint64     `json:"index"`
		Instances []Instance `json:"instances"`
	}
	if err := c.do(ctx, timeout, http.MethodGet, path, nil, &out); err != nil {
		return nil, 0, err
	}
	return out.Instances, out.Index, nil
}

func (c *Client) Register(ctx context.Context, service string, inst Instance, ttl time.Duration) (Lease, error) {
	in := struct {
		Addr string            `json:"addr"`
		Meta map[string]string `json:"meta,omitempty"`
		TTL  string            `json:"ttl"`
	}{inst.Addr, inst.Meta, ttl.String()}
	var out struct {
		Lease uint64   `json:"lease"`
		TTL   duration `json:"ttl"`
	}
	path := "/v1/registry/" + url.PathEscape(service) + "/" + url.PathEscape(inst.Name)
	if err := c.do(ctx, callTimeout, http.MethodPut, path, in, &out); err != nil {
		return Lease{}, err
	}
	if out.Lease == 0 || out.TTL <= 0 {
		return Lease{}, fmt.Errorf("registry PUT %s: an answer without a lease or a TTL", path)
	}
	return Lease{ID: out.Lease, TTL: time.Duration(out.TTL)}, nil
}

func leasePath(id uint64) string {
	return "/v1/leases/" + strconv.FormatUint(id, 10)
}

func (c *Client) KeepAlive(ctx context.Context, lease uint64) error {
	return c.do(ctx, callTimeout, http.MethodPost, leasePath(lease)+"/keepalive", nil, nil)
}

func (c *Client) Revoke(ctx context.Context, lease uint64) error {
	return c.do(ctx, callTimeout, http.MethodDelete, leasePath(lease), nil, nil)
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (c *Client) Watch(ctx context.Context, service string, wait time.Duration, logger *slog.Logger) <-chan []Instance {
	out := make(chan []Instance)
	go func() {
		defer close(out)
		var index uint64
		var last []Instance
		sent, failing := false, false
		backoff := firstBackoff
		for ctx.Err() == nil {
			list, next, err := c.Lookup(ctx, service, index, wait)
			if err != nil {
				if !failing && ctx.Err() == nil {
					logger.Warn("the registry does not answer, keeping the last list", "service", service, "err", err)
				}
				failing = true
				if !sleep(ctx, backoff) {
					return
				}
				backoff = min(2*backoff, maxBackoff)
				continue
			}
			if failing {
				logger.Info("the registry answers again", "service", service, "index", next)
			}
			failing, backoff, index = false, firstBackoff, next
			if sent && slices.EqualFunc(list, last, sameInstance) {
				continue
			}
			select {
			case out <- list:
				sent, last = true, cloneInstances(list)
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func cloneInstances(list []Instance) []Instance {
	out := slices.Clone(list)
	for i := range out {
		out[i].Meta = maps.Clone(out[i].Meta)
	}
	return out
}

func sameInstance(a, b Instance) bool {
	return a.Name == b.Name && a.Addr == b.Addr && maps.Equal(a.Meta, b.Meta)
}

func (c *Client) Keep(ctx context.Context, service string, inst Instance, ttl time.Duration, logger *slog.Logger) {
	failing := false
	backoff := firstBackoff
	for ctx.Err() == nil {
		l, err := c.registerPastStop(ctx, service, inst, ttl)
		if err != nil {
			if !failing && ctx.Err() == nil {
				logger.Warn("registering failed, retrying", "service", service, "instance", inst.Name, "err", err)
			}
			failing = true
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(2*backoff, maxBackoff)
			continue
		}
		failing, backoff = false, firstBackoff
		if ctx.Err() == nil {
			logger.Info("registered", "service", service, "instance", inst.Name, "addr", inst.Addr, "lease", l.ID, "ttl", l.TTL)
			err = c.renew(ctx, l, logger)
		}
		if ctx.Err() != nil {
			logger.Info("deregistering", "service", service, "instance", inst.Name, "lease", l.ID)
			if err := c.revokeSomewhere(context.WithoutCancel(ctx), l.ID); err != nil {
				logger.Warn("deregistering failed, the lease runs out on its own", "lease", l.ID, "ttl", l.TTL, "err", err)
				return
			}
			logger.Info("deregistered", "service", service, "instance", inst.Name, "lease", l.ID)
			return
		}
		logger.Warn("the registration was lost, registering again", "service", service, "instance", inst.Name, "lease", l.ID, "err", err)
	}
}

func (c *Client) registerPastStop(ctx context.Context, service string, inst Instance, ttl time.Duration) (Lease, error) {
	rctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	stop := context.AfterFunc(ctx, func() { time.AfterFunc(revokeTime, cancel) })
	defer stop()
	return c.Register(rctx, service, inst, ttl)
}

func (c *Client) revokeSomewhere(ctx context.Context, lease uint64) error {
	ctx, cancel := context.WithTimeout(ctx, revokeTime)
	defer cancel()
	for {
		tctx, cancelTry := context.WithTimeout(ctx, revokeTime/time.Duration(len(c.endpoints)))
		err := c.Revoke(tctx, lease)
		cancelTry()
		if err == nil || errors.Is(err, ErrLeaseNotFound) {
			return nil
		}
		if !sleep(ctx, firstBackoff) {
			return err
		}
	}
}

func (c *Client) renew(ctx context.Context, l Lease, logger *slog.Logger) error {
	every := l.TTL / 3
	next := every
	failing := false
	for sleep(ctx, next) {
		kctx, cancel := context.WithTimeout(ctx, every)
		err := c.KeepAlive(kctx, l.ID)
		cancel()
		switch {
		case err == nil:
			if failing {
				logger.Info("keepalives work again", "lease", l.ID)
			}
			failing, next = false, every
		case errors.Is(err, ErrLeaseNotFound):
			return err
		case ctx.Err() == nil:
			if failing {
				next = min(2*next, every)
			} else {
				logger.Warn("keepalive failed, trying the next node", "lease", l.ID, "err", err)
				failing, next = true, min(firstBackoff, every)
			}
		}
	}
	return ctx.Err()
}
