package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/httpx"
)

type Toxiproxy struct {
	BaseURL string
	Client  *http.Client
}

type Proxy struct {
	Name     string `json:"name"`
	Listen   string `json:"listen"`
	Upstream string `json:"upstream"`
	Enabled  bool   `json:"enabled"`
}

type Toxic struct {
	Name       string         `json:"name"`
	Type       string         `json:"type"`
	Stream     string         `json:"stream"`
	Toxicity   float64        `json:"toxicity"`
	Attributes map[string]int `json:"attributes"`
}

func NewToxiproxy(baseURL string) *Toxiproxy {
	return &Toxiproxy{BaseURL: baseURL, Client: &http.Client{Timeout: 5 * time.Second}}
}

func (t *Toxiproxy) do(ctx context.Context, method, path string, body any, want ...int) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, t.BaseURL+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if err := httpx.Do(t.Client, req, nil, want...); err != nil {
		return fmt.Errorf("toxiproxy: %w", err)
	}
	return nil
}

func (t *Toxiproxy) Populate(ctx context.Context, proxies []Proxy) error {
	return t.do(ctx, http.MethodPost, "/populate", proxies, http.StatusCreated, http.StatusOK)
}

func (t *Toxiproxy) SetEnabled(ctx context.Context, proxy string, enabled bool) error {
	return t.do(ctx, http.MethodPost, "/proxies/"+url.PathEscape(proxy), map[string]bool{"enabled": enabled}, http.StatusOK)
}

func (t *Toxiproxy) AddToxic(ctx context.Context, proxy string, toxic Toxic) error {
	return t.do(ctx, http.MethodPost, "/proxies/"+url.PathEscape(proxy)+"/toxics", toxic, http.StatusOK)
}

func (t *Toxiproxy) RemoveToxic(ctx context.Context, proxy, toxic string) error {
	return t.do(ctx, http.MethodDelete, "/proxies/"+url.PathEscape(proxy)+"/toxics/"+url.PathEscape(toxic), nil, http.StatusNoContent, http.StatusNotFound)
}

func (t *Toxiproxy) Reset(ctx context.Context) error {
	return t.do(ctx, http.MethodPost, "/reset", nil, http.StatusNoContent, http.StatusOK)
}
