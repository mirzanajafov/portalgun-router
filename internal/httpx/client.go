package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
)

type StatusError struct {
	Method string
	URL    string
	Code   int
	Status string
	Body   string
}

func (e *StatusError) Error() string {
	msg := fmt.Sprintf("%s %s: %s", e.Method, e.URL, e.Status)
	if e.Body != "" {
		msg += ": " + e.Body
	}
	return msg
}

func Do(client *http.Client, req *http.Request, out any, ok ...int) error {
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if !slices.Contains(ok, resp.StatusCode) {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return &StatusError{Method: req.Method, URL: req.URL.Redacted(), Code: resp.StatusCode, Status: resp.Status, Body: string(bytes.TrimSpace(body))}
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("%s %s: %w", req.Method, req.URL.Redacted(), err)
	}
	return nil
}

func GetJSON(ctx context.Context, client *http.Client, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	return Do(client, req, out, http.StatusOK)
}
