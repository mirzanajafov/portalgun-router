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
	Reason string
}

func (e *StatusError) Error() string {
	msg := fmt.Sprintf("%s %s: %s", e.Method, e.URL, e.Status)
	if e.Reason != "" {
		msg += ": " + e.Reason
	}
	return msg
}

func Do(client *http.Client, req *http.Request, out any, want int, alsoWant ...int) error {
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != want && !slices.Contains(alsoWant, resp.StatusCode) {
		return &StatusError{Method: req.Method, URL: req.URL.Redacted(), Code: resp.StatusCode, Status: resp.Status, Reason: reason(resp.Body)}
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("%s %s: %w", req.Method, req.URL.Redacted(), err)
		}
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	return nil
}

func reason(body io.Reader) string {
	raw, _ := io.ReadAll(io.LimitReader(body, 4<<10))
	var answer struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(raw, &answer) == nil && answer.Error != "" {
		return answer.Error
	}
	return string(bytes.TrimSpace(raw))
}

func GetJSON(ctx context.Context, client *http.Client, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	return Do(client, req, out, http.StatusOK)
}
