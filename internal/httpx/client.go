package httpx

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

type StatusError struct {
	Code   int
	Status string
	Reason string
}

func (e *StatusError) Error() string {
	if e.Reason == "" {
		return e.Status
	}
	return e.Status + ": " + e.Reason
}

func Do(client *http.Client, req *http.Request, out any, want int, alsoWant ...int) error {
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := CheckStatus(req, resp, want, alsoWant...); err != nil {
		return err
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return requestError(req, err)
		}
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	return nil
}

func CheckStatus(req *http.Request, resp *http.Response, want int, alsoWant ...int) error {
	if resp.StatusCode == want || slices.Contains(alsoWant, resp.StatusCode) {
		return nil
	}
	return requestError(req, &StatusError{Code: resp.StatusCode, Status: resp.Status, Reason: reason(resp.Body)})
}

func requestError(req *http.Request, err error) error {
	method := cmp.Or(req.Method, http.MethodGet)
	return &url.Error{Op: method[:1] + strings.ToLower(method[1:]), URL: req.URL.Redacted(), Err: err}
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

func GetJSON(ctx context.Context, client *http.Client, target string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	return Do(client, req, out, http.StatusOK)
}
