package httpx

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestGetJSONDecodesWhatItAcceptsAndNamesTheRequestItRefuses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/count":
			WriteJSON(w, http.StatusOK, map[string]int{"n": 7})
		case "/garbled":
			_, _ = w.Write([]byte("not json"))
		case "/busy":
			WriteError(w, http.StatusServiceUnavailable, "no leader")
		default:
			http.Error(w, "no such thing", http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	redacted := strings.Replace(srv.URL, "http://", "http://admin:xxxxx@", 1)
	cases := []struct {
		name, url, wantErr string
		wantCode           int
	}{
		{name: "an accepted answer is decoded", url: srv.URL + "/count"},
		{name: "a refused answer names the request and the reason", url: srv.URL + "/missing", wantErr: `Get "` + srv.URL + `/missing": 404 Not Found: no such thing`, wantCode: http.StatusNotFound},
		{name: "a JSON error answer gives its error field as the reason", url: srv.URL + "/busy", wantErr: `Get "` + srv.URL + `/busy": 503 Service Unavailable: no leader`, wantCode: http.StatusServiceUnavailable},
		{name: "an answer that isn't JSON names the request", url: srv.URL + "/garbled", wantErr: `Get "` + srv.URL + `/garbled": `},
		{name: "a password in the URL stays out of the error", url: strings.Replace(srv.URL, "http://", "http://admin:hunter2@", 1) + "/missing", wantErr: `Get "` + redacted + `/missing": 404 Not Found`, wantCode: http.StatusNotFound},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got struct {
				N int `json:"n"`
			}
			err := GetJSON(context.Background(), srv.Client(), c.url, &got)
			if c.wantErr == "" {
				if err != nil || got.N != 7 {
					t.Fatalf("got %+v, %v", got, err)
				}
				return
			}
			if err == nil || !strings.HasPrefix(err.Error(), c.wantErr) {
				t.Fatalf("got %v, want it to start with %s", err, c.wantErr)
			}
			code := 0
			var refused *StatusError
			if errors.As(err, &refused) {
				code = refused.Code
			}
			if code != c.wantCode {
				t.Fatalf("StatusError code %d, want %d", code, c.wantCode)
			}
		})
	}
}

func TestDoAcceptsTheStatusesItIsGivenAndNoOthers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusCreated) }))
	t.Cleanup(srv.Close)
	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name     string
		accepted []int
		wantErr  bool
	}{
		{"a 201 among the accepted statuses", []int{http.StatusOK, http.StatusCreated}, false},
		{"a 201 when only 200 and 204 are accepted", []int{http.StatusOK, http.StatusNoContent}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			req := &http.Request{URL: target, Header: http.Header{}}
			if err := Do(srv.Client(), req, nil, c.accepted[0], c.accepted[1:]...); (err != nil) != c.wantErr {
				t.Fatalf("got %v, want an error: %v", err, c.wantErr)
			}
		})
	}
}
