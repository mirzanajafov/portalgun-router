package httpx

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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
		{name: "a refused answer names the request and the reason", url: srv.URL + "/missing", wantErr: "GET " + srv.URL + "/missing: 404 Not Found: no such thing", wantCode: http.StatusNotFound},
		{name: "a JSON error answer gives its error field as the reason", url: srv.URL + "/busy", wantErr: "GET " + srv.URL + "/busy: 503 Service Unavailable: no leader", wantCode: http.StatusServiceUnavailable},
		{name: "an answer that isn't JSON names the request", url: srv.URL + "/garbled", wantErr: "GET " + srv.URL + "/garbled: "},
		{name: "a password in the URL stays out of the error", url: strings.Replace(srv.URL, "http://", "http://admin:hunter2@", 1) + "/missing", wantErr: "GET " + redacted + "/missing: 404 Not Found", wantCode: http.StatusNotFound},
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

func TestDoAcceptsEveryStatusItIsGiven(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusCreated) }))
	t.Cleanup(srv.Close)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Do(srv.Client(), req, nil, http.StatusOK, http.StatusCreated); err != nil {
		t.Fatalf("an accepted 201 came back as %v", err)
	}
}
