package httpx

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDoDecodesWhatItAcceptsAndNamesTheRequestItRefuses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/count":
			WriteJSON(w, http.StatusOK, map[string]int{"n": 7})
		case "/created":
			w.WriteHeader(http.StatusCreated)
		case "/busy":
			WriteError(w, http.StatusServiceUnavailable, "no leader")
		default:
			http.Error(w, "no such thing", http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	ctx := context.Background()

	var got struct {
		N int `json:"n"`
	}
	if err := GetJSON(ctx, srv.Client(), srv.URL+"/count", &got); err != nil || got.N != 7 {
		t.Fatalf("got %+v, %v", got, err)
	}
	err := GetJSON(ctx, srv.Client(), srv.URL+"/missing", &got)
	var refused *StatusError
	if !errors.As(err, &refused) || refused.Code != http.StatusNotFound || err.Error() != "GET "+srv.URL+"/missing: 404 Not Found: no such thing" {
		t.Fatalf("a 404 came back as %v", err)
	}
	if err := GetJSON(ctx, srv.Client(), srv.URL+"/busy", &got); err == nil || err.Error() != "GET "+srv.URL+"/busy: 503 Service Unavailable: no leader" {
		t.Fatalf("a JSON error answer came back as %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/created", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Do(srv.Client(), req, nil, http.StatusOK, http.StatusCreated); err != nil {
		t.Fatalf("an accepted 201 came back as %v", err)
	}
}
