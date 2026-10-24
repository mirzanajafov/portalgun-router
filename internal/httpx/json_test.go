package httpx

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReadJSONRefusesWhatItCannotTrust(t *testing.T) {
	read := func(body string) error {
		var v struct {
			Rate float64 `json:"rate"`
		}
		return ReadJSON(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)), &v)
	}
	if err := read(`{"rate": 1}`); err != nil {
		t.Fatalf("a valid body: %v", err)
	}
	if err := read(""); !errors.Is(err, io.EOF) {
		t.Fatalf("an empty body came back as %v, want io.EOF so callers can choose to allow it", err)
	}
	if err := read(`{"rate": 1, "burst": 2}`); err == nil {
		t.Fatal("an unknown field was accepted")
	}
	var tooBig *http.MaxBytesError
	if err := read(`{"rate": 1` + strings.Repeat(" ", 64<<10) + `}`); !errors.As(err, &tooBig) {
		t.Fatalf("a body over 64 KiB came back as %v", err)
	}
}
