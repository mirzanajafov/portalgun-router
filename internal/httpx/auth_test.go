package httpx

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOnlyTheConfiguredTokenAuthorizes(t *testing.T) {
	cases := []struct {
		name, token, header string
		want                bool
	}{
		{"the token", "s3cret", "Bearer s3cret", true},
		{"another token", "s3cret", "Bearer guess", false},
		{"the token without its scheme", "s3cret", "s3cret", false},
		{"no header", "s3cret", "", false},
		{"no token configured and an empty bearer", "", "Bearer ", false},
		{"no token configured and no header", "", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := &http.Request{Header: http.Header{}}
			if c.header != "" {
				r.Header.Set("Authorization", c.header)
			}
			if got := Authorized(r, c.token); got != c.want {
				t.Fatalf("authorized %v, want %v", got, c.want)
			}
		})
	}
}

func TestAnEmptySecretMatchesNothing(t *testing.T) {
	if Carries(&http.Request{Header: http.Header{}}, "X-Token", "") {
		t.Fatal("an empty secret matched a request without the header")
	}
}

func TestRequireBearerRefusesWithTheGivenReasonAndLetsTheTokenThrough(t *testing.T) {
	reached := false
	h := RequireBearer("s3cret", "this needs the admin token", func(http.ResponseWriter, *http.Request) { reached = true })

	w := httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodPut, "/config", nil))
	if w.Code != http.StatusUnauthorized || w.Header().Get("Content-Type") != "application/json" || strings.TrimSpace(w.Body.String()) != `{"error":"this needs the admin token"}` || reached {
		t.Fatalf("without the token: %d %q %s, reached %v", w.Code, w.Header().Get("Content-Type"), w.Body, reached)
	}

	r := httptest.NewRequest(http.MethodPut, "/config", nil)
	r.Header.Set("Authorization", "Bearer s3cret")
	h(httptest.NewRecorder(), r)
	if !reached {
		t.Fatal("the token did not get through")
	}
}
