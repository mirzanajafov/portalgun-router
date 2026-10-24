package httpx

import (
	"net/http"
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
