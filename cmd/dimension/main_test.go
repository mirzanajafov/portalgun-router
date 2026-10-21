package main

import (
	"strings"
	"testing"
	"time"
)

func TestARegistrationThatCouldNeverWorkIsRefusedAtStart(t *testing.T) {
	for name, c := range map[string]struct {
		service, id, version, advertise string
		ttl                             time.Duration
	}{
		"an empty id":             {"dimensions", "", "v1", "dim-e:8080", 10 * time.Second},
		"an id with a slash":      {"dimensions", "e/7", "v1", "dim-e:8080", 10 * time.Second},
		"an empty service":        {"", "e-7", "v1", "dim-e:8080", 10 * time.Second},
		"an address without port": {"dimensions", "e-7", "v1", "dim-e", 10 * time.Second},
		"a URL as the address":    {"dimensions", "e-7", "v1", "http://dim-e:8080", 10 * time.Second},
		"no TTL":                  {"dimensions", "e-7", "v1", "dim-e:8080", 0},
		"a TTL over a day":        {"dimensions", "e-7", "v1", "dim-e:8080", 48 * time.Hour},
		"a dot dot id":            {"dimensions", "..", "v1", "dim-e:8080", 10 * time.Second},
		"a long version":          {"dimensions", "e-7", strings.Repeat("v", 300), "dim-e:8080", 10 * time.Second},
	} {
		if _, err := registration("http://ff1:3171", c.service, c.id, c.version, c.advertise, c.ttl); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	if _, err := registration(" http://ff1:3171, http://ff2:3171,", "dimensions", "e-7", "v1", "dim-e:8080", 10*time.Second); err != nil {
		t.Errorf("a valid registration with spaces and a trailing comma: %v", err)
	}
}
