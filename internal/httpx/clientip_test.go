package httpx

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClientIPTrustsOnlyTheConfiguredHops(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.2:4000"
	req.Header.Add("X-Forwarded-For", "6.6.6.6, 1.2.3.4")
	req.Header.Add("X-Forwarded-For", "10.0.0.1")
	for hops, want := range map[int]string{0: "10.0.0.2", 1: "10.0.0.1", 2: "1.2.3.4", 3: "6.6.6.6", 4: "10.0.0.2"} {
		if got := ClientIP(req, hops); got != want {
			t.Errorf("hops %d: %s, want %s", hops, got, want)
		}
	}
}

func clientIPReference(req *http.Request, hops int) string {
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		host = req.RemoteAddr
	}
	if hops <= 0 {
		return host
	}
	var seen []string
	for _, v := range req.Header.Values("X-Forwarded-For") {
		for part := range strings.SplitSeq(v, ",") {
			if p := strings.TrimSpace(part); p != "" {
				seen = append(seen, p)
			}
		}
	}
	if len(seen) >= hops {
		return seen[len(seen)-hops]
	}
	return host
}

func FuzzClientIP(f *testing.F) {
	f.Add("10.0.0.2:4000", "6.6.6.6, 1.2.3.4", "10.0.0.1", uint8(2))
	f.Add("[::1]:80", ",,", " , 9.9.9.9 ,", uint8(1))
	f.Add("no-port", "", "\t1.1.1.1 ", uint8(3))
	f.Fuzz(func(t *testing.T, remote, first, second string, hops uint8) {
		req := &http.Request{RemoteAddr: remote, Header: http.Header{"X-Forwarded-For": {first, second}}}
		n := int(hops % 12)
		if got, want := ClientIP(req, n), clientIPReference(req, n); got != want {
			t.Fatalf("ClientIP(%q, %q, %q, %d) = %q, the plain version says %q", remote, first, second, n, got, want)
		}
	})
}
