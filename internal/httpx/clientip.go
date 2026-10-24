package httpx

import (
	"net"
	"net/http"
	"strings"
)

func ClientIP(req *http.Request, hops int) string {
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		host = req.RemoteAddr
	}
	if hops <= 0 {
		return host
	}
	values := req.Header["X-Forwarded-For"]
	seen := 0
	for i := len(values) - 1; i >= 0; i-- {
		rest := values[i]
		for rest != "" {
			part := rest
			if j := strings.LastIndexByte(rest, ','); j >= 0 {
				part, rest = rest[j+1:], rest[:j]
			} else {
				rest = ""
			}
			if p := strings.TrimSpace(part); p != "" {
				if seen++; seen == hops {
					return p
				}
			}
		}
	}
	return host
}
