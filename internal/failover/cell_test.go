package failover

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestACellThatDrivesControlPortsNeedsTheirToken(t *testing.T) {
	c := Cell{
		Gateways:  []string{"http://gateway-1:8080"},
		Admins:    []string{"http://gateway-1:9090"},
		Toxiproxy: "http://toxiproxy:8474",
		Controls:  map[string]string{"a": "http://dim-a:8081"},
		Rate:      100,
		Before:    time.Second,
		After:     time.Second,
	}
	if _, err := Run(context.Background(), c); err == nil || !strings.Contains(err.Error(), "needs a token") {
		t.Fatalf("ran without the control token: %v", err)
	}
}
