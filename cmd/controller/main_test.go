package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mirzanajafov/portalgun-router/internal/chaos"
	"github.com/mirzanajafov/portalgun-router/internal/dimension"
)

func TestChaosTargetsNeedTheControlToken(t *testing.T) {
	if _, _, err := chaosTargets("a=http://dim-a:8081", ""); err == nil {
		t.Fatal("targets were accepted without a control token")
	}
	names, controls, err := chaosTargets("a=http://dim-a:8081, b=http://dim-b:8081", "control-token")
	if err != nil || len(names) != 2 || controls["b"].Token != "control-token" {
		t.Fatalf("names %v, controls %v, err %v", names, controls, err)
	}
	if _, _, err := chaosTargets("", ""); err != nil {
		t.Fatalf("no targets and no token: %v", err)
	}
}

func TestTheControllerStopsAtStartOnlyWhenADimensionRefusesItsToken(t *testing.T) {
	toxiproxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	t.Cleanup(toxiproxy.Close)
	s, err := dimension.New("a", "v1", dimension.DefaultBehavior())
	if err != nil {
		t.Fatal(err)
	}
	control := httptest.NewServer(s.ControlHandler("control-token"))
	t.Cleanup(control.Close)
	gone := httptest.NewServer(http.NotFoundHandler())
	gone.Close()
	ctx, logger := context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil))

	o := options{toxiproxy: toxiproxy.URL, targets: "a=" + control.URL + ",b=" + gone.URL, controlToken: "control-token"}
	if inj, names, err := chaosInjector(ctx, o, logger); err != nil || inj == nil || len(names) != 2 {
		t.Fatalf("a dimension that isn't up yet stopped the controller: %v", err)
	}
	o.controlToken = "old-token"
	if _, _, err := chaosInjector(ctx, o, logger); !errors.Is(err, chaos.ErrControlToken) {
		t.Fatalf("a refused token came back as %v", err)
	}
	if inj, _, err := chaosInjector(ctx, options{targets: o.targets}, logger); err != nil || inj != nil {
		t.Fatalf("without Toxiproxy the controller drives no chaos: %v, %v", inj, err)
	}
}
