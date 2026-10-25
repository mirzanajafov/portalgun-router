package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/dimension"
	"github.com/mirzanajafov/portalgun-router/internal/registry"
)

func flags(change func(*registrationFlags)) registrationFlags {
	f := registrationFlags{endpoints: "http://ff1:3171", service: "dimensions", id: "e-7", version: "v1", advertise: "dim-e:8080", ttl: 10 * time.Second}
	change(&f)
	return f
}

func TestARegistrationThatCouldNeverWorkIsRefusedAtStart(t *testing.T) {
	for name, f := range map[string]registrationFlags{
		"an empty id":                  flags(func(f *registrationFlags) { f.id = "" }),
		"an id with a slash":           flags(func(f *registrationFlags) { f.id = "e/7" }),
		"an empty service":             flags(func(f *registrationFlags) { f.service = "" }),
		"an address without port":      flags(func(f *registrationFlags) { f.advertise = "dim-e" }),
		"a URL as the address":         flags(func(f *registrationFlags) { f.advertise = "http://dim-e:8080" }),
		"no TTL":                       flags(func(f *registrationFlags) { f.ttl = 0 }),
		"a TTL over a day":             flags(func(f *registrationFlags) { f.ttl = 48 * time.Hour }),
		"a dot dot id":                 flags(func(f *registrationFlags) { f.id = ".." }),
		"a version over 1 KiB as sent": flags(func(f *registrationFlags) { f.version = strings.Repeat("&", 200) }),
		"a long id":                    flags(func(f *registrationFlags) { f.id = strings.Repeat("e", 129) }),
		"an id that isn't UTF-8":       flags(func(f *registrationFlags) { f.id = "caf\xe9" }),
		"a port over 65535":            flags(func(f *registrationFlags) { f.advertise = "dim-e:70000" }),
		"a maxInflight of zero":        flags(func(f *registrationFlags) { f.maxInflight = "0" }),
		"a maxInflight of words":       flags(func(f *registrationFlags) { f.maxInflight = "many" }),
	} {
		if _, _, err := registration(f); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestARegistrationAnnouncesTheVersionAndTheCapAsTheRegistryWillList(t *testing.T) {
	_, inst, err := registration(flags(func(f *registrationFlags) {
		f.endpoints = " http://ff1:3171, http://ff2:3171,"
		f.maxInflight = "+0256"
	}))
	want := registry.Instance{Name: "e-7", Addr: "dim-e:8080", Meta: map[string]string{"version": "v1", "maxInflight": "256"}}
	if err != nil || !reflect.DeepEqual(inst, want) {
		t.Fatalf("%+v, %v", inst, err)
	}
	if _, inst, err := registration(flags(func(*registrationFlags) {})); err != nil || len(inst.Meta) != 1 {
		t.Fatalf("without -max-inflight: %v, metadata %v, want only the version", err, inst.Meta)
	}
}

func TestNamesUpToTheRegistrysLimitAreAccepted(t *testing.T) {
	if _, _, err := registration(flags(func(f *registrationFlags) { f.id = strings.Repeat("e", 128) })); err != nil {
		t.Fatalf("a 128-byte id: %v", err)
	}
}

func TestADimensionThatRegistersIsReadyOnlyOnceItHoldsALease(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	control := ln.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	reports := make(chan func(bool), 1)
	keep := func(ctx context.Context, held func(bool)) {
		reports <- held
		<-ctx.Done()
	}
	stopped := make(chan error, 1)
	go func() {
		stopped <- run(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), options{id: "e-7", version: "v1", addr: "127.0.0.1:0", control: ln, controlToken: "control-token", behavior: dimension.DefaultBehavior(), keep: keep})
	}()
	var held func(bool)
	select {
	case held = <-reports:
	case err := <-stopped:
		t.Fatalf("the dimension stopped before registering: %v", err)
	}
	ready := func() int {
		resp, err := http.Get("http://" + control + "/ready")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if got := ready(); got != http.StatusServiceUnavailable {
		t.Fatalf("before the first lease: %d", got)
	}
	held(true)
	if got := ready(); got != http.StatusOK {
		t.Fatalf("with a lease: %d", got)
	}
	cancel()
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
}
