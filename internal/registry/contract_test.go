package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"
)

type exchange struct {
	Name    string `json:"name"`
	Request struct {
		Method string          `json:"method"`
		Path   string          `json:"path"`
		Body   json.RawMessage `json:"body"`
	} `json:"request"`
	Response struct {
		Status  int               `json:"status"`
		Headers map[string]string `json:"headers"`
		Body    json.RawMessage   `json:"body"`
	} `json:"response"`
}

func sameJSON(a, b []byte) bool {
	if len(bytes.TrimSpace(a)) == 0 || len(bytes.TrimSpace(b)) == 0 {
		return len(bytes.TrimSpace(a)) == len(bytes.TrimSpace(b))
	}
	var x, y any
	return json.Unmarshal(a, &x) == nil && json.Unmarshal(b, &y) == nil && reflect.DeepEqual(x, y)
}

func versioned(name, addr, version string) Instance {
	return Instance{Name: name, Addr: addr, Meta: map[string]string{"version": version}}
}

func TestTheClientSpeaksEveryExampleOfTheContract(t *testing.T) {
	raw, err := os.ReadFile("testdata/contract-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var exchanges []exchange
	if err := json.Unmarshal(raw, &exchanges); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	dims := []Instance{versioned("c-137", "toxiproxy:18080", "v1"), versioned("d-99", "toxiproxy:18082", "v2")}
	cases := map[string]func(t *testing.T, c *Client, endpoint string){
		"list a service": func(t *testing.T, c *Client, _ string) {
			list, index, err := c.Lookup(ctx, "dimensions", 0, 30*time.Second)
			if err != nil || index != 57 || !reflect.DeepEqual(list, dims) {
				t.Errorf("%v at %d, %v", list, index, err)
			}
		},
		"wait for a change after an index": func(t *testing.T, c *Client, _ string) {
			list, index, err := c.Lookup(ctx, "dimensions", 57, 30*time.Second)
			want := append(dims, Instance{Name: "e-7", Addr: "dim-e:8080"})
			if err != nil || index != 64 || !reflect.DeepEqual(list, want) {
				t.Errorf("%v at %d, %v", list, index, err)
			}
		},
		"a service with no instances": func(t *testing.T, c *Client, _ string) {
			list, index, err := c.Lookup(ctx, "billing", 0, 30*time.Second)
			if err != nil || index != 12 || len(list) != 0 {
				t.Errorf("%v at %d, %v", list, index, err)
			}
		},
		"the cluster cannot answer in time": func(t *testing.T, c *Client, _ string) {
			if _, _, err := c.Lookup(ctx, "dimensions", 64, 30*time.Second); err == nil {
				t.Error("a 503 came back as an answer")
			}
		},
		"register an instance": func(t *testing.T, c *Client, _ string) {
			l, err := c.Register(ctx, "dimensions", Instance{Name: "e-7", Addr: "dim-e:8080", Meta: map[string]string{"version": "v1"}}, 10*time.Second)
			if err != nil || l != (Lease{ID: 63, TTL: 10 * time.Second}) {
				t.Errorf("%+v, %v", l, err)
			}
		},
		"register under a name with a slash": func(t *testing.T, c *Client, _ string) {
			if _, err := c.Register(ctx, "dimensions", Instance{Name: "e/7", Addr: "dim-e:8080", Meta: map[string]string{"version": "v1"}}, 10*time.Second); err == nil {
				t.Error("a 400 came back as a lease")
			}
		},
		"renew a lease": func(t *testing.T, c *Client, _ string) {
			if ttl, err := c.KeepAlive(ctx, 63); err != nil || ttl != 10*time.Second {
				t.Errorf("%v, %v", ttl, err)
			}
		},
		"renew a lease that is gone": func(t *testing.T, c *Client, _ string) {
			if _, err := c.KeepAlive(ctx, 63); !errors.Is(err, ErrLeaseNotFound) {
				t.Errorf("%v, want ErrLeaseNotFound", err)
			}
		},
		"deregister": func(t *testing.T, c *Client, _ string) {
			if err := c.Revoke(ctx, 63); err != nil {
				t.Error(err)
			}
		},
		"this node's view": func(t *testing.T, c *Client, endpoint string) {
			if st, err := c.Status(ctx, endpoint); err != nil || st != (Status{ID: 3, Role: "leader", Leader: 3, Term: 7}) {
				t.Errorf("%+v, %v", st, err)
			}
		},
	}
	if len(exchanges) != len(cases) {
		t.Fatalf("the contract has %d examples and the test %d cases", len(exchanges), len(cases))
	}
	for _, ex := range exchanges {
		t.Run(ex.Name, func(t *testing.T) {
			run, ok := cases[ex.Name]
			if !ok {
				t.Fatalf("no case for the example %q", ex.Name)
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				if r.Method != ex.Request.Method || r.URL.RequestURI() != ex.Request.Path || !sameJSON(body, ex.Request.Body) {
					t.Errorf("sent %s %s %s, want %s %s %s", r.Method, r.URL.RequestURI(), body, ex.Request.Method, ex.Request.Path, ex.Request.Body)
				}
				for k, v := range ex.Response.Headers {
					w.Header().Set(k, v)
				}
				w.WriteHeader(ex.Response.Status)
				_, _ = w.Write(ex.Response.Body)
			}))
			defer srv.Close()
			c, err := New([]string{srv.URL})
			if err != nil {
				t.Fatal(err)
			}
			run(t, c, srv.URL)
		})
	}
}
