package proxy

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/config"
	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
	"github.com/redis/go-redis/v9"
)

func limited(t *testing.T, rl *config.RateLimit, hops int) *config.Config {
	return parse(t, config.Config{
		TrustedHops: hops,
		Routes:      []config.Route{{Name: "api", PathPrefix: "/", Pool: "p", RateLimit: rl}},
		Pools:       []config.Pool{{Name: "p", Dimensions: []config.Dimension{{ID: "a", Address: dim(t, "a", nil)}}}},
	})
}

func hit(t *testing.T, url string, header http.Header) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	maps.Copy(req.Header, header)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode, resp.Header.Get("Retry-After")
}

func TestStaticLimiterAnswers429(t *testing.T) {
	g, srv := serve(t, limited(t, &config.RateLimit{Design: config.LimitStaticSplit, Rate: 0.5, Burst: 3}, 0))
	codes := map[int]int{}
	var retry string
	for range 5 {
		code, ra := hit(t, srv.URL+"/", nil)
		codes[code]++
		if code == http.StatusTooManyRequests {
			retry = ra
		}
	}
	if codes[200] != 3 || codes[429] != 2 {
		t.Fatalf("codes %v", codes)
	}
	if secs, err := strconv.Atoi(retry); err != nil || secs < 1 || secs > 2 {
		t.Fatalf("Retry-After %q", retry)
	}
	if !strings.Contains(scrape(t, g), "portal_rate_limited_total{route=\"api\"} 2\n") {
		t.Fatal("limited requests not counted")
	}
}

func TestDecisionsAreRecordedAndDrained(t *testing.T) {
	cfg := limited(t, &config.RateLimit{Design: config.LimitStaticSplit, Rate: 0.1, Burst: 2, Key: "header:X-Api-Key"}, 0)
	g, err := New(cfg, Options{Decisions: 3})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.Close)
	srv := httptest.NewServer(g)
	t.Cleanup(srv.Close)
	admin := httptest.NewServer(AdminHandler(g, ""))
	t.Cleanup(admin.Close)
	before := time.Now().UnixNano()
	for range 4 {
		hit(t, srv.URL+"/", http.Header{"X-Api-Key": {"rick"}})
	}
	resp, err := http.Post(admin.URL+"/decisions/drain", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	var got gatewayapi.Decisions
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !got.Recording || got.Dropped != 1 || len(got.Decisions) != 3 {
		t.Fatalf("%+v", got)
	}
	for i, d := range got.Decisions {
		if d.Allowed != (i < 2) || d.UnixNano < before || !strings.Contains(d.Key, "rick") {
			t.Fatalf("decision %d: %+v", i, d)
		}
	}
	if again := g.DrainDecisions(); len(again.Decisions) != 0 || again.Dropped != 0 {
		t.Fatalf("second drain %+v", again)
	}
	hit(t, srv.URL+"/", http.Header{"X-Api-Key": {"morty"}})
	if next := g.DrainDecisions(); len(next.Decisions) != 1 || !next.Decisions[0].Allowed {
		t.Fatalf("after drain %+v", next)
	}
}

func TestNoDecisionsUnlessAsked(t *testing.T) {
	g, srv := serve(t, limited(t, &config.RateLimit{Design: config.LimitStaticSplit, Rate: 1, Burst: 1}, 0))
	hit(t, srv.URL+"/", nil)
	if d := g.DrainDecisions(); d.Recording || len(d.Decisions) != 0 {
		t.Fatalf("%+v", d)
	}
}

func TestLimitKeyFromAHeader(t *testing.T) {
	_, srv := serve(t, limited(t, &config.RateLimit{Design: config.LimitStaticSplit, Rate: 0.1, Burst: 2, Key: "header:X-Api-Key"}, 0))
	for _, key := range []string{"rick", "morty"} {
		for i := range 3 {
			code, _ := hit(t, srv.URL+"/", http.Header{"X-Api-Key": {key}})
			if want := map[bool]int{true: 200, false: 429}[i < 2]; code != want {
				t.Fatalf("%s request %d: %d, want %d", key, i+1, code, want)
			}
		}
	}
}

func TestSpoofedForwardedForDoesNotDodgeTheLimit(t *testing.T) {
	_, srv := serve(t, limited(t, &config.RateLimit{Design: config.LimitStaticSplit, Rate: 0.1, Burst: 2}, 0))
	codes := map[int]int{}
	for i := range 4 {
		code, _ := hit(t, srv.URL+"/", http.Header{"X-Forwarded-For": {strconv.Itoa(i) + ".1.1.1"}})
		codes[code]++
	}
	if codes[429] != 2 {
		t.Fatalf("codes %v: a client picked its own address", codes)
	}
}

func TestRedisDesignsNeedRedis(t *testing.T) {
	cfg := limited(t, &config.RateLimit{Design: config.LimitRedisExact, Rate: 10, Burst: 10}, 0)
	if _, err := New(cfg, Options{}); err == nil || !strings.Contains(err.Error(), "needs Redis") {
		t.Fatalf("got %v", err)
	}
}

func TestLimiterStateSurvivesAnUnchangedConfig(t *testing.T) {
	rl := &config.RateLimit{Design: config.LimitStaticSplit, Rate: 0.1, Burst: 2}
	cfg := limited(t, rl, 0)
	g, srv := serve(t, cfg)
	hit(t, srv.URL+"/", nil)
	hit(t, srv.URL+"/", nil)
	if err := g.Apply(limited(t, &config.RateLimit{Design: config.LimitStaticSplit, Rate: 0.1, Burst: 2}, 0)); err != nil {
		t.Fatal(err)
	}
	if code, _ := hit(t, srv.URL+"/", nil); code != 429 {
		t.Fatalf("got %d: a config swap refilled the bucket", code)
	}
	if err := g.Apply(limited(t, &config.RateLimit{Design: config.LimitStaticSplit, Rate: 0.1, Burst: 5}, 0)); err != nil {
		t.Fatal(err)
	}
	if code, _ := hit(t, srv.URL+"/", nil); code != 200 {
		t.Fatalf("got %d after the limit changed", code)
	}
}

func TestTwoGatewaysShareARedisLimit(t *testing.T) {
	addr := os.Getenv("PORTAL_TEST_REDIS")
	if addr == "" {
		t.Skip("PORTAL_TEST_REDIS not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	route := "api-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	build := func() *config.Config {
		return parse(t, config.Config{
			Routes: []config.Route{{Name: route, PathPrefix: "/", Pool: "p", RateLimit: &config.RateLimit{Design: config.LimitRedisExact, Rate: 0.1, Burst: 4, Instances: 2, RedisTimeout: config.Duration(time.Second)}}},
			Pools:  []config.Pool{{Name: "p", Dimensions: []config.Dimension{{ID: "a", Address: dim(t, "a", nil)}}}},
		})
	}
	var urls []string
	for range 2 {
		g, err := New(build(), Options{Redis: rdb})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(g.Close)
		srv := httptest.NewServer(g)
		t.Cleanup(srv.Close)
		urls = append(urls, srv.URL)
	}
	ok := 0
	for i := range 10 {
		if code, _ := hit(t, urls[i%2]+"/", nil); code == 200 {
			ok++
		}
	}
	if ok != 4 {
		t.Fatalf("two gateways admitted %d against a shared burst of 4", ok)
	}
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
}
