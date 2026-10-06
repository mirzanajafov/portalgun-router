package loadgen

import (
	"math"
	"net/url"
	"testing"
	"time"
)

func urls(t *testing.T, raw ...string) []*url.URL {
	t.Helper()
	var out []*url.URL
	for _, r := range raw {
		u, err := url.Parse(r)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, u)
	}
	return out
}

func TestBurstScheduleRaisesTheRateInsideTheBurst(t *testing.T) {
	for _, arrival := range []Arrival{Constant, Poisson} {
		s, _ := NewSchedule(1000, arrival, 5)
		if err := s.WithBurst(Burst{Rate: 5000, Length: 200 * time.Millisecond, Every: 2 * time.Second}); err != nil {
			t.Fatal(err)
		}
		var inBurst, outside int
		for {
			at := s.Next()
			if at >= int64(20*time.Second) {
				break
			}
			if at%int64(2*time.Second) < int64(200*time.Millisecond) {
				inBurst++
			} else {
				outside++
			}
		}
		if math.Abs(float64(inBurst)-10*1000) > 300 || math.Abs(float64(outside)-10*1800) > 400 {
			t.Fatalf("%s: %d in bursts, want about 10000; %d outside, want about 18000", arrival, inBurst, outside)
		}
	}
	s, _ := NewSchedule(10, Constant, 1)
	if s.WithBurst(Burst{Rate: 50, Length: time.Second, Every: time.Second}) == nil {
		t.Fatal("a burst as long as its period was accepted")
	}
}

func TestWeightsSplitAndShift(t *testing.T) {
	us := urls(t, "http://a/", "http://b/")
	p, err := newPicker(Config{Weights: []float64{9, 1}, ShiftEvery: 5 * time.Second, Seed: 3}, us)
	if err != nil {
		t.Fatal(err)
	}
	count := func(from time.Duration) int {
		a := 0
		for i := range 10000 {
			if p.url(i, int64(from)+int64(i)).Host == "a" {
				a++
			}
		}
		return a
	}
	if a := count(time.Second); a < 8800 || a > 9200 {
		t.Fatalf("first period: %d of 10000 to a, want about 9000", a)
	}
	if a := count(6 * time.Second); a < 800 || a > 1200 {
		t.Fatalf("second period: %d of 10000 to a, want about 1000", a)
	}
	plain, _ := newPicker(Config{}, us)
	if plain.url(0, 0).Host != "a" || plain.url(1, 0).Host != "b" || plain.url(2, 0).Host != "a" {
		t.Fatal("without weights the urls should take turns")
	}
	for _, bad := range []Config{{Weights: []float64{1}}, {Weights: []float64{0, 0}}, {Weights: []float64{1, -1}}, {ShiftEvery: time.Second}} {
		if _, err := newPicker(bad, us); err == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
}

func TestKeysAreSkewedUnderZipf(t *testing.T) {
	us := urls(t, "http://a/")
	p, err := newPicker(Config{KeyHeader: "X-Key", KeyPrefix: "run1-", Keys: 10000, ZipfS: 1.1, Seed: 1}, us)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for range 100000 {
		seen[p.key()]++
	}
	if seen["run1-0"] < 5000 || len(seen) < 2000 || seen["run1-0"] < 10*seen["run1-100"] {
		t.Fatalf("top key %d, distinct %d, key 100 %d", seen["run1-0"], len(seen), seen["run1-100"])
	}
	one, _ := newPicker(Config{KeyHeader: "X-Key", KeyPrefix: "k"}, us)
	if one.key() != "k0" || one.key() != "k0" {
		t.Fatal("a single key should always be the same")
	}
	none, _ := newPicker(Config{}, us)
	if none.key() != "" {
		t.Fatal("no key header, no key")
	}
	if _, err := newPicker(Config{KeyHeader: "X-Key", Keys: 5, ZipfS: 0.5}, us); err == nil {
		t.Fatal("a zipf exponent below 1 was accepted")
	}
}
