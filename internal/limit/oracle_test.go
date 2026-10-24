package limit

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/gatewayapi"
)

const base = int64(1_700_000_000_000_000_000)

func at(ms float64) int64 { return base + int64(ms*float64(time.Millisecond)) }

func TestReplayAgreesWithAPerfectLimiter(t *testing.T) {
	rule := Rule{Rate: 100, Burst: 10}
	exact := NewBuckets(rule)
	var ds []gatewayapi.Decision
	for i := range 3000 {
		now := at(float64(i) * 2)
		ok, _ := exact.Take("k", now)
		ds = append(ds, gatewayapi.Decision{UnixNano: now, Key: "k", Allowed: ok})
	}
	rand.New(rand.NewPCG(1, 2)).Shuffle(len(ds), func(i, j int) { ds[i], ds[j] = ds[j], ds[i] })
	v := Replay(ds, rule)
	if v.FalseAdmits != 0 || v.FalseRejects != 0 || v.OverAdmission != 0 {
		t.Fatalf("%+v", v)
	}
	if v.Admitted < 600 || v.Admitted > 612 {
		t.Fatalf("admitted %d over 6 s at 100/s with a burst of 10", v.Admitted)
	}
	if v.Worst.Admitted > 110 || v.WorstRatio > 1 {
		t.Fatalf("worst window %+v", v.Worst)
	}
}

func TestReplayCountsTwoInstancesThatEachThinkTheyAreAlone(t *testing.T) {
	rule := Rule{Rate: 100, Burst: 10}
	a, b := NewBuckets(rule), NewBuckets(rule)
	var ds []gatewayapi.Decision
	for i := range 2000 {
		now := at(float64(i) * 2.5)
		inst := a
		if i%2 == 1 {
			inst = b
		}
		ok, _ := inst.Take("k", now)
		ds = append(ds, gatewayapi.Decision{UnixNano: now, Key: "k", Allowed: ok})
	}
	v := Replay(ds, rule)
	if v.OverAdmission < 0.9 || v.OverAdmission > 1.1 {
		t.Fatalf("over-admission %.2f, want about 1: two unsynced buckets admit twice the rate", v.OverAdmission)
	}
	if v.FalseAdmits < 450 || v.WorstRatio < 1.8 || v.MaxAhead < 450 {
		t.Fatalf("%+v", v)
	}
}

func TestReplayFindsFalseRejects(t *testing.T) {
	ds := []gatewayapi.Decision{
		{UnixNano: at(0), Key: "a", Allowed: true},
		{UnixNano: at(1), Key: "a", Allowed: false},
		{UnixNano: at(2), Key: "b", Allowed: false},
		{UnixNano: at(3), Key: "a", Allowed: false},
	}
	v := Replay(ds, Rule{Rate: 1, Burst: 2})
	if v.FalseRejects != 2 || v.FalseAdmits != 0 || v.OracleAdmitted != 3 || v.Admitted != 1 {
		t.Fatalf("%+v", v)
	}
	if v.OverAdmission > -0.66 || v.OverAdmission < -0.67 {
		t.Fatalf("over-admission %v", v.OverAdmission)
	}
}

func TestWorstWindowIsPerKey(t *testing.T) {
	var ds []gatewayapi.Decision
	for i := range 8 {
		ds = append(ds, gatewayapi.Decision{UnixNano: at(float64(i) * 100), Key: "a", Allowed: true})
		ds = append(ds, gatewayapi.Decision{UnixNano: at(float64(i)*100 + 50), Key: "b", Allowed: i < 3})
	}
	v := Replay(ds, Rule{Rate: 2, Burst: 2})
	if v.Worst.Key != "a" || v.Worst.Admitted != 8 || v.Worst.Allowed != 4 || v.WorstRatio != 2 {
		t.Fatalf("%+v", v.Worst)
	}
}

func TestJitteredTimestampsSwapDecisionsButNotTheCount(t *testing.T) {
	rule := Rule{Rate: 1000, Burst: 100}
	exact := NewBuckets(rule)
	rng := rand.New(rand.NewPCG(9, 9))
	var ds []gatewayapi.Decision
	for i := range 45000 {
		now := at(float64(i) / 1.5)
		ok, _ := exact.Take("k", now)
		seen := now + int64(rng.Float64()*float64(2*time.Millisecond))
		ds = append(ds, gatewayapi.Decision{UnixNano: seen, Key: "k", Allowed: ok})
	}
	v := Replay(ds, rule)
	if v.FalseAdmits < 1000 || v.FalseRejects < 1000 {
		t.Fatalf("false admits %d, rejects %d: at the limit every request is marginal, so jitter alone should swap many", v.FalseAdmits, v.FalseRejects)
	}
	if v.OverAdmission > 0.002 || v.OverAdmission < -0.002 || v.MaxAhead > 5 || v.MaxBehind > 5 {
		t.Fatalf("over-admission %v, ahead %d, behind %d: the count should not move", v.OverAdmission, v.MaxAhead, v.MaxBehind)
	}
}
