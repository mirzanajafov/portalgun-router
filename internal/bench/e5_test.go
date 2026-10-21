package bench

import (
	"testing"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
)

func requestLog(start time.Time, recs ...loadgen.Record) *loadgen.Log {
	return &loadgen.Log{StartUnixNano: start.UnixNano(), Dimensions: []string{"a", "e"}, Records: recs}
}

func answer(dim uint8, at, took time.Duration) loadgen.Record {
	return loadgen.Record{Started: int64(at), Ended: int64(at + took), Status: 200, Dimension: dim}
}

func TestARequestCountsForTheDimensionThatAnsweredIt(t *testing.T) {
	start := time.Unix(1_000, 0)
	lg := requestLog(start,
		answer(1, 10*time.Millisecond, time.Millisecond),
		answer(2, 30*time.Millisecond, 2*time.Millisecond),
		answer(2, 50*time.Millisecond, 2*time.Millisecond),
		loadgen.Record{Started: int64(60 * time.Millisecond), Err: loadgen.ErrRefused, Dimension: 2},
		loadgen.Record{Started: int64(70 * time.Millisecond), Status: 503, Dimension: 1},
		loadgen.Record{Started: int64(80 * time.Millisecond), Err: loadgen.ErrDropped},
	)
	firstAt, lastAt, ok := servedBy(lg, "e")
	if !ok || !firstAt.Equal(start.Add(30*time.Millisecond)) || !lastAt.Equal(start.Add(52*time.Millisecond)) {
		t.Fatalf("e served from %v to %v (%v)", firstAt.Sub(start), lastAt.Sub(start), ok)
	}
	if _, _, ok := servedBy(lg, "b"); ok {
		t.Fatal("a dimension that answered nothing served something")
	}
	var trial RegistryTrial
	tally(&trial, lg)
	if trial.Requests != 6 || trial.Lost != 2 || trial.Dropped != 1 {
		t.Fatalf("requests %d, lost %d, dropped %d", trial.Requests, trial.Lost, trial.Dropped)
	}
}

func TestTheSummaryCountsLossesInFailedTrialsButNotTheirTimings(t *testing.T) {
	ten, twenty, nine := 10.0, 20.0, 9000.0
	s := SummarizeE5([]RegistryTrial{
		{Cell: "join", Requests: 100, JoinMs: &ten},
		{Cell: "join", Requests: 100, JoinMs: &twenty, Invalid: []string{"lag"}},
		{Cell: "join", Requests: 100, Lost: 4, JoinMs: &nine, Error: "dim-e served nothing"},
		{Cell: "leader", Requests: 50, LostRegistrations: 2, Versions: []string{"v3 b left"}},
	})
	join := s["join"]
	if join.Trials != 3 || join.Failed != 1 || join.Invalid != 1 || join.Lost != 4 || join.Requests != 300 {
		t.Fatalf("join %+v", join)
	}
	if join.Join == nil || join.Join.Max != 20 {
		t.Fatalf("join times %+v, want the failed trial's 9 s left out", join.Join)
	}
	leader := s["leader"]
	if leader.Join != nil || leader.LostRegistrations != 2 || leader.Versions != 1 {
		t.Fatalf("leader %+v, want no join times and the lost registrations counted", leader)
	}
}
