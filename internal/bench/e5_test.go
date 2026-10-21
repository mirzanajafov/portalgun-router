package bench

import (
	"strings"
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
		loadgen.Record{Started: int64(20 * time.Millisecond), Err: loadgen.ErrRefused, Dimension: 2},
		answer(2, 50*time.Millisecond, 2*time.Millisecond),
		answer(2, 30*time.Millisecond, 2*time.Millisecond),
	)
	if first, ok := firstServedBy(lg, "e"); !ok || !first.Equal(start.Add(30*time.Millisecond)) {
		t.Fatalf("e first served at %v (%v), want 30ms", first.Sub(start), ok)
	}
	if _, ok := firstServedBy(lg, "b"); ok {
		t.Fatal("a dimension that answered nothing served something")
	}
}

func TestErrorsAndServerFailuresAreLostWhileDroppedRequestsAreCountedApart(t *testing.T) {
	lg := requestLog(time.Unix(1_000, 0),
		answer(1, 0, time.Millisecond),
		loadgen.Record{Err: loadgen.ErrRefused, Dimension: 2},
		loadgen.Record{Status: 503, Dimension: 1},
		loadgen.Record{Err: loadgen.ErrDropped},
	)
	var run RegistryRun
	tally(&run, lg)
	if run.Requests != 4 || run.Lost != 2 || run.Dropped != 1 {
		t.Fatalf("requests %d, lost %d, dropped %d", run.Requests, run.Lost, run.Dropped)
	}
}

func TestTheSummaryCountsLossesInFailedAndInvalidRunsButNotTheirTimings(t *testing.T) {
	ten, twenty, nine := 10.0, 20.0, 9000.0
	sums := SummarizeE5([]RegistryRun{
		{Cell: "join", Requests: 100, JoinMs: &ten},
		{Cell: "join", Requests: 100, Lost: 1, JoinMs: &nine, Invalid: []string{"lag"}},
		{Cell: "join", Requests: 100, Lost: 4, JoinMs: &nine, Error: "dim-e served nothing"},
		{Cell: "join", Requests: 100, JoinMs: &twenty},
		{Cell: "leader", Requests: 50, Dropped: 3, LostRegistrations: 2, Refusals: 1, Versions: []string{"v3 b left"}},
	})
	if len(sums) != 2 || sums[0].Cell != "join" || sums[1].Cell != "leader" {
		t.Fatalf("cells %+v, want join then leader", sums)
	}
	join, leader := sums[0], sums[1]
	if join.Runs != 4 || join.Failed != 1 || join.Invalid != 1 || join.Lost != 5 || join.Requests != 400 {
		t.Fatalf("join %+v", join)
	}
	if join.Join == nil || join.Join.Max != 20 {
		t.Fatalf("join times %+v, want the 9 s of the failed and the invalid run left out", join.Join)
	}
	if leader.Join != nil || leader.Dropped != 3 || leader.LostRegistrations != 2 || leader.Refusals != 1 || leader.Versions != 1 {
		t.Fatalf("leader %+v, want no join times and the drops, lost registrations, refusals and versions counted", leader)
	}
}

func TestALogIsReadAsItsJSONLinesAndOnlyANumberIsAConfigVersion(t *testing.T) {
	out := strings.Join([]string{
		`starting up`,
		`{"msg":"no time"}`,
		`{"time":"2026-10-21T19:00:00.5Z","msg":"cluster config applied","version":7}`,
		`{"time":"2026-10-21T19:00:01Z","msg":"dimension up","version":"v1"}`,
	}, "\n")
	lines, err := parseLog([]byte(out))
	if err != nil || len(lines) != 2 {
		t.Fatalf("%+v, %v", lines, err)
	}
	if lines[0].configVersion() != 7 || lines[1].configVersion() != 0 {
		t.Fatalf("versions %d and %d, want 7 and none for a dimension's version", lines[0].configVersion(), lines[1].configVersion())
	}
}

func line(sec float64, msg string) logLine {
	return logLine{Time: time.Unix(1_000, 0).Add(time.Duration(sec * float64(time.Second))), Msg: msg}
}

func TestOnlyLinesFromTheRunItselfCount(t *testing.T) {
	lines := []logLine{line(1, "registered"), line(5, "dimension up"), line(6, "registered"), line(9, "registered")}
	at, err := firstLogged(lines, time.Unix(1_004, 0), "dimension up", "registered")
	if err != nil || at[0] != lines[1].Time || at[1] != lines[2].Time {
		t.Fatalf("%v, %v, want the first lines after the run started", at, err)
	}
	if _, err := firstLogged(lines, time.Unix(1_004, 0), "deregistered"); err == nil {
		t.Fatal("a line that never came was found")
	}
	if got := within(lines, "registered", time.Unix(1_002, 0), time.Unix(1_007, 0)); len(got) != 1 || got[0] != lines[2] {
		t.Fatalf("within %v", got)
	}
}

func TestAChangeIsFoundByTheWholeNameInTheReason(t *testing.T) {
	versions := []logLine{{Reason: "ee joined"}, {Reason: "a joined, e left", Version: 9.0}}
	if _, ok := versionWith(versions, "e joined"); ok {
		t.Fatal("ee joining was taken for e joining")
	}
	if v, ok := versionWith(versions, "e left"); !ok || v.configVersion() != 9 {
		t.Fatalf("%+v %v", v, ok)
	}
}

func TestAGatewayThatSkippedAVersionStillAppliedItUnlessItSkippedPastTheNextChange(t *testing.T) {
	lines := []logLine{
		{Time: time.Unix(1, 0), Msg: "cluster config applied", Version: 4.0},
		{Time: time.Unix(2, 0), Msg: "cluster config rejected", Version: 5.0},
		{Time: time.Unix(3, 0), Msg: "cluster config applied", Version: 6.0},
	}
	if at, ok := firstApplied(lines, 5, 7); !ok || at != time.Unix(3, 0) {
		t.Fatalf("%v %v, want the version after it", at, ok)
	}
	if _, ok := firstApplied(lines, 5, 6); ok {
		t.Fatal("the version that undid the change was taken for the change")
	}
	if _, ok := firstApplied(lines, 7, 9); ok {
		t.Fatal("a version never applied was found")
	}
}
