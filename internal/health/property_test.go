package health

import (
	"testing"
	"time"

	"pgregory.net/rapid"
)

type budgetEvent struct {
	gap   time.Duration
	retry bool
}

func TestBudgetMatchesACountOverTheLastTenSeconds(t *testing.T) {
	var cases, refusing int
	rapid.Check(t, func(t *rapid.T) {
		ratio := rapid.Float64Range(0, 1).Draw(t, "ratio")
		floor := rapid.Float64Range(0, 5).Draw(t, "minPerSecond")
		gap := rapid.OneOf(
			rapid.Int64Range(0, int64(300*time.Millisecond)),
			rapid.SampledFrom([]int64{int64(time.Second), int64(9 * time.Second), int64(10 * time.Second), int64(25 * time.Second)}),
		)
		events := rapid.SliceOfN(rapid.Custom(func(t *rapid.T) budgetEvent {
			return budgetEvent{gap: time.Duration(gap.Draw(t, "gap")), retry: rapid.Bool().Draw(t, "retry")}
		}), 20, 300).Draw(t, "events")

		cases++
		refused := false
		b := NewBudget(ratio, floor)
		var now int64
		var requests, granted []int64
		inWindow := func(secs []int64, sec int64) float64 {
			n := 0
			for _, s := range secs {
				if s > sec-budgetWindow && s <= sec {
					n++
				}
			}
			return float64(n)
		}
		for i, e := range events {
			now += int64(e.gap)
			sec := now / int64(time.Second)
			if !e.retry {
				b.Request(now)
				requests = append(requests, sec)
				continue
			}
			allowed := max(ratio*inWindow(requests, sec), floor*budgetWindow)
			spent := inWindow(granted, sec)
			ok := b.TryRetry(now)
			if ok {
				granted = append(granted, sec)
			} else if !refused {
				refused = true
				refusing++
			}
			if want := spent+1 <= allowed; ok != want {
				t.Fatalf("event %d at %ds: retry granted %v, but %0.f of %.2f retries were spent in the last ten seconds", i, sec, ok, spent, allowed)
			}
		}
	})
	if refusing < cases/5 {
		t.Fatalf("only %d of %d cases ever refused a retry, so the property was barely exercised", refusing, cases)
	}
}
