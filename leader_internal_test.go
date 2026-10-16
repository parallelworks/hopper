package hopper

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/parallelworks/hopper/driver"
)

// A pass that ends before reaching the work deferred last time (it failed,
// or found another session's build lock) neither ends a run of deferred
// passes nor extends it, so that such passes between deferred ones do not
// put off the warning or shorten the lock timeout.
func TestNextDeferrals(t *testing.T) {
	for _, c := range []struct {
		n                    int
		deferred, endedEarly bool
		want                 int
	}{
		{3, true, false, 4},
		{3, true, true, 4},
		{3, false, true, 3},
		{3, false, false, 0},
	} {
		if got := nextDeferrals(c.n, c.deferred, c.endedEarly); got != c.want {
			t.Errorf("nextDeferrals(%d, %v, %v) = %d, want %d", c.n, c.deferred, c.endedEarly, got, c.want)
		}
	}
}

// A pass that tried a backed-off name and finished without reporting it
// again forgets its backoff, so that a later failure starts over at the
// shortest wait; one still failing, skipped, or not reached keeps it. A
// failed repair is reported after the rest of the pass, which it does not
// leave unreached.
func TestSettleBackoffs(t *testing.T) {
	backoffs := func() map[string]indexBackoff {
		return map[string]indexBackoff{"repaired": {failures: 3}, "invalid": {failures: 3}, "timedout": {failures: 2}, "waiting": {failures: 1}}
	}
	failed := errors.New("pass failed")
	for _, c := range []struct {
		name string
		res  driver.HistoryIndexResult
		err  error
		want []string
	}{
		{"finished", driver.HistoryIndexResult{Invalid: []string{"invalid"}, TimedOut: []string{"timedout"}}, nil, []string{"invalid", "timedout", "waiting"}},
		{"failed", driver.HistoryIndexResult{}, failed, []string{"invalid", "repaired", "timedout", "waiting"}},
		{"repair failed", driver.HistoryIndexResult{}, fmt.Errorf("%w: %w", driver.ErrHistoryIndexRepair, failed), []string{"waiting"}},
		{"busy", driver.HistoryIndexResult{Busy: "x_ordering"}, nil, []string{"invalid", "repaired", "timedout", "waiting"}},
		{"deferred", driver.HistoryIndexResult{Deferred: []string{"x_ordering"}}, nil, []string{"invalid", "repaired", "timedout", "waiting"}},
	} {
		b := backoffs()
		settleBackoffs(b, []string{"waiting"}, c.res, c.err)
		if got := slices.Sorted(maps.Keys(b)); !slices.Equal(got, c.want) {
			t.Errorf("%s: backoffs left = %q, want %q", c.name, got, c.want)
		}
	}
}

// Only a failure other than a repair's ends a pass before the rest of its
// work: a repair's is reported once the rest is done.
func TestIndexPassFailed(t *testing.T) {
	failed := errors.New("pass failed")
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"failed", failed, true},
		{"repair failed", fmt.Errorf("%w: %w", driver.ErrHistoryIndexRepair, failed), false},
		{"failed after a repair", errors.Join(failed, errors.New("probe failed")), true},
	} {
		if got := indexPassFailed(c.err); got != c.want {
			t.Errorf("%s: indexPassFailed = %v, want %v", c.name, got, c.want)
		}
	}
}

// A pass held off by another session's build lock, or that failed other than
// in a repair, neither ends nor extends the run of deferred passes, nor does
// a failure end the run of held-off ones.
func TestNextIndexRuns(t *testing.T) {
	failed := errors.New("pass failed")
	repair := fmt.Errorf("%w: %w", driver.ErrHistoryIndexRepair, failed)
	for _, c := range []struct {
		name                    string
		res                     driver.HistoryIndexResult
		err                     error
		wantDeferrals, wantBusy int
	}{
		{"deferred", driver.HistoryIndexResult{Deferred: []string{"x_ordering"}}, nil, 5, 0},
		{"busy", driver.HistoryIndexResult{Busy: "x_ordering"}, nil, 4, 3},
		{"failed", driver.HistoryIndexResult{}, failed, 4, 2},
		{"repair failed", driver.HistoryIndexResult{}, repair, 0, 0},
		{"finished", driver.HistoryIndexResult{}, nil, 0, 0},
	} {
		if d, b := nextIndexRuns(4, 2, c.res, c.err); d != c.wantDeferrals || b != c.wantBusy {
			t.Errorf("%s: nextIndexRuns(4, 2) = %d, %d, want %d, %d", c.name, d, b, c.wantDeferrals, c.wantBusy)
		}
	}
}
