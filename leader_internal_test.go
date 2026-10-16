package hopper

import (
	"errors"
	"maps"
	"slices"
	"testing"

	"github.com/parallelworks/hopper/driver"
)

// A pass that fails before reaching the work deferred last time neither
// ends a run of deferred passes nor extends it, so that failing passes
// between deferred ones do not put off the warning or shorten the lock
// timeout.
func TestNextDeferrals(t *testing.T) {
	failed := errors.New("pass failed")
	for _, c := range []struct {
		n        int
		deferred bool
		err      error
		want     int
	}{
		{3, true, nil, 4},
		{3, true, failed, 4},
		{3, false, failed, 3},
		{3, false, nil, 0},
	} {
		if got := nextDeferrals(c.n, c.deferred, c.err); got != c.want {
			t.Errorf("nextDeferrals(%d, %v, %v) = %d, want %d", c.n, c.deferred, c.err, got, c.want)
		}
	}
}

// A pass that tried a backed-off name and finished without reporting it
// again forgets its backoff, so that a later failure starts over at the
// shortest wait; one still failing, skipped, or not reached keeps it.
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
