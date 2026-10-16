package hopper

import (
	"errors"
	"testing"
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
