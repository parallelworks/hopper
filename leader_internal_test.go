package hopper

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"testing"
	"time"

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
		return map[string]indexBackoff{"repaired": {failures: 3}, "invalid": {failures: 3}, "waiting": {failures: 1}}
	}
	failed := errors.New("pass failed")
	for _, c := range []struct {
		name string
		res  driver.HistoryIndexResult
		err  error
		want []string
	}{
		{"finished", driver.HistoryIndexResult{Invalid: []string{"invalid"}}, nil, []string{"invalid", "waiting"}},
		{"failed", driver.HistoryIndexResult{}, failed, []string{"invalid", "repaired", "waiting"}},
		// A failed repair reports its index invalid, whose backoff stays.
		{"repair failed", driver.HistoryIndexResult{Invalid: []string{"invalid"}}, fmt.Errorf("%w: %w", driver.ErrHistoryIndexRepair, failed), []string{"invalid", "waiting"}},
		{"busy", driver.HistoryIndexResult{Busy: "x_ordering"}, nil, []string{"invalid", "repaired", "waiting"}},
		{"deferred", driver.HistoryIndexResult{Deferred: []string{"x_ordering"}}, nil, []string{"invalid", "repaired", "waiting"}},
	} {
		b := backoffs()
		settleBackoffs(b, []string{"waiting"}, c.res.Invalid, c.res, c.err)
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

// maintainExec answers HistoryMaintain with res and err, and records the lock
// timeouts it is asked for.
type maintainExec struct {
	driver.Executor
	res      driver.HistoryMaintainResult
	err      error
	timeouts []time.Duration
}

func (e *maintainExec) HistoryMaintain(_ context.Context, p driver.HistoryMaintainParams) (driver.HistoryMaintainResult, error) {
	e.timeouts = append(e.timeouts, p.LockTimeout)
	return e.res, e.err
}

// A pass that failed in one group but deferred work in another lengthens the
// next try's lock timeout, as a pass without a failure does, so that a
// failure that lasts does not hold the other groups' deferred work at the
// shortest timeout for good; it is still retried at the next maintenance
// interval rather than on every leader pass. A failed pass that deferred
// nothing leaves the count.
func TestMaintainHistoryCountsDeferralsPastFailure(t *testing.T) {
	ctx := context.Background()
	exec := &maintainExec{res: driver.HistoryMaintainResult{Deferred: []string{"p"}}, err: errors.New("another group failed")}
	c := &Client[struct{}]{exec: exec, logger: slog.New(slog.DiscardHandler), tuning: defaultTuning}
	for range 3 {
		if !c.maintainHistory(ctx) {
			t.Fatal("a failed pass asked to be retried on the next leader pass")
		}
	}
	exec.res = driver.HistoryMaintainResult{}
	c.maintainHistory(ctx)
	exec.err = nil
	c.maintainHistory(ctx)
	want := []time.Duration{ddlLockTimeout(0), ddlLockTimeout(1), ddlLockTimeout(2), ddlLockTimeout(3), ddlLockTimeout(3)}
	if !slices.Equal(exec.timeouts, want) || c.ddlDeferrals != 0 {
		t.Errorf("lock timeouts = %v, then %d deferrals; want %v, then 0", exec.timeouts, c.ddlDeferrals, want)
	}
}

// heldRescueExec has a full batch of rescue candidates whose rows other
// transactions hold, so that the rescue's finalize, which skips them,
// applies none. It stops offering them after a few reads.
type heldRescueExec struct {
	driver.Executor
	batch int
	reads int
}

func (e *heldRescueExec) JobRescueCandidates(context.Context, driver.JobRescueParams) ([]driver.RescueCandidate, error) {
	e.reads++
	if e.reads > 3 {
		return nil, nil
	}
	out := make([]driver.RescueCandidate, e.batch)
	for i := range out {
		out[i] = driver.RescueCandidate{Job: &driver.JobRow{ID: driver.JobID{byte(i + 1)}, Queue: "q", AttemptedBy: 1, Fence: 1, Attempt: 1, MaxAttempts: 5}}
	}
	return out, nil
}

func (e *heldRescueExec) JobFinalizeMany(context.Context, driver.JobFinalizeParams) ([]driver.JobID, error) {
	return nil, nil
}

// A full batch of candidates that the rescue applies to none of ends the
// pass, rather than being read again at once until their rows are free.
func TestRescueEndsAPassThatRescuedNothing(t *testing.T) {
	exec := &heldRescueExec{batch: 4}
	tun := defaultTuning
	tun.rescueBatch = exec.batch
	c := &Client[struct{}]{exec: exec, logger: slog.New(slog.DiscardHandler), tuning: tun}
	c.clientID.Store(2)
	c.rescue(context.Background())
	if exec.reads != 1 {
		t.Errorf("the rescue read its candidates %d times in one pass, want 1", exec.reads)
	}
}
