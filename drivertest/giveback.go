package drivertest

import (
	"context"
	"slices"
	"testing"

	"github.com/parallelworks/hopper/driver"
)

// testJobGiveBack checks a result that gives its attempt back as an
// interruption (JobFinalize.GiveBack): the attempt is not counted, the
// error is recorded, and the run's log is kept marked Interrupted, which no
// snooze of the same attempt number removes, and which itself removes no
// snooze's log. No snooze reason is stored. A result that both snoozes and
// gives its attempt back is refused, as is one that gives it back and
// finishes the job.
func testJobGiveBack[TTx any](t *testing.T, f Fixture[TTx], noBatches bool) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := executor(d, noBatches)
	p := params("k", 1)
	p[0].MaxAttempts = 10
	id := insert(ctx, t, exec, p)[0].Job.ID
	clientID := register(ctx, t, exec)
	claimNext := func() *driver.JobRow {
		t.Helper()
		f.MakeClaimable(ctx, t, d, id)
		return claimOne(ctx, t, exec, clientID, id)
	}
	run := func(result driver.JobFinalize) {
		t.Helper()
		j := claimNext()
		if j.Attempt != 1 {
			t.Fatalf("claimed attempt %d, want 1: every run so far gave its attempt back", j.Attempt)
		}
		result.ID, result.AttemptedBy, result.Fence = id, clientID, j.Fence
		applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{result}})
		if err != nil || len(applied) != 1 {
			t.Fatalf("finalize = %v, %v", applied, err)
		}
	}
	check := func(what string, errors int, want ...driver.AttemptLog) {
		t.Helper()
		got, err := exec.JobLogs(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Errorf("%s: logs %+v, want %+v", what, got, want)
		}
		if j := get(ctx, t, exec, id); len(j.Errors) != errors || j.SnoozeReason != "" {
			t.Errorf("%s: job has %d errors and snooze reason %q, want %d and none", what, len(j.Errors), j.SnoozeReason, errors)
		}
	}
	stopped := &driver.AttemptError{Attempt: 1, Error: "stopped"}
	interrupted := driver.AttemptLog{Attempt: 1, Interrupted: true, Log: "first run\n"}
	again := driver.AttemptLog{Attempt: 1, Interrupted: true, Log: "second run\n"}
	snoozed := driver.AttemptLog{Attempt: 1, Snoozed: true, Log: "snoozed run\n"}
	logOf := func(l driver.AttemptLog) *driver.AttemptLog {
		return &driver.AttemptLog{Attempt: l.Attempt, Log: l.Log}
	}

	run(driver.JobFinalize{State: driver.JobStateRetryable, GiveBack: true, SnoozeReason: "not a snooze", Error: stopped, Log: logOf(interrupted)})
	check("after an interrupted run", 1, interrupted)
	run(driver.JobFinalize{State: driver.JobStateScheduled, Snooze: true, Log: logOf(snoozed)})
	check("after a snooze of the same number", 1, interrupted, snoozed)
	run(driver.JobFinalize{State: driver.JobStateScheduled, Snooze: true})
	check("after a snooze that logged nothing", 1, interrupted)
	run(driver.JobFinalize{State: driver.JobStateRetryable, GiveBack: true, Error: stopped, Log: logOf(again)})
	check("after a second interrupted run", 2, interrupted, again)
	run(driver.JobFinalize{State: driver.JobStateScheduled, Snooze: true, Log: logOf(snoozed)})
	// A job reports a snooze reason only while scheduled, so it is this
	// give-back's reason that shows whether one is stored.
	run(driver.JobFinalize{State: driver.JobStateScheduled, GiveBack: true, SnoozeReason: "not a snooze"})
	check("after a give-back that logged nothing", 2, interrupted, again, snoozed)

	j := claimNext()
	both := driver.JobFinalize{ID: id, AttemptedBy: clientID, Fence: j.Fence, State: driver.JobStateScheduled, Snooze: true, GiveBack: true}
	if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{both}}); err == nil {
		t.Error("a result that both snoozes and gives its attempt back was accepted")
	}
	finished := driver.JobFinalize{ID: id, AttemptedBy: clientID, Fence: j.Fence, State: driver.JobStateCompleted, Archive: true, GiveBack: true}
	if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{finished}}); err == nil {
		t.Error("a completed result that gives its attempt back was accepted")
	}
	done := driver.AttemptLog{Attempt: 1, Log: "done\n"}
	both.Snooze, both.GiveBack, both.State, both.Archive, both.Log = false, false, driver.JobStateCompleted, true, logOf(done)
	if applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{both}}); err != nil || len(applied) != 1 {
		t.Fatalf("complete = %v, %v", applied, err)
	}
	got, err := exec.JobLogs(ctx, id)
	if err != nil || !slices.Equal(got, []driver.AttemptLog{interrupted, again, snoozed, done}) {
		t.Errorf("in history: logs %+v, %v", got, err)
	}
}
