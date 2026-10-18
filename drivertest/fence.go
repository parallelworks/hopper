package drivertest

import (
	"context"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// get returns a job, live or from history.
func get(ctx context.Context, t *testing.T, exec driver.Executor, id driver.JobID) *driver.JobRow {
	t.Helper()
	j, err := exec.JobGet(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

// claimOne claims the only claimable job and checks that it is id.
func claimOne(ctx context.Context, t *testing.T, exec driver.Executor, clientID int64, id driver.JobID) *driver.JobRow {
	t.Helper()
	jobs := claim(ctx, t, exec, clientID, 1)
	if len(jobs) != 1 || jobs[0].ID != id {
		t.Fatalf("claim = %+v, want job %s", jobs, id)
	}
	return jobs[0]
}

// testClaimFence checks that every claim increments a job's fence, and that
// snoozes, retries, the archive and JobRetry from history keep it, as do
// the error entries of the claims that failed.
func testClaimFence[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	id := insert(ctx, t, exec, params("k", 1))[0].Job.ID
	clientID := register(ctx, t, exec)
	if j := get(ctx, t, exec, id); j.Fence != 0 {
		t.Fatalf("fence before any claim = %d", j.Fence)
	}

	j := claimOne(ctx, t, exec, clientID, id)
	if j.Fence != 1 || j.Attempt != 1 {
		t.Fatalf("first claim: fence %d, attempt %d", j.Fence, j.Attempt)
	}
	// A snooze gives the attempt back but keeps the fence.
	finalize(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: clientID, Fence: 1, State: driver.JobStateScheduled, Snooze: true, Delay: time.Hour})
	if j := get(ctx, t, exec, id); j.Fence != 1 || j.Attempt != 0 {
		t.Fatalf("snoozed: fence %d, attempt %d", j.Fence, j.Attempt)
	}
	f.MakeClaimable(ctx, t, d, id)
	if j := claimOne(ctx, t, exec, clientID, id); j.Fence != 2 || j.Attempt != 1 {
		t.Fatalf("claim after the snooze: fence %d, attempt %d; want a new fence at the same attempt", j.Fence, j.Attempt)
	}
	finalize(ctx, t, exec, driver.JobFinalize{
		ID: id, AttemptedBy: clientID, Fence: 2, State: driver.JobStateRetryable,
		Error: &driver.AttemptError{Attempt: 1, Fence: 2, Error: "x"},
	})
	if j := get(ctx, t, exec, id); j.Fence != 2 || len(j.Errors) != 1 || j.Errors[0].Fence != 2 {
		t.Fatalf("retried: fence %d, errors %+v", j.Fence, j.Errors)
	}
	f.MakeClaimable(ctx, t, d, id)
	if j := claimOne(ctx, t, exec, clientID, id); j.Fence != 3 {
		t.Fatalf("claim after the retry: fence %d", j.Fence)
	}
	// History keeps it, and a JobRetry brings it back.
	finalize(ctx, t, exec, driver.JobFinalize{
		ID: id, AttemptedBy: clientID, Fence: 3, State: driver.JobStateDiscarded, Archive: true,
		Error: &driver.AttemptError{Attempt: 2, Fence: 3, Error: "x"},
	})
	if j := get(ctx, t, exec, id); j.State != driver.JobStateDiscarded || j.Fence != 3 || j.Errors[1].Fence != 3 {
		t.Fatalf("archived: %+v", j)
	}
	retried, err := exec.JobRetry(ctx, driver.JobRetryParams{ID: id})
	if err != nil {
		t.Fatal(err)
	}
	if retried.Fence != 3 {
		t.Fatalf("JobRetry from history: fence %d, want 3", retried.Fence)
	}
	if j := claimOne(ctx, t, exec, clientID, id); j.Fence != 4 {
		t.Fatalf("claim after JobRetry: fence %d, want 4", j.Fence)
	}
	// A retry of a waiting job keeps it too.
	finalize(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: clientID, Fence: 4, State: driver.JobStateRetryable, Delay: time.Hour})
	if retried, err := exec.JobRetry(ctx, driver.JobRetryParams{ID: id}); err != nil || retried.Fence != 4 {
		t.Fatalf("JobRetry of a waiting job = %+v, %v", retried, err)
	}
}

// testFinalizeFencesReclaim rescues a job while its client is alive, as
// RescueStuckAfter does, and has the same client claim it again. A result of
// the first claim, from the same client, must not finalize the second.
func testFinalizeFencesReclaim[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	id := insert(ctx, t, exec, params("k", 1))[0].Job.ID
	clientID := register(ctx, t, exec)
	first := claimOne(ctx, t, exec, clientID, id)
	rescue := driver.JobFinalize{
		ID: id, AttemptedBy: clientID, Fence: first.Fence, State: driver.JobStateRetryable,
		Error: &driver.AttemptError{Attempt: 1, Fence: first.Fence, Error: "hopper: job ran longer than RescueStuckAfter"},
	}
	if applied := finalize(ctx, t, exec, rescue); len(applied) != 1 {
		t.Fatal("rescue not applied")
	}
	second := claimOne(ctx, t, exec, clientID, id)
	if second.AttemptedBy != first.AttemptedBy || second.Fence == first.Fence {
		t.Fatalf("second claim: client %d, fence %d; first: client %d, fence %d", second.AttemptedBy, second.Fence, first.AttemptedBy, first.Fence)
	}
	for _, state := range []driver.JobState{driver.JobStateCompleted, driver.JobStateRetryable, driver.JobStateScheduled} {
		late := driver.JobFinalize{ID: id, AttemptedBy: clientID, Fence: first.Fence, State: state, Archive: true, Snooze: state == driver.JobStateScheduled}
		if applied := finalize(ctx, t, exec, late); len(applied) != 0 {
			t.Fatalf("the first claim's %s result finalized the second claim", state)
		}
	}
	if j := get(ctx, t, exec, id); j.State != driver.JobStateRunning || j.Fence != second.Fence || j.Attempt != 2 {
		t.Fatalf("after the late results: %+v", j)
	}
	if applied := finalize(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: clientID, Fence: second.Fence, State: driver.JobStateCompleted, Archive: true}); len(applied) != 1 {
		t.Fatal("the second claim's result was not applied")
	}
}

// testRescueRacesReclaim finds a stuck job, then lets its client finalize it
// and claim it again before the rescue applies: the rescue, fenced to the
// claim it found, must leave the new claim alone.
func testRescueRacesReclaim[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	id := insert(ctx, t, exec, params("k", 1))[0].Job.ID
	clientID := register(ctx, t, exec)
	claimOne(ctx, t, exec, clientID, id)
	f.AgeAttempt(ctx, t, d, id, 10*time.Minute)
	candidates, err := exec.JobRescueCandidates(ctx, driver.JobRescueParams{StuckAfter: 5 * time.Minute, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].Job.ID != id || !candidates[0].Stuck {
		t.Fatalf("candidates = %+v", candidates)
	}
	found := candidates[0].Job

	finalize(ctx, t, exec, driver.JobFinalize{
		ID: id, AttemptedBy: clientID, Fence: found.Fence, State: driver.JobStateRetryable,
		Error: &driver.AttemptError{Attempt: 1, Fence: found.Fence, Error: "x"},
	})
	again := claimOne(ctx, t, exec, clientID, id)

	rescue := driver.JobFinalize{
		ID: id, AttemptedBy: found.AttemptedBy, Fence: found.Fence, State: driver.JobStateRetryable,
		Error: &driver.AttemptError{Attempt: found.Attempt, Fence: found.Fence, Error: "hopper: job ran longer than RescueStuckAfter"},
	}
	if applied := finalize(ctx, t, exec, rescue); len(applied) != 0 {
		t.Fatal("the rescue of the claim it found applied to the claim after it")
	}
	if j := get(ctx, t, exec, id); j.State != driver.JobStateRunning || j.Fence != again.Fence || len(j.Errors) != 1 {
		t.Fatalf("after the rescue: %+v", j)
	}
}

// testRecordProgressFencesSnooze snoozes a job and has the same client claim
// it again, at the same attempt number. A progress write of the first claim
// must not land on the second.
func testRecordProgressFencesSnooze[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	id := insert(ctx, t, exec, params("k", 1))[0].Job.ID
	clientID := register(ctx, t, exec)
	first := claimOne(ctx, t, exec, clientID, id)
	finalize(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: clientID, Fence: first.Fence, State: driver.JobStateScheduled, Snooze: true, Delay: time.Hour})
	f.MakeClaimable(ctx, t, d, id)
	second := claimOne(ctx, t, exec, clientID, id)
	if second.Attempt != first.Attempt || second.Fence == first.Fence {
		t.Fatalf("claims: attempt %d fence %d, then attempt %d fence %d", first.Attempt, first.Fence, second.Attempt, second.Fence)
	}
	p, err := exec.JobRecordProgress(ctx, driver.JobRecordProgressParams{ID: id, AttemptedBy: clientID, Fence: first.Fence, Step: "stale"})
	if err != nil || p != nil {
		t.Fatalf("the first claim's progress write = %+v, %v; want it fenced", p, err)
	}
	p, err = exec.JobRecordProgress(ctx, driver.JobRecordProgressParams{ID: id, AttemptedBy: clientID, Fence: second.Fence, Step: "current"})
	if err != nil || p == nil || p.Step != "current" {
		t.Fatalf("the second claim's progress write = %+v, %v", p, err)
	}
}
