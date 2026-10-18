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
