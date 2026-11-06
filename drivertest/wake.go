package drivertest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// wakeRunning is JobRetry with IfRunning of a job that must be running.
func wakeRunning(ctx context.Context, t *testing.T, exec driver.Executor, id driver.JobID) {
	t.Helper()
	j, err := exec.JobRetry(ctx, driver.JobRetryParams{ID: id, IfRunning: true})
	if err != nil || j.State != driver.JobStateRunning || j.ID != id {
		t.Fatalf("JobRetry IfRunning of a running job = %+v, %v", j, err)
	}
}

// testRetryIfRunning checks what a wake of a running job does to each way
// its attempt can end.
func testRetryIfRunning[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := expirer(ctx, t, f, d)
	start := func() driver.JobID {
		t.Helper()
		p := params("k", 1)
		p[0].TTL = time.Hour
		id := insert(ctx, t, exec, p)[0].Job.ID
		claimOne(ctx, t, exec, clientID, id)
		return id
	}
	snooze := func(id driver.JobID) driver.JobFinalize {
		return driver.JobFinalize{ID: id, AttemptedBy: clientID, State: driver.JobStateScheduled, Delay: time.Hour, Snooze: true, SnoozeReason: "waiting"}
	}
	remove := func(id driver.JobID) {
		t.Helper()
		if _, err := exec.JobDelete(ctx, id); err != nil {
			t.Fatal(err)
		}
	}

	// Without the option a running job is refused and left unmarked; a
	// waiting one is retried as without it, its TTL cleared.
	id := start()
	if _, err := exec.JobRetry(ctx, driver.JobRetryParams{ID: id}); !errors.Is(err, driver.ErrJobRunning) {
		t.Fatalf("JobRetry of a running job = %v, want ErrJobRunning", err)
	}
	finalizeCurrent(ctx, t, exec, snooze(id))
	if due(ctx, t, exec, id) || f.SnoozeReason(ctx, t, d, id) != "waiting" {
		t.Fatal("a refused retry woke the job")
	}
	if j, err := exec.JobRetry(ctx, driver.JobRetryParams{ID: id, IfRunning: true}); err != nil || j.State != driver.JobStateAvailable || !j.ExpiresAt.IsZero() {
		t.Fatalf("JobRetry IfRunning of a waiting job = %+v, %v", j, err)
	}
	remove(id)

	// A wake makes the snooze the attempt ends in run at once, without its
	// reason and keeping the TTL. The mark is the attempt's: the next
	// attempt's snooze waits.
	id = start()
	wakeRunning(ctx, t, exec, id)
	finalizeCurrent(ctx, t, exec, snooze(id))
	if j := get(ctx, t, exec, id); !due(ctx, t, exec, id) || f.SnoozeReason(ctx, t, d, id) != "" || j.ExpiresAt.IsZero() {
		t.Fatalf("woken snooze = %+v, reason %q", j, f.SnoozeReason(ctx, t, d, id))
	}
	claimOne(ctx, t, exec, clientID, id)
	finalizeCurrent(ctx, t, exec, snooze(id))
	if due(ctx, t, exec, id) {
		t.Fatal("a wake of an earlier attempt woke the next one")
	}
	remove(id)

	// A retry runs at once too; several wakes are one.
	id = start()
	wakeRunning(ctx, t, exec, id)
	wakeRunning(ctx, t, exec, id)
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: clientID, State: driver.JobStateRetryable, Delay: time.Hour, Error: &driver.AttemptError{Attempt: 1, Error: "x"}})
	if j := get(ctx, t, exec, id); j.State != driver.JobStateRetryable || !due(ctx, t, exec, id) {
		t.Fatalf("woken retry = %+v", j)
	}
	remove(id)

	// A terminal end is not changed.
	for _, state := range []driver.JobState{driver.JobStateCompleted, driver.JobStateCanceled, driver.JobStateDiscarded} {
		id = start()
		wakeRunning(ctx, t, exec, id)
		finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: clientID, State: state, Archive: true})
		if j := get(ctx, t, exec, id); j.State != state {
			t.Errorf("woken job ending %s = %+v", state, j)
		}
	}

	// A job claimed by ID is marked the same way, and its lease's snooze
	// runs at once.
	p := params("k", 1)
	id = insert(ctx, t, exec, p)[0].Job.ID
	l := claimByID(ctx, t, exec, id)
	wakeRunning(ctx, t, exec, id)
	res := leaseResult(l, driver.JobStateScheduled)
	res.Snooze, res.Delay = true, time.Hour
	finalize(ctx, t, exec, res)
	if !due(ctx, t, exec, id) {
		t.Fatal("a woken lease's snooze waits")
	}
	remove(id)

	// A lease that expires retries its job at once anyway, and takes the
	// mark with its attempt.
	id = insert(ctx, t, exec, p)[0].Job.ID
	claimByID(ctx, t, exec, id)
	wakeRunning(ctx, t, exec, id)
	f.ExpireJobLease(ctx, t, d, id)
	if jobs, _, err := exec.JobExpireLeases(ctx, driver.JobExpireLeasesParams{Limit: 10, ClientID: clientID}); err != nil || len(jobs) != 1 {
		t.Fatalf("expire = %v, %v", jobs, err)
	}
	if j := get(ctx, t, exec, id); j.State != driver.JobStateRetryable || !due(ctx, t, exec, id) {
		t.Fatalf("expired lease's job = %+v", j)
	}
	claimOne(ctx, t, exec, clientID, id)
	finalizeCurrent(ctx, t, exec, snooze(id))
	if due(ctx, t, exec, id) {
		t.Fatal("a wake of an expired lease's attempt woke the next one")
	}

	if _, err := exec.JobRetry(ctx, driver.JobRetryParams{ID: driver.JobID{9}, IfRunning: true}); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("JobRetry IfRunning of an unknown job = %v", err)
	}
}

// testRetryIfRunningConcurrent checks that a wake racing the end of an
// attempt is never lost: either it finds the job waiting and retries it,
// or the snooze finds the mark.
func testRetryIfRunningConcurrent[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
	clientID := register(ctx, t, exec)
	for round := range 40 {
		id := insert(ctx, t, exec, params("k", 1))[0].Job.ID
		claimOne(ctx, t, exec, clientID, id)
		var wg sync.WaitGroup
		wg.Go(func() {
			j, err := exec.JobRetry(ctx, driver.JobRetryParams{ID: id, IfRunning: true})
			if err != nil || (j.State != driver.JobStateRunning && j.State != driver.JobStateAvailable) {
				t.Errorf("JobRetry IfRunning = %+v, %v", j, err)
			}
		})
		wg.Go(func() {
			res := driver.JobFinalize{ID: id, AttemptedBy: clientID, Fence: 1, State: driver.JobStateScheduled, Delay: time.Hour, Snooze: true}
			if applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{res}}); err != nil || len(applied) != 1 {
				t.Errorf("finalize = %v, %v", applied, err)
			}
		})
		wg.Wait()
		if !due(ctx, t, exec, id) {
			t.Fatalf("round %d: a wake racing the snooze was lost", round)
		}
		if _, err := exec.JobDelete(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
}

// testRetryIfRunningTx wakes a running job in a transaction while its
// attempt's snooze waits in a client's finalize buffer. A flush that skips
// held rows, as the finalizer's does, leaves the result for later while
// the transaction holds the row, and applies it, woken, once the
// transaction commits; a flush that waits for the row reads the mark when
// it gets it. A wake that rolls back leaves the snooze as it was.
func testRetryIfRunningTx[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := register(ctx, t, exec)
	for _, tc := range []struct {
		skipLocked, commit bool
	}{{true, true}, {false, true}, {false, false}} {
		t.Run(fmt.Sprintf("skipLocked=%t,commit=%t", tc.skipLocked, tc.commit), func(t *testing.T) {
			id := insert(ctx, t, exec, params("k", 1))[0].Job.ID
			claimOne(ctx, t, exec, clientID, id)
			res := driver.JobFinalize{ID: id, AttemptedBy: clientID, Fence: 1, State: driver.JobStateScheduled, Delay: time.Hour, Snooze: true, SnoozeReason: "waiting"}
			flush := func() []driver.JobFinalized {
				applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{res}, SkipLocked: tc.skipLocked})
				if err != nil {
					t.Error(err)
				}
				return applied
			}
			tx, commit, rollback := f.Begin(ctx, t, d)
			defer rollback() //nolint:errcheck // ended below
			wakeRunning(ctx, t, d.UnwrapTx(tx), id)
			end := rollback
			if tc.commit {
				end = commit
			}
			if tc.skipLocked {
				if applied := flush(); len(applied) != 0 {
					t.Fatalf("a flush that skips held rows applied %v", applied)
				}
				if err := end(); err != nil {
					t.Fatal(err)
				}
				if applied := flush(); len(applied) != 1 {
					t.Fatalf("flush once the row is free = %v", applied)
				}
			} else {
				var wg sync.WaitGroup
				wg.Go(func() {
					if applied := flush(); len(applied) != 1 {
						t.Errorf("flush waiting for the row = %v", applied)
					}
				})
				f.WaitForLockWait(ctx, t, d)
				if err := end(); err != nil {
					t.Fatal(err)
				}
				wg.Wait()
			}
			reason := f.SnoozeReason(ctx, t, d, id)
			if woken := due(ctx, t, exec, id); woken != tc.commit || (reason == "") != tc.commit {
				t.Errorf("snooze after a wake that committed: %t = due %t, reason %q", tc.commit, woken, reason)
			}
			if _, err := exec.JobDelete(ctx, id); err != nil {
				t.Fatal(err)
			}
		})
	}
}
