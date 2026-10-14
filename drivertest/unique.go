package drivertest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/parallelworks/hopper/driver"
)

func uniqueFinished(key string, v int, finished ...driver.JobState) driver.JobInsertParams {
	return driver.JobInsertParams{
		Kind: "u", Queue: "default", Priority: 2, MaxAttempts: 1, UniqueKey: key, UniqueFinished: finished,
		Args: json.RawMessage(fmt.Sprintf(`{"i":%d}`, v)),
	}
}

// runTo inserts a unique job, claims it and finalizes it in state.
func runTo(ctx context.Context, t *testing.T, exec driver.Executor, clientID int64, key string, state driver.JobState) *driver.JobRow {
	t.Helper()
	job := insert(ctx, t, exec, []driver.JobInsertParams{uniqueFinished(key, 0)})[0].Job
	if claimed := claim(ctx, t, exec, clientID, 1); len(claimed) != 1 || claimed[0].ID != job.ID {
		t.Fatalf("claimed %v, want %s", claimed, job.ID)
	}
	fin := driver.JobFinalize{ID: job.ID, AttemptedBy: clientID, State: state, Archive: true}
	if state != driver.JobStateCompleted {
		fin.Error = &driver.AttemptError{Attempt: 1, Error: "x"}
	}
	if applied := finalize(ctx, t, exec, fin); len(applied) != 1 {
		t.Fatalf("finalize %s applied %v", key, applied)
	}
	return job
}

func testInsertUniqueFinished[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := register(ctx, t, exec)
	completed := runTo(ctx, t, exec, clientID, "completed", driver.JobStateCompleted)
	runTo(ctx, t, exec, clientID, "discarded", driver.JobStateDiscarded)
	canceled := runTo(ctx, t, exec, clientID, "canceled", driver.JobStateCanceled)
	runTo(ctx, t, exec, clientID, "batch-ok", driver.JobStateCompleted)
	runTo(ctx, t, exec, clientID, "batch-bad", driver.JobStateDiscarded)

	res := insert(ctx, t, exec, []driver.JobInsertParams{
		uniqueFinished("fresh", 1, driver.JobStateCompleted),
		uniqueFinished("completed", 2, driver.JobStateCompleted),
		uniqueFinished("discarded", 3, driver.JobStateCompleted, driver.JobStateCanceled),
		uniqueFinished("canceled", 4, driver.JobStateCompleted, driver.JobStateCanceled),
	})
	for i, want := range []*driver.JobRow{nil, completed, nil, canceled} {
		r := res[i]
		if want == nil {
			if r.Duplicate || r.Job.State != driver.JobStateAvailable || argsN(r.Job) != i+1 {
				t.Errorf("input %d: %+v, want a new job", i, r)
			}
			continue
		}
		if !r.Duplicate || r.Job.ID != want.ID || r.Job.FinalizedAt.IsZero() || r.Job.UniqueKey != want.UniqueKey {
			t.Errorf("input %d: %+v, want the finished duplicate %s", i, r.Job, want.ID)
		}
	}
	if res[1].Job.State != driver.JobStateCompleted || res[3].Job.State != driver.JobStateCanceled {
		t.Errorf("finished duplicates in states %s and %s", res[1].Job.State, res[3].Job.State)
	}

	// Without UniqueFinished, history does not count.
	if r := insert(ctx, t, exec, []driver.JobInsertParams{uniqueFinished("canceled", 6)})[0]; r.Duplicate {
		t.Errorf("history counted without UniqueFinished: %+v", r.Job)
	}
	// A live duplicate takes precedence over a finished one, and is the one
	// replaced.
	rep, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{uniqueFinished("canceled", 7, driver.JobStateCanceled)}, driver.JobInsertOpts{OnConflict: driver.ConflictReplace})
	if err != nil {
		t.Fatal(err)
	}
	if r := rep[0]; !r.Duplicate || r.Job.State != driver.JobStateAvailable || argsN(r.Job) != 7 {
		t.Errorf("live duplicate = %+v", r.Job)
	}
	// A finished duplicate is never replaced.
	rep, err = exec.JobInsertMany(ctx, []driver.JobInsertParams{uniqueFinished("completed", 8, driver.JobStateCompleted)}, driver.JobInsertOpts{OnConflict: driver.ConflictReplace})
	if err != nil {
		t.Fatal(err)
	}
	if r := rep[0]; !r.Duplicate || r.Job.ID != completed.ID || argsN(r.Job) != 0 {
		t.Errorf("replace over history = %+v", r.Job)
	}

	// In a transaction, alike.
	tx, commit, rollback := f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // already ended on the happy path
	inTx, err := d.UnwrapTx(tx).JobInsertMany(ctx, []driver.JobInsertParams{
		uniqueFinished("completed", 9, driver.JobStateCompleted),
		uniqueFinished("tx", 10, driver.JobStateDiscarded),
	}, driver.JobInsertOpts{Notify: true})
	if err != nil {
		t.Fatal(err)
	}
	if !inTx[0].Duplicate || inTx[0].Job.ID != completed.ID || inTx[1].Duplicate {
		t.Errorf("insert in tx = %+v, %+v", inTx[0], inTx[1])
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := exec.JobGet(ctx, inTx[1].Job.ID); err != nil {
		t.Errorf("job inserted in tx: %v", err)
	}

	// Jobs of a batch found finished count toward the batch as their
	// outcomes.
	batch, err := exec.BatchInsert(ctx, driver.BatchInsertParams{Total: 2})
	if err != nil {
		t.Fatal(err)
	}
	inBatch := []driver.JobInsertParams{
		uniqueFinished("batch-ok", 11, driver.JobStateCompleted, driver.JobStateDiscarded),
		uniqueFinished("batch-bad", 12, driver.JobStateCompleted, driver.JobStateDiscarded),
	}
	for i := range inBatch {
		inBatch[i].BatchID = batch
	}
	if res := insert(ctx, t, exec, inBatch); !res[0].Duplicate || !res[1].Duplicate {
		t.Fatalf("batch jobs = %+v, %+v, want finished duplicates", res[0], res[1])
	}
	if _, err := exec.BatchSettle(ctx, driver.BatchSettleParams{IDs: []driver.JobID{batch}}); err != nil {
		t.Fatal(err)
	}
	if b, err := exec.BatchGet(ctx, batch); err != nil || b.CompletedAt.IsZero() || b.Pending != 0 || b.Failed != 1 {
		t.Errorf("batch of finished duplicates = %+v, %v, want completed with 1 failed", b, err)
	}

	if n := f.CountLive(ctx, t, d); n != 4 {
		t.Errorf("%d live jobs, want 4 (fresh, discarded, canceled, tx)", n)
	}
}

// testInsertUniqueFinishedRacesFinalize inserts a job while its duplicate's
// finalize is held open. The insert waits for that finalize, and then must
// find the duplicate in history, which its snapshot predates.
func testInsertUniqueFinishedRacesFinalize[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := register(ctx, t, exec)
	for i, inTx := range []bool{false, true} {
		key := fmt.Sprintf("race%d", i)
		job := insert(ctx, t, exec, []driver.JobInsertParams{uniqueFinished(key, 0)})[0].Job
		if claimed := claim(ctx, t, exec, clientID, 1); len(claimed) != 1 {
			t.Fatalf("claimed %v", claimed)
		}
		tx, commit, rollback := f.Begin(ctx, t, d)
		// Rolled back on exit too, so a failed check cannot leave it open
		// with the insert waiting on it.
		defer rollback() //nolint:errcheck // already ended on the happy path
		finalize(ctx, t, d.UnwrapTx(tx), driver.JobFinalize{ID: job.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})

		// The insert's transaction begins here, since Begin may call
		// t.Fatal, which only the test's goroutine may.
		ins, insCommit, insRollback := exec, func() error { return nil }, func() error { return nil }
		if inTx {
			var itx TTx
			itx, insCommit, insRollback = f.Begin(ctx, t, d)
			ins = d.UnwrapTx(itx)
		}
		done := make(chan driver.JobInsertResult, 1)
		go func() {
			defer insRollback() //nolint:errcheck // already ended on the happy path
			res, err := ins.JobInsertMany(ctx, []driver.JobInsertParams{uniqueFinished(key, 1, driver.JobStateCompleted)}, driver.JobInsertOpts{})
			if err == nil {
				err = insCommit()
			}
			if err != nil {
				t.Error(err)
				close(done)
				return
			}
			done <- res[0]
		}()
		waitForLockWait(ctx, t, f, d)
		if err := commit(); err != nil {
			t.Fatal(err)
		}
		r, ok := <-done
		if !ok {
			t.FailNow()
		}
		if !r.Duplicate || r.Job.ID != job.ID || r.Job.State != driver.JobStateCompleted {
			t.Errorf("in tx %v: insert racing the duplicate's finalize = %+v, want the finished duplicate %s", inTx, r.Job, job.ID)
		}
		if n := f.CountLive(ctx, t, d); n != 0 {
			t.Errorf("in tx %v: %d live jobs, want 0", inTx, n)
		}
	}
}
