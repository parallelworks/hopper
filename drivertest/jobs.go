package drivertest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

func testInsertCopy[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	if !d.Capabilities().Copy {
		t.Skip("driver has no COPY path")
	}
	exec := d.Executor()
	p := params("copy", 1000)
	p[5].ScheduledAt = time.Now().Add(time.Hour)
	p[6].TTL = time.Hour
	p[7].Await = true
	results, err := exec.JobInsertCopy(ctx, p, driver.JobInsertOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != len(p) {
		t.Fatalf("got %d results", len(results))
	}
	for i, r := range results {
		if r.Job.ID.IsZero() || r.Job.CreatedAt.IsZero() || r.Job.ScheduledAt.IsZero() || argsN(r.Job) != i {
			t.Fatalf("result %d incomplete: %+v", i, r.Job)
		}
	}
	if results[5].Job.State != driver.JobStateScheduled || results[4].Job.State != driver.JobStateAvailable {
		t.Errorf("states = %s, %s", results[4].Job.State, results[5].Job.State)
	}
	got, err := exec.JobGet(ctx, results[7].Job.ID)
	if err != nil || got.Kind != "copy" || argsN(got) != 7 || !got.Await || got.MaxAttempts != 3 {
		t.Errorf("JobGet = %+v, %v", got, err)
	}
	if got, err := exec.JobGet(ctx, results[6].Job.ID); err != nil || got.ExpiresAt.IsZero() {
		t.Errorf("TTL not applied by COPY: %+v, %v", got, err)
	}
	if n := f.CountLive(ctx, t, d); n != 1000 {
		t.Errorf("live = %d", n)
	}
	// Inside a transaction, the rows roll back with it.
	tx, _, rollback := f.Begin(ctx, t, d)
	if _, err := d.UnwrapTx(tx).JobInsertCopy(ctx, params("copytx", 300), driver.JobInsertOpts{}); err != nil {
		t.Fatal(err)
	}
	if err := rollback(); err != nil {
		t.Fatal(err)
	}
	if n := f.CountLive(ctx, t, d); n != 1000 {
		t.Errorf("rolled-back COPY left rows: live = %d", n)
	}
}

func testInsertTx[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	tx, _, rollback := f.Begin(ctx, t, d)
	res, err := d.UnwrapTx(tx).JobInsertMany(ctx, params("tx", 1), driver.JobInsertOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if err := rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Executor().JobGet(ctx, res[0].Job.ID); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("job survived rollback: %v", err)
	}
	tx, commit, _ := f.Begin(ctx, t, d)
	res, err = d.UnwrapTx(tx).JobInsertMany(ctx, params("tx", 1), driver.JobInsertOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Executor().JobGet(ctx, res[0].Job.ID); err != nil {
		t.Errorf("committed job missing: %v", err)
	}
}

func testClaim[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	p := []driver.JobInsertParams{
		{Kind: "k", Queue: "default", Priority: 3, MaxAttempts: 1, Args: []byte(`{"i":0}`)},
		{Kind: "k", Queue: "default", Priority: 1, MaxAttempts: 1, Args: []byte(`{"i":1}`)},
		{Kind: "k", Queue: "default", Priority: 2, MaxAttempts: 1, Args: []byte(`{"i":2}`)},
		{Kind: "k", Queue: "default", Priority: 1, MaxAttempts: 1, Args: []byte(`{"i":3}`)},
		{Kind: "k", Queue: "other", Priority: 1, MaxAttempts: 1, Args: []byte(`{"i":4}`)},
		{Kind: "k", Queue: "default", Priority: 1, MaxAttempts: 1, Args: []byte(`{"i":5}`), ScheduledAt: time.Now().Add(time.Hour)},
		{Kind: "k", Queue: "default", Priority: 1, MaxAttempts: 1, Args: []byte(`{"i":6}`), TTL: time.Millisecond},
	}
	insert(ctx, t, exec, p)
	time.Sleep(20 * time.Millisecond)

	clientID := register(ctx, t, exec)
	jobs := claim(ctx, t, exec, clientID, 3)
	var got []int
	for _, j := range jobs {
		got = append(got, argsN(j))
		if j.State != driver.JobStateRunning || j.Attempt != 1 || j.AttemptedBy != clientID || j.AttemptedAt.IsZero() {
			t.Errorf("claimed job not marked running by %d: %+v", clientID, j)
		}
	}
	if fmt.Sprint(got) != "[1 3 2]" {
		t.Errorf("claim order = %v, want [1 3 2] (priority, then insertion)", got)
	}
	// The rest: only the low-priority one is claimable (not the other queue,
	// the future one, or the expired one).
	other := register(ctx, t, exec)
	jobs = claim(ctx, t, exec, other, 10)
	if len(jobs) != 1 || argsN(jobs[0]) != 0 {
		t.Errorf("second claim = %d jobs (%v), want only job 0", len(jobs), jobs)
	}
	if jobs := claim(ctx, t, exec, other, 0); len(jobs) != 0 {
		t.Error("limit 0 claimed jobs")
	}
}

func testClaimSkipsLocked[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	insert(ctx, t, exec, params("k", 4))
	a, b := register(ctx, t, exec), register(ctx, t, exec)

	tx, _, rollback := f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // cleanup
	heldRes, err := d.UnwrapTx(tx).JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: a, Limit: 2})
	held := heldRes.Jobs
	if err != nil || len(held) != 2 {
		t.Fatalf("held %d, %v", len(held), err)
	}
	done := make(chan []*driver.JobRow, 1)
	go func() {
		res, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: b, Limit: 10})
		if err != nil {
			t.Error(err)
		}
		done <- res.Jobs
	}()
	select {
	case jobs := <-done:
		if len(jobs) != 2 {
			t.Fatalf("B claimed %d jobs, want 2", len(jobs))
		}
		for _, j := range jobs {
			for _, h := range held {
				if j.ID == h.ID {
					t.Fatalf("B claimed a job locked by A: %s", j.ID)
				}
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("claim blocked on another client's locked rows")
	}
}

func testFinalize[TTx any](t *testing.T, f Fixture[TTx], noBatches bool) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := executor(d, noBatches)
	p := params("k", 6)
	p[5].Await = true
	inserted := insert(ctx, t, exec, p)
	clientID := register(ctx, t, exec)
	if jobs := claim(ctx, t, exec, clientID, 10); len(jobs) != 6 {
		t.Fatalf("claimed %d", len(jobs))
	}
	id := func(i int) driver.JobID { return inserted[i].Job.ID }
	now, err := exec.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	applied := finalize(ctx, t, exec,
		driver.JobFinalize{ID: id(0), AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true, Output: []byte(`{"ok":true}`)},
		driver.JobFinalize{ID: id(1), AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: false},
		driver.JobFinalize{ID: id(2), AttemptedBy: clientID, State: driver.JobStateRetryable, Delay: 30 * time.Second, Error: &driver.AttemptError{Attempt: 1, Error: "boom"}},
		driver.JobFinalize{ID: id(3), AttemptedBy: clientID, State: driver.JobStateScheduled, Delay: time.Minute, Snooze: true},
		driver.JobFinalize{ID: id(4), AttemptedBy: clientID, State: driver.JobStateDiscarded, Archive: false, Error: &driver.AttemptError{Attempt: 1, Error: "fatal", Trace: "stack"}},
		driver.JobFinalize{ID: id(5), AttemptedBy: clientID, State: driver.JobStateCanceled},
	)
	if len(applied) != 6 {
		t.Fatalf("applied %d, want 6", len(applied))
	}
	get := func(i int) *driver.JobRow {
		t.Helper()
		j, err := exec.JobGet(ctx, id(i))
		if err != nil {
			t.Fatalf("job %d: %v", i, err)
		}
		return j
	}
	if j := get(0); j.State != driver.JobStateCompleted || j.FinalizedAt.IsZero() || len(j.Output) == 0 || j.Attempt != 1 {
		t.Errorf("job 0 = %+v", j)
	}
	if _, err := exec.JobGet(ctx, id(1)); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("job 1 (completed, not archived): err = %v, want ErrNotFound", err)
	}
	if j := get(2); j.State != driver.JobStateRetryable || j.Attempt != 1 || j.AttemptedBy != 0 || len(j.Errors) != 1 {
		t.Errorf("job 2 = %+v", j)
	} else {
		e := j.Errors[0]
		if e.Error != "boom" || e.Attempt != 1 || e.At.IsZero() || e.At.Sub(now).Abs() > 5*time.Second {
			t.Errorf("job 2 error = %+v (db now %s)", e, now)
		}
		if until := j.ScheduledAt.Sub(now); until < 25*time.Second || until > 35*time.Second {
			t.Errorf("job 2 scheduled in %s, want ~30s", until)
		}
	}
	if j := get(3); j.State != driver.JobStateScheduled || j.Attempt != 0 || len(j.Errors) != 0 {
		t.Errorf("job 3 (snoozed) = %+v", j)
	}
	if j := get(4); j.State != driver.JobStateDiscarded || len(j.Errors) != 1 || j.Errors[0].Trace != "stack" {
		t.Errorf("job 4 (discarded is always archived) = %+v", j)
	}
	if j := get(5); j.State != driver.JobStateCanceled || j.FinalizedAt.IsZero() {
		t.Errorf("job 5 = %+v", j)
	}
	if live, hist := f.CountLive(ctx, t, d), f.CountHistory(ctx, t, d); live != 2 || hist != 3 {
		t.Errorf("live = %d, history = %d; want 2, 3", live, hist)
	}
	// The retryable job is claimable again once its time comes.
	f.MakeClaimable(ctx, t, d, id(2))
	if jobs := claim(ctx, t, exec, clientID, 10); len(jobs) != 1 || jobs[0].ID != id(2) || jobs[0].Attempt != 2 {
		t.Errorf("reclaim = %+v", jobs)
	}
	if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{{ID: id(2), AttemptedBy: clientID, State: driver.JobStateRunning}}}); err == nil {
		t.Error("finalize to running succeeded")
	}
	if applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{}); err != nil || len(applied) != 0 {
		t.Errorf("empty finalize = %v, %v", applied, err)
	}
}

// testFinalizeSkipLocked finalizes with SkipLocked while another
// transaction holds one of the rows, as a worker's CompleteTx that has not
// ended does: the statement returns at once, without that job, and applies
// it once the row is free.
func testFinalizeSkipLocked[TTx any](t *testing.T, f Fixture[TTx], noBatches bool) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := executor(d, noBatches)
	insert(ctx, t, exec, params("k", 2))
	clientID := register(ctx, t, exec)
	jobs := claim(ctx, t, exec, clientID, 2)
	if len(jobs) != 2 {
		t.Fatalf("claimed %d", len(jobs))
	}
	held, free := jobs[0].ID, jobs[1].ID
	tx, _, rollback := f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // rolled back below
	finalize(ctx, t, d.UnwrapTx(tx), driver.JobFinalize{ID: held, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	failed := driver.JobFinalize{ID: held, AttemptedBy: clientID, State: driver.JobStateRetryable, Delay: time.Hour, Error: &driver.AttemptError{Attempt: 1, Error: "x"}}
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	applied, err := exec.JobFinalizeMany(sctx, driver.JobFinalizeParams{SkipLocked: true, Jobs: []driver.JobFinalize{
		failed, {ID: free, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true},
	}})
	if err != nil || !slices.Equal(applied, []driver.JobID{free}) {
		t.Fatalf("finalize with a held row = %v, %v; want only %s", applied, err, free)
	}
	if err := rollback(); err != nil {
		t.Fatal(err)
	}
	applied, err = exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{SkipLocked: true, Jobs: []driver.JobFinalize{failed}})
	if err != nil || !slices.Equal(applied, []driver.JobID{held}) {
		t.Fatalf("finalize once the row is free = %v, %v", applied, err)
	}
	if j, err := exec.JobGet(ctx, held); err != nil || j.State != driver.JobStateRetryable {
		t.Errorf("job = %+v, %v", j, err)
	}
}

func testFinalizeFences[TTx any](t *testing.T, f Fixture[TTx], noBatches bool) {
	ctx := context.Background()
	exec := executor(f.NewDriver(t), noBatches)
	insert(ctx, t, exec, params("k", 1))
	a, b := register(ctx, t, exec), register(ctx, t, exec)
	jobs := claim(ctx, t, exec, a, 1)
	if len(jobs) != 1 {
		t.Fatal("claim")
	}
	id := jobs[0].ID
	// A result from a client that does not own the job is dropped.
	if applied := finalize(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: b, State: driver.JobStateCompleted, Archive: true}); len(applied) != 0 {
		t.Fatalf("stale finalize applied: %v", applied)
	}
	if j, _ := exec.JobGet(ctx, id); j.State != driver.JobStateRunning || j.AttemptedBy != a {
		t.Errorf("job after stale finalize = %+v", j)
	}
	// A result for a job that is no longer running is dropped too.
	finalize(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: a, State: driver.JobStateRetryable, Delay: time.Hour})
	if applied := finalize(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: a, State: driver.JobStateCompleted, Archive: true}); len(applied) != 0 {
		t.Fatalf("finalize of a non-running job applied: %v", applied)
	}
	if j, _ := exec.JobGet(ctx, id); j.State != driver.JobStateRetryable {
		t.Errorf("job = %+v", j)
	}
}

// testFinalizeTx finalizes inside a caller's transaction, as CompleteTx
// does: nothing changes until the transaction commits, a rollback leaves the
// attempt running and owned, and the fence applies inside the transaction.
func testFinalizeTx[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	insert(ctx, t, exec, params("k", 1))
	a, b := register(ctx, t, exec), register(ctx, t, exec)
	jobs := claim(ctx, t, exec, a, 1)
	if len(jobs) != 1 {
		t.Fatal("claim")
	}
	id := jobs[0].ID
	result := driver.JobFinalize{ID: id, AttemptedBy: a, State: driver.JobStateCompleted, Archive: true, Output: []byte(`{"ok":true}`)}

	// Each transaction is also rolled back on exit, so a failed check
	// cannot leave it open and hang the pool's close.
	tx, _, rollback := f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // already ended on the happy path
	if applied := finalize(ctx, t, d.UnwrapTx(tx), result); len(applied) != 1 {
		t.Fatalf("finalize in tx applied %v", applied)
	}
	if j, err := exec.JobGet(ctx, id); err != nil || j.State != driver.JobStateRunning {
		t.Errorf("job before commit = %+v, %v", j, err)
	}
	if err := rollback(); err != nil {
		t.Fatal(err)
	}
	if j, err := exec.JobGet(ctx, id); err != nil || j.State != driver.JobStateRunning || j.AttemptedBy != a {
		t.Errorf("job after rollback = %+v, %v", j, err)
	}

	tx, _, rollback = f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // already ended on the happy path
	if applied := finalize(ctx, t, d.UnwrapTx(tx), driver.JobFinalize{ID: id, AttemptedBy: b, State: driver.JobStateCompleted, Archive: true}); len(applied) != 0 {
		t.Errorf("finalize in tx by a client that does not own the job applied %v", applied)
	}
	if err := rollback(); err != nil {
		t.Fatal(err)
	}

	tx, commit, rollback := f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // already committed on the happy path
	if applied := finalize(ctx, t, d.UnwrapTx(tx), result); len(applied) != 1 {
		t.Fatalf("finalize in tx applied %v", applied)
	}
	if applied := finalize(ctx, t, d.UnwrapTx(tx), result); len(applied) != 0 {
		t.Errorf("second finalize in the same tx applied %v", applied)
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	j, err := exec.JobGet(ctx, id)
	if err != nil || j.State != driver.JobStateCompleted || j.FinalizedAt.IsZero() || string(j.Output) != `{"ok": true}` {
		t.Errorf("job after commit = %+v, %v", j, err)
	}
	if live, hist := f.CountLive(ctx, t, d), f.CountHistory(ctx, t, d); live != 0 || hist != 1 {
		t.Errorf("live = %d, history = %d; want 0, 1", live, hist)
	}
}

// testFinalizeTxConcurrent races a finalize held open in a caller's
// transaction against another finalize of the same attempt, as a rescue
// after a lost lease would send. The other waits for the transaction:
// after a commit it finds nothing to apply, after a rollback it applies.
func testFinalizeTxConcurrent[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	const n = 8
	inserted := insert(ctx, t, exec, params("k", n))
	clientID := register(ctx, t, exec)
	if jobs := claim(ctx, t, exec, clientID, n); len(jobs) != n {
		t.Fatalf("claimed %d", len(jobs))
	}
	for i, r := range inserted {
		commitTx := i%2 == 0
		tx, commit, rollback := f.Begin(ctx, t, d)
		// Rolled back on exit too, so a failed check cannot leave it open
		// with the competing finalize waiting on it.
		defer rollback() //nolint:errcheck // already ended on the happy path
		if applied := finalize(ctx, t, d.UnwrapTx(tx), driver.JobFinalize{ID: r.Job.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true}); len(applied) != 1 {
			t.Fatalf("finalize in tx applied %v", applied)
		}
		done := make(chan []driver.JobID, 1)
		go func() {
			applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{
				{ID: r.Job.ID, AttemptedBy: clientID, State: driver.JobStateRetryable, Error: &driver.AttemptError{Attempt: 1, Error: "hopper: client lost"}},
			}})
			if err != nil {
				t.Error(err)
			}
			done <- applied
		}()
		waitForLockWait(ctx, t, f, d)
		end := rollback
		if commitTx {
			end = commit
		}
		if err := end(); err != nil {
			t.Fatal(err)
		}
		competing := len(<-done)
		j, err := exec.JobGet(ctx, r.Job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if commitTx {
			if competing != 0 || j.State != driver.JobStateCompleted {
				t.Errorf("job %d: committed in tx, competing finalize applied %d, state %s", i, competing, j.State)
			}
		} else if competing != 1 || j.State != driver.JobStateRetryable {
			t.Errorf("job %d: rolled back, competing finalize applied %d, state %s", i, competing, j.State)
		}
	}
}

// waitForLockWait waits until a statement on the driver's schema is waiting
// for a lock, so a test knows its competing statement is blocked rather
// than not yet started. The schema's name is in every statement's text.
func waitForLockWait[TTx any](ctx context.Context, t *testing.T, f Fixture[TTx], d driver.Driver[TTx]) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for f.QueryText(ctx, t, d, `SELECT count(*)::text FROM pg_stat_activity
		WHERE wait_event_type = 'Lock' AND position(current_schema() IN query) > 0 AND pid <> pg_backend_pid()`) == "0" {
		if time.Now().After(deadline) {
			t.Fatal("no statement started waiting for a lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func testCancel[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
	inserted := insert(ctx, t, exec, params("k", 3))
	clientID := register(ctx, t, exec)
	running := claim(ctx, t, exec, clientID, 1)
	if len(running) != 1 {
		t.Fatal("claim")
	}
	waiting := inserted[1].Job.ID
	if waiting == running[0].ID {
		waiting = inserted[2].Job.ID
	}
	job, err := exec.JobCancel(ctx, waiting)
	if err != nil || job.State != driver.JobStateCanceled || job.FinalizedAt.IsZero() || len(job.Errors) != 1 {
		t.Errorf("canceled waiting job = %+v, %v", job, err)
	}
	job, err = exec.JobCancel(ctx, running[0].ID)
	if err != nil || job.State != driver.JobStateRunning || job.CancelRequestedAt.IsZero() {
		t.Errorf("canceled running job = %+v, %v", job, err)
	}
	res, err := exec.ClientRenew(ctx, driver.ClientRenewParams{ClientID: clientID, TTL: time.Hour})
	if err != nil || !res.Renewed || len(res.CancelRequested) != 1 || res.CancelRequested[0] != running[0].ID {
		t.Errorf("renew = %+v, %v", res, err)
	}
	if again, err := exec.JobCancel(ctx, running[0].ID); err != nil || !again.CancelRequestedAt.Equal(job.CancelRequestedAt) {
		t.Errorf("second cancel = %+v, %v", again, err)
	}
	if fin, err := exec.JobCancel(ctx, waiting); err != nil || fin.State != driver.JobStateCanceled {
		t.Errorf("cancel of finalized job = %+v, %v", fin, err)
	}
	if _, err := exec.JobCancel(ctx, driver.JobID{9}); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("cancel of unknown job = %v", err)
	}
}

func testRetry[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
	p := params("k", 3)
	p[0].UniqueKey = "u"
	p[2].ScheduledAt = time.Now().Add(time.Hour)
	p[2].TTL = time.Millisecond
	inserted := insert(ctx, t, exec, p)
	clientID := register(ctx, t, exec)
	jobs := claim(ctx, t, exec, clientID, 2)
	if len(jobs) != 2 {
		t.Fatal("claim")
	}
	if _, err := exec.JobRetry(ctx, jobs[0].ID); !errors.Is(err, driver.ErrJobRunning) {
		t.Errorf("retry of running job = %v", err)
	}
	// A retry overrides the wait, so a TTL that has passed no longer
	// applies: the job is neither discarded nor skipped by the claim.
	time.Sleep(10 * time.Millisecond)
	if job, err := exec.JobRetry(ctx, inserted[2].Job.ID); err != nil || job.State != driver.JobStateAvailable || !job.ExpiresAt.IsZero() {
		t.Errorf("retry of scheduled job = %+v, %v", job, err)
	}
	if expired, err := exec.JobDiscardExpired(ctx, 10); err != nil || len(expired) != 0 {
		t.Errorf("expired after retry = %+v, %v", expired, err)
	}
	if got := claim(ctx, t, exec, clientID, 1); len(got) != 1 || got[0].ID != inserted[2].Job.ID {
		t.Errorf("claim after retry = %+v", got)
	}
	finalize(ctx, t, exec,
		driver.JobFinalize{ID: inserted[0].Job.ID, AttemptedBy: clientID, State: driver.JobStateDiscarded, Error: &driver.AttemptError{Attempt: 1, Error: "x"}},
		driver.JobFinalize{ID: inserted[1].Job.ID, AttemptedBy: clientID, State: driver.JobStateDiscarded, Error: &driver.AttemptError{Attempt: 1, Error: "x"}},
	)
	job, err := exec.JobRetry(ctx, inserted[0].Job.ID)
	if err != nil || job.State != driver.JobStateAvailable || job.Attempt != 1 || job.MaxAttempts != 3 || len(job.Errors) != 1 || job.UniqueKey != "u" {
		t.Errorf("re-driven job = %+v, %v", job, err)
	}
	if dup := insert(ctx, t, exec, []driver.JobInsertParams{p[0]}); !dup[0].Duplicate {
		t.Error("re-driven job's unique key is not live")
	}
	// A retry that would violate a unique key is refused, and history kept.
	if _, err := exec.JobCancel(ctx, inserted[0].Job.ID); err != nil {
		t.Fatal(err)
	}
	insert(ctx, t, exec, []driver.JobInsertParams{p[0]})
	if _, err := exec.JobRetry(ctx, inserted[0].Job.ID); !errors.Is(err, driver.ErrUniqueConflict) {
		t.Errorf("retry with a live duplicate = %v", err)
	}
	if got, err := exec.JobGet(ctx, inserted[0].Job.ID); err != nil || got.State != driver.JobStateCanceled {
		t.Errorf("history row after refused retry = %+v, %v", got, err)
	}
	if _, err := exec.JobRetry(ctx, driver.JobID{9}); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("retry of unknown job = %v", err)
	}
}

func testRetryOrderingKey[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	later := time.Now().Add(time.Hour)
	p := params("k", 7)
	for i := range p {
		p[i].OrderingKey = "a"
		if i > 0 {
			p[i].ScheduledAt = later
		}
	}
	p[1].TTL = 2 * time.Hour
	p[2].OrderingKey = "b"
	p[3].Queue = "other"
	inserted := insert(ctx, t, exec, p)
	id := func(i int) driver.JobID { return inserted[i].Job.ID }
	if _, err := exec.JobCancel(ctx, id(4)); err != nil {
		t.Fatal(err)
	}
	f.Exec(ctx, t, d, "UPDATE hopper_jobs SET expires_at = now() - interval '1s' WHERE id = $1::uuid", id(6).String())
	wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
		Name: "w", Jobs: []driver.JobInsertParams{{Kind: "w", Queue: "wf", Priority: 2, MaxAttempts: 1}, {Kind: "w", Queue: "wf", Priority: 2, MaxAttempts: 1, OrderingKey: "a"}},
		Deps: []driver.JobDependency{{Job: 1, DependsOn: 0}},
	})
	if err != nil {
		t.Fatal(err)
	}
	clientID := register(ctx, t, exec)
	if jobs := claim(ctx, t, exec, clientID, 10); len(jobs) != 1 || jobs[0].ID != id(0) {
		t.Fatalf("claim = %+v", jobs)
	}
	get := func(id driver.JobID) *driver.JobRow {
		t.Helper()
		j, err := exec.JobGet(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return j
	}
	wake := func(queue string, limit, want int) {
		t.Helper()
		n, err := exec.JobRetryOrderingKey(ctx, driver.JobRetryOrderingKeyParams{Queue: queue, OrderingKey: "a", Limit: limit})
		if err != nil || n != want {
			t.Fatalf("JobRetryOrderingKey(%s, limit %d) = %d, %v, want %d", queue, limit, n, err, want)
		}
	}
	claimed := func(want ...driver.JobID) {
		t.Helper()
		jobs := claim(ctx, t, exec, clientID, 10)
		if len(jobs) != len(want) || (len(want) == 1 && jobs[0].ID != want[0]) {
			t.Fatalf("claim = %+v, want %v", jobs, want)
		}
	}

	// Waiting jobs of the key in this queue wake oldest first, keeping their
	// TTL; another key, another queue, a finalized job, a pending job and a
	// job past its TTL stay as they are, and the running job runs on.
	wake("default", 1, 1)
	if j := get(id(1)); j.State != driver.JobStateAvailable || j.ExpiresAt.IsZero() || get(id(5)).State != driver.JobStateScheduled {
		t.Errorf("after one: %+v, %s", j, get(id(5)).State)
	}
	wake("default", 10, 1)
	wake("wf", 10, 0)
	states := map[int]driver.JobState{
		0: driver.JobStateRunning, 2: driver.JobStateScheduled, 3: driver.JobStateScheduled,
		4: driver.JobStateCanceled, 5: driver.JobStateAvailable, 6: driver.JobStateScheduled,
	}
	for i, want := range states {
		if got := get(id(i)).State; got != want {
			t.Errorf("job %d is %s, want %s", i, got, want)
		}
	}
	if got := get(wf.Jobs[1].Job.ID).State; got != driver.JobStatePending {
		t.Errorf("pending job is %s", got)
	}

	// The running job was marked, so its retry runs at once.
	finalize(ctx, t, exec, driver.JobFinalize{ID: id(0), AttemptedBy: clientID, State: driver.JobStateRetryable, Delay: time.Hour, Error: &driver.AttemptError{Attempt: 1, Error: "x"}})
	claimed(id(0))

	// A snooze with no wake waits, and blocks the key, until a wake.
	finalize(ctx, t, exec, driver.JobFinalize{ID: id(0), AttemptedBy: clientID, State: driver.JobStateScheduled, Delay: time.Hour, Snooze: true, SnoozeReason: "waiting"})
	claimed()
	wake("default", 10, 1)
	if r := storedReason(ctx, t, f, d, id(0)); r != "" {
		t.Errorf("woken job kept reason %q", r)
	}
	claimed(id(0))

	// A wake during the attempt makes its snooze run at once, without a reason.
	wake("default", 10, 0)
	finalize(ctx, t, exec, driver.JobFinalize{ID: id(0), AttemptedBy: clientID, State: driver.JobStateScheduled, Delay: time.Hour, Snooze: true, SnoozeReason: "waiting"})
	if r := storedReason(ctx, t, f, d, id(0)); r != "" {
		t.Errorf("snooze after a wake kept reason %q", r)
	}
	claimed(id(0))

	// A mark from before the attempt started does not count.
	f.Exec(ctx, t, d, "UPDATE hopper_jobs SET wake_requested_at = attempted_at - interval '1s' WHERE id = $1::uuid", id(0).String())
	finalize(ctx, t, exec, driver.JobFinalize{ID: id(0), AttemptedBy: clientID, State: driver.JobStateRetryable, Delay: time.Hour, Error: &driver.AttemptError{Attempt: 1, Error: "x"}})
	claimed()
	if n, err := exec.JobRetryOrderingKey(ctx, driver.JobRetryOrderingKeyParams{Queue: "default", OrderingKey: "none", Limit: 10}); err != nil || n != 0 {
		t.Errorf("unknown key = %d, %v", n, err)
	}
}

// testRetryOrderingKeyConcurrent checks that a wake racing the end of an
// attempt is never lost, and that a wake and the leader's aging, which lock
// the same rows, never deadlock.
func testRetryOrderingKeyConcurrent[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := register(ctx, t, exec)
	wakeKey := func(key string) {
		if _, err := exec.JobRetryOrderingKey(ctx, driver.JobRetryOrderingKeyParams{Queue: "default", OrderingKey: key, Limit: 1000}); err != nil {
			t.Error(err)
		}
	}

	for round := range 40 {
		key := fmt.Sprintf("race-%d", round)
		p := params("k", 1)
		p[0].OrderingKey = key
		job := insert(ctx, t, exec, p)[0].Job.ID
		if jobs := claim(ctx, t, exec, clientID, 1); len(jobs) != 1 || jobs[0].ID != job {
			t.Fatalf("claim = %+v", jobs)
		}
		var wg sync.WaitGroup
		wg.Go(func() { wakeKey(key) })
		wg.Go(func() {
			res := driver.JobFinalize{ID: job, AttemptedBy: clientID, State: driver.JobStateScheduled, Delay: time.Hour, Snooze: true}
			if applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{res}}); err != nil || len(applied) != 1 {
				t.Errorf("finalize = %v, %v", applied, err)
			}
		})
		wg.Wait()
		if due := f.QueryText(ctx, t, d, "SELECT (scheduled_at <= now())::text FROM hopper_jobs WHERE id = $1::uuid", job.String()); due != "true" {
			t.Fatalf("round %d: a wake racing the snooze was lost", round)
		}
		f.Exec(ctx, t, d, "DELETE FROM hopper_jobs WHERE id = $1::uuid", job.String())
	}

	// Rows laid out in the heap in the reverse of their seq order: the
	// aging pass meets them in one order and the wake locks them in the
	// other. Each round rewrites them one by one, newest first, which
	// appends their new versions in that order.
	p := params("k", 300)
	for i := range p {
		p[i].OrderingKey, p[i].Priority, p[i].ScheduledAt = "aging", 3, time.Now().Add(time.Hour)
	}
	aging := insert(ctx, t, exec, p)
	for range 20 {
		for i := len(aging) - 1; i >= 0; i-- {
			f.Exec(ctx, t, d, `UPDATE hopper_jobs SET state = 'scheduled', priority = 3, scheduled_at = now() - interval '1 hour'
			  WHERE id = $1::uuid`, aging[i].Job.ID.String())
		}
		var wg sync.WaitGroup
		wg.Go(func() { wakeKey("aging") })
		wg.Go(func() {
			if _, err := exec.JobAge(ctx, "default", time.Millisecond); err != nil {
				t.Error(err)
			}
		})
		wg.Wait()
	}
}

func testTTL[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	p := params("k", 3)
	p[0].TTL = time.Hour
	p[1].TTL = time.Millisecond
	inserted := insert(ctx, t, exec, p)
	if inserted[0].Job.ExpiresAt.IsZero() || !inserted[2].Job.ExpiresAt.IsZero() {
		t.Errorf("expires_at = %s, %s", inserted[0].Job.ExpiresAt, inserted[2].Job.ExpiresAt)
	}
	time.Sleep(20 * time.Millisecond)
	expired, err := exec.JobDiscardExpired(ctx, 100)
	if err != nil || len(expired) != 1 || expired[0].ID != inserted[1].Job.ID {
		t.Fatalf("expired = %v, %v", expired, err)
	}
	if expired[0].State != driver.JobStateDiscarded || len(expired[0].Errors) != 1 {
		t.Errorf("expired job = %+v", expired[0])
	}
	if n := f.CountLive(ctx, t, d); n != 2 {
		t.Errorf("live = %d", n)
	}
	if again, err := exec.JobDiscardExpired(ctx, 100); err != nil || len(again) != 0 {
		t.Errorf("second expiry = %v, %v", again, err)
	}

	// The deadline bounds every wait, not only the first: a job that has
	// run and waits to run again, to retry or after a snooze, expires too.
	// A running job does not.
	p = params("again", 3)
	for i := range p {
		p[i].Queue, p[i].TTL = "again", 500*time.Millisecond
	}
	insert(ctx, t, exec, p)
	clientID := register(ctx, t, exec)
	claimAgain := func() []*driver.JobRow {
		t.Helper()
		res, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "again", ClientID: clientID, Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		return res.Jobs
	}
	ran := claimAgain()
	if len(ran) != 3 {
		t.Fatalf("claimed %d", len(ran))
	}
	finalize(ctx, t, exec,
		driver.JobFinalize{ID: ran[0].ID, AttemptedBy: clientID, State: driver.JobStateRetryable, Error: &driver.AttemptError{Attempt: 1, Error: "boom"}},
		driver.JobFinalize{ID: ran[1].ID, AttemptedBy: clientID, State: driver.JobStateScheduled, Snooze: true, SnoozeReason: "later"},
	)
	time.Sleep(600 * time.Millisecond)
	if jobs := claimAgain(); len(jobs) != 0 {
		t.Errorf("claimed expired jobs: %+v", jobs)
	}
	expired, err = exec.JobDiscardExpired(ctx, 100)
	if err != nil || len(expired) != 2 {
		t.Fatalf("expired = %v, %v", expired, err)
	}
	// The snoozed job's reason is kept in its error, as the row that held
	// it has left the live table.
	want := map[driver.JobID]string{ran[0].ID: "hopper: expired", ran[1].ID: "hopper: expired while snoozed: later"}
	for _, j := range expired {
		if j.State != driver.JobStateDiscarded || j.SnoozeReason != "" || j.Errors[len(j.Errors)-1].Error != want[j.ID] {
			t.Errorf("expired job = %+v, want error %q", j, want[j.ID])
		}
	}
	if j, err := exec.JobGet(ctx, ran[2].ID); err != nil || j.State != driver.JobStateRunning {
		t.Errorf("running job past its TTL = %+v, %v", j, err)
	}
}

// testListOrderingKeyLane lists a key's jobs in every live state, each found
// through a different index, and from history.
func testListOrderingKeyLane[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
	clientID := register(ctx, t, exec)
	keyed := func(n int) []driver.JobInsertParams {
		p := params("a", n)
		for i := range p {
			p[i].OrderingKey = "k"
		}
		return p
	}
	archived := insert(ctx, t, exec, keyed(1))[0].Job.ID
	claim(ctx, t, exec, clientID, 1)
	finalize(ctx, t, exec, driver.JobFinalize{ID: archived, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})

	other := params("a", 2)
	other[1].OrderingKey = "other"
	insert(ctx, t, exec, other)
	wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
		Name: "w", Jobs: keyed(2), Deps: []driver.JobDependency{{Job: 1, DependsOn: 0}},
	})
	if err != nil {
		t.Fatal(err)
	}
	running, pending := wf.Jobs[0].Job.ID, wf.Jobs[1].Job.ID
	if jobs := claim(ctx, t, exec, clientID, 10); len(jobs) != 3 {
		t.Fatalf("claimed %d, want the key's first job and the two others", len(jobs))
	}
	later := keyed(2)
	later[1].ScheduledAt = time.Now().Add(time.Hour)
	waiting := insert(ctx, t, exec, later)
	available, scheduled := waiting[0].Job.ID, waiting[1].Job.ID

	list := func(states ...driver.JobState) []driver.JobID {
		t.Helper()
		jobs, err := exec.JobList(ctx, driver.JobListParams{Queue: "default", OrderingKey: "k", States: states, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]driver.JobID, len(jobs))
		for i, j := range jobs {
			ids[i] = j.ID
		}
		slices.SortFunc(ids, func(a, b driver.JobID) int { return bytes.Compare(a[:], b[:]) })
		return ids
	}
	sorted := func(ids ...driver.JobID) []driver.JobID {
		slices.SortFunc(ids, func(a, b driver.JobID) int { return bytes.Compare(a[:], b[:]) })
		return ids
	}
	if got, want := list(), sorted(archived, running, pending, available, scheduled); !slices.Equal(got, want) {
		t.Errorf("key k = %v, want %v", got, want)
	}
	for _, c := range []struct {
		state driver.JobState
		want  driver.JobID
	}{
		{driver.JobStatePending, pending},
		{driver.JobStateRunning, running},
		{driver.JobStateAvailable, available},
		{driver.JobStateScheduled, scheduled},
		{driver.JobStateCompleted, archived},
	} {
		if got := list(c.state); !slices.Equal(got, []driver.JobID{c.want}) {
			t.Errorf("key k, %s = %v, want %v", c.state, got, c.want)
		}
	}
}

func testList[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
	p := params("a", 5)
	p[1].Kind, p[3].Kind = "b", "b"
	p[4].Queue = "other"
	p[0].OrderingKey, p[2].OrderingKey = "k", "k"
	inserted := insert(ctx, t, exec, p)
	clientID := register(ctx, t, exec)
	claimed := claim(ctx, t, exec, clientID, 1)
	finalize(ctx, t, exec, driver.JobFinalize{ID: claimed[0].ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})

	list := func(q driver.JobListParams) []driver.JobID {
		t.Helper()
		if q.Limit == 0 {
			q.Limit = 100
		}
		jobs, err := exec.JobList(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]driver.JobID, len(jobs))
		for i, j := range jobs {
			ids[i] = j.ID
			if i == 0 {
				continue
			}
			if c := bytes.Compare(jobs[i-1].ID[:], j.ID[:]); c == 0 || (c > 0) != q.NewestFirst {
				t.Errorf("not in ID order (newest first: %v)", q.NewestFirst)
			}
		}
		return ids
	}
	if got := list(driver.JobListParams{}); len(got) != 5 {
		t.Errorf("all = %d, want 5 (live and history)", len(got))
	}
	if got := list(driver.JobListParams{States: []driver.JobState{driver.JobStateCompleted}}); len(got) != 1 || got[0] != claimed[0].ID {
		t.Errorf("completed = %v", got)
	}
	if got := list(driver.JobListParams{Kinds: []string{"b"}}); len(got) != 2 {
		t.Errorf("kind b = %v", got)
	}
	if got := list(driver.JobListParams{Queue: "other"}); len(got) != 1 || got[0] != inserted[4].Job.ID {
		t.Errorf("queue other = %v", got)
	}
	first := list(driver.JobListParams{Limit: 2})
	if rest := list(driver.JobListParams{After: first[1]}); len(rest) != 3 || rest[0].String() <= first[1].String() {
		t.Errorf("after cursor = %v", rest)
	}

	// IDs inserted in one batch need not be in input order, so expectations
	// sort them.
	byID := func(ids ...driver.JobID) []driver.JobID {
		slices.SortFunc(ids, func(a, b driver.JobID) int { return bytes.Compare(a[:], b[:]) })
		return ids
	}
	all := make([]driver.JobID, len(inserted))
	for i, r := range inserted {
		all[i] = r.Job.ID
	}
	all = byID(all...)

	if _, err := exec.JobList(ctx, driver.JobListParams{OrderingKey: "k", Limit: 10}); err == nil {
		t.Error("an ordering key without a queue was listed")
	}

	// An ordering key's jobs in its queue, live and from history.
	if got, want := list(driver.JobListParams{Queue: "default", OrderingKey: "k"}), byID(inserted[0].Job.ID, inserted[2].Job.ID); !slices.Equal(got, want) {
		t.Errorf("ordering key k = %v, want %v", got, want)
	}

	// Newest first, paged with the same cursor, live and from history.
	newest := list(driver.JobListParams{NewestFirst: true, Limit: 2})
	if want := []driver.JobID{all[4], all[3]}; !slices.Equal(newest, want) {
		t.Fatalf("newest first = %v, want %v", newest, want)
	}
	if got, want := list(driver.JobListParams{NewestFirst: true, After: newest[1]}), []driver.JobID{all[2], all[1], all[0]}; !slices.Equal(got, want) {
		t.Errorf("newest first after cursor = %v, want %v", got, want)
	}
	waitingK := inserted[2].Job.ID
	if claimed[0].ID == waitingK {
		waitingK = inserted[0].Job.ID
	}
	if got := list(driver.JobListParams{NewestFirst: true, Queue: "default", OrderingKey: "k", States: []driver.JobState{driver.JobStateAvailable}}); !slices.Equal(got, []driver.JobID{waitingK}) {
		t.Errorf("newest first, key k, available = %v", got)
	}
}

// testJobDelete deletes jobs in every state: a finalized job leaves history,
// and a waiting one is canceled on the way out, so that its batch and its
// dependents go on as after a cancel.
func testJobDelete[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := register(ctx, t, exec)
	gone := func(id driver.JobID) {
		t.Helper()
		if _, err := exec.JobGet(ctx, id); !errors.Is(err, driver.ErrNotFound) {
			t.Errorf("job %s after delete: %v, want ErrNotFound", id, err)
		}
	}

	if _, err := exec.JobDelete(ctx, driver.JobID{1}); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("delete unknown = %v, want ErrNotFound", err)
	}

	done := insert(ctx, t, exec, params("a", 1))[0].Job.ID
	claim(ctx, t, exec, clientID, 1)
	finalize(ctx, t, exec, driver.JobFinalize{ID: done, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	if job, err := exec.JobDelete(ctx, done); err != nil || job.ID != done || job.State != driver.JobStateCompleted {
		t.Errorf("delete completed = %+v, %v", job, err)
	}
	gone(done)

	running := insert(ctx, t, exec, params("a", 1))[0].Job.ID
	claim(ctx, t, exec, clientID, 1)
	if _, err := exec.JobDelete(ctx, running); !errors.Is(err, driver.ErrJobRunning) {
		t.Errorf("delete running = %v, want ErrJobRunning", err)
	}
	if j, err := exec.JobGet(ctx, running); err != nil || j.State != driver.JobStateRunning {
		t.Errorf("running job after a refused delete = %+v, %v", j, err)
	}

	// A workflow: deleting its first job, still waiting, cancels the job
	// that depends on it, and the workflow's batch completes with both
	// failed.
	wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
		Name: "w", Jobs: []driver.JobInsertParams{params("w", 1)[0], params("w", 1)[0]},
		Deps: []driver.JobDependency{{Job: 1, DependsOn: 0}},
	})
	if err != nil {
		t.Fatal(err)
	}
	first, second := wf.Jobs[0].Job.ID, wf.Jobs[1].Job.ID
	if job, err := exec.JobDelete(ctx, first); err != nil || job.ID != first || job.State != driver.JobStateCanceled {
		t.Errorf("delete waiting = %+v, %v", job, err)
	}
	gone(first)
	if j, err := exec.JobGet(ctx, second); err != nil || j.State != driver.JobStateCanceled {
		t.Errorf("dependent of a deleted job = %+v, %v, want canceled", j, err)
	}
	if b, err := exec.BatchGet(ctx, wf.ID); err != nil || b.CompletedAt.IsZero() || b.Failed != 2 {
		t.Errorf("workflow batch after the delete = %+v, %v", b, err)
	}

	// In a caller's transaction, a refused delete leaves the transaction
	// usable for the next one.
	tx, commit, _ := f.Begin(ctx, t, d)
	txExec := d.UnwrapTx(tx)
	if _, err := txExec.JobDelete(ctx, running); !errors.Is(err, driver.ErrJobRunning) {
		t.Errorf("delete running in a transaction = %v, want ErrJobRunning", err)
	}
	if job, err := txExec.JobDelete(ctx, second); err != nil || job.ID != second {
		t.Errorf("delete after a refused delete in a transaction = %+v, %v", job, err)
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	gone(second)
}

// testJobDeleteRacesRetry deletes a finalized job while a transaction that
// retries it is open: once the retry commits, the delete finds the job live
// again and deletes it there, instead of reporting it unknown and leaving
// it to run.
func testJobDeleteRacesRetry[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	id := insert(ctx, t, exec, params("a", 1))[0].Job.ID
	if _, err := exec.JobCancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	tx, commit, _ := f.Begin(ctx, t, d)
	if _, err := d.UnwrapTx(tx).JobRetry(ctx, id); err != nil {
		t.Fatal(err)
	}
	var deleted *driver.JobRow
	done := make(chan error, 1)
	go func() {
		var err error
		deleted, err = exec.JobDelete(ctx, id)
		done <- err
	}()
	waitForLockWait(ctx, t, f, d)
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil || deleted.State != driver.JobStateCanceled {
		t.Errorf("delete racing a retry = %+v, %v", deleted, err)
	}
	if _, err := exec.JobGet(ctx, id); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("job after a delete that raced its retry: %v, want ErrNotFound", err)
	}
}

// testRetryOrderingKeyTx wakes a key in a transaction that began before the
// attempt of the key's running job: the mark is that attempt's, so the
// snooze the attempt ends in runs at once.
func testRetryOrderingKeyTx[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := register(ctx, t, exec)
	p := params("k", 1)
	p[0].OrderingKey = "a"
	job := insert(ctx, t, exec, p)[0].Job.ID
	tx, commit, _ := f.Begin(ctx, t, d)
	txExec := d.UnwrapTx(tx)
	if _, err := txExec.Now(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if jobs := claim(ctx, t, exec, clientID, 1); len(jobs) != 1 || jobs[0].ID != job {
		t.Fatalf("claim = %+v", jobs)
	}
	if n, err := txExec.JobRetryOrderingKey(ctx, driver.JobRetryOrderingKeyParams{Queue: "default", OrderingKey: "a", Limit: 10}); err != nil || n != 0 {
		t.Fatalf("JobRetryOrderingKey in a transaction = %d, %v", n, err)
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	finalize(ctx, t, exec, driver.JobFinalize{ID: job, AttemptedBy: clientID, State: driver.JobStateScheduled, Delay: time.Hour, Snooze: true})
	if due := f.QueryText(ctx, t, d, "SELECT (scheduled_at <= now())::text FROM hopper_jobs WHERE id = $1::uuid", job.String()); due != "true" {
		t.Error("a wake in a transaction older than the attempt was lost")
	}
}

// testJobDeleteRacesClaim deletes waiting jobs while claims take them: each
// job either runs or is deleted, never both, and none is lost.
func testJobDeleteRacesClaim[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
	const n = 200
	inserted := insert(ctx, t, exec, params("a", n))
	clientID := register(ctx, t, exec)
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		claimed = map[driver.JobID]bool{}
		deleted = map[driver.JobID]bool{}
	)
	wg.Go(func() {
		for range n {
			res, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 5})
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			for _, j := range res.Jobs {
				claimed[j.ID] = true
			}
			mu.Unlock()
			if len(res.Jobs) == 0 {
				return
			}
		}
	})
	for i := range 4 {
		wg.Go(func() {
			for k := i; k < n; k += 4 {
				id := inserted[k].Job.ID
				_, err := exec.JobDelete(ctx, id)
				switch {
				case err == nil:
					mu.Lock()
					deleted[id] = true
					mu.Unlock()
				case !errors.Is(err, driver.ErrJobRunning):
					t.Errorf("delete %s: %v", id, err)
				}
			}
		})
	}
	wg.Wait()
	for _, r := range inserted {
		id := r.Job.ID
		switch {
		case claimed[id] && deleted[id]:
			t.Errorf("job %s was both claimed and deleted", id)
		case !claimed[id] && !deleted[id]:
			t.Errorf("job %s was neither claimed nor deleted", id)
		case claimed[id]:
			if j, err := exec.JobGet(ctx, id); err != nil || j.State != driver.JobStateRunning {
				t.Errorf("claimed job %s = %+v, %v", id, j, err)
			}
		default:
			if _, err := exec.JobGet(ctx, id); !errors.Is(err, driver.ErrNotFound) {
				t.Errorf("deleted job %s: %v", id, err)
			}
		}
	}
}
