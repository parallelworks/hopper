package drivertest

import (
	"context"
	"errors"
	"fmt"
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

func testFinalize[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
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
		driver.JobFinalize{ID: id(5), AttemptedBy: clientID, State: driver.JobStateCancelled},
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
	if j := get(5); j.State != driver.JobStateCancelled || j.FinalizedAt.IsZero() {
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

func testFinalizeFences[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
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
	if err != nil || job.State != driver.JobStateCancelled || job.FinalizedAt.IsZero() || len(job.Errors) != 1 {
		t.Errorf("cancelled waiting job = %+v, %v", job, err)
	}
	job, err = exec.JobCancel(ctx, running[0].ID)
	if err != nil || job.State != driver.JobStateRunning || job.CancelRequestedAt.IsZero() {
		t.Errorf("cancelled running job = %+v, %v", job, err)
	}
	res, err := exec.ClientRenew(ctx, driver.ClientRenewParams{ClientID: clientID, TTL: time.Hour})
	if err != nil || !res.Renewed || len(res.CancelRequested) != 1 || res.CancelRequested[0] != running[0].ID {
		t.Errorf("renew = %+v, %v", res, err)
	}
	if again, err := exec.JobCancel(ctx, running[0].ID); err != nil || !again.CancelRequestedAt.Equal(job.CancelRequestedAt) {
		t.Errorf("second cancel = %+v, %v", again, err)
	}
	if fin, err := exec.JobCancel(ctx, waiting); err != nil || fin.State != driver.JobStateCancelled {
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
	inserted := insert(ctx, t, exec, p)
	clientID := register(ctx, t, exec)
	jobs := claim(ctx, t, exec, clientID, 2)
	if len(jobs) != 2 {
		t.Fatal("claim")
	}
	if _, err := exec.JobRetry(ctx, jobs[0].ID); !errors.Is(err, driver.ErrJobRunning) {
		t.Errorf("retry of running job = %v", err)
	}
	if job, err := exec.JobRetry(ctx, inserted[2].Job.ID); err != nil || job.State != driver.JobStateAvailable {
		t.Errorf("retry of scheduled job = %+v, %v", job, err)
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
	if got, err := exec.JobGet(ctx, inserted[0].Job.ID); err != nil || got.State != driver.JobStateCancelled {
		t.Errorf("history row after refused retry = %+v, %v", got, err)
	}
	if _, err := exec.JobRetry(ctx, driver.JobID{9}); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("retry of unknown job = %v", err)
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
}

func testList[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
	p := params("a", 5)
	p[1].Kind, p[3].Kind = "b", "b"
	p[4].Queue = "other"
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
			if i > 0 && jobs[i-1].ID.String() >= j.ID.String() {
				t.Error("not in ID order")
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
}
