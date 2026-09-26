package hopperpgx_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/hopperpgx"
	"github.com/parallelworks/hopper/internal/testdb"
)

func setup(t *testing.T) (context.Context, *hopperpgx.Driver, driver.Executor) {
	t.Helper()
	pool := testdb.Pool(t)
	d := hopperpgx.New(pool)
	return context.Background(), d, d.Executor()
}

func insertParams(kind string, n int) []driver.JobInsertParams {
	out := make([]driver.JobInsertParams, n)
	for i := range out {
		out[i] = driver.JobInsertParams{
			Kind: kind, Queue: "default", Priority: 2, MaxAttempts: 3,
			Args: json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)),
		}
	}
	return out
}

func register(t *testing.T, ctx context.Context, exec driver.Executor) int64 {
	t.Helper()
	id, err := exec.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "test", TTL: 15 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestJobInsertManyReturnsInputOrder(t *testing.T) {
	t.Parallel()
	ctx, _, exec := setup(t)

	params := insertParams("order", 50)
	for i := range params {
		params[i].Queue = fmt.Sprintf("q%d", i%3)
	}
	results, err := exec.JobInsertMany(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != len(params) {
		t.Fatalf("got %d results for %d params", len(results), len(params))
	}
	for i, r := range results {
		if r.Duplicate {
			t.Errorf("result %d marked duplicate", i)
		}
		if string(r.Job.Args) != fmt.Sprintf(`{"i": %d}`, i) || r.Job.Queue != params[i].Queue {
			t.Errorf("result %d = %s in %s, want %s in %s", i, r.Job.Args, r.Job.Queue, params[i].Args, params[i].Queue)
		}
		if r.Job.State != driver.JobStateAvailable || r.Job.ID.IsZero() || r.Job.CreatedAt.IsZero() {
			t.Errorf("result %d incomplete: %+v", i, r.Job)
		}
		if r.Job.Errors == nil || len(r.Job.Errors) != 0 {
			t.Errorf("result %d errors = %v, want empty", i, r.Job.Errors)
		}
	}
}

func TestJobInsertManySchedulesFutureJobs(t *testing.T) {
	t.Parallel()
	ctx, _, exec := setup(t)

	future := time.Now().Add(time.Hour)
	params := insertParams("sched", 2)
	params[1].ScheduledAt = future
	results, err := exec.JobInsertMany(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Job.State != driver.JobStateAvailable {
		t.Errorf("job 0 state = %s, want available", results[0].Job.State)
	}
	if results[1].Job.State != driver.JobStateScheduled {
		t.Errorf("job 1 state = %s, want scheduled", results[1].Job.State)
	}
	if !results[1].Job.ScheduledAt.Equal(future.Truncate(time.Microsecond)) {
		t.Errorf("job 1 scheduled_at = %s, want %s", results[1].Job.ScheduledAt, future)
	}

	// A future job is not claimable.
	id := register(t, ctx, exec)
	jobs, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: id, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != results[0].Job.ID {
		t.Fatalf("claimed %d jobs, want only the available one", len(jobs))
	}
}

func TestJobInsertManyUniqueKey(t *testing.T) {
	t.Parallel()
	ctx, _, exec := setup(t)

	first, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{
		{Kind: "u", Queue: "default", Priority: 2, MaxAttempts: 1, UniqueKey: "k1", Args: json.RawMessage(`{"v":1}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{
		{Kind: "u", Queue: "default", Priority: 2, MaxAttempts: 1, UniqueKey: "k1", Args: json.RawMessage(`{"v":2}`)},
		{Kind: "u", Queue: "default", Priority: 2, MaxAttempts: 1, UniqueKey: "k2", Args: json.RawMessage(`{"v":3}`)},
		{Kind: "other", Queue: "default", Priority: 2, MaxAttempts: 1, UniqueKey: "k1", Args: json.RawMessage(`{"v":4}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !second[0].Duplicate || second[0].Job.ID != first[0].Job.ID || string(second[0].Job.Args) != `{"v": 1}` {
		t.Errorf("duplicate not resolved to the existing job: %+v", second[0])
	}
	if second[1].Duplicate || second[2].Duplicate {
		t.Errorf("distinct keys marked duplicate: %+v %+v", second[1], second[2])
	}

	// Uniqueness covers live jobs only: once the job is finalized, the key is free.
	clientID := register(t, ctx, exec)
	jobs, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	var fin []driver.JobFinalize
	for _, j := range jobs {
		fin = append(fin, driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	}
	if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: fin}); err != nil {
		t.Fatal(err)
	}
	third, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{
		{Kind: "u", Queue: "default", Priority: 2, MaxAttempts: 1, UniqueKey: "k1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if third[0].Duplicate {
		t.Error("finalized job still blocks its unique key")
	}
}

func TestJobInsertCopy(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)

	params := insertParams("copy", 1000)
	params[5].ScheduledAt = time.Now().Add(time.Hour)
	results, err := exec.JobInsertCopy(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != len(params) {
		t.Fatalf("got %d results", len(results))
	}
	for i, r := range results {
		if r.Job.ID.IsZero() || r.Job.CreatedAt.IsZero() || r.Job.ScheduledAt.IsZero() {
			t.Fatalf("result %d incomplete: %+v", i, r.Job)
		}
	}
	if results[5].Job.State != driver.JobStateScheduled || results[4].Job.State != driver.JobStateAvailable {
		t.Errorf("states = %s, %s", results[4].Job.State, results[5].Job.State)
	}

	// The rows are really there, with the fields we reported.
	got, err := exec.JobGet(ctx, results[7].Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != "copy" || string(got.Args) != `{"i": 7}` || got.State != driver.JobStateAvailable || got.MaxAttempts != 3 {
		t.Errorf("JobGet = %+v", got)
	}
	var count int
	if err := d.Pool().QueryRow(ctx, "SELECT count(*) FROM hopper_jobs WHERE kind = 'copy'").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1000 {
		t.Errorf("count = %d", count)
	}

	// Inside a transaction, the rows roll back with it.
	tx, err := d.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.UnwrapTx(tx).JobInsertCopy(ctx, insertParams("copytx", 300)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := d.Pool().QueryRow(ctx, "SELECT count(*) FROM hopper_jobs WHERE kind = 'copytx'").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("rolled-back COPY left %d rows", count)
	}
}

func TestJobClaimOrderAndOwnership(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)

	// Insert in an order that differs from claim order.
	params := []driver.JobInsertParams{
		{Kind: "k", Queue: "default", Priority: 3, MaxAttempts: 1, Args: json.RawMessage(`"low"`)},
		{Kind: "k", Queue: "default", Priority: 1, MaxAttempts: 1, Args: json.RawMessage(`"high-1"`)},
		{Kind: "k", Queue: "default", Priority: 2, MaxAttempts: 1, Args: json.RawMessage(`"normal"`)},
		{Kind: "k", Queue: "default", Priority: 1, MaxAttempts: 1, Args: json.RawMessage(`"high-2"`)},
		{Kind: "k", Queue: "other", Priority: 1, MaxAttempts: 1, Args: json.RawMessage(`"other queue"`)},
	}
	if _, err := exec.JobInsertMany(ctx, params); err != nil {
		t.Fatal(err)
	}

	clientID := register(t, ctx, exec)
	jobs, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, j := range jobs {
		got = append(got, string(j.Args))
		if j.State != driver.JobStateRunning || j.Attempt != 1 || j.AttemptedBy != clientID || j.AttemptedAt.IsZero() {
			t.Errorf("claimed job not marked running by %d: %+v", clientID, j)
		}
	}
	if want := []string{`"high-1"`, `"high-2"`, `"normal"`}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("claim order = %v, want %v", got, want)
	}

	// The rest is claimable by another client; the running ones are not.
	other := register(t, ctx, exec)
	jobs, err = exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: other, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || string(jobs[0].Args) != `"low"` {
		t.Errorf("second claim = %d jobs, want only the low priority one", len(jobs))
	}

	var running int
	if err := d.Pool().QueryRow(ctx, "SELECT count(*) FROM hopper_jobs WHERE state = 'running'").Scan(&running); err != nil {
		t.Fatal(err)
	}
	if running != 4 {
		t.Errorf("running = %d, want 4", running)
	}
}

func TestJobClaimSkipsLockedRows(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	if _, err := exec.JobInsertMany(ctx, insertParams("k", 4)); err != nil {
		t.Fatal(err)
	}
	a := register(t, ctx, exec)
	b := register(t, ctx, exec)

	// Client A claims two jobs in a transaction it has not committed.
	tx, err := d.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // cleanup
	held, err := d.UnwrapTx(tx).JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: a, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(held) != 2 {
		t.Fatalf("held %d", len(held))
	}

	// Client B does not block on A's locks; it gets the other two.
	done := make(chan []*driver.JobRow, 1)
	go func() {
		jobs, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: b, Limit: 10})
		if err != nil {
			t.Error(err)
		}
		done <- jobs
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

func TestJobFinalizeManyTransitions(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	params := insertParams("k", 6)
	inserted, err := exec.JobInsertMany(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	clientID := register(t, ctx, exec)
	jobs, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 6 {
		t.Fatalf("claimed %d", len(jobs))
	}
	byID := map[driver.JobID]*driver.JobRow{}
	for _, j := range jobs {
		byID[j.ID] = j
	}
	id := func(i int) driver.JobID { return inserted[i].Job.ID }

	applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{
		{ID: id(0), AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true, Output: json.RawMessage(`{"ok":true}`)},
		{ID: id(1), AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: false},
		{ID: id(2), AttemptedBy: clientID, State: driver.JobStateRetryable, Delay: 30 * time.Second, Error: &driver.AttemptError{Attempt: 1, Error: "boom"}},
		{ID: id(3), AttemptedBy: clientID, State: driver.JobStateScheduled, Delay: time.Minute, Snooze: true},
		{ID: id(4), AttemptedBy: clientID, State: driver.JobStateDiscarded, Archive: false, Error: &driver.AttemptError{Attempt: 1, Error: "fatal", Trace: "stack"}},
		{ID: id(5), AttemptedBy: clientID, State: driver.JobStateCancelled},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 6 {
		t.Fatalf("applied %d, want 6", len(applied))
	}

	var dbNow time.Time
	if err := d.Pool().QueryRow(ctx, "SELECT now()").Scan(&dbNow); err != nil {
		t.Fatal(err)
	}

	// 0: completed and archived, with output.
	j, err := exec.JobGet(ctx, id(0))
	if err != nil {
		t.Fatal(err)
	}
	if j.State != driver.JobStateCompleted || j.FinalizedAt.IsZero() || string(j.Output) != `{"ok": true}` || j.Attempt != 1 {
		t.Errorf("job 0 = %+v", j)
	}
	// 1: completed without archive: gone entirely.
	if _, err := exec.JobGet(ctx, id(1)); err != driver.ErrNotFound { //nolint:errorlint // sentinel
		t.Errorf("job 1: err = %v, want ErrNotFound", err)
	}
	// 2: retryable in 30s with the error recorded, attempt kept, ownership released.
	j, err = exec.JobGet(ctx, id(2))
	if err != nil {
		t.Fatal(err)
	}
	if j.State != driver.JobStateRetryable || j.Attempt != 1 || j.AttemptedBy != 0 || len(j.Errors) != 1 {
		t.Errorf("job 2 = %+v", j)
	} else {
		e := j.Errors[0]
		if e.Error != "boom" || e.Attempt != 1 || e.At.IsZero() || e.At.Sub(dbNow).Abs() > 5*time.Second {
			t.Errorf("job 2 error = %+v (db now %s)", e, dbNow)
		}
		if until := j.ScheduledAt.Sub(dbNow); until < 25*time.Second || until > 35*time.Second {
			t.Errorf("job 2 scheduled in %s, want ~30s", until)
		}
	}
	// 3: snoozed: scheduled in 1m, attempt given back.
	j, err = exec.JobGet(ctx, id(3))
	if err != nil {
		t.Fatal(err)
	}
	if j.State != driver.JobStateScheduled || j.Attempt != 0 || len(j.Errors) != 0 {
		t.Errorf("job 3 = %+v", j)
	}
	// 4: discarded is always archived even with Archive=false, trace kept.
	j, err = exec.JobGet(ctx, id(4))
	if err != nil {
		t.Fatal(err)
	}
	if j.State != driver.JobStateDiscarded || len(j.Errors) != 1 || j.Errors[0].Trace != "stack" {
		t.Errorf("job 4 = %+v", j)
	}
	// 5: cancelled, archived.
	j, err = exec.JobGet(ctx, id(5))
	if err != nil {
		t.Fatal(err)
	}
	if j.State != driver.JobStateCancelled || j.FinalizedAt.IsZero() {
		t.Errorf("job 5 = %+v", j)
	}

	var live, history int
	if err := d.Pool().QueryRow(ctx, "SELECT (SELECT count(*) FROM hopper_jobs), (SELECT count(*) FROM hopper_job_history)").Scan(&live, &history); err != nil {
		t.Fatal(err)
	}
	if live != 2 || history != 3 {
		t.Errorf("live = %d, history = %d; want 2, 3", live, history)
	}
	var completedPartition, failedPartition int
	if err := d.Pool().QueryRow(ctx, "SELECT (SELECT count(*) FROM hopper_job_history_completed), (SELECT count(*) FROM hopper_job_history_failed)").Scan(&completedPartition, &failedPartition); err != nil {
		t.Fatal(err)
	}
	if completedPartition != 1 || failedPartition != 2 {
		t.Errorf("history partitions: completed = %d, failed = %d", completedPartition, failedPartition)
	}

	// The retryable job becomes claimable once its time comes. Move it back.
	if _, err := d.Pool().Exec(ctx, "UPDATE hopper_jobs SET scheduled_at = now() WHERE id = $1", [16]byte(id(2))); err != nil {
		t.Fatal(err)
	}
	jobs, err = exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != id(2) || jobs[0].Attempt != 2 {
		t.Errorf("reclaim = %+v", jobs)
	}
}

func TestJobFinalizeManyFencesStaleOwner(t *testing.T) {
	t.Parallel()
	ctx, _, exec := setup(t)
	if _, err := exec.JobInsertMany(ctx, insertParams("k", 1)); err != nil {
		t.Fatal(err)
	}
	a := register(t, ctx, exec)
	b := register(t, ctx, exec)

	jobs, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: a, Limit: 1})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: %v, %d jobs", err, len(jobs))
	}
	id := jobs[0].ID

	// A result from a client that does not own the job is dropped.
	applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{
		{ID: id, AttemptedBy: b, State: driver.JobStateCompleted, Archive: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 0 {
		t.Fatalf("stale finalize applied: %v", applied)
	}
	j, err := exec.JobGet(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if j.State != driver.JobStateRunning || j.AttemptedBy != a {
		t.Errorf("job after stale finalize = %+v", j)
	}

	// A result for a job that is no longer running is dropped too: finalize
	// once, then try again with the same (now stale) owner.
	if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{
		{ID: id, AttemptedBy: a, State: driver.JobStateRetryable, Delay: time.Hour},
	}}); err != nil {
		t.Fatal(err)
	}
	applied, err = exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{
		{ID: id, AttemptedBy: a, State: driver.JobStateCompleted, Archive: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 0 {
		t.Fatalf("finalize of a non-running job applied: %v", applied)
	}
	j, err = exec.JobGet(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if j.State != driver.JobStateRetryable {
		t.Errorf("job = %+v", j)
	}
}

func TestJobFinalizeManyRejectsBadState(t *testing.T) {
	t.Parallel()
	ctx, _, exec := setup(t)
	_, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{
		{ID: driver.JobID{1}, AttemptedBy: 1, State: driver.JobStateRunning},
	}})
	if err == nil {
		t.Fatal("finalize to running succeeded")
	}
}

func TestClientLease(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)

	id, err := exec.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "h", TTL: time.Second, Info: json.RawMessage(`{"pid":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	ok, err := exec.ClientRenew(ctx, driver.ClientRenewParams{ClientID: id, TTL: time.Hour})
	if err != nil || !ok {
		t.Fatalf("renew = %v, %v", ok, err)
	}

	// Expire it behind the client's back, as a partition would.
	if _, err := d.Pool().Exec(ctx, "UPDATE hopper_clients SET expires_at = now() - interval '1 second' WHERE id = $1", id); err != nil {
		t.Fatal(err)
	}
	ok, err = exec.ClientRenew(ctx, driver.ClientRenewParams{ClientID: id, TTL: time.Hour})
	if err != nil || ok {
		t.Fatalf("renew of expired lease = %v, %v; want false", ok, err)
	}

	if err := exec.ClientDelete(ctx, id); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := d.Pool().QueryRow(ctx, "SELECT count(*) FROM hopper_clients").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("%d client rows after delete", count)
	}
}

func TestJobGetNotFound(t *testing.T) {
	t.Parallel()
	ctx, _, exec := setup(t)
	_, err := exec.JobGet(ctx, driver.JobID{1, 2, 3})
	if err != driver.ErrNotFound { //nolint:errorlint // sentinel
		t.Fatalf("err = %v", err)
	}
}

func TestInsertInTransactionRollsBack(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	tx, err := d.Pool().BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	res, err := d.UnwrapTx(tx).JobInsertMany(ctx, insertParams("tx", 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := exec.JobGet(ctx, res[0].Job.ID); err != driver.ErrNotFound { //nolint:errorlint // sentinel
		t.Fatalf("job survived rollback: %v", err)
	}
}
