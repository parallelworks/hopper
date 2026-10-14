package drivertest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// testJobRecordProgress records a running job's progress, fenced to its
// attempt, and checks that it survives a retry, archiving, a JobRetry from
// history and a caller's transaction only if it commits.
func testJobRecordProgress[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	id := insert(ctx, t, exec, params("k", 1))[0].Job.ID
	clientID := register(ctx, t, exec)
	if jobs := claim(ctx, t, exec, clientID, 1); len(jobs) != 1 {
		t.Fatal("claim")
	}
	record := func(e driver.Executor, p driver.JobRecordProgressParams) *driver.Progress {
		t.Helper()
		p.ID = id
		if p.AttemptedBy == 0 {
			p.AttemptedBy = clientID
		}
		got, err := e.JobRecordProgress(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	progress := func() *driver.Progress {
		t.Helper()
		j, err := exec.JobGet(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return j.Progress
	}

	if p := progress(); p != nil {
		t.Errorf("progress before any = %+v", p)
	}
	// A cursor is stored as given, a null within it included.
	got := record(exec, driver.JobRecordProgressParams{Attempt: 1, Step: "download", Cursor: json.RawMessage(`{"page": 2, "etag": null}`)})
	if got == nil || got.Step != "download" || string(got.Cursor) != `{"etag": null, "page": 2}` || got.RecordedAt.IsZero() {
		t.Fatalf("recorded = %+v", got)
	}
	if p := progress(); p == nil || p.Step != "download" || string(p.Cursor) != string(got.Cursor) || !p.RecordedAt.Equal(got.RecordedAt) {
		t.Errorf("JobGet progress = %+v, want %+v", p, got)
	}
	// A cursor alone keeps the step, and no cursor clears it.
	if got := record(exec, driver.JobRecordProgressParams{Attempt: 1, KeepStep: true, Step: "ignored", Cursor: json.RawMessage(`3`)}); got == nil || got.Step != "download" || string(got.Cursor) != "3" {
		t.Errorf("cursor kept the step: %+v", got)
	}
	if got := record(exec, driver.JobRecordProgressParams{Attempt: 1, KeepStep: true}); got == nil || got.Step != "download" || got.Cursor != nil {
		t.Errorf("cursor cleared: %+v", got)
	}
	// Fenced: another client, another attempt.
	if got := record(exec, driver.JobRecordProgressParams{AttemptedBy: clientID + 1, Attempt: 1, Step: "x"}); got != nil {
		t.Errorf("another client recorded %+v", got)
	}
	if got := record(exec, driver.JobRecordProgressParams{Attempt: 2, Step: "x"}); got != nil {
		t.Errorf("another attempt recorded %+v", got)
	}

	// In a caller's transaction: kept only if it commits.
	for _, commitTx := range []bool{false, true} {
		tx, commit, rollback := f.Begin(ctx, t, d)
		record(d.UnwrapTx(tx), driver.JobRecordProgressParams{Attempt: 1, Step: "load"})
		end := rollback
		if commitTx {
			end = commit
		}
		if err := end(); err != nil {
			t.Fatal(err)
		}
		want := map[bool]string{false: "download", true: "load"}[commitTx]
		if p := progress(); p == nil || p.Step != want {
			t.Errorf("committed %v: progress %+v, want step %q", commitTx, p, want)
		}
	}

	// A retry keeps it, and the next attempt can record.
	finalize(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: clientID, State: driver.JobStateRetryable, Error: &driver.AttemptError{Attempt: 1, Error: "x"}})
	if got := record(exec, driver.JobRecordProgressParams{Attempt: 1, Step: "x"}); got != nil {
		t.Errorf("recorded on a job that is not running: %+v", got)
	}
	jobs := claim(ctx, t, exec, clientID, 1)
	if len(jobs) != 1 || jobs[0].Progress == nil || jobs[0].Progress.Step != "load" {
		t.Fatalf("claimed after retry: %+v", jobs)
	}
	record(exec, driver.JobRecordProgressParams{Attempt: 2, Step: "notify", Cursor: json.RawMessage(`"c"`)})

	// History keeps it, and a JobRetry brings it back.
	finalize(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: clientID, State: driver.JobStateDiscarded, Archive: true, Error: &driver.AttemptError{Attempt: 2, Error: "x"}})
	if p := progress(); p == nil || p.Step != "notify" || string(p.Cursor) != `"c"` {
		t.Errorf("history progress = %+v", p)
	}
	retried, err := exec.JobRetry(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if p := retried.Progress; p == nil || p.Step != "notify" || string(p.Cursor) != `"c"` {
		t.Errorf("progress after JobRetry = %+v", p)
	}
}

// testJobRecordProgressRacesFinalize holds a progress write open in a
// caller's transaction while a rescue-like finalize of the same attempt
// waits on the row: whichever commits first wins, and the other sees the
// job as it left it. A progress write that commits first is kept by the
// retry; one that comes after the finalize is fenced.
func testJobRecordProgressRacesFinalize[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	inserted := insert(ctx, t, exec, params("k", 2))
	clientID := register(ctx, t, exec)
	if jobs := claim(ctx, t, exec, clientID, 2); len(jobs) != 2 {
		t.Fatal("claim")
	}
	lost := driver.JobFinalize{AttemptedBy: clientID, State: driver.JobStateRetryable, Delay: time.Hour, Error: &driver.AttemptError{Attempt: 1, Error: "hopper: client lost"}}

	// The progress write first: the finalize waits for it, and keeps it.
	id := inserted[0].Job.ID
	tx, commit, rollback := f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // already ended on the happy path
	if p, err := d.UnwrapTx(tx).JobRecordProgress(ctx, driver.JobRecordProgressParams{ID: id, AttemptedBy: clientID, Attempt: 1, Step: "a"}); err != nil || p == nil {
		t.Fatalf("record in tx = %+v, %v", p, err)
	}
	done := make(chan []driver.JobID, 1)
	go func() {
		fin := lost
		fin.ID = id
		applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{fin}})
		if err != nil {
			t.Error(err)
		}
		done <- applied
	}()
	waitForLockWait(ctx, t, f, d)
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	if applied := <-done; len(applied) != 1 {
		t.Fatalf("finalize after the progress write applied %v", applied)
	}
	if j, err := exec.JobGet(ctx, id); err != nil || j.State != driver.JobStateRetryable || j.Progress == nil || j.Progress.Step != "a" {
		t.Errorf("job after both = %+v, %v", j, err)
	}

	// The finalize first: the progress write waits for it, and is fenced.
	id = inserted[1].Job.ID
	tx, commit, rollback = f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // already ended on the happy path
	fin := lost
	fin.ID = id
	if applied := finalize(ctx, t, d.UnwrapTx(tx), fin); len(applied) != 1 {
		t.Fatalf("finalize in tx applied %v", applied)
	}
	recorded := make(chan *driver.Progress, 1)
	go func() {
		p, err := exec.JobRecordProgress(ctx, driver.JobRecordProgressParams{ID: id, AttemptedBy: clientID, Attempt: 1, Step: "b"})
		if err != nil {
			t.Error(err)
		}
		recorded <- p
	}()
	waitForLockWait(ctx, t, f, d)
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	if p := <-recorded; p != nil {
		t.Errorf("progress recorded after the finalize: %+v", p)
	}
	if j, err := exec.JobGet(ctx, id); err != nil || j.Progress != nil {
		t.Errorf("job after both = %+v, %v", j, err)
	}
}
