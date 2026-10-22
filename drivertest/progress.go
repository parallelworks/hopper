package drivertest

import (
	"context"
	"encoding/json"
	"errors"
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
	got := record(exec, driver.JobRecordProgressParams{Fence: 1, Attempt: 1, Step: "download", Cursor: json.RawMessage(`{"page": 2, "etag": null}`)})
	if got == nil || got.Step != "download" || !jsonEqual(got.Cursor, `{"etag": null, "page": 2}`) || got.RecordedAt.IsZero() {
		t.Fatalf("recorded = %+v", got)
	}
	if p := progress(); p == nil || p.Step != "download" || string(p.Cursor) != string(got.Cursor) || !p.RecordedAt.Equal(got.RecordedAt) {
		t.Errorf("JobGet progress = %+v, want %+v", p, got)
	}
	// A cursor alone keeps the step, and no cursor clears it.
	if got := record(exec, driver.JobRecordProgressParams{Fence: 1, Attempt: 1, KeepStep: true, Step: "ignored", Cursor: json.RawMessage(`3`)}); got == nil || got.Step != "download" || string(got.Cursor) != "3" {
		t.Errorf("cursor kept the step: %+v", got)
	}
	if got := record(exec, driver.JobRecordProgressParams{Fence: 1, Attempt: 1, KeepStep: true}); got == nil || got.Step != "download" || got.Cursor != nil {
		t.Errorf("cursor cleared: %+v", got)
	}
	// Fenced: another client, another claim, and another attempt at the same
	// fence, which a client of the previous release can repeat.
	if got := record(exec, driver.JobRecordProgressParams{AttemptedBy: clientID + 1, Fence: 1, Attempt: 1, Step: "x"}); got != nil {
		t.Errorf("another client recorded %+v", got)
	}
	if got := record(exec, driver.JobRecordProgressParams{Fence: 2, Attempt: 1, Step: "x"}); got != nil {
		t.Errorf("another claim recorded %+v", got)
	}
	for _, noWait := range []bool{false, true} {
		if got := record(exec, driver.JobRecordProgressParams{Fence: 1, Attempt: 2, Step: "x", NoWait: noWait}); got != nil {
			t.Errorf("another attempt at the same fence recorded %+v (NoWait %t)", got, noWait)
		}
	}

	// In a caller's transaction: kept only if it commits.
	for _, commitTx := range []bool{false, true} {
		tx, commit, rollback := f.Begin(ctx, t, d)
		record(d.UnwrapTx(tx), driver.JobRecordProgressParams{Fence: 1, Attempt: 1, Step: "load"})
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
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: clientID, State: driver.JobStateRetryable, Error: &driver.AttemptError{Attempt: 1, Error: "x"}})
	if got := record(exec, driver.JobRecordProgressParams{Fence: 1, Attempt: 1, Step: "x"}); got != nil {
		t.Errorf("recorded on a job that is not running: %+v", got)
	}
	jobs := claim(ctx, t, exec, clientID, 1)
	if len(jobs) != 1 || jobs[0].Progress == nil || jobs[0].Progress.Step != "load" {
		t.Fatalf("claimed after retry: %+v", jobs)
	}
	record(exec, driver.JobRecordProgressParams{Fence: 2, Attempt: 2, Step: "notify", Cursor: json.RawMessage(`"c"`)})

	// History keeps it, and a JobRetry brings it back.
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: clientID, State: driver.JobStateDiscarded, Archive: true, Error: &driver.AttemptError{Attempt: 2, Error: "x"}})
	if p := progress(); p == nil || p.Step != "notify" || string(p.Cursor) != `"c"` {
		t.Errorf("history progress = %+v", p)
	}
	retried, err := exec.JobRetry(ctx, driver.JobRetryParams{ID: id})
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
	lost := driver.JobFinalize{AttemptedBy: clientID, Fence: 1, State: driver.JobStateRetryable, Delay: time.Hour, Error: &driver.AttemptError{Attempt: 1, Error: "hopper: client lost"}}

	// The progress write first: the finalize waits for it, and keeps it.
	id := inserted[0].Job.ID
	tx, commit, rollback := f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // already ended on the happy path
	if p, err := d.UnwrapTx(tx).JobRecordProgress(ctx, driver.JobRecordProgressParams{ID: id, AttemptedBy: clientID, Fence: 1, Attempt: 1, Step: "a"}); err != nil || p == nil {
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
	f.WaitForLockWait(ctx, t, d)
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
	if applied := finalizeCurrent(ctx, t, d.UnwrapTx(tx), fin); len(applied) != 1 {
		t.Fatalf("finalize in tx applied %v", applied)
	}
	recorded := make(chan *driver.Progress, 1)
	go func() {
		p, err := exec.JobRecordProgress(ctx, driver.JobRecordProgressParams{ID: id, AttemptedBy: clientID, Fence: 1, Attempt: 1, Step: "b"})
		if err != nil {
			t.Error(err)
		}
		recorded <- p
	}()
	f.WaitForLockWait(ctx, t, d)
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

// testJobProgressReset checks what clears progress and what a hand-written
// value reads as, and that NoWait does not wait for a held row.
func testJobProgressReset[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := register(ctx, t, exec)
	p := params("k", 2)
	p[0].UniqueKey = "u"
	inserted := insert(ctx, t, exec, p)
	if jobs := claim(ctx, t, exec, clientID, 2); len(jobs) != 2 {
		t.Fatal("claim")
	}
	for _, r := range inserted {
		if got, err := exec.JobRecordProgress(ctx, driver.JobRecordProgressParams{ID: r.Job.ID, AttemptedBy: clientID, Fence: 1, Attempt: 1, Step: "a"}); err != nil || got == nil {
			t.Fatalf("record = %+v, %v", got, err)
		}
	}
	get := func(id driver.JobID) *driver.JobRow {
		t.Helper()
		j, err := exec.JobGet(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return j
	}

	// NoWait fails at once on a row another transaction holds.
	tx, _, rollback := f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // already ended on the happy path
	if got, err := d.UnwrapTx(tx).JobRecordProgress(ctx, driver.JobRecordProgressParams{ID: inserted[1].Job.ID, AttemptedBy: clientID, Fence: 1, Attempt: 1, Step: "b"}); err != nil || got == nil {
		t.Fatalf("record in tx = %+v, %v", got, err)
	}
	start := time.Now()
	if _, err := exec.JobRecordProgress(ctx, driver.JobRecordProgressParams{ID: inserted[1].Job.ID, AttemptedBy: clientID, Fence: 1, Attempt: 1, Step: "c", NoWait: true}); !errors.Is(err, driver.ErrJobLocked) || time.Since(start) > 5*time.Second {
		t.Errorf("NoWait on a held row = %v after %s, want ErrJobLocked at once", err, time.Since(start))
	}
	if err := rollback(); err != nil {
		t.Fatal(err)
	}
	if got, err := exec.JobRecordProgress(ctx, driver.JobRecordProgressParams{ID: inserted[1].Job.ID, AttemptedBy: clientID, Fence: 1, Attempt: 1, Step: "c", NoWait: true}); err != nil || got == nil || got.Step != "c" {
		t.Errorf("NoWait on a free row = %+v, %v", got, err)
	}

	// A replacing insert gives the waiting job new args, and clears its
	// progress; a running job is not replaced and keeps it.
	rep, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{{Kind: "k", Queue: "default", Priority: 2, MaxAttempts: 3, UniqueKey: "u"}}, driver.JobInsertOpts{OnConflict: driver.ConflictReplace})
	if err != nil || rep[0].Job.Progress == nil {
		t.Fatalf("replace of a running job = %+v, %v", rep, err)
	}
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: inserted[0].Job.ID, AttemptedBy: clientID, State: driver.JobStateRetryable, Delay: time.Hour, Error: &driver.AttemptError{Attempt: 1, Error: "x"}})
	rep, err = exec.JobInsertMany(ctx, []driver.JobInsertParams{{Kind: "k", Queue: "default", Priority: 2, MaxAttempts: 3, UniqueKey: "u", Args: json.RawMessage(`{"i": 9}`)}}, driver.JobInsertOpts{OnConflict: driver.ConflictReplace})
	if err != nil || rep[0].Job.Progress != nil || argsN(rep[0].Job) != 9 {
		t.Errorf("replace of a waiting job = %+v, %v", rep[0].Job, err)
	}

	// JobRetry keeps progress; FromStart clears it, live and from history.
	if _, err := exec.JobRecordProgress(ctx, driver.JobRecordProgressParams{ID: inserted[1].Job.ID, AttemptedBy: clientID, Fence: 1, Attempt: 1, Step: "d"}); err != nil {
		t.Fatal(err)
	}
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: inserted[1].Job.ID, AttemptedBy: clientID, State: driver.JobStateDiscarded, Archive: true, Error: &driver.AttemptError{Attempt: 1, Error: "x"}})
	if j, err := exec.JobRetry(ctx, driver.JobRetryParams{ID: inserted[1].Job.ID, FromStart: true}); err != nil || j.Progress != nil {
		t.Errorf("JobRetry FromStart from history = %+v, %v", j, err)
	}
	f.SetProgress(ctx, t, d, inserted[0].Job.ID, `{"step": "e"}`)
	if j, err := exec.JobRetry(ctx, driver.JobRetryParams{ID: inserted[0].Job.ID}); err != nil || j.Progress == nil || j.Progress.Step != "e" {
		t.Errorf("JobRetry of a waiting job = %+v, %v", j, err)
	}
	if j, err := exec.JobRetry(ctx, driver.JobRetryParams{ID: inserted[0].Job.ID, FromStart: true}); err != nil || j.Progress != nil {
		t.Errorf("JobRetry FromStart of a waiting job = %+v, %v", j, err)
	}

	// Values written by hand read as what can be made of them, and never
	// fail the read.
	for value, want := range map[string]*driver.Progress{
		`"fetch"`: nil,
		`null`:    nil,
		`[1]`:     nil,
		`{"step": 3, "cursor": null, "at": "yesterday"}`:                  {},
		`{"step": "s", "cursor": {"p": 1}, "at": "2026-01-02T03:04:05Z"}`: {Step: "s", Cursor: json.RawMessage(`{"p": 1}`), RecordedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)},
	} {
		f.SetProgress(ctx, t, d, inserted[0].Job.ID, value)
		got := get(inserted[0].Job.ID).Progress
		if (got == nil) != (want == nil) || (got != nil && (got.Step != want.Step || (got.Cursor != nil) != (want.Cursor != nil) ||
			(got.Cursor != nil && !jsonEqual(got.Cursor, string(want.Cursor))) || !got.RecordedAt.Equal(want.RecordedAt))) {
			t.Errorf("progress %s reads as %+v, want %+v", value, got, want)
		}
	}
	if jobs := claim(ctx, t, exec, clientID, 2); len(jobs) != 2 {
		t.Errorf("claim with a hand-written progress = %d jobs, want 2", len(jobs))
	}
}
