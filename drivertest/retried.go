package drivertest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// retriedWorkflow inserts a workflow of two independent steps with all
// three callbacks, in the queue "callbacks", and returns it.
func retriedWorkflow(ctx context.Context, t *testing.T, exec driver.Executor) *driver.WorkflowInsertResult {
	t.Helper()
	cb := func(kind string) *driver.JobInsertParams {
		return &driver.JobInsertParams{Kind: kind, Queue: "callbacks", Priority: 2, MaxAttempts: 3, Args: json.RawMessage(`{}`)}
	}
	wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
		Name: "retried", Jobs: params("step", 2),
		OnSuccess: cb("on-success"), OnFailure: cb("on-failure"), OnComplete: cb("on-complete"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return wf
}

// checkCallbacks checks that the callbacks in the queue "callbacks" are
// on-complete and, of on-success and on-failure, the one failed calls
// for, each inserted once.
func checkCallbacks(ctx context.Context, t *testing.T, exec driver.Executor, failed bool) {
	t.Helper()
	jobs, err := exec.JobList(ctx, driver.JobListParams{Queue: "callbacks", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, j := range jobs {
		got[j.Kind]++
	}
	want := map[string]int{"on-complete": 1, "on-success": 1, "on-failure": 0}
	if failed {
		want["on-success"], want["on-failure"] = 0, 1
	}
	for kind, n := range want {
		if got[kind] != n {
			t.Errorf("callback %s inserted %d times, want %d (callbacks %v)", kind, got[kind], n, got)
		}
	}
}

// testWorkflowRetriedFailure checks that a failed workflow step retried
// from history no longer counts as a failure of its workflow: a workflow
// whose retried step completes runs OnSuccess and OnComplete, not
// OnFailure, and one whose retried step fails again counts it once.
func testWorkflowRetriedFailure[TTx any](t *testing.T, f Fixture[TTx]) {
	for _, again := range []driver.JobState{driver.JobStateCompleted, driver.JobStateDiscarded} {
		t.Run(string(again), func(t *testing.T) {
			ctx := context.Background()
			d := f.NewDriver(t)
			exec := d.Executor()
			clientID := register(ctx, t, exec)
			wf := retriedWorkflow(ctx, t, exec)
			a, b := wf.Jobs[0].Job.ID, wf.Jobs[1].Job.ID
			if jobs := claim(ctx, t, exec, clientID, 2); len(jobs) != 2 {
				t.Fatalf("claimed %d steps, want both", len(jobs))
			}
			finalizeCurrent(ctx, t, exec, driver.JobFinalize{
				ID: a, AttemptedBy: clientID, State: driver.JobStateDiscarded, Archive: true,
				Error: &driver.AttemptError{Attempt: 1, Error: "boom"},
			})
			if _, err := exec.JobRetry(ctx, driver.JobRetryParams{ID: a}); err != nil {
				t.Fatal(err)
			}
			finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: b, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
			if batch, err := exec.BatchGet(ctx, wf.ID); err != nil || !batch.CompletedAt.IsZero() || batch.Failed != 0 {
				t.Fatalf("workflow = %+v, %v; want it open, with no failure, while the retried step runs", batch, err)
			}
			claimOne(ctx, t, exec, clientID, a)
			fin := driver.JobFinalize{ID: a, AttemptedBy: clientID, State: again, Archive: true}
			if again != driver.JobStateCompleted {
				fin.Error = &driver.AttemptError{Attempt: 2, Error: "boom again"}
			}
			finalizeCurrent(ctx, t, exec, fin)
			if _, err := exec.BatchSettle(ctx, driver.BatchSettleParams{Limit: 10}); err != nil {
				t.Fatal(err)
			}
			batch, err := exec.BatchGet(ctx, wf.ID)
			wantFailed := 0
			if again != driver.JobStateCompleted {
				wantFailed = 1
			}
			if err != nil || batch.CompletedAt.IsZero() || batch.Pending != 0 || batch.Failed != wantFailed {
				t.Fatalf("workflow = %+v, %v; want it completed with %d failed", batch, err, wantFailed)
			}
			checkCallbacks(ctx, t, exec, again != driver.JobStateCompleted)
		})
	}
}

// testWorkflowRetriedFailureHeld checks that a retry of a failed workflow
// step held open in a caller's transaction holds up neither the finalize of
// its workflow's last other step nor the leader's settle. The finalize,
// which decides from its snapshot, completes the workflow without the step
// and with its failure, and runs OnFailure. Once the retry commits and the
// step finishes again, completed or failed, the workflow keeps that
// failure, as one whose retry found it completed does, rather than report
// none, or two, after its OnFailure has run.
func testWorkflowRetriedFailureHeld[TTx any](t *testing.T, f Fixture[TTx]) {
	for _, again := range []driver.JobState{driver.JobStateCompleted, driver.JobStateDiscarded} {
		t.Run(string(again), func(t *testing.T) {
			ctx := context.Background()
			d := f.NewDriver(t)
			if singleWriter(d) {
				t.Skip("on an engine that runs one writer at a time, every write waits for an open transaction")
			}
			exec := d.Executor()
			clientID := register(ctx, t, exec)
			wf := retriedWorkflow(ctx, t, exec)
			a, b := wf.Jobs[0].Job.ID, wf.Jobs[1].Job.ID
			if jobs := claim(ctx, t, exec, clientID, 2); len(jobs) != 2 {
				t.Fatalf("claimed %d steps, want both", len(jobs))
			}
			finalizeCurrent(ctx, t, exec, driver.JobFinalize{
				ID: a, AttemptedBy: clientID, State: driver.JobStateDiscarded, Archive: true,
				Error: &driver.AttemptError{Attempt: 1, Error: "boom"},
			})

			tx, commit, rollback := f.Begin(ctx, t, d)
			// A helper that fails the test while the transaction is open must
			// not leave it open, for the driver's cleanup would wait for it.
			t.Cleanup(func() { _ = rollback() })
			if _, err := d.UnwrapTx(tx).JobRetry(ctx, driver.JobRetryParams{ID: a}); err != nil {
				t.Fatal(err)
			}
			fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			result := withFences(ctx, t, exec, []driver.JobFinalize{{ID: b, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true}})
			applied, err := exec.JobFinalizeMany(fctx, driver.JobFinalizeParams{Jobs: result})
			cancel()
			if err != nil || len(applied) != 1 {
				t.Fatalf("finalize of the last step while a retry of its failed sibling is held open = %v, %v", applied, err)
			}
			sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, err = exec.BatchSettle(sctx, driver.BatchSettleParams{Limit: 10, Results: true})
			cancel()
			if err != nil {
				t.Fatalf("the leader's settle while a retry is held open: %v", err)
			}
			if batch, err := exec.BatchGet(ctx, wf.ID); err != nil || batch.CompletedAt.IsZero() || batch.Failed != 1 {
				t.Fatalf("workflow = %+v, %v; want it completed by its last other step, with the held step's failure", batch, err)
			}
			if err := commit(); err != nil {
				t.Fatal(err)
			}
			if batch, err := exec.BatchGet(ctx, wf.ID); err != nil || batch.Pending != 0 || batch.Failed != 1 {
				t.Fatalf("workflow = %+v, %v; want the retry, which found it completed, to leave its counts", batch, err)
			}
			finishAgain(ctx, t, exec, clientID, a, again)
			if batch, err := exec.BatchGet(ctx, wf.ID); err != nil || batch.CompletedAt.IsZero() || batch.Pending != 0 || batch.Failed != 1 {
				t.Fatalf("workflow = %+v, %v; want it completed with the failure its callbacks were given", batch, err)
			}
			checkCallbacks(ctx, t, exec, true)
		})
	}
}

// testWorkflowRetriedAfterCompletion checks that a failed step retried
// from history after its workflow completed changes none of the
// workflow's counts, then or when the step finishes again, completed or
// failed: they stay those its callbacks were given.
func testWorkflowRetriedAfterCompletion[TTx any](t *testing.T, f Fixture[TTx]) {
	for _, again := range []driver.JobState{driver.JobStateCompleted, driver.JobStateDiscarded} {
		t.Run(string(again), func(t *testing.T) {
			ctx := context.Background()
			d := f.NewDriver(t)
			exec := d.Executor()
			clientID := register(ctx, t, exec)
			wf := retriedWorkflow(ctx, t, exec)
			a, b := wf.Jobs[0].Job.ID, wf.Jobs[1].Job.ID
			if jobs := claim(ctx, t, exec, clientID, 2); len(jobs) != 2 {
				t.Fatalf("claimed %d steps, want both", len(jobs))
			}
			finalizeCurrent(ctx, t, exec, driver.JobFinalize{
				ID: a, AttemptedBy: clientID, State: driver.JobStateDiscarded, Archive: true,
				Error: &driver.AttemptError{Attempt: 1, Error: "boom"},
			})
			finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: b, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
			if _, err := exec.BatchSettle(ctx, driver.BatchSettleParams{Limit: 10}); err != nil {
				t.Fatal(err)
			}
			if batch, err := exec.BatchGet(ctx, wf.ID); err != nil || batch.CompletedAt.IsZero() || batch.Failed != 1 {
				t.Fatalf("workflow = %+v, %v; want it completed with one failure", batch, err)
			}
			if _, err := exec.JobRetry(ctx, driver.JobRetryParams{ID: a}); err != nil {
				t.Fatal(err)
			}
			finishAgain(ctx, t, exec, clientID, a, again)
			if batch, err := exec.BatchGet(ctx, wf.ID); err != nil || batch.Pending != 0 || batch.Failed != 1 {
				t.Fatalf("workflow = %+v, %v; want the failure its callbacks were given", batch, err)
			}
			checkCallbacks(ctx, t, exec, true)
		})
	}
}

// finishAgain claims a retried step and finalizes it as again, failed with
// an error unless it completed, then settles its workflow as the leader
// would.
func finishAgain(ctx context.Context, t *testing.T, exec driver.Executor, clientID int64, id driver.JobID, again driver.JobState) {
	t.Helper()
	claimOne(ctx, t, exec, clientID, id)
	fin := driver.JobFinalize{ID: id, AttemptedBy: clientID, State: again, Archive: true}
	if again != driver.JobStateCompleted {
		fin.Error = &driver.AttemptError{Attempt: 2, Error: "boom again"}
	}
	finalizeCurrent(ctx, t, exec, fin)
	if _, err := exec.BatchSettle(ctx, driver.BatchSettleParams{Limit: 10}); err != nil {
		t.Fatal(err)
	}
}
