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

// callbackKinds returns the kinds of the callbacks in the queue "callbacks".
func callbackKinds(ctx context.Context, t *testing.T, exec driver.Executor) map[string]bool {
	t.Helper()
	jobs, err := exec.JobList(ctx, driver.JobListParams{Queue: "callbacks", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]bool{}
	for _, j := range jobs {
		kinds[j.Kind] = true
	}
	return kinds
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
			want := map[string]bool{"on-complete": true, "on-success": again == driver.JobStateCompleted, "on-failure": again != driver.JobStateCompleted}
			got := callbackKinds(ctx, t, exec)
			for kind, ran := range want {
				if got[kind] != ran {
					t.Errorf("callback %s ran = %v, want %v (callbacks %v)", kind, got[kind], ran, got)
				}
			}
		})
	}
}

// testWorkflowRetriedFailureHeld checks that a retry of a failed workflow
// step held open in a caller's transaction holds up neither the finalize of
// its workflow's last other step nor the leader's settle, and that once it
// commits and the step completes, the workflow's counts come out right.
func testWorkflowRetriedFailureHeld[TTx any](t *testing.T, f Fixture[TTx]) {
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
	if _, err := d.UnwrapTx(tx).JobRetry(ctx, driver.JobRetryParams{ID: a}); err != nil {
		_ = rollback()
		t.Fatal(err)
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	result := withFences(ctx, t, exec, []driver.JobFinalize{{ID: b, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true}})
	applied, err := exec.JobFinalizeMany(fctx, driver.JobFinalizeParams{Jobs: result})
	cancel()
	if err != nil || len(applied) != 1 {
		_ = rollback()
		t.Fatalf("finalize of the last step while a retry of its failed sibling is held open = %v, %v", applied, err)
	}
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	_, err = exec.BatchSettle(sctx, driver.BatchSettleParams{Limit: 10, Results: true})
	cancel()
	if err != nil {
		_ = rollback()
		t.Fatalf("the leader's settle while a retry is held open: %v", err)
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	claimOne(ctx, t, exec, clientID, a)
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: a, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	if _, err := exec.BatchSettle(ctx, driver.BatchSettleParams{Limit: 10}); err != nil {
		t.Fatal(err)
	}
	if batch, err := exec.BatchGet(ctx, wf.ID); err != nil || batch.CompletedAt.IsZero() || batch.Pending != 0 || batch.Failed != 0 {
		t.Fatalf("workflow = %+v, %v; want it completed, with no pending step and no failure", batch, err)
	}
}
