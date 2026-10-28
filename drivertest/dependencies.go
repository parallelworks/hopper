package drivertest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// wantDependency is what a claim should report of a dependency.
type wantDependency struct {
	step   string
	state  driver.JobState
	output string
}

// checkDependencies checks a claimed job's dependencies against want, by
// step name, and that they are in ID order.
func checkDependencies(t *testing.T, job *driver.JobRow, want ...wantDependency) {
	t.Helper()
	if len(job.Dependencies) != len(want) {
		t.Errorf("job %s has %d dependencies %+v, want %d", job.ID, len(job.Dependencies), job.Dependencies, len(want))
		return
	}
	if !slices.IsSortedFunc(job.Dependencies, func(a, b driver.Dependency) int { return bytes.Compare(a.ID[:], b.ID[:]) }) {
		t.Errorf("job %s's dependencies are not in ID order: %+v", job.ID, job.Dependencies)
	}
	for _, w := range want {
		got, ok := job.Dependency(w.step)
		switch {
		case !ok:
			t.Errorf("job %s has no dependency %q: %+v", job.ID, w.step, job.Dependencies)
		case got.State != w.state:
			t.Errorf("job %s's dependency %q is %q, want %q", job.ID, w.step, got.State, w.state)
		case w.output == "" && got.Output != nil:
			t.Errorf("job %s's dependency %q has output %s, want none", job.ID, w.step, got.Output)
		case w.output != "" && !jsonEqual(got.Output, w.output):
			t.Errorf("job %s's dependency %q has output %s, want %s", job.ID, w.step, got.Output, w.output)
		}
	}
}

// testDependencyResults claims workflow steps and checks that each carries
// its finished dependencies' names, states and outputs: through history's
// retention and DeleteCompleted, a snooze and a retry, and a claim by ID,
// until the workflow completes, which deletes them.
func testDependencyResults[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := register(ctx, t, exec)

	// build, lint and flaky run first. report waits for all three behind
	// ignore edges, notify for build alone, and the last, unnamed step for
	// report, which it does not read, so report's result is not recorded.
	wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
		Name: "deps", Jobs: params("step", 6),
		Steps: []string{"build", "lint", "flaky", "report", "notify", ""},
		Deps: []driver.JobDependency{
			{Job: 3, DependsOn: 0, OnFailure: driver.DependencyIgnore, Reads: true},
			{Job: 3, DependsOn: 1, OnFailure: driver.DependencyIgnore, Reads: true},
			{Job: 3, DependsOn: 2, OnFailure: driver.DependencyIgnore, Reads: true},
			{Job: 4, DependsOn: 0, Reads: true},
			{Job: 5, DependsOn: 3},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := func(i int) driver.JobID { return wf.Jobs[i].Job.ID }
	plain := insert(ctx, t, exec, params("plain", 1))[0].Job.ID
	first := claim(ctx, t, exec, clientID, 10)
	if len(first) != 4 {
		t.Fatalf("claimed %d jobs, want build, lint, flaky and the plain job", len(first))
	}
	for _, j := range first {
		if j.Dependencies != nil {
			t.Errorf("job %s with no dependencies claimed with %+v", j.ID, j.Dependencies)
		}
	}
	finalizeCurrent(ctx, t, exec,
		driver.JobFinalize{ID: id(0), AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true, Output: json.RawMessage(`{"image":"img:1"}`)},
		driver.JobFinalize{ID: id(1), AttemptedBy: clientID, State: driver.JobStateCompleted, Output: json.RawMessage(`{"warnings":2}`)},
		driver.JobFinalize{
			ID: id(2), AttemptedBy: clientID, State: driver.JobStateDiscarded, Archive: true, Output: json.RawMessage(`{"exit":3}`),
			Error: &driver.AttemptError{Attempt: 1, Error: "boom"},
		},
		driver.JobFinalize{ID: plain, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true, Output: json.RawMessage(`{"plain":true}`)},
	)
	if n := f.CountJobResults(ctx, t, d); n != 3 {
		t.Errorf("%d dependency results after the first steps, want build's, lint's and flaky's", n)
	}

	// History's retention takes build's row; lint's queue keeps none.
	if _, err := exec.JobDelete(ctx, id(0)); err != nil {
		t.Fatal(err)
	}
	report := []wantDependency{
		{"build", driver.JobStateCompleted, `{"image":"img:1"}`},
		{"lint", driver.JobStateCompleted, `{"warnings":2}`},
		{"flaky", driver.JobStateDiscarded, `{"exit":3}`},
	}
	notify := claimByID(ctx, t, exec, id(4))
	checkDependencies(t, notify.Job, wantDependency{"build", driver.JobStateCompleted, `{"image":"img:1"}`})
	j := claimOne(ctx, t, exec, clientID, id(3))
	checkDependencies(t, j, report...)

	// A snooze and a retry claim it again with the same dependencies.
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: id(3), AttemptedBy: clientID, State: driver.JobStateScheduled, Snooze: true, Delay: time.Hour})
	f.MakeClaimable(ctx, t, d, id(3))
	checkDependencies(t, claimOne(ctx, t, exec, clientID, id(3)), report...)
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{
		ID: id(3), AttemptedBy: clientID, State: driver.JobStateRetryable, Delay: time.Hour,
		Error: &driver.AttemptError{Attempt: 2, Error: "retry"},
	})
	f.MakeClaimable(ctx, t, d, id(3))
	checkDependencies(t, claimOne(ctx, t, exec, clientID, id(3)), report...)

	finalizeCurrent(ctx, t, exec, driver.JobFinalize{
		ID: id(3), AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true,
		Output: json.RawMessage(`{"pages":7}`),
	})
	done := leaseResult(notify, driver.JobStateCompleted)
	done.Output = json.RawMessage(`{"sent":true}`)
	if applied := finalize(ctx, t, exec, done); len(applied) != 1 {
		t.Fatalf("notify's result applied %v", applied)
	}
	if last := claimOne(ctx, t, exec, clientID, id(5)); last.Dependencies != nil {
		t.Errorf("a step that does not read its dependencies claimed with %+v", last.Dependencies)
	}
	if n := f.CountJobResults(ctx, t, d); n != 3 {
		t.Errorf("%d dependency results before the workflow completes, want build's, lint's and flaky's", n)
	}
	w, err := exec.WorkflowGet(ctx, wf.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := map[driver.JobID]string{id(0): "build", id(1): "lint", id(2): "flaky", id(3): "report", id(4): "notify"}
	if len(w.Steps) != len(want) {
		t.Errorf("workflow steps = %v, want %v", w.Steps, want)
	}
	for k, v := range want {
		if w.Steps[k] != v {
			t.Errorf("step %s is named %q, want %q", k, w.Steps[k], v)
		}
	}

	finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: id(5), AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	if b, err := exec.BatchGet(ctx, wf.ID); err != nil || b.CompletedAt.IsZero() {
		t.Fatalf("workflow = %+v, %v; want it completed", b, err)
	}
	if n := f.CountJobResults(ctx, t, d); n != 0 {
		t.Errorf("%d dependency results after the workflow completed, want none", n)
	}
	if j := get(ctx, t, exec, plain); j.Dependencies != nil {
		t.Errorf("a read other than a claim returned dependencies %+v", j.Dependencies)
	}
}

// testDependencyResultsFailed checks the states of dependencies that did
// not complete: one canceled while it waited, one canceled because its own
// dependency was, and one that finished before the engine recorded
// results, which reads with no state.
func testDependencyResultsFailed[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := register(ctx, t, exec)

	// q is canceled with p; r and s run behind ignore edges.
	wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
		Name: "failed", Jobs: params("step", 4), Steps: []string{"p", "q", "r", "s"},
		Deps: []driver.JobDependency{
			{Job: 1, DependsOn: 0, Reads: true},
			{Job: 2, DependsOn: 1, OnFailure: driver.DependencyIgnore, Reads: true},
			{Job: 3, DependsOn: 0, OnFailure: driver.DependencyIgnore, Reads: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := func(i int) driver.JobID { return wf.Jobs[i].Job.ID }
	if _, err := exec.JobCancel(ctx, id(0)); err != nil {
		t.Fatal(err)
	}
	if j := get(ctx, t, exec, id(1)); j.State != driver.JobStateCanceled {
		t.Fatalf("q = %s, want canceled with p", j.State)
	}
	claimed := claim(ctx, t, exec, clientID, 10)
	if len(claimed) != 2 {
		t.Fatalf("claimed %d jobs, want r and s", len(claimed))
	}
	for _, j := range claimed {
		switch j.ID {
		case id(2):
			checkDependencies(t, j, wantDependency{"q", driver.JobStateCanceled, ""})
		case id(3):
			checkDependencies(t, j, wantDependency{"p", driver.JobStateCanceled, ""})
		default:
			t.Errorf("claimed %s", j.ID)
		}
	}

	chain, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
		Name: "before", Jobs: params("chain", 2), Steps: []string{"a", "b"}, Deps: []driver.JobDependency{{Job: 1, DependsOn: 0, Reads: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	a := claimOne(ctx, t, exec, clientID, chain.Jobs[0].Job.ID)
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{
		ID: a.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true,
		Output: json.RawMessage(`1`),
	})
	f.DeleteJobResult(ctx, t, d, a.ID)
	b := claimOne(ctx, t, exec, clientID, chain.Jobs[1].Job.ID)
	checkDependencies(t, b, wantDependency{"a", "", ""})
}

// testDependencyResultsConcurrent finalizes two dependencies of a step in
// concurrent transactions, which each miss the other's: the step is
// promoted once both have committed, and its claim carries both results.
func testDependencyResultsConcurrent[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := register(ctx, t, exec)
	wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
		Name: "fan-in", Jobs: params("step", 3), Steps: []string{"x", "y", "z"},
		Deps: []driver.JobDependency{{Job: 2, DependsOn: 0, Reads: true}, {Job: 2, DependsOn: 1, OnFailure: driver.DependencyIgnore, Reads: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	x, y, z := wf.Jobs[0].Job.ID, wf.Jobs[1].Job.ID, wf.Jobs[2].Job.ID
	if jobs := claim(ctx, t, exec, clientID, 2); len(jobs) != 2 {
		t.Fatalf("claimed %d", len(jobs))
	}
	xResult := driver.JobFinalize{ID: x, AttemptedBy: clientID, Fence: 1, State: driver.JobStateCompleted, Output: json.RawMessage(`"x"`)}
	yResult := driver.JobFinalize{
		ID: y, AttemptedBy: clientID, Fence: 1, State: driver.JobStateDiscarded, Archive: true,
		Output: json.RawMessage(`"y"`), Error: &driver.AttemptError{Attempt: 1, Error: "boom"},
	}

	tx, commit, rollback := f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // already committed on the happy path
	if applied := finalize(ctx, t, d.UnwrapTx(tx), xResult); len(applied) != 1 {
		t.Fatalf("finalize in tx applied %v", applied)
	}
	if singleWriter(d) {
		done := make(chan []driver.JobID, 1)
		go func() {
			applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{yResult}})
			if err != nil {
				t.Error(err)
			}
			done <- appliedIDs(applied)
		}()
		f.WaitForLockWait(ctx, t, d)
		if err := commit(); err != nil {
			t.Fatal(err)
		}
		if applied := <-done; len(applied) != 1 {
			t.Fatalf("finalize after the transaction applied %v", applied)
		}
	} else {
		fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if applied, err := exec.JobFinalizeMany(fctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{yResult}}); err != nil || len(applied) != 1 {
			t.Fatalf("concurrent finalize = %v, %v", applied, err)
		}
		if err := commit(); err != nil {
			t.Fatal(err)
		}
		if _, err := exec.WorkflowPromoteReady(ctx, driver.WorkflowPromoteParams{DependsOn: []driver.JobID{x, y}}); err != nil {
			t.Fatal(err)
		}
	}
	checkDependencies(t, claimOne(ctx, t, exec, clientID, z),
		wantDependency{"x", driver.JobStateCompleted, `"x"`}, wantDependency{"y", driver.JobStateDiscarded, `"y"`})
}

// testDependencyResultsLeaseExpiry races a leased dependency's result
// against the expiry of its lease, on its last attempt: whichever applies,
// its dependent's claim carries the state it applied, with the output only
// of a result that applied.
func testDependencyResultsLeaseExpiry[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	// One client claims and expires, started before any lease's renewal, so
	// that no lease is given a grace for its start.
	clientID := expirer(ctx, t, f, d)
	me := clientID
	workflow := func() (dep, dependent driver.JobID) {
		t.Helper()
		jobs := params("step", 2)
		jobs[0].MaxAttempts = 1
		wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
			Name: "leased", Jobs: jobs, Steps: []string{"remote", "after"},
			Deps: []driver.JobDependency{{Job: 1, DependsOn: 0, OnFailure: driver.DependencyIgnore, Reads: true}},
		})
		if err != nil {
			t.Fatal(err)
		}
		return wf.Jobs[0].Job.ID, wf.Jobs[1].Job.ID
	}

	// The result's transaction holds the job while the expiry runs.
	dep, dependent := workflow()
	l := claimByID(ctx, t, exec, dep)
	f.ExpireJobLease(ctx, t, d, dep)
	result := leaseResult(l, driver.JobStateCompleted)
	result.Output = json.RawMessage(`{"remote":1}`)
	tx, commit, rollback := f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // already committed on the happy path
	if applied := finalize(ctx, t, d.UnwrapTx(tx), result); len(applied) != 1 {
		t.Fatal("finalize in a transaction did not apply")
	}
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	jobs, err := exec.JobExpireLeases(sctx, driver.JobExpireLeasesParams{Limit: 10, ClientID: me})
	cancel()
	if err != nil || len(jobs) != 0 {
		t.Errorf("expire of a job being finalized = %d jobs, %v; want none", len(jobs), err)
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	checkDependencies(t, claimOne(ctx, t, exec, clientID, dependent), wantDependency{"remote", driver.JobStateCompleted, `{"remote":1}`})

	// The expiry discards the job first, and the result is fenced.
	dep, dependent = workflow()
	l = claimByID(ctx, t, exec, dep)
	f.ExpireJobLease(ctx, t, d, dep)
	if jobs, err := exec.JobExpireLeases(ctx, driver.JobExpireLeasesParams{Limit: 10, ClientID: me}); err != nil || len(jobs) != 1 {
		t.Fatalf("expire = %d, %v", len(jobs), err)
	}
	result = leaseResult(l, driver.JobStateCompleted)
	result.Output = json.RawMessage(`{"remote":2}`)
	if applied := finalize(ctx, t, exec, result); len(applied) != 0 {
		t.Error("a result after the expiry applied")
	}
	if j := get(ctx, t, exec, dep); j.State != driver.JobStateDiscarded {
		t.Errorf("dependency after the expiry = %s, want discarded", j.State)
	}
	checkDependencies(t, claimOne(ctx, t, exec, clientID, dependent), wantDependency{"remote", driver.JobStateDiscarded, ""})
}

// testDependencyResultsOrphans checks that the leader's settle, when asked
// for results, deletes those of a workflow that no longer exists and keeps
// those of one in flight.
func testDependencyResultsOrphans[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := register(ctx, t, exec)

	wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
		Name: "orphans", Jobs: params("step", 2), Steps: []string{"build", "report"},
		Deps: []driver.JobDependency{{Job: 1, DependsOn: 0, Reads: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	claimOne(ctx, t, exec, clientID, wf.Jobs[0].Job.ID)
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: wf.Jobs[0].Job.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	f.OrphanJobResult(ctx, t, d, wf.Jobs[0].Job.ID)
	if n := f.CountJobResults(ctx, t, d); n != 2 {
		t.Fatalf("%d dependency results, want the workflow's and an orphan", n)
	}
	if _, err := exec.BatchSettle(ctx, driver.BatchSettleParams{Limit: 10}); err != nil {
		t.Fatal(err)
	}
	if n := f.CountJobResults(ctx, t, d); n != 2 {
		t.Errorf("%d dependency results after a settle not asked for them, want 2", n)
	}
	if _, err := exec.BatchSettle(ctx, driver.BatchSettleParams{Limit: 10, Results: true}); err != nil {
		t.Fatal(err)
	}
	if n := f.CountJobResults(ctx, t, d); n != 1 {
		t.Errorf("%d dependency results after the sweep, want the workflow's alone", n)
	}
	checkDependencies(t, claimOne(ctx, t, exec, clientID, wf.Jobs[1].Job.ID), wantDependency{"build", driver.JobStateCompleted, ""})
}

// testDependencyResultsSweepConcurrent runs the leader's sweep of orphaned
// results without pause while workflows are worked: it deletes no result of
// a workflow in flight, so every dependent is claimed with both of its
// dependencies' states and outputs, and once the workflows complete no
// result is left.
func testDependencyResultsSweepConcurrent[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := register(ctx, t, exec)

	const workflows = 20
	type step struct {
		w int
		s string
	}
	output := func(s step) string { return fmt.Sprintf(`{"w":%d,"s":%q}`, s.w, s.s) }
	steps := map[driver.JobID]step{}
	for w := range workflows {
		wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
			Name: "swept", Jobs: params("step", 3), Steps: []string{"a", "c", "b"},
			Deps: []driver.JobDependency{
				{Job: 2, DependsOn: 0, Reads: true},
				{Job: 2, DependsOn: 1, OnFailure: driver.DependencyIgnore, Reads: true},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		for i, s := range []string{"a", "c", "b"} {
			steps[wf.Jobs[i].Job.ID] = step{w, s}
		}
	}

	sctx, stop := context.WithCancel(ctx)
	defer stop()
	swept := make(chan error, 1)
	go func() {
		for sctx.Err() == nil {
			if _, err := exec.BatchSettle(sctx, driver.BatchSettleParams{Limit: 1, Results: true}); err != nil && sctx.Err() == nil {
				swept <- err
				return
			}
		}
		swept <- nil
	}()

	claimed := map[driver.JobID][]driver.Dependency{}
	for finished, deadline := 0, time.Now().Add(30*time.Second); finished < len(steps); {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d steps finished", finished, len(steps))
		}
		for _, j := range claim(ctx, t, exec, clientID, 4) {
			claimed[j.ID] = j.Dependencies
			if applied := finalize(ctx, t, exec, driver.JobFinalize{
				ID: j.ID, AttemptedBy: clientID, Fence: j.Fence, State: driver.JobStateCompleted, Archive: true,
				Output: json.RawMessage(output(steps[j.ID])),
			}); len(applied) != 1 {
				t.Fatalf("finalize of %s applied %v", j.ID, applied)
			}
			finished++
		}
	}
	stop()
	if err := <-swept; err != nil {
		t.Fatal(err)
	}

	for id, deps := range claimed {
		s := steps[id]
		if s.s != "b" {
			continue
		}
		checkDependencies(t, &driver.JobRow{ID: id, Dependencies: deps},
			wantDependency{"a", driver.JobStateCompleted, output(step{s.w, "a"})},
			wantDependency{"c", driver.JobStateCompleted, output(step{s.w, "c"})})
	}
	if n := f.CountJobResults(ctx, t, d); n != 0 {
		t.Errorf("%d dependency results after the workflows completed, want none", n)
	}
}

// testDependencyResultsRetried checks that a dependency retried from history
// is not reported finished with its old result while it is live again, and
// that its next finish, which leaves the workflow open while its dependent
// has yet to finish, records the new one.
func testDependencyResultsRetried[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := register(ctx, t, exec)

	wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
		Name: "retried", Jobs: params("step", 2), Steps: []string{"build", "report"},
		Deps: []driver.JobDependency{{Job: 1, DependsOn: 0, Reads: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	build, report := wf.Jobs[0].Job.ID, wf.Jobs[1].Job.ID
	claimOne(ctx, t, exec, clientID, build)
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: build, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true, Output: json.RawMessage(`{"n":1}`)})
	checkDependencies(t, claimOne(ctx, t, exec, clientID, report), wantDependency{"build", driver.JobStateCompleted, `{"n":1}`})
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{
		ID: report, AttemptedBy: clientID, State: driver.JobStateRetryable, Delay: time.Hour,
		Error: &driver.AttemptError{Attempt: 1, Error: "retry"},
	})

	if _, err := exec.JobRetry(ctx, driver.JobRetryParams{ID: build}); err != nil {
		t.Fatal(err)
	}
	claimOne(ctx, t, exec, clientID, build)
	f.MakeClaimable(ctx, t, d, report)
	if j := claimOne(ctx, t, exec, clientID, report); len(j.Dependencies) != 0 {
		t.Errorf("claimed with a retried dependency live again, dependencies = %+v, want none", j.Dependencies)
	}
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: build, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true, Output: json.RawMessage(`{"n":2}`)})
	if b, err := exec.BatchGet(ctx, wf.ID); err != nil || !b.CompletedAt.IsZero() {
		t.Fatalf("workflow = %+v, %v; want it open while report has yet to finish", b, err)
	}
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{
		ID: report, AttemptedBy: clientID, State: driver.JobStateRetryable, Delay: time.Hour,
		Error: &driver.AttemptError{Attempt: 2, Error: "retry"},
	})
	f.MakeClaimable(ctx, t, d, report)
	checkDependencies(t, claimOne(ctx, t, exec, clientID, report), wantDependency{"build", driver.JobStateCompleted, `{"n":2}`})
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: report, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	if b, err := exec.BatchGet(ctx, wf.ID); err != nil || b.CompletedAt.IsZero() {
		t.Fatalf("workflow = %+v, %v; want it completed", b, err)
	}
}

// testDependencyResultsUnread checks that a workflow none of whose steps
// reads its dependencies records no results and claims none.
func testDependencyResultsUnread[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := register(ctx, t, exec)

	wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
		Name: "unread", Jobs: params("step", 4), Steps: []string{"a", "b", "c", "d"},
		Deps: []driver.JobDependency{{Job: 1, DependsOn: 0}, {Job: 2, DependsOn: 0}, {Job: 3, DependsOn: 1}, {Job: 3, DependsOn: 2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, wave := range []int{1, 2, 1} {
		jobs := claim(ctx, t, exec, clientID, 10)
		if len(jobs) != wave {
			t.Fatalf("claimed %d steps, want %d", len(jobs), wave)
		}
		for _, j := range jobs {
			if j.Dependencies != nil {
				t.Errorf("step %s claimed with dependencies %+v, though none reads them", j.ID, j.Dependencies)
			}
		}
		for _, j := range jobs {
			finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true, Output: json.RawMessage(`{"x":1}`)})
			if n := f.CountJobResults(ctx, t, d); n != 0 {
				t.Errorf("%d dependency results after step %s finished, want none", n, j.ID)
			}
		}
	}
	if b, err := exec.BatchGet(ctx, wf.ID); err != nil || b.CompletedAt.IsZero() {
		t.Fatalf("workflow = %+v, %v; want it completed", b, err)
	}
}
