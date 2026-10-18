package drivertest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

func testWorkflows[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := register(ctx, t, exec)
	cb := func(kind string) *driver.JobInsertParams {
		return &driver.JobInsertParams{Kind: kind, Queue: "callbacks", Priority: 2, MaxAttempts: 3, Args: json.RawMessage(`{}`)}
	}
	state := func(id driver.JobID) driver.JobState {
		t.Helper()
		j, err := exec.JobGet(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return j.State
	}
	complete := func(jobs ...*driver.JobRow) {
		t.Helper()
		fin := make([]driver.JobFinalize, len(jobs))
		for i, j := range jobs {
			fin[i] = driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true}
		}
		if applied := finalize(ctx, t, exec, fin...); len(applied) != len(jobs) {
			t.Fatalf("applied %d of %d", len(applied), len(jobs))
		}
	}
	callbacks := func() map[string]*driver.JobRow {
		t.Helper()
		out, err := exec.JobList(ctx, driver.JobListParams{Queue: "callbacks", Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		m := map[string]*driver.JobRow{}
		for _, j := range out {
			m[j.Kind] = j
		}
		return m
	}

	// A diamond: b and c after a, d after b and c.
	wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
		Name: "diamond", Jobs: params("step", 4),
		Deps:      []driver.JobDependency{{Job: 1, DependsOn: 0}, {Job: 2, DependsOn: 0}, {Job: 3, DependsOn: 1}, {Job: 3, DependsOn: 2}},
		OnSuccess: cb("wf-done"), OnFailure: cb("wf-alert"), Metadata: json.RawMessage(`{"run":1}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(wf.Jobs) != 4 || wf.ID.IsZero() {
		t.Fatalf("workflow result = %+v", wf)
	}
	a, b, c, dd := wf.Jobs[0].Job, wf.Jobs[1].Job, wf.Jobs[2].Job, wf.Jobs[3].Job
	for i, j := range wf.Jobs {
		if argsN(j.Job) != i || j.Job.BatchID != wf.ID || j.Duplicate {
			t.Errorf("job %d = %+v", i, j.Job)
		}
	}
	if a.State != driver.JobStateAvailable || b.State != driver.JobStatePending || c.State != driver.JobStatePending || dd.State != driver.JobStatePending {
		t.Errorf("states = %s %s %s %s", a.State, b.State, c.State, dd.State)
	}
	row, err := exec.WorkflowGet(ctx, wf.ID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Batch.Name != "diamond" || row.Batch.Pending != 4 || row.Batch.Total != 4 || len(row.Jobs) != 4 || len(row.Edges) != 4 {
		t.Fatalf("workflow = %+v", row)
	}
	if row.Jobs[0].ID != a.ID || row.Jobs[3].ID != dd.ID || row.Edges[0] != (driver.JobEdge{Job: b.ID, DependsOn: a.ID}) {
		t.Errorf("workflow jobs/edges out of order: %+v", row)
	}

	// Only the root is claimable. Completing it promotes b and c, not d.
	running := claim(ctx, t, exec, clientID, 10)
	if len(running) != 1 || running[0].ID != a.ID {
		t.Fatalf("claimed %v, want only the root", running)
	}
	complete(running[0])
	if state(b.ID) != driver.JobStateAvailable || state(c.ID) != driver.JobStateAvailable || state(dd.ID) != driver.JobStatePending {
		t.Errorf("after a: b %s c %s d %s", state(b.ID), state(c.ID), state(dd.ID))
	}
	running = claim(ctx, t, exec, clientID, 10)
	if len(running) != 2 {
		t.Fatalf("claimed %d, want b and c", len(running))
	}
	complete(running[0])
	if state(dd.ID) != driver.JobStatePending {
		t.Errorf("d promoted with one dependency left")
	}
	complete(running[1])
	if state(dd.ID) != driver.JobStateAvailable {
		t.Errorf("d not promoted after b and c")
	}
	running = claim(ctx, t, exec, clientID, 10)
	if len(running) != 1 || running[0].ID != dd.ID {
		t.Fatalf("claimed %v, want d", running)
	}
	if cbs := callbacks(); len(cbs) != 0 {
		t.Errorf("callbacks before completion: %v", cbs)
	}
	complete(running[0])
	row, _ = exec.WorkflowGet(ctx, wf.ID)
	if row.Batch.Pending != 0 || row.Batch.Failed != 0 || row.Batch.CompletedAt.IsZero() || len(row.Jobs) != 4 || len(row.Edges) != 4 {
		t.Errorf("completed workflow = %+v", row.Batch)
	}
	for _, j := range row.Jobs {
		if j.State != driver.JobStateCompleted || j.FinalizedAt.IsZero() {
			t.Errorf("finished job = %+v", j)
		}
	}
	if cbs := callbacks(); len(cbs) != 1 || cbs["wf-done"] == nil {
		t.Errorf("callbacks = %v", cbs)
	}

	// A failure cascades: e -> f -> g are canceled below e, while h, which
	// ignores f's failure, runs once f has finished.
	wf2, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
		Name: "chain", Jobs: params("chain", 4),
		Deps: []driver.JobDependency{
			{Job: 1, DependsOn: 0}, {Job: 2, DependsOn: 1}, {Job: 3, DependsOn: 1, OnFailure: driver.DependencyIgnore},
		},
		OnSuccess: cb("wf-done2"), OnFailure: cb("wf-alert2"),
	})
	if err != nil {
		t.Fatal(err)
	}
	e, ff, g, h := wf2.Jobs[0].Job, wf2.Jobs[1].Job, wf2.Jobs[2].Job, wf2.Jobs[3].Job
	running = claim(ctx, t, exec, clientID, 10)
	if len(running) != 1 || running[0].ID != e.ID {
		t.Fatalf("claimed %v, want e", running)
	}
	finalize(ctx, t, exec, driver.JobFinalize{ID: e.ID, AttemptedBy: clientID, State: driver.JobStateDiscarded, Error: &driver.AttemptError{Attempt: 1, Error: "boom"}})
	if state(ff.ID) != driver.JobStateCanceled || state(g.ID) != driver.JobStateCanceled {
		t.Errorf("after e failed: f %s g %s, want canceled", state(ff.ID), state(g.ID))
	}
	if state(h.ID) != driver.JobStateAvailable {
		t.Errorf("h = %s, want available (ignores f's failure)", state(h.ID))
	}
	if j, _ := exec.JobGet(ctx, g.ID); len(j.Errors) != 1 || !strings.Contains(j.Errors[0].Error, "dependency failed") || j.Errors[0].At.IsZero() {
		t.Errorf("canceled dependent errors = %+v", j.Errors)
	}
	row, _ = exec.WorkflowGet(ctx, wf2.ID)
	if row.Batch.Pending != 1 || row.Batch.Failed != 3 {
		t.Errorf("chain after cascade = %+v", row.Batch)
	}
	running = claim(ctx, t, exec, clientID, 10)
	if len(running) != 1 || running[0].ID != h.ID {
		t.Fatalf("claimed %v, want h", running)
	}
	complete(running[0])
	row, _ = exec.WorkflowGet(ctx, wf2.ID)
	if row.Batch.Pending != 0 || row.Batch.Failed != 3 || row.Batch.CompletedAt.IsZero() {
		t.Errorf("chain completed = %+v", row.Batch)
	}
	cbs := callbacks()
	if cbs["wf-alert2"] == nil || cbs["wf-done2"] != nil {
		t.Errorf("chain callbacks = %v", cbs)
	} else if !strings.Contains(string(cbs["wf-alert2"].Metadata), `"batch_failed": 3`) {
		t.Errorf("alert metadata = %s", cbs["wf-alert2"].Metadata)
	}

	// Canceling a pending job cascades too, and counts as a failure.
	wf3, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
		Jobs: params("pair", 3), Deps: []driver.JobDependency{{Job: 1, DependsOn: 0}, {Job: 2, DependsOn: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if j, err := exec.JobCancel(ctx, wf3.Jobs[1].Job.ID); err != nil || j.State != driver.JobStateCanceled {
		t.Fatalf("cancel pending = %+v, %v", j, err)
	}
	if state(wf3.Jobs[2].Job.ID) != driver.JobStateCanceled {
		t.Errorf("dependent of a canceled job = %s", state(wf3.Jobs[2].Job.ID))
	}
	running = claim(ctx, t, exec, clientID, 10)
	if len(running) != 1 {
		t.Fatalf("claimed %d, want the root", len(running))
	}
	complete(running[0])
	row, _ = exec.WorkflowGet(ctx, wf3.ID)
	if row.Batch.Name != "" || row.Batch.Pending != 0 || row.Batch.Failed != 2 || row.Batch.CompletedAt.IsZero() {
		t.Errorf("pair = %+v", row.Batch)
	}

	// Validation.
	p := params("bad", 2)
	p[1].UniqueKey = "k"
	if _, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{Jobs: p}); err == nil {
		t.Error("unique key accepted")
	}
	if _, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{Jobs: params("bad", 2), Deps: []driver.JobDependency{{Job: 1, DependsOn: 5}}}); err == nil {
		t.Error("dangling dependency accepted")
	}
	if _, err := exec.WorkflowGet(ctx, driver.JobID{9}); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("unknown workflow = %v", err)
	}
}

// testWorkflowFanInConcurrent finalizes a job's two dependencies in
// concurrent transactions. Each statement still sees the other dependency
// live, so neither promotes the dependent; WorkflowPromoteReady, run by
// each finalizing client after its statement commits, does once both have
// committed, and the leader's sweep finds what clients left.
func testWorkflowFanInConcurrent[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := register(ctx, t, exec)
	wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
		Name: "fan-in", Jobs: params("step", 3),
		Deps: []driver.JobDependency{{Job: 2, DependsOn: 0}, {Job: 2, DependsOn: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	x, y, dep := wf.Jobs[0].Job.ID, wf.Jobs[1].Job.ID, wf.Jobs[2].Job.ID
	if jobs := claim(ctx, t, exec, clientID, 2); len(jobs) != 2 {
		t.Fatalf("claimed %d", len(jobs))
	}
	promote := func(dependsOn ...driver.JobID) int64 {
		t.Helper()
		res, err := exec.WorkflowPromoteReady(ctx, driver.WorkflowPromoteParams{DependsOn: dependsOn, Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		return res.Promoted
	}
	completed := func(id driver.JobID) driver.JobFinalize {
		return driver.JobFinalize{ID: id, AttemptedBy: clientID, Fence: 1, State: driver.JobStateCompleted, Archive: true}
	}

	tx, commit, rollback := f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // already committed on the happy path
	if applied := finalize(ctx, t, d.UnwrapTx(tx), completed(x)); len(applied) != 1 {
		t.Fatalf("finalize in tx applied %v", applied)
	}
	// The other dependency's finalize does not wait for the transaction.
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if applied, err := exec.JobFinalizeMany(fctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{completed(y)}}); err != nil || len(applied) != 1 {
		t.Fatalf("concurrent finalize = %v, %v", applied, err)
	}
	if n := promote(y); n != 0 {
		t.Errorf("promoted %d while a dependency's finalize was uncommitted", n)
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	if j, err := exec.JobGet(ctx, dep); err != nil || j.State != driver.JobStatePending {
		t.Fatalf("dependent after both finalizes = %+v, %v; the race should leave it pending", j, err)
	}
	if n := promote(x); n != 1 {
		t.Errorf("promoted %d, want 1", n)
	}
	if j, err := exec.JobGet(ctx, dep); err != nil || j.State != driver.JobStateAvailable {
		t.Errorf("dependent after promote = %+v, %v", j, err)
	}
	if n := promote(x, y); n != 0 {
		t.Errorf("second promote = %d", n)
	}
	if n := promote(); n != 0 {
		t.Errorf("sweep after the dependent was promoted = %d", n)
	}

	// The sweep walks the jobs with dependencies a chunk at a time, in ID
	// order from After. Each of two workflows has a step whose dependency
	// finished, left pending as a client that stopped before promoting it
	// would leave it.
	if jobs := claim(ctx, t, exec, clientID, 1); len(jobs) != 1 || jobs[0].ID != dep {
		t.Fatalf("claimed %+v, want the fan-in dependent", jobs)
	}
	finalize(ctx, t, exec, completed(dep))
	var steps []driver.JobID
	for range 2 {
		wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
			Name: "chain", Jobs: params("chain", 2), Deps: []driver.JobDependency{{Job: 1, DependsOn: 0}},
		})
		if err != nil {
			t.Fatal(err)
		}
		jobs := claim(ctx, t, exec, clientID, 1)
		if len(jobs) != 1 || jobs[0].ID != wf.Jobs[0].Job.ID {
			t.Fatalf("claimed %+v", jobs)
		}
		finalize(ctx, t, exec, completed(jobs[0].ID))
		f.Exec(ctx, t, d, `UPDATE hopper_jobs SET state = 'pending' WHERE id = $1::uuid`, wf.Jobs[1].Job.ID.String())
		steps = append(steps, wf.Jobs[1].Job.ID)
	}
	sweep := func(after driver.JobID) driver.WorkflowPromoteResult {
		t.Helper()
		res, err := exec.WorkflowPromoteReady(ctx, driver.WorkflowPromoteParams{After: after, Limit: 1})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	var after driver.JobID
	for _, step := range steps {
		if res := sweep(after); res.Scanned != 1 || res.Promoted != 1 || res.Last != step {
			t.Errorf("chunk = %+v; want %s promoted", res, step)
		}
		after = step
	}
	if res := sweep(after); res.Scanned != 0 || res.Promoted != 0 {
		t.Errorf("chunk past the last = %+v", res)
	}
	for _, step := range steps {
		if j, err := exec.JobGet(ctx, step); err != nil || j.State != driver.JobStateAvailable {
			t.Errorf("swept step = %+v, %v", j, err)
		}
	}
}
