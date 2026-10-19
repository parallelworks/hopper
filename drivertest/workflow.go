package drivertest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
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
		if applied := finalizeCurrent(ctx, t, exec, fin...); len(applied) != len(jobs) {
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
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: e.ID, AttemptedBy: clientID, State: driver.JobStateDiscarded, Error: &driver.AttemptError{Attempt: 1, Error: "boom"}})
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
	if applied := finalizeCurrent(ctx, t, d.UnwrapTx(tx), completed(x)); len(applied) != 1 {
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
	finalizeCurrent(ctx, t, exec, completed(dep))
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
		finalizeCurrent(ctx, t, exec, completed(jobs[0].ID))
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

// testWorkflowsConcurrent works a hundred workflows (a step, four after it,
// one after those) with eight workers at once, each finalizing what it
// claimed in one statement and following up as a client does, while the
// leader's sweeps run and every statement's results are sent a second time.
// In one workflow in five, a middle step fails. Every job is finalized
// once, no step is claimed while a dependency is live, the step after
// a failed one is canceled, and every workflow completes once, with its
// failures counted.
func testWorkflowsConcurrent[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	const workflows, middles = 100, 4
	cb := &driver.JobInsertParams{Kind: "done", Queue: "callbacks", Priority: 2, MaxAttempts: 3, Args: json.RawMessage(`{}`)}
	for w := range workflows {
		jobs := params("root", 1)
		mids := params("mid", middles)
		if w%5 == 0 {
			mids[0].Kind = "fail"
		}
		jobs = append(append(jobs, mids...), params("last", 1)...)
		var deps []driver.JobDependency
		for m := 1; m <= middles; m++ {
			deps = append(deps, driver.JobDependency{Job: m, DependsOn: 0}, driver.JobDependency{Job: middles + 1, DependsOn: m})
		}
		if _, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{Name: "chaos", Jobs: jobs, Deps: deps, OnComplete: cb}); err != nil {
			t.Fatal(err)
		}
	}

	wctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	var (
		mu      sync.Mutex
		applied = map[driver.JobID]int{}
		lastErr error
		errs    = make(chan error, 16)
		replay  = make(chan []driver.JobFinalize, 256)
		wg      sync.WaitGroup
	)
	record := func(ids []driver.JobID) {
		mu.Lock()
		defer mu.Unlock()
		for _, id := range ids {
			applied[id]++
		}
	}
	fail := func(err error) {
		if wctx.Err() == nil {
			select {
			case errs <- err:
			default:
			}
			cancel()
		}
	}
	// A failed statement is sent again whole, as the finalizer does; a
	// deadlock between two finalizes completing batches fails one of them.
	// The statement runs on a context the end of the test does not cancel:
	// canceled while its reply was on the way, a finalize the database
	// applied would count as never applied.
	finalizeRetrying := func(fin []driver.JobFinalize) []driver.JobID {
		for wctx.Err() == nil {
			sctx, scancel := context.WithTimeout(ctx, 10*time.Second)
			ids, err := exec.JobFinalizeMany(sctx, driver.JobFinalizeParams{Jobs: fin})
			scancel()
			if err == nil {
				return ids
			}
			mu.Lock()
			lastErr = err
			mu.Unlock()
			time.Sleep(time.Millisecond)
		}
		return nil
	}
	// Small claims split a workflow's middle steps across statements.
	for w := range 8 {
		clientID := register(ctx, t, exec)
		wg.Go(func() {
			for wctx.Err() == nil {
				res, err := exec.JobClaim(wctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 1 + w%3})
				if err != nil {
					fail(fmt.Errorf("claim: %w", err))
					return
				}
				if len(res.Jobs) == 0 {
					time.Sleep(2 * time.Millisecond)
					continue
				}
				fin := make([]driver.JobFinalize, len(res.Jobs))
				batches := make([]driver.JobID, len(res.Jobs))
				ids := make([]string, len(res.Jobs))
				for i, j := range res.Jobs {
					fin[i] = driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, Fence: j.Fence, State: driver.JobStateCompleted, Archive: true}
					if j.Kind == "fail" {
						fin[i].State = driver.JobStateDiscarded
					}
					batches[i] = j.BatchID
					ids[i] = j.ID.String()
				}
				// A dependency leaves the live table when its finalize
				// commits, and never returns: one still there ran after
				// its dependent was claimed.
				if n := f.QueryText(ctx, t, d, `SELECT count(*)::text FROM hopper_job_deps e
					JOIN hopper_jobs x ON x.id = e.depends_on WHERE e.job_id = ANY($1::uuid[])`, "{"+strings.Join(ids, ",")+"}"); n != "0" {
					fail(fmt.Errorf("claimed %d jobs with %s dependencies still live", len(ids), n))
					return
				}
				finished := finalizeRetrying(fin)
				record(finished)
				select {
				case replay <- fin:
				case <-wctx.Done():
				}
				// As the client does, a flush that finished nothing is not
				// followed up: a nil DependsOn is the leader's sweep.
				if len(finished) == 0 {
					continue
				}
				// The follow-up may time out on a batch row; the sweeps
				// below settle what it leaves.
				_, _ = exec.BatchSettle(wctx, driver.BatchSettleParams{IDs: batches})
				_, _ = exec.WorkflowPromoteReady(wctx, driver.WorkflowPromoteParams{DependsOn: finished})
			}
		})
	}
	wg.Go(func() {
		for wctx.Err() == nil {
			select {
			case fin := <-replay:
				record(finalizeRetrying(fin))
			case <-wctx.Done():
			}
		}
	})
	wg.Go(func() {
		var settleAfter, promoteAfter driver.JobID
		for wctx.Err() == nil {
			s, err := exec.BatchSettle(wctx, driver.BatchSettleParams{After: settleAfter, Limit: 16})
			if err == nil {
				settleAfter = s.Last
				if s.Folded < 16 {
					settleAfter = driver.JobID{}
				}
			}
			p, err := exec.WorkflowPromoteReady(wctx, driver.WorkflowPromoteParams{After: promoteAfter, Limit: 16})
			if err == nil {
				promoteAfter = p.Last
				if p.Scanned < 16 {
					promoteAfter = driver.JobID{}
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
	})
	for wctx.Err() == nil && f.QueryText(ctx, t, d, `SELECT ((SELECT count(*) FROM hopper_jobs WHERE queue = 'default') = 0
		AND (SELECT count(*) FROM hopper_batches WHERE completed_at IS NULL) = 0)::text`) != "true" {
		time.Sleep(20 * time.Millisecond)
	}
	timedOut := wctx.Err() != nil
	cancel()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if timedOut {
		t.Fatalf("not finished in a minute (last finalize error: %v): %s", lastErr, f.QueryText(ctx, t, d,
			"SELECT coalesce(string_agg(state || ' ' || n, ', '), 'none') FROM (SELECT state::text, count(*) AS n FROM hopper_jobs GROUP BY state) s"))
	}

	failing := workflows / 5
	want := workflows*(middles+2) - failing
	twice := 0
	for _, n := range applied {
		if n != 1 {
			twice++
		}
	}
	if len(applied) != want || twice != 0 {
		t.Errorf("results applied for %d jobs, %d of them more than once; want %d jobs, once each", len(applied), twice, want)
	}
	checks := []struct{ what, query, want string }{
		{
			"history rows, distinct jobs", `SELECT count(*) || ' ' || count(DISTINCT id) FROM hopper_job_history WHERE queue = 'default'`,
			fmt.Sprintf("%d %d", workflows*(middles+2), workflows*(middles+2)),
		},
		{
			"last steps by state", `SELECT string_agg(state || ' ' || n, ', ' ORDER BY state) FROM (
			SELECT state::text, count(*) AS n FROM hopper_job_history WHERE kind = 'last' GROUP BY state) s`,
			fmt.Sprintf("canceled %d, completed %d", failing, workflows-failing),
		},
		{"canceled last steps that ran", `SELECT count(*) FROM hopper_job_history WHERE kind = 'last' AND state = 'canceled' AND attempted_at IS NOT NULL`, "0"},
		{"workflows whose failures miscount", `SELECT count(*) FROM hopper_batches b
			WHERE b.failed <> (SELECT count(*) FROM hopper_job_history h WHERE h.batch_id = b.id AND h.state <> 'completed')`, "0"},
		{"failures", `SELECT sum(failed) FROM hopper_batches`, strconv.Itoa(2 * failing)},
		{"completion callbacks", `SELECT count(*) FROM hopper_jobs WHERE queue = 'callbacks'`, strconv.Itoa(workflows)},
		{"edges left", `SELECT count(*) FROM hopper_job_deps`, "0"},
	}
	for _, c := range checks {
		if got := f.QueryText(ctx, t, d, "SELECT ("+c.query+")::text"); got != c.want {
			t.Errorf("%s = %s, want %s", c.what, got, c.want)
		}
	}
}
