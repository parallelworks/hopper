package drivertest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	var alert struct {
		BatchFailed int `json:"batch_failed"`
	}
	if cbs["wf-alert2"] == nil || cbs["wf-done2"] != nil {
		t.Errorf("chain callbacks = %v", cbs)
	} else if err := json.Unmarshal(cbs["wf-alert2"].Metadata, &alert); err != nil || alert.BatchFailed != 3 {
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
// concurrent transactions. On an engine that locks rows, each statement
// still sees the other dependency live, so neither promotes the dependent;
// WorkflowPromoteReady, run by each finalizing client after its statement
// commits, does once both have committed. On an engine that runs one writer
// at a time, the second finalize waits for the first and promotes the
// dependent itself. Either way, the leader's sweep finds what clients left.
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
	if singleWriter(d) {
		done := make(chan []driver.JobID, 1)
		go func() {
			applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{completed(y)}})
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
		if j, err := exec.JobGet(ctx, dep); err != nil || j.State != driver.JobStateAvailable {
			t.Fatalf("dependent after both finalizes = %+v, %v; the second should promote it", j, err)
		}
	} else {
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
		f.MakePending(ctx, t, d, wf.Jobs[1].Job.ID)
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
	var (
		batchIDs  []driver.JobID
		dependsOn = map[driver.JobID][]driver.JobID{}
	)
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
		wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{Name: "chaos", Jobs: jobs, Deps: deps, OnComplete: cb})
		if err != nil {
			t.Fatal(err)
		}
		batchIDs = append(batchIDs, wf.ID)
		for _, dep := range deps {
			job := wf.Jobs[dep.Job].Job.ID
			dependsOn[job] = append(dependsOn[job], wf.Jobs[dep.DependsOn].Job.ID)
		}
	}
	// live reports whether a job is still in the live table.
	live := func(id driver.JobID) (bool, error) {
		j, err := exec.JobGet(ctx, id)
		if errors.Is(err, driver.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return j.FinalizedAt.IsZero(), nil
	}
	list := func(p driver.JobListParams) []*driver.JobRow {
		t.Helper()
		p.Limit = 10000
		jobs, err := exec.JobList(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		return jobs
	}
	liveStates := []driver.JobState{driver.JobStatePending, driver.JobStateAvailable, driver.JobStateScheduled, driver.JobStateRunning, driver.JobStateRetryable}
	finished := []driver.JobState{driver.JobStateCompleted, driver.JobStateCanceled, driver.JobStateDiscarded}

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
				return appliedIDs(ids)
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
				for i, j := range res.Jobs {
					fin[i] = driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, Fence: j.Fence, State: driver.JobStateCompleted, Archive: true}
					if j.Kind == "fail" {
						fin[i].State = driver.JobStateDiscarded
					}
					batches[i] = j.BatchID
					// A dependency leaves the live table when its finalize
					// commits, and never returns: one still there ran after
					// its dependent was claimed.
					for _, dep := range dependsOn[j.ID] {
						isLive, err := live(dep)
						if err != nil {
							fail(err)
							return
						}
						if isLive {
							fail(fmt.Errorf("claimed job %s with dependency %s still live", j.ID, dep))
							return
						}
					}
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
	done := func() bool {
		if len(list(driver.JobListParams{Queue: "default", States: liveStates})) > 0 {
			return false
		}
		for _, id := range batchIDs {
			b, err := exec.BatchGet(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if b.CompletedAt.IsZero() {
				return false
			}
		}
		return true
	}
	for wctx.Err() == nil && !done() {
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
		states := map[driver.JobState]int{}
		for _, j := range list(driver.JobListParams{Queue: "default", States: liveStates}) {
			states[j.State]++
		}
		t.Fatalf("not finished in a minute (last finalize error: %v): %v", lastErr, states)
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
	history := list(driver.JobListParams{Queue: "default", States: finished})
	distinct := map[driver.JobID]bool{}
	lastByState := map[driver.JobState]int{}
	ranCanceled := 0
	failedByBatch := map[driver.JobID]int{}
	for _, j := range history {
		distinct[j.ID] = true
		if j.Kind == "last" {
			lastByState[j.State]++
			if j.State == driver.JobStateCanceled && !j.AttemptedAt.IsZero() {
				ranCanceled++
			}
		}
		if j.State != driver.JobStateCompleted {
			failedByBatch[j.BatchID]++
		}
	}
	if len(history) != workflows*(middles+2) || len(distinct) != len(history) {
		t.Errorf("history has %d rows of %d distinct jobs, want %d of as many", len(history), len(distinct), workflows*(middles+2))
	}
	if lastByState[driver.JobStateCanceled] != failing || lastByState[driver.JobStateCompleted] != workflows-failing || len(lastByState) != 2 {
		t.Errorf("last steps by state = %v, want %d canceled and %d completed", lastByState, failing, workflows-failing)
	}
	if ranCanceled != 0 {
		t.Errorf("%d canceled last steps ran", ranCanceled)
	}
	failures := 0
	for _, id := range batchIDs {
		b, err := exec.BatchGet(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		failures += b.Failed
		if b.Failed != failedByBatch[id] {
			t.Errorf("workflow %s counts %d failures, its history %d", id, b.Failed, failedByBatch[id])
		}
	}
	if failures != 2*failing {
		t.Errorf("failures = %d, want %d", failures, 2*failing)
	}
	if n := len(list(driver.JobListParams{Queue: "callbacks"})); n != workflows {
		t.Errorf("completion callbacks = %d, want %d", n, workflows)
	}
	if n := f.CountEdges(ctx, t, d); n != 0 {
		t.Errorf("edges left = %d, want 0", n)
	}
}

// testWorkflowCascadeHeld checks that the leader's statements that finalize
// jobs, the discard of jobs past their TTL, the expiry of job leases and the
// rescue's finalize, each of which skips the jobs another transaction
// holds, do not wait for the caller's
// transaction that holds a row their cascade reaches: a dependent of two
// steps that a JobCancelTx or a CompleteTx of the other step, held open,
// has canceled by cascade. The leader runs them in its loop, so a wait
// would hold up its other duties and its lease's renewal for as long as
// the caller kept the transaction open. The statement gives up on its jobs
// instead, which the next pass, once the transaction has rolled back,
// finalizes, canceling the dependent.
func testWorkflowCascadeHeld[TTx any](t *testing.T, f Fixture[TTx]) {
	for _, duty := range []string{"discard expired", "lease expiry", "rescue"} {
		t.Run(duty, func(t *testing.T) {
			ctx := context.Background()
			d := f.NewDriver(t)
			if singleWriter(d) {
				t.Skip("on an engine that runs one writer at a time, every write waits for an open transaction")
			}
			exec := d.Executor()
			// The expiry's client is the cluster's first: a lease is due no
			// earlier than its TTL past the start of a client that started
			// after its last renewal.
			var clientID int64
			if duty != "lease expiry" {
				clientID = register(ctx, t, exec)
			}
			jobs := params("step", 3)
			switch duty {
			case "discard expired":
				jobs[1].TTL = 100 * time.Millisecond
			case "lease expiry":
				jobs[1].MaxAttempts = 1
			}
			wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
				Name: "fan-in", Jobs: jobs,
				Deps: []driver.JobDependency{{Job: 2, DependsOn: 0}, {Job: 2, DependsOn: 1}},
			})
			if err != nil {
				t.Fatal(err)
			}
			a, b, dep := wf.Jobs[0].Job.ID, wf.Jobs[1].Job.ID, wf.Jobs[2].Job.ID
			failed := func(id driver.JobID) []driver.JobFinalize {
				return withFences(ctx, t, exec, []driver.JobFinalize{{
					ID: id, AttemptedBy: clientID, State: driver.JobStateDiscarded, Archive: true,
					Error: &driver.AttemptError{Attempt: 1, Error: "boom"},
				}})
			}
			var expirerID int64
			switch duty {
			case "discard expired":
				time.Sleep(500 * time.Millisecond)
			case "lease expiry":
				expirerID = expirer(ctx, t, f, d)
				claimByID(ctx, t, exec, b)
				f.ExpireJobLease(ctx, t, d, b)
			case "rescue":
				if running := claim(ctx, t, exec, clientID, 2); len(running) != 2 {
					t.Fatalf("claimed %d steps, want both", len(running))
				}
			}

			tx, _, rollback := f.Begin(ctx, t, d)
			t.Cleanup(func() { _ = rollback() })
			if duty == "rescue" {
				_, err = d.UnwrapTx(tx).JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: failed(a)})
			} else {
				_, err = d.UnwrapTx(tx).JobCancel(ctx, a)
			}
			if err != nil {
				t.Fatal(err)
			}
			var gaveUp bool
			run := func() int {
				t.Helper()
				lctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				var (
					n   int
					err error
				)
				switch duty {
				case "discard expired":
					var discarded []*driver.JobRow
					discarded, err = exec.JobDiscardExpired(lctx, 10)
					n = len(discarded)
					if errors.Is(err, driver.ErrRowsHeld) {
						gaveUp, err = true, nil
					}
				case "lease expiry":
					var expired []*driver.JobRow
					expired, err = exec.JobExpireLeases(lctx, driver.JobExpireLeasesParams{Limit: 10, ClientID: expirerID})
					n = len(expired)
					if errors.Is(err, driver.ErrRowsHeld) {
						gaveUp, err = true, nil
					}
				case "rescue":
					var applied []driver.JobFinalized
					applied, err = exec.JobFinalizeMany(lctx, driver.JobFinalizeParams{Jobs: failed(b), SkipLocked: true})
					n = len(applied)
					if errors.Is(err, driver.ErrRowsHeld) {
						gaveUp, err = true, nil
					}
				}
				if err != nil {
					t.Fatalf("%s while a transaction that canceled the dependent is held open: %v", duty, err)
				}
				return n
			}
			// Each gives up with ErrRowsHeld, which the leader logs, and
			// for which the finalizer writes a batch again in halves.
			if n := run(); n != 0 || !gaveUp {
				t.Fatalf("%s while a transaction that canceled the dependent is held open finalized %d jobs (ErrRowsHeld: %t); want it to give up", duty, n, gaveUp)
			}
			if err := rollback(); err != nil {
				t.Fatal(err)
			}
			if n := run(); n != 1 {
				t.Fatalf("%s once the transaction rolled back finalized %d jobs, want 1", duty, n)
			}
			if j, err := exec.JobGet(ctx, dep); err != nil || j.State != driver.JobStateCanceled {
				t.Fatalf("dependent = %+v, %v; want it canceled by the cascade", j, err)
			}
			if j, err := exec.JobGet(ctx, a); err != nil || j.State.Terminal() {
				t.Fatalf("step whose cancel rolled back = %+v, %v; want it live", j, err)
			}
		})
	}
}
