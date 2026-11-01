package drivertest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// anyOfFixture holds what the any-of tests share: an executor, a client,
// and helpers that claim and finalize jobs and read their states.
type anyOfFixture struct {
	ctx      context.Context
	t        *testing.T
	exec     driver.Executor
	clientID int64
}

func (a anyOfFixture) insert(name string, n int, deps ...driver.JobDependency) (driver.JobID, []driver.JobID) {
	a.t.Helper()
	steps := make([]string, n)
	for i := range steps {
		steps[i] = fmt.Sprintf("s%d", i)
	}
	wf, err := a.exec.WorkflowInsert(a.ctx, driver.WorkflowInsertParams{Name: name, Jobs: params(name, n), Steps: steps, Deps: deps})
	if err != nil {
		a.t.Fatal(err)
	}
	ids := make([]driver.JobID, n)
	for i, j := range wf.Jobs {
		ids[i] = j.Job.ID
	}
	return wf.ID, ids
}

// claimAll claims every available job, by ID.
func (a anyOfFixture) claimAll() map[driver.JobID]*driver.JobRow {
	a.t.Helper()
	out := map[driver.JobID]*driver.JobRow{}
	for _, j := range claim(a.ctx, a.t, a.exec, a.clientID, 100) {
		out[j.ID] = j
	}
	return out
}

// finish finalizes a job and follows it up as a client does: an engine
// that locks rows leaves the promotion of a dependent with a group to the
// follow-up.
func (a anyOfFixture) finish(id driver.JobID, state driver.JobState) {
	a.t.Helper()
	result := driver.JobFinalize{ID: id, AttemptedBy: a.clientID, State: state, Archive: true}
	out, err := a.exec.JobFinalizeMany(a.ctx, driver.JobFinalizeParams{Jobs: withFences(a.ctx, a.t, a.exec, []driver.JobFinalize{result})})
	if err != nil {
		a.t.Fatal(err)
	}
	if applied := appliedIDs(out); len(applied) != 1 {
		a.t.Fatalf("finalize %s as %s applied %v", id, state, applied)
	}
	if _, err := a.exec.WorkflowPromoteReady(a.ctx, driver.WorkflowPromoteParams{DependsOn: append(appliedIDs(out), cascadedIDs(out)...)}); err != nil {
		a.t.Fatal(err)
	}
}

func (a anyOfFixture) state(id driver.JobID) driver.JobState {
	a.t.Helper()
	j, err := a.exec.JobGet(a.ctx, id)
	if err != nil {
		a.t.Fatal(err)
	}
	return j.State
}

func (a anyOfFixture) wantStates(what string, want map[driver.JobID]driver.JobState) {
	a.t.Helper()
	for id, s := range want {
		if got := a.state(id); got != s {
			a.t.Errorf("%s: job %s is %s, want %s", what, id, got, s)
		}
	}
}

// testWorkflowAnyOf checks any-of groups: a dependent waits for its
// ordinary dependencies and for one job of each group to complete, the
// other jobs of a group run on without affecting it, its claim carries the
// dependencies that have finished, and a group whose jobs all fail without
// completing cancels the dependent or lets it run, by its policy.
func testWorkflowAnyOf[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	a := anyOfFixture{ctx: ctx, t: t, exec: exec, clientID: register(ctx, t, exec)}

	t.Run("first completion", func(t *testing.T) {
		a := a
		a.t = t
		// 4 waits for 3 and for any of 0, 1 and 2, and reads them; 5 for 4.
		batch, ids := a.insert("any", 6,
			driver.JobDependency{Job: 4, DependsOn: 3, Reads: true},
			driver.JobDependency{Job: 4, DependsOn: 0, Reads: true, AnyOf: 1},
			driver.JobDependency{Job: 4, DependsOn: 1, Reads: true, AnyOf: 1},
			driver.JobDependency{Job: 4, DependsOn: 2, Reads: true, AnyOf: 1},
			driver.JobDependency{Job: 5, DependsOn: 4})
		if got := a.claimAll(); len(got) != 4 {
			t.Fatalf("claimed %d, want the group and the ordinary dependency", len(got))
		}
		a.finish(ids[0], driver.JobStateDiscarded)
		a.wantStates("a member discarded", map[driver.JobID]driver.JobState{ids[4]: driver.JobStatePending})
		a.finish(ids[3], driver.JobStateCompleted)
		a.wantStates("the ordinary dependency completed", map[driver.JobID]driver.JobState{ids[4]: driver.JobStatePending})
		a.finish(ids[1], driver.JobStateCompleted)
		a.wantStates("a member completed", map[driver.JobID]driver.JobState{ids[4]: driver.JobStateAvailable, ids[2]: driver.JobStateRunning})
		claimed := a.claimAll()
		dep := claimed[ids[4]]
		if len(claimed) != 1 || dep == nil {
			t.Fatalf("claimed %v, want the dependent", claimed)
		}
		got := map[driver.JobID]driver.JobState{}
		for _, x := range dep.Dependencies {
			got[x.ID] = x.State
		}
		want := map[driver.JobID]driver.JobState{ids[0]: driver.JobStateDiscarded, ids[1]: driver.JobStateCompleted, ids[3]: driver.JobStateCompleted}
		if len(got) != len(want) {
			t.Errorf("dependencies = %v, want the three that finished, %v", got, want)
		}
		for id, s := range want {
			if got[id] != s {
				t.Errorf("dependency %s = %q, want %s", id, got[id], s)
			}
		}
		// The group's last job fails after the dependent started: nothing
		// changes for it, and the workflow waits for it all the same.
		a.finish(ids[2], driver.JobStateDiscarded)
		a.wantStates("the last member discarded", map[driver.JobID]driver.JobState{ids[4]: driver.JobStateRunning, ids[5]: driver.JobStatePending})
		a.finish(ids[4], driver.JobStateCompleted)
		if got := a.claimAll(); len(got) != 1 || got[ids[5]] == nil {
			t.Fatalf("claimed %v, want the last step", got)
		}
		a.finish(ids[5], driver.JobStateCompleted)
		b, err := exec.BatchGet(ctx, batch)
		if err != nil || b.CompletedAt.IsZero() || b.Failed != 2 {
			t.Errorf("workflow = %+v, %v; want it completed with 2 failures", b, err)
		}
		row, err := exec.WorkflowGet(ctx, batch)
		if err != nil {
			t.Fatal(err)
		}
		groups := 0
		for _, e := range row.Edges {
			if e.AnyOf == 1 && e.Job == ids[4] {
				groups++
			} else if e.AnyOf != 0 {
				t.Errorf("edge %+v in a group", e)
			}
		}
		if len(row.Edges) != 5 || groups != 3 {
			t.Errorf("edges = %+v, want 5 with 3 in the group", row.Edges)
		}
		if n := f.CountEdges(ctx, t, d); n != 0 {
			t.Errorf("edges left = %d", n)
		}
	})

	for _, policy := range []driver.DependencyFailure{driver.DependencyCancel, driver.DependencyIgnore} {
		t.Run("exhausted "+string(policy), func(t *testing.T) {
			a := a
			a.t = t
			// 2 waits for any of 0 and 1; 3 for 2.
			batch, ids := a.insert("exhausted", 4,
				driver.JobDependency{Job: 2, DependsOn: 0, OnFailure: policy, AnyOf: 1},
				driver.JobDependency{Job: 2, DependsOn: 1, OnFailure: policy, AnyOf: 1},
				driver.JobDependency{Job: 3, DependsOn: 2})
			if got := a.claimAll(); len(got) != 2 {
				t.Fatalf("claimed %d, want the group", len(got))
			}
			a.finish(ids[0], driver.JobStateDiscarded)
			a.wantStates("a member discarded", map[driver.JobID]driver.JobState{ids[2]: driver.JobStatePending})
			a.finish(ids[1], driver.JobStateCanceled)
			if policy == driver.DependencyCancel {
				a.wantStates("every member failed", map[driver.JobID]driver.JobState{ids[2]: driver.JobStateCanceled, ids[3]: driver.JobStateCanceled})
				b, err := exec.BatchGet(ctx, batch)
				if err != nil || b.CompletedAt.IsZero() || b.Failed != 4 {
					t.Errorf("workflow = %+v, %v; want it completed with 4 failures", b, err)
				}
				return
			}
			a.wantStates("every member failed", map[driver.JobID]driver.JobState{ids[2]: driver.JobStateAvailable})
			if got := a.claimAll(); len(got) != 1 || len(got[ids[2]].Dependencies) != 0 {
				t.Fatalf("claimed %v, want the dependent, which reads nothing", got)
			}
		})
	}

	t.Run("satisfied before a member fails", func(t *testing.T) {
		a := a
		a.t = t
		// 3 waits for 2 and for any of 0 and 1, canceled if both fail.
		_, ids := a.insert("satisfied", 4,
			driver.JobDependency{Job: 3, DependsOn: 2},
			driver.JobDependency{Job: 3, DependsOn: 0, AnyOf: 1},
			driver.JobDependency{Job: 3, DependsOn: 1, AnyOf: 1})
		a.claimAll()
		a.finish(ids[0], driver.JobStateCompleted)
		a.finish(ids[1], driver.JobStateDiscarded)
		a.wantStates("the group satisfied, then a member failed", map[driver.JobID]driver.JobState{ids[3]: driver.JobStatePending})
		a.finish(ids[2], driver.JobStateCompleted)
		a.wantStates("the ordinary dependency completed", map[driver.JobID]driver.JobState{ids[3]: driver.JobStateAvailable})
		a.claimAll()
	})

	t.Run("two groups", func(t *testing.T) {
		a := a
		a.t = t
		// 4 waits for any of 0 and 1, and any of 2 and 3.
		_, ids := a.insert("groups", 5,
			driver.JobDependency{Job: 4, DependsOn: 0, AnyOf: 1},
			driver.JobDependency{Job: 4, DependsOn: 1, AnyOf: 1},
			driver.JobDependency{Job: 4, DependsOn: 2, AnyOf: 2},
			driver.JobDependency{Job: 4, DependsOn: 3, AnyOf: 2})
		a.claimAll()
		a.finish(ids[0], driver.JobStateCompleted)
		a.wantStates("one group satisfied", map[driver.JobID]driver.JobState{ids[4]: driver.JobStatePending})
		a.finish(ids[2], driver.JobStateDiscarded)
		a.wantStates("the other's member failed", map[driver.JobID]driver.JobState{ids[4]: driver.JobStatePending})
		a.finish(ids[3], driver.JobStateDiscarded)
		a.wantStates("the other group exhausted", map[driver.JobID]driver.JobState{ids[4]: driver.JobStateCanceled, ids[1]: driver.JobStateRunning})
		a.finish(ids[1], driver.JobStateCompleted)
	})

	if _, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
		Jobs: params("bad", 3),
		Deps: []driver.JobDependency{
			{Job: 2, DependsOn: 0, AnyOf: 1, OnFailure: driver.DependencyCancel},
			{Job: 2, DependsOn: 1, AnyOf: 1, OnFailure: driver.DependencyIgnore},
		},
	}); err == nil {
		t.Error("an any-of group whose edges have different policies was inserted")
	}
	if _, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
		Jobs: params("bad", 2),
		Deps: []driver.JobDependency{{Job: 1, DependsOn: 0, AnyOf: 1 << 15}},
	}); err == nil {
		t.Error("a group number out of range was inserted")
	}
}

// testWorkflowAnyOfConcurrent finalizes jobs of an any-of group in
// concurrent transactions. On an engine that locks rows, each statement
// sees the other job live: the last two jobs of a group that cancels its
// dependent fail, and neither cancels it; a job of a group completes while
// the dependent's ordinary dependency does, and neither promotes it.
// WorkflowPromoteReady, run by each finalizing client after its statement
// commits, does, once. On an engine that runs one writer at a time, the
// second finalize waits for the first and does it itself.
func testWorkflowAnyOfConcurrent[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	a := anyOfFixture{ctx: ctx, t: t, exec: exec, clientID: register(ctx, t, exec)}
	promote := func(dependsOn ...driver.JobID) driver.WorkflowPromoteResult {
		t.Helper()
		res, err := exec.WorkflowPromoteReady(ctx, driver.WorkflowPromoteParams{DependsOn: dependsOn, Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	// race finalizes first in a transaction and second outside it, and
	// returns once both have committed, reporting what a follow-up of the
	// second, before the first committed, did.
	race := func(first, second driver.JobFinalize) driver.WorkflowPromoteResult {
		t.Helper()
		first.AttemptedBy, second.AttemptedBy = a.clientID, a.clientID
		tx, commit, rollback := f.Begin(ctx, t, d)
		defer rollback() //nolint:errcheck // committed below
		if applied := finalizeCurrent(ctx, t, d.UnwrapTx(tx), first); len(applied) != 1 {
			t.Fatalf("finalize in tx applied %v", applied)
		}
		second = withFences(ctx, t, exec, []driver.JobFinalize{second})[0]
		if singleWriter(d) {
			done := make(chan error, 1)
			go func() {
				_, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{second}})
				done <- err
			}()
			f.WaitForLockWait(ctx, t, d)
			if err := commit(); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			return driver.WorkflowPromoteResult{}
		}
		fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if applied, err := exec.JobFinalizeMany(fctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{second}}); err != nil || len(applied) != 1 {
			t.Fatalf("concurrent finalize = %v, %v", applied, err)
		}
		res := promote(second.ID)
		if err := commit(); err != nil {
			t.Fatal(err)
		}
		return res
	}

	t.Run("exhausted", func(t *testing.T) {
		a := a
		a.t = t
		_, ids := a.insert("exhausted", 3,
			driver.JobDependency{Job: 2, DependsOn: 0, AnyOf: 1},
			driver.JobDependency{Job: 2, DependsOn: 1, AnyOf: 1})
		a.claimAll()
		early := race(driver.JobFinalize{ID: ids[0], State: driver.JobStateDiscarded, Archive: true},
			driver.JobFinalize{ID: ids[1], State: driver.JobStateDiscarded, Archive: true})
		if !singleWriter(d) {
			if early.Canceled != 0 {
				t.Errorf("canceled %d while a member's finalize was uncommitted", early.Canceled)
			}
			a.wantStates("both members failed concurrently", map[driver.JobID]driver.JobState{ids[2]: driver.JobStatePending})
			if res := promote(ids[0]); res.Canceled != 1 || res.Promoted != 0 {
				t.Errorf("follow-up = %+v, want the dependent canceled", res)
			}
		}
		a.wantStates("after the follow-up", map[driver.JobID]driver.JobState{ids[2]: driver.JobStateCanceled})
		if res := promote(ids[0], ids[1]); res.Canceled != 0 || res.Promoted != 0 {
			t.Errorf("second follow-up = %+v", res)
		}
	})

	t.Run("satisfied", func(t *testing.T) {
		a := a
		a.t = t
		// 3 waits for 2 and any of 0 and 1.
		_, ids := a.insert("satisfied", 4,
			driver.JobDependency{Job: 3, DependsOn: 2},
			driver.JobDependency{Job: 3, DependsOn: 0, AnyOf: 1},
			driver.JobDependency{Job: 3, DependsOn: 1, AnyOf: 1})
		a.claimAll()
		early := race(driver.JobFinalize{ID: ids[0], State: driver.JobStateCompleted, Archive: true},
			driver.JobFinalize{ID: ids[2], State: driver.JobStateCompleted, Archive: true})
		if !singleWriter(d) {
			if early.Promoted != 0 {
				t.Errorf("promoted %d while a dependency's finalize was uncommitted", early.Promoted)
			}
			a.wantStates("member and dependency completed concurrently", map[driver.JobID]driver.JobState{ids[3]: driver.JobStatePending})
			if res := promote(ids[0]); res.Promoted != 1 {
				t.Errorf("follow-up = %+v, want the dependent promoted", res)
			}
		}
		a.wantStates("after the follow-up", map[driver.JobID]driver.JobState{ids[3]: driver.JobStateAvailable, ids[1]: driver.JobStateRunning})
		if res := promote(ids[0], ids[2]); res.Promoted != 0 {
			t.Errorf("second follow-up = %+v", res)
		}
		// The group's other job fails later; the dependent stays as it is.
		a.finish(ids[1], driver.JobStateDiscarded)
		if res := promote(ids[1]); res.Promoted != 0 || res.Canceled != 0 {
			t.Errorf("follow-up of the late member = %+v", res)
		}
		a.wantStates("the late member failed", map[driver.JobID]driver.JobState{ids[3]: driver.JobStateAvailable})
	})

	// A group exhausted by a job that a cascade canceled: 1 waits for 0,
	// and 2 for any of 1, so 0's failure cancels 1 and then 2. The
	// finalize returns 1 among the jobs it canceled by cascade, and the
	// finalizing client's follow-up, which takes those as it takes the
	// jobs it finalized, cancels 2 if the statement did not, with no
	// leader's sweep: a follow-up of 0 alone finds no dependent, 1's edge
	// to it gone with 1.
	cascaded := func(a anyOfFixture) (dependent driver.JobID, followUp []driver.JobID) {
		a.t.Helper()
		_, ids := a.insert("cascaded", 3,
			driver.JobDependency{Job: 1, DependsOn: 0},
			driver.JobDependency{Job: 2, DependsOn: 1, AnyOf: 1})
		a.claimAll()
		result := driver.JobFinalize{ID: ids[0], AttemptedBy: a.clientID, State: driver.JobStateDiscarded, Archive: true}
		out, err := a.exec.JobFinalizeMany(a.ctx, driver.JobFinalizeParams{Jobs: withFences(a.ctx, a.t, a.exec, []driver.JobFinalize{result})})
		if err != nil {
			a.t.Fatal(err)
		}
		if got := appliedIDs(out); len(got) != 1 || got[0] != ids[0] {
			a.t.Fatalf("finalize of 0 applied %v", got)
		}
		if got := cascadedIDs(out); !slices.Contains(got, ids[1]) {
			a.t.Fatalf("finalize of 0 canceled %v by cascade, want 1 among them", got)
		}
		return ids[2], append(appliedIDs(out), cascadedIDs(out)...)
	}
	t.Run("cascaded follow-up", func(t *testing.T) {
		a := a
		a.t = t
		dependent, followUp := cascaded(a)
		if _, err := exec.WorkflowPromoteReady(ctx, driver.WorkflowPromoteParams{DependsOn: followUp}); err != nil {
			t.Fatal(err)
		}
		a.wantStates("after the follow-up", map[driver.JobID]driver.JobState{dependent: driver.JobStateCanceled})
	})
	t.Run("sweep", func(t *testing.T) {
		a := a
		a.t = t
		// Without the follow-up, the leader's sweep cancels it.
		dependent, _ := cascaded(a)
		for range 3 {
			if a.state(dependent) != driver.JobStatePending {
				break
			}
			if _, err := exec.WorkflowPromoteReady(ctx, driver.WorkflowPromoteParams{Limit: 1000}); err != nil {
				t.Fatal(err)
			}
		}
		a.wantStates("after the sweep", map[driver.JobID]driver.JobState{dependent: driver.JobStateCanceled})
	})
}

// testWorkflowsAnyOfConcurrent runs workflows with an any-of group of four
// jobs after a root, and a step after the group's dependent, with eight
// workers at once, each finalizing what it claimed in one statement and
// following up as a client does, while the leader's sweeps run. A
// workflow's group has every job complete, one, or none; its dependent
// cancels or ignores an exhausted group. Every job is finalized once, the
// dependent is never claimed before a job of its group completed (or every
// job of an ignored group finished), it is canceled exactly when its
// group failed and cancels it, and every workflow completes.
func testWorkflowsAnyOfConcurrent[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	const workflows, members = 90, 4
	type wf struct {
		members    []driver.JobID
		dependent  driver.JobID
		after      driver.JobID
		completing int
		policy     driver.DependencyFailure
	}
	var (
		wfs        []wf
		dependents = map[driver.JobID]int{}
		batchIDs   []driver.JobID
	)
	for w := range workflows {
		jobs := params("root", 1)
		completing := []int{members, 1, 0}[w%3]
		for m := range members {
			kind := "ok"
			if m >= completing {
				kind = "fail"
			}
			jobs = append(jobs, params(kind, 1)...)
		}
		jobs = append(jobs, params("dependent", 1)...)
		jobs = append(jobs, params("after", 1)...)
		policy := []driver.DependencyFailure{driver.DependencyCancel, driver.DependencyIgnore}[(w/3)%2]
		var deps []driver.JobDependency
		for m := 1; m <= members; m++ {
			deps = append(deps, driver.JobDependency{Job: m, DependsOn: 0},
				driver.JobDependency{Job: members + 1, DependsOn: m, AnyOf: 1, OnFailure: policy})
		}
		deps = append(deps, driver.JobDependency{Job: members + 2, DependsOn: members + 1})
		res, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{Name: "any-chaos", Jobs: jobs, Deps: deps})
		if err != nil {
			t.Fatal(err)
		}
		batchIDs = append(batchIDs, res.ID)
		x := wf{dependent: res.Jobs[members+1].Job.ID, after: res.Jobs[members+2].Job.ID, completing: completing, policy: policy}
		for m := 1; m <= members; m++ {
			x.members = append(x.members, res.Jobs[m].Job.ID)
		}
		dependents[x.dependent] = len(wfs)
		wfs = append(wfs, x)
	}
	stateOf := func(id driver.JobID) (driver.JobState, bool, error) {
		j, err := exec.JobGet(ctx, id)
		if err != nil {
			return "", false, err
		}
		return j.State, j.FinalizedAt.IsZero(), nil
	}

	wctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	var (
		mu      sync.Mutex
		applied = map[driver.JobID]int{}
		errs    = make(chan error, 16)
		wg      sync.WaitGroup
	)
	fail := func(err error) {
		if wctx.Err() == nil {
			select {
			case errs <- err:
			default:
			}
			cancel()
		}
	}
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
					k, ok := dependents[j.ID]
					if !ok {
						continue
					}
					completed, live := 0, 0
					for _, m := range wfs[k].members {
						s, isLive, err := stateOf(m)
						if err != nil {
							fail(err)
							return
						}
						if isLive {
							live++
						} else if s == driver.JobStateCompleted {
							completed++
						}
					}
					if completed == 0 && (live > 0 || wfs[k].policy == driver.DependencyCancel) {
						fail(fmt.Errorf("claimed dependent %s with no member completed and %d live", j.ID, live))
						return
					}
				}
				var ids, cascaded []driver.JobID
				for wctx.Err() == nil {
					sctx, scancel := context.WithTimeout(ctx, 10*time.Second)
					out, err := exec.JobFinalizeMany(sctx, driver.JobFinalizeParams{Jobs: fin})
					scancel()
					if err == nil {
						ids, cascaded = appliedIDs(out), cascadedIDs(out)
						break
					}
					time.Sleep(time.Millisecond)
				}
				mu.Lock()
				for _, id := range ids {
					applied[id]++
				}
				mu.Unlock()
				if len(ids) == 0 {
					continue
				}
				_, _ = exec.BatchSettle(wctx, driver.BatchSettleParams{IDs: batches})
				_, _ = exec.WorkflowPromoteReady(wctx, driver.WorkflowPromoteParams{DependsOn: append(ids, cascaded...)})
			}
		})
	}
	wg.Go(func() {
		var settleAfter, promoteAfter driver.JobID
		for wctx.Err() == nil {
			if s, err := exec.BatchSettle(wctx, driver.BatchSettleParams{After: settleAfter, Limit: 16}); err == nil {
				settleAfter = s.Last
				if s.Folded < 16 {
					settleAfter = driver.JobID{}
				}
			}
			if p, err := exec.WorkflowPromoteReady(wctx, driver.WorkflowPromoteParams{After: promoteAfter, Limit: 16}); err == nil {
				promoteAfter = p.Last
				if p.Scanned < 16 {
					promoteAfter = driver.JobID{}
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
	})
	done := func() bool {
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
		t.Fatal("not finished in a minute")
	}
	for id, n := range applied {
		if n != 1 {
			t.Errorf("job %s applied %d times", id, n)
		}
	}
	for _, x := range wfs {
		want, wantAfter := driver.JobStateCompleted, driver.JobStateCompleted
		if x.completing == 0 && x.policy == driver.DependencyCancel {
			want, wantAfter = driver.JobStateCanceled, driver.JobStateCanceled
		}
		for id, s := range map[driver.JobID]driver.JobState{x.dependent: want, x.after: wantAfter} {
			got, _, err := stateOf(id)
			if errors.Is(err, driver.ErrNotFound) || err == nil && got != s {
				t.Errorf("workflow with %d completing members and policy %s: job %s is %s, want %s", x.completing, x.policy, id, got, s)
			} else if err != nil {
				t.Fatal(err)
			}
		}
	}
	if n := f.CountEdges(ctx, t, d); n != 0 {
		t.Errorf("edges left = %d, want 0", n)
	}
}

// testWorkflowAnyOfHeld finalizes two jobs of one any-of group, each of
// which satisfies it: one in a caller's transaction held open, as
// CompleteTx does, the other in a client's flush. A finalize does not
// promote a dependent with a group, so the flush, and its follow-up, which
// skips rows another transaction holds, do not wait for the held one; the
// follow-up promotes the dependent, once, whether the transaction then
// commits or rolls back, and the held finalize's own follow-up after its
// commit finds nothing more to do. On an engine that runs one writer at a
// time, the flush waits for the transaction instead, as any write does.
func testWorkflowAnyOfHeld[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	if singleWriter(d) {
		t.Skip("one writer at a time: a write waits for a transaction held open")
	}
	exec := d.Executor()
	clientID := register(ctx, t, exec)
	for _, end := range []string{"commit", "rollback"} {
		t.Run(end, func(t *testing.T) {
			wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{
				Name: "held", Jobs: params("held", 3),
				Deps: []driver.JobDependency{{Job: 2, DependsOn: 0, AnyOf: 1}, {Job: 2, DependsOn: 1, AnyOf: 1}},
			})
			if err != nil {
				t.Fatal(err)
			}
			x, y, dep := wf.Jobs[0].Job.ID, wf.Jobs[1].Job.ID, wf.Jobs[2].Job.ID
			// The claim takes the previous subtest's dependent too.
			claimed := map[driver.JobID]bool{}
			for _, j := range claim(ctx, t, exec, clientID, 10) {
				claimed[j.ID] = true
			}
			if !claimed[x] || !claimed[y] {
				t.Fatalf("claimed %v, want both jobs of the group", claimed)
			}
			completed := func(id driver.JobID) driver.JobFinalize {
				return driver.JobFinalize{ID: id, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true}
			}
			var promoted int64
			promote := func(c context.Context, dependsOn ...driver.JobID) error {
				res, err := exec.WorkflowPromoteReady(c, driver.WorkflowPromoteParams{DependsOn: dependsOn})
				promoted += res.Promoted
				return err
			}

			tx, commit, rollback := f.Begin(ctx, t, d)
			defer rollback() //nolint:errcheck // already ended on the happy path
			if applied := finalizeCurrent(ctx, t, d.UnwrapTx(tx), completed(x)); len(applied) != 1 {
				t.Fatalf("finalize in the transaction applied %v", applied)
			}
			fctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			out, err := exec.JobFinalizeMany(fctx, driver.JobFinalizeParams{Jobs: withFences(ctx, t, exec, []driver.JobFinalize{completed(y)})})
			if err != nil || len(appliedIDs(out)) != 1 {
				t.Fatalf("finalize of the other job of the group while the transaction is held open = %v, %v; want it not to wait", out, err)
			}
			if err := promote(fctx, y); err != nil {
				t.Fatalf("follow-up while the transaction is held open: %v; want it not to wait", err)
			}
			if j, err := exec.JobGet(ctx, dep); err != nil || j.State != driver.JobStateAvailable {
				t.Fatalf("dependent after the follow-up = %+v, %v; want it promoted", j, err)
			}
			if end == "commit" {
				if err := commit(); err != nil {
					t.Fatal(err)
				}
				if err := promote(ctx, x); err != nil {
					t.Fatal(err)
				}
			} else if err := rollback(); err != nil {
				t.Fatal(err)
			}
			if _, err := exec.WorkflowPromoteReady(ctx, driver.WorkflowPromoteParams{Limit: 1000}); err != nil {
				t.Fatal(err)
			}
			if promoted != 1 {
				t.Errorf("follow-ups promoted %d, want 1", promoted)
			}
			if j, err := exec.JobGet(ctx, dep); err != nil || j.State != driver.JobStateAvailable {
				t.Errorf("dependent = %+v, %v; want it available", j, err)
			}
		})
	}
}
