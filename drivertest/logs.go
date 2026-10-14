package drivertest

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// testJobLogs appends attempt logs with retries, snoozes and finalizes,
// keeping the latest KeepLogs, and checks that they go to history with the
// job, come back with a JobRetry from history, and are read by JobGet only.
func testJobLogs[TTx any](t *testing.T, f Fixture[TTx], noBatches bool) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := executor(d, noBatches)
	p := params("k", 2)
	for i := range p {
		p[i].MaxAttempts = 10
	}
	inserted := insert(ctx, t, exec, p)
	id, other := inserted[0].Job.ID, inserted[1].Job.ID
	clientID := register(ctx, t, exec)
	run := func(id driver.JobID, result driver.JobFinalize, keep int, skipLocked bool) {
		t.Helper()
		f.MakeClaimable(ctx, t, d, id)
		jobs := claim(ctx, t, exec, clientID, 2)
		if !slices.ContainsFunc(jobs, func(j *driver.JobRow) bool { return j.ID == id }) {
			t.Fatalf("claim of %s = %d jobs", id, len(jobs))
		}
		if len(jobs[0].Logs) > 0 {
			t.Errorf("the claim read logs: %+v", jobs[0].Logs)
		}
		var results []driver.JobFinalize
		for _, j := range jobs {
			r := driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateRetryable}
			if j.ID == id {
				r = result
				r.ID, r.AttemptedBy = id, clientID
			}
			results = append(results, r)
		}
		applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: results, KeepLogs: keep, SkipLocked: skipLocked})
		if err != nil || len(applied) != len(results) {
			t.Fatalf("finalize = %v, %v", applied, err)
		}
	}
	logs := func(id driver.JobID) []driver.AttemptLog {
		t.Helper()
		j, err := exec.JobGet(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return j.Logs
	}
	check := func(what string, got []driver.AttemptLog, want ...driver.AttemptLog) {
		t.Helper()
		if !slices.Equal(got, want) {
			t.Errorf("%s: logs %+v, want %+v", what, got, want)
		}
	}
	a1 := driver.AttemptLog{Attempt: 1, Log: "level=INFO msg=one\n"}
	a2 := driver.AttemptLog{Attempt: 2, Log: "level=INFO msg=two\nlevel=WARN msg=more\n"}
	a3 := driver.AttemptLog{Attempt: 3, Log: "nul \x00 replaced\n"}
	a4 := driver.AttemptLog{Attempt: 4, Log: "four\n"}

	check("before any attempt", logs(id))
	run(id, driver.JobFinalize{State: driver.JobStateRetryable, Error: &driver.AttemptError{Attempt: 1, Error: "x"}, Log: &a1}, 2, false)
	check("after a retry", logs(id), a1)
	check("a job that logged nothing", logs(other))
	run(id, driver.JobFinalize{State: driver.JobStateScheduled, Snooze: true, Log: &a2}, 2, true)
	check("after a snooze", logs(id), a1, a2)
	run(id, driver.JobFinalize{State: driver.JobStateRetryable, Error: &driver.AttemptError{Attempt: 2, Error: "x"}}, 2, false)
	check("after an attempt that logged nothing", logs(id), a1, a2)
	run(id, driver.JobFinalize{State: driver.JobStateRetryable, Error: &driver.AttemptError{Attempt: 3, Error: "x"}, Log: &a3}, 2, false)
	stored := driver.AttemptLog{Attempt: 3, Log: "nul � replaced\n"}
	check("past KeepLogs", logs(id), a2, stored)

	// Into history and back, with KeepLogs zero keeping them all.
	run(id, driver.JobFinalize{State: driver.JobStateCompleted, Archive: true, Log: &a4}, 0, false)
	check("in history", logs(id), a2, stored, a4)
	if _, err := exec.JobRetry(ctx, driver.JobRetryParams{ID: id}); err != nil {
		t.Fatal(err)
	}
	check("after a JobRetry from history", logs(id), a2, stored, a4)
	list, err := exec.JobList(ctx, driver.JobListParams{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range list {
		if len(j.Logs) > 0 {
			t.Errorf("JobList read the logs of %s", j.ID)
		}
	}

	// A canceled job takes them to history too.
	if _, err := exec.JobCancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	if j, err := exec.JobGet(ctx, id); err != nil || j.State != driver.JobStateCanceled {
		t.Fatalf("canceled job = %+v, %v", j, err)
	}
	check("canceled", logs(id), a2, stored, a4)
}

// testJobLogsFenced finalizes a job from its attempt and, concurrently,
// from clients that do not own it, such as one whose lease lapsed: only the
// owner's log is appended, once.
func testJobLogsFenced[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	for round := range 20 {
		id := insert(ctx, t, exec, params("k", 1))[0].Job.ID
		owner := register(ctx, t, exec)
		if jobs := claim(ctx, t, exec, owner, 1); len(jobs) != 1 || jobs[0].ID != id {
			t.Fatal("claim")
		}
		state := driver.JobStateRetryable
		if round%2 == 1 {
			state = driver.JobStateCompleted
		}
		const stale = 4
		var wg sync.WaitGroup
		applied := make([]int, stale+1)
		for i := range stale + 1 {
			by := owner
			if i > 0 {
				by = owner + int64(i)*1000
			}
			wg.Go(func() {
				log := driver.AttemptLog{Attempt: 1, Log: fmt.Sprintf("client %d\n", by)}
				ids, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{
					Jobs: []driver.JobFinalize{{
						ID: id, AttemptedBy: by, State: state, Delay: time.Hour, Archive: true, Log: &log,
						Error: &driver.AttemptError{Attempt: 1, Error: "x"},
					}},
				})
				if err != nil {
					t.Error(err)
				}
				applied[i] = len(ids)
			})
		}
		wg.Wait()
		if applied[0] != 1 || slices.Max(applied[1:]) != 0 {
			t.Fatalf("round %d: applied %v, want only the owner's", round, applied)
		}
		j, err := exec.JobGet(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		want := []driver.AttemptLog{{Attempt: 1, Log: fmt.Sprintf("client %d\n", owner)}}
		if !slices.Equal(j.Logs, want) {
			t.Fatalf("round %d: logs %+v, want %+v", round, j.Logs, want)
		}
	}
}
