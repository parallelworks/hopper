package drivertest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// testJobLogs appends attempt logs with retries, snoozes and finalizes,
// keeping the latest KeepLogs, and checks that they go to history with the
// job, come back with a JobRetry from history, and are read by JobLogs.
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
		l, err := exec.JobLogs(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	check := func(what string, got []driver.AttemptLog, want ...driver.AttemptLog) {
		t.Helper()
		if !slices.Equal(got, want) {
			t.Errorf("%s: logs %+v, want %+v", what, got, want)
		}
	}
	a1 := driver.AttemptLog{Attempt: 1, Log: "level=INFO msg=one\n"}
	a2 := driver.AttemptLog{Attempt: 2, Log: "level=INFO msg=two\nlevel=WARN msg=more\n"}
	a2again := driver.AttemptLog{Attempt: 2, Log: "level=INFO msg=again\n"}
	a3 := driver.AttemptLog{Attempt: 3, Log: "nul \x00 replaced\n"}
	a4 := driver.AttemptLog{Attempt: 4, Log: "four\n"}

	check("before any attempt", logs(id))
	run(id, driver.JobFinalize{State: driver.JobStateRetryable, Error: &driver.AttemptError{Attempt: 1, Error: "x"}, Log: &a1}, 2, false)
	check("after a retry", logs(id), a1)
	check("a job that logged nothing", logs(other))
	// A snooze gives the attempt back, and the next snooze of the same
	// attempt number replaces its log.
	run(id, driver.JobFinalize{State: driver.JobStateScheduled, Snooze: true, Log: &a2}, 2, true)
	s2 := a2
	s2.Snoozed = true
	check("after a snooze", logs(id), a1, s2)
	run(id, driver.JobFinalize{State: driver.JobStateScheduled, Snooze: true, Log: &a2again}, 2, false)
	s2.Log = a2again.Log
	check("after another snooze", logs(id), a1, s2)
	// A snooze that logs nothing replaces it too.
	run(id, driver.JobFinalize{State: driver.JobStateScheduled, Snooze: true}, 2, false)
	check("after a snooze that logged nothing", logs(id), a1)
	run(id, driver.JobFinalize{State: driver.JobStateScheduled, Snooze: true, Log: &a2again}, 2, false)
	check("after a snooze that logged again", logs(id), a1, s2)
	run(id, driver.JobFinalize{State: driver.JobStateRetryable, Error: &driver.AttemptError{Attempt: 2, Error: "x"}}, 2, false)
	check("after an attempt that logged nothing", logs(id), a1, s2)
	run(id, driver.JobFinalize{State: driver.JobStateRetryable, Error: &driver.AttemptError{Attempt: 3, Error: "x"}, Log: &a3}, 2, false)
	stored := driver.AttemptLog{Attempt: 3, Log: "nul � replaced\n"}
	check("past KeepLogs", logs(id), s2, stored)

	// Into history and back, with KeepLogs zero keeping them all.
	run(id, driver.JobFinalize{State: driver.JobStateCompleted, Archive: true, Log: &a4}, 0, false)
	check("in history", logs(id), s2, stored, a4)
	if _, err := exec.JobRetry(ctx, driver.JobRetryParams{ID: id}); err != nil {
		t.Fatal(err)
	}
	check("after a JobRetry from history", logs(id), s2, stored, a4)

	// A canceled job takes them to history too.
	if _, err := exec.JobCancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	if j, err := exec.JobGet(ctx, id); err != nil || j.State != driver.JobStateCanceled {
		t.Fatalf("canceled job = %+v, %v", j, err)
	}
	check("canceled", logs(id), s2, stored, a4)

	// A snooze that logs nothing leaves a job without logs as it is, and
	// takes one whose only log is a snooze's back to none.
	noLogs := func(what string) {
		t.Helper()
		if got := f.QueryText(ctx, t, d, "SELECT coalesce(logs::text, 'NULL') FROM hopper_jobs WHERE id = $1::uuid", other.String()); got != "NULL" {
			t.Errorf("logs %s = %s, want NULL", what, got)
		}
	}
	run(other, driver.JobFinalize{State: driver.JobStateScheduled, Snooze: true}, 2, false)
	noLogs("after a snooze that logged nothing")
	attempt, err := strconv.Atoi(f.QueryText(ctx, t, d, "SELECT attempt::text FROM hopper_jobs WHERE id = $1::uuid", other.String()))
	if err != nil {
		t.Fatal(err)
	}
	run(other, driver.JobFinalize{State: driver.JobStateScheduled, Snooze: true, Log: &driver.AttemptLog{Attempt: attempt + 1, Log: "x\n"}}, 2, false)
	run(other, driver.JobFinalize{State: driver.JobStateScheduled, Snooze: true}, 2, false)
	noLogs("after a snooze that logged nothing removed the only log")

	// An expired job takes its logs to history too.
	run(other, driver.JobFinalize{State: driver.JobStateRetryable, Error: &driver.AttemptError{Attempt: 1, Error: "x"}, Log: &a1}, 2, false)
	f.Exec(ctx, t, d, "UPDATE hopper_jobs SET expires_at = now() - interval '1s' WHERE id = $1::uuid", other.String())
	if expired, err := exec.JobDiscardExpired(ctx, 10); err != nil || len(expired) != 1 || expired[0].ID != other {
		t.Fatalf("expired = %v, %v", expired, err)
	}
	check("expired", logs(other), a1)

	if _, err := exec.JobLogs(ctx, driver.JobID{9}); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("JobLogs of an unknown job = %v", err)
	}
}

// testJobLogsFenced finalizes a job from its attempt and, concurrently,
// from clients that do not own it, such as one whose lease lapsed: only the
// owner's log is appended, once, and only the owner's snooze that logged
// nothing removes the log of the snooze before it.
func testJobLogsFenced[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	for round := range 30 {
		id := insert(ctx, t, exec, params("k", 1))[0].Job.ID
		owner := register(ctx, t, exec)
		if jobs := claim(ctx, t, exec, owner, 1); len(jobs) != 1 || jobs[0].ID != id {
			t.Fatal("claim")
		}
		state := []driver.JobState{driver.JobStateRetryable, driver.JobStateCompleted, driver.JobStateScheduled}[round%3]
		snooze := state == driver.JobStateScheduled
		if snooze {
			log := driver.AttemptLog{Attempt: 1, Log: "first snooze\n"}
			if ids, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{{
				ID: id, AttemptedBy: owner, State: state, Snooze: true, Log: &log,
			}}}); err != nil || len(ids) != 1 {
				t.Fatalf("round %d: first snooze = %v, %v", round, ids, err)
			}
			f.MakeClaimable(ctx, t, d, id)
			if jobs := claim(ctx, t, exec, owner, 1); len(jobs) != 1 || jobs[0].ID != id {
				t.Fatal("claim after the snooze")
			}
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
				r := driver.JobFinalize{ID: id, AttemptedBy: by, State: state, Delay: time.Hour, Snooze: snooze}
				if !snooze {
					r.Archive, r.Error = true, &driver.AttemptError{Attempt: 1, Error: "x"}
					r.Log = &driver.AttemptLog{Attempt: 1, Log: fmt.Sprintf("client %d\n", by)}
				}
				ids, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{r}})
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
		got, err := exec.JobLogs(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		want := []driver.AttemptLog{{Attempt: 1, Log: fmt.Sprintf("client %d\n", owner)}}
		if snooze {
			want = nil
		}
		if !slices.Equal(got, want) {
			t.Fatalf("round %d: logs %+v, want %+v", round, got, want)
		}
	}
}
