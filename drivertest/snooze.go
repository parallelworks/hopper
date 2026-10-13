package drivertest

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

func testSnoozeReason[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	p := params("k", 1)
	p[0].UniqueKey = "nap"
	id := insert(ctx, t, exec, p)[0].Job.ID
	clientID := register(ctx, t, exec)

	get := func() *driver.JobRow {
		t.Helper()
		j, err := exec.JobGet(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return j
	}
	claimIt := func() {
		t.Helper()
		f.MakeClaimable(ctx, t, d, id)
		jobs := claim(ctx, t, exec, clientID, 1)
		if len(jobs) != 1 || jobs[0].ID != id {
			t.Fatalf("claim = %+v", jobs)
		}
		if jobs[0].SnoozeReason != "" {
			t.Errorf("claimed job has snooze reason %q", jobs[0].SnoozeReason)
		}
		if j := get(); j.SnoozeReason != "" {
			t.Errorf("running job has snooze reason %q", j.SnoozeReason)
		}
	}
	snooze := func(reason string) {
		t.Helper()
		claimIt()
		if applied := finalize(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: clientID, State: driver.JobStateScheduled, Delay: time.Hour, Snooze: true, SnoozeReason: reason}); len(applied) != 1 {
			t.Fatal("snooze not applied")
		}
		if j := get(); j.State != driver.JobStateScheduled || j.SnoozeReason != reason {
			t.Fatalf("snoozed job = %+v, want reason %q", j, reason)
		}
	}

	snooze("waiting for upstream")
	jobs, err := exec.JobList(ctx, driver.JobListParams{States: []driver.JobState{driver.JobStateScheduled}, Limit: 10})
	if err != nil || len(jobs) != 1 || jobs[0].SnoozeReason != "waiting for upstream" {
		t.Errorf("JobList = %+v, %v", jobs, err)
	}

	// A snooze without a reason leaves none behind.
	snooze("")

	// A retry clears the reason; so does the claim, checked by claimIt.
	snooze("first")
	claimIt()
	finalize(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: clientID, State: driver.JobStateRetryable, Delay: time.Hour, Error: &driver.AttemptError{Attempt: 1, Error: "boom"}})
	if j := get(); j.State != driver.JobStateRetryable || j.SnoozeReason != "" {
		t.Errorf("retryable job = %+v", j)
	}
	// A reason given with a result that is not a snooze is not stored.
	claimIt()
	finalize(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: clientID, State: driver.JobStateRetryable, Delay: time.Hour, SnoozeReason: "ignored"})
	if j := get(); j.SnoozeReason != "" {
		t.Errorf("retryable job kept reason %q", j.SnoozeReason)
	}

	// A fenced result does not touch the reason.
	snooze("second")
	if applied := finalize(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: clientID, State: driver.JobStateScheduled, Snooze: true, SnoozeReason: "stale"}); len(applied) != 0 {
		t.Fatal("finalize of a waiting job applied")
	}
	if j := get(); j.SnoozeReason != "second" {
		t.Errorf("reason after a fenced result = %q", j.SnoozeReason)
	}

	// JobRetry runs the job now, so the reason no longer applies.
	if j, err := exec.JobRetry(ctx, id); err != nil || j.State != driver.JobStateAvailable || j.SnoozeReason != "" {
		t.Errorf("JobRetry = %+v, %v", j, err)
	}

	// Neither does a replacing insert's schedule, while a skipped one keeps it.
	snooze("third")
	res, err := exec.JobInsertMany(ctx, p, driver.JobInsertOpts{})
	if err != nil || !res[0].Duplicate || res[0].Job.SnoozeReason != "third" {
		t.Errorf("skipped insert = %+v, %v", res, err)
	}
	res, err = exec.JobInsertMany(ctx, p, driver.JobInsertOpts{OnConflict: driver.ConflictReplace})
	if err != nil || !res[0].Duplicate || res[0].Job.SnoozeReason != "" {
		t.Errorf("replacing insert = %+v, %v", res, err)
	}

	// A finalized job is not waiting.
	snooze("fourth")
	if j, err := exec.JobCancel(ctx, id); err != nil || j.State != driver.JobStateCancelled || j.SnoozeReason != "" {
		t.Errorf("JobCancel = %+v, %v", j, err)
	}
	if j := get(); j.SnoozeReason != "" {
		t.Errorf("history row has reason %q", j.SnoozeReason)
	}
}

// testSnoozeReasonConcurrent has clients claim the same jobs and finalize
// them as snoozes with reasons or as retries, while an idle client sends
// stale results. The claim must clear a job's reason in the same statement
// that takes it, and a reason must only ever come from the result of the
// claim that holds the job: so a snoozed job's reason names its latest
// claim, and a retried job has none.
func testSnoozeReasonConcurrent[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
	p := params("k", 64)
	for i := range p {
		p[i].MaxAttempts = 10000
	}
	inserted := insert(ctx, t, exec, p)
	stale := register(ctx, t, exec)

	const clients, rounds = 4, 60
	var wg sync.WaitGroup
	for c := range clients {
		clientID := register(ctx, t, exec)
		wg.Go(func() {
			for round := range rounds {
				res, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 8})
				if err != nil {
					t.Error(err)
					return
				}
				results := make([]driver.JobFinalize, 0, 2*len(res.Jobs))
				for i, j := range res.Jobs {
					if j.SnoozeReason != "" {
						t.Errorf("claim returned job %s with reason %q", j.ID, j.SnoozeReason)
					}
					if (c+round+i)%3 == 0 {
						results = append(results, driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateRetryable})
					} else {
						reason := fmt.Sprintf("client %d claim %d", clientID, j.AttemptedAt.UnixMicro())
						results = append(results, driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateScheduled, Snooze: true, SnoozeReason: reason})
					}
					results = append(results, driver.JobFinalize{ID: j.ID, AttemptedBy: stale, State: driver.JobStateScheduled, Snooze: true, SnoozeReason: "stale"})
				}
				applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: results})
				if err != nil {
					t.Error(err)
					return
				}
				if len(applied) != len(res.Jobs) {
					t.Errorf("applied %d results for %d claimed jobs", len(applied), len(res.Jobs))
				}
			}
		})
	}
	wg.Wait()

	for _, r := range inserted {
		j, err := exec.JobGet(ctx, r.Job.ID)
		if err != nil {
			t.Fatal(err)
		}
		switch j.State {
		case driver.JobStateScheduled:
			claimAt, ok := strings.CutPrefix(j.SnoozeReason, "client ")
			_, at, _ := strings.Cut(claimAt, " claim ")
			if !ok || at != strconv.FormatInt(j.AttemptedAt.UnixMicro(), 10) {
				t.Errorf("snoozed job %s: reason %q does not name its latest claim at %d", j.ID, j.SnoozeReason, j.AttemptedAt.UnixMicro())
			}
		case driver.JobStateRetryable, driver.JobStateAvailable:
			if j.SnoozeReason != "" {
				t.Errorf("%s job %s has reason %q", j.State, j.ID, j.SnoozeReason)
			}
		default:
			t.Errorf("job %s is %s", j.ID, j.State)
		}
	}
}
