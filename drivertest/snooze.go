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

// storedReason reads the snooze_reason column itself, which the driver's
// reads show only for a scheduled job.
func storedReason[TTx any](ctx context.Context, t *testing.T, f Fixture[TTx], d driver.Driver[TTx], id driver.JobID) string {
	t.Helper()
	return f.QueryText(ctx, t, d, "SELECT coalesce(snooze_reason, '') FROM hopper_jobs WHERE id = $1::uuid", id.String())
}

// leaveReason writes a reason on a job directly, as a client of the
// previous release, which neither clears it on claim nor on finalize,
// leaves one on a running or retrying job.
func leaveReason[TTx any](ctx context.Context, t *testing.T, f Fixture[TTx], d driver.Driver[TTx], id driver.JobID) {
	t.Helper()
	f.Exec(ctx, t, d, "UPDATE hopper_jobs SET snooze_reason = 'left behind' WHERE id = $1::uuid", id.String())
}

func testSnoozeReason[TTx any](t *testing.T, f Fixture[TTx], noBatches bool) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := executor(d, noBatches)
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
		if r := storedReason(ctx, t, f, d, id); r != "" {
			t.Errorf("claim left snooze reason %q", r)
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

	// Text PostgreSQL cannot store is repaired, not refused: a refused
	// result would fail every result finalized in the same statement.
	claimIt()
	if applied := finalize(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: clientID, State: driver.JobStateScheduled, Delay: time.Hour, Snooze: true, SnoozeReason: "bad \xff\x00 reason"}); len(applied) != 1 {
		t.Fatal("snooze with unstorable reason not applied")
	}
	if j := get(); j.SnoozeReason != "bad �� reason" {
		t.Errorf("unstorable reason stored as %q", j.SnoozeReason)
	}
	claimIt()
	finalize(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: clientID, State: driver.JobStateRetryable, Delay: time.Hour, Error: &driver.AttemptError{Attempt: 1, Error: "bad \x00 error", Trace: "bad \x00 trace"}})
	if j := get(); j.Errors[len(j.Errors)-1].Error != "bad � error" || j.Errors[len(j.Errors)-1].Trace != "bad � trace" {
		t.Errorf("unstorable error stored as %+v", j.Errors[len(j.Errors)-1])
	}

	// A reason left on a job that is not snoozed is never reported: not
	// while it runs, nor once it waits to retry.
	claimIt()
	leaveReason(ctx, t, f, d, id)
	if j := get(); j.State != driver.JobStateRunning || j.SnoozeReason != "" {
		t.Errorf("running job = %+v", j)
	}
	// A retry clears the reason, even one the claim did not.
	finalize(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: clientID, State: driver.JobStateRetryable, Delay: time.Hour, Error: &driver.AttemptError{Attempt: 1, Error: "boom"}})
	if r := storedReason(ctx, t, f, d, id); r != "" {
		t.Errorf("retry kept reason %q", r)
	}
	leaveReason(ctx, t, f, d, id)
	if j := get(); j.State != driver.JobStateRetryable || j.SnoozeReason != "" {
		t.Errorf("retryable job = %+v", j)
	}
	if jobs, err := exec.JobList(ctx, driver.JobListParams{States: []driver.JobState{driver.JobStateRetryable}, Limit: 10}); err != nil || len(jobs) != 1 || jobs[0].SnoozeReason != "" {
		t.Errorf("JobList of retryable jobs = %+v, %v", jobs, err)
	}
	// A reason given with a result that is not a snooze is not stored.
	claimIt()
	finalize(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: clientID, State: driver.JobStateRetryable, Delay: time.Hour, SnoozeReason: "ignored"})
	if r := storedReason(ctx, t, f, d, id); r != "" {
		t.Errorf("retryable job kept reason %q", r)
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
	if j, err := exec.JobRetry(ctx, id); err != nil || j.State != driver.JobStateAvailable || storedReason(ctx, t, f, d, id) != "" {
		t.Errorf("JobRetry = %+v, %v", j, err)
	}

	// Neither does a replacing insert's schedule, while a skipped one keeps it.
	snooze("third")
	res, err := exec.JobInsertMany(ctx, p, driver.JobInsertOpts{})
	if err != nil || !res[0].Duplicate || res[0].Job.SnoozeReason != "third" {
		t.Errorf("skipped insert = %+v, %v", res, err)
	}
	res, err = exec.JobInsertMany(ctx, p, driver.JobInsertOpts{OnConflict: driver.ConflictReplace})
	if err != nil || !res[0].Duplicate || storedReason(ctx, t, f, d, id) != "" {
		t.Errorf("replacing insert = %+v, %v", res, err)
	}

	// A finalized job is not waiting, but the error it is cancelled or
	// expires with keeps the reason it was waiting for.
	snooze("fourth")
	if j, err := exec.JobCancel(ctx, id); err != nil || j.State != driver.JobStateCancelled || j.SnoozeReason != "" ||
		j.Errors[len(j.Errors)-1].Error != "hopper: cancelled while snoozed: fourth" {
		t.Errorf("JobCancel = %+v, %v", j, err)
	}
	if j := get(); j.SnoozeReason != "" {
		t.Errorf("history row has reason %q", j.SnoozeReason)
	}
	if _, err := exec.JobRetry(ctx, id); err != nil {
		t.Fatal(err)
	}
	snooze("fifth")
	f.Exec(ctx, t, d, "UPDATE hopper_jobs SET expires_at = now() - interval '1s' WHERE id = $1::uuid", id.String())
	expired, err := exec.JobDiscardExpired(ctx, 10)
	if err != nil || len(expired) != 1 || expired[0].Errors[len(expired[0].Errors)-1].Error != "hopper: expired while snoozed: fifth" {
		t.Errorf("expired = %+v, %v", expired, err)
	}
	// A reason left on a job that is not snoozed is not taken for one.
	if _, err := exec.JobRetry(ctx, id); err != nil {
		t.Fatal(err)
	}
	leaveReason(ctx, t, f, d, id)
	if j, err := exec.JobCancel(ctx, id); err != nil || j.Errors[len(j.Errors)-1].Error != "hopper: cancelled" {
		t.Errorf("JobCancel of an available job = %+v, %v", j, err)
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
	d := f.NewDriver(t)
	exec := d.Executor()
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
				// Only this client finalizes the jobs it holds, so what the
				// claim cleared stays cleared until then.
				ids := make([]string, len(res.Jobs))
				for i, j := range res.Jobs {
					ids[i] = j.ID.String()
				}
				if left := f.QueryText(ctx, t, d, "SELECT count(snooze_reason)::text FROM hopper_jobs WHERE id = ANY($1::text[]::uuid[])",
					"{"+strings.Join(ids, ",")+"}"); left != "0" {
					t.Errorf("claim left snooze reasons on %s of the jobs it took", left)
				}
				results := make([]driver.JobFinalize, 0, 2*len(res.Jobs))
				for i, j := range res.Jobs {
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
			if r := storedReason(ctx, t, f, d, j.ID); r != "" {
				t.Errorf("%s job %s has reason %q", j.State, j.ID, r)
			}
		default:
			t.Errorf("job %s is %s", j.ID, j.State)
		}
	}
}
