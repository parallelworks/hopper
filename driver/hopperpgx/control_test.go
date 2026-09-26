package hopperpgx_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

func TestJobCancel(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	inserted, err := exec.JobInsertMany(ctx, insertParams("k", 3), driver.JobInsertOpts{})
	if err != nil {
		t.Fatal(err)
	}
	clientID := register(t, ctx, exec)
	running, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 1})
	if err != nil || len(running) != 1 {
		t.Fatalf("claim: %v", err)
	}

	l, err := d.Listener(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close(ctx) //nolint:errcheck // cleanup
	if err := l.Listen(ctx, driver.ChannelControl); err != nil {
		t.Fatal(err)
	}

	// A waiting job is archived as cancelled.
	waiting := inserted[1].Job.ID
	if waiting == running[0].ID {
		waiting = inserted[2].Job.ID
	}
	job, err := exec.JobCancel(ctx, waiting)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != driver.JobStateCancelled || job.FinalizedAt.IsZero() || len(job.Errors) != 1 || job.Errors[0].Error != "hopper: cancelled" {
		t.Errorf("cancelled waiting job = %+v", job)
	}
	if got, err := exec.JobGet(ctx, waiting); err != nil || got.State != driver.JobStateCancelled {
		t.Errorf("JobGet after cancel = %+v, %v", got, err)
	}

	// A running job gets a cancel request and a control notification.
	job, err = exec.JobCancel(ctx, running[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != driver.JobStateRunning || job.CancelRequestedAt.IsZero() {
		t.Errorf("cancelled running job = %+v", job)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		nctx, cancel := context.WithDeadline(ctx, deadline)
		n, err := l.Next(nctx)
		cancel()
		if err != nil {
			t.Fatal("no cancel notification")
		}
		if n.Payload == "cancel:"+running[0].ID.String() {
			break
		}
	}
	// The renewal reports it.
	res, err := exec.ClientRenew(ctx, driver.ClientRenewParams{ClientID: clientID, TTL: time.Hour})
	if err != nil || !res.Renewed || len(res.CancelRequested) != 1 || res.CancelRequested[0] != running[0].ID {
		t.Errorf("renew = %+v, %v", res, err)
	}

	// Cancelling again is idempotent; a finalized job is returned as is.
	if again, err := exec.JobCancel(ctx, running[0].ID); err != nil || !again.CancelRequestedAt.Equal(job.CancelRequestedAt) {
		t.Errorf("second cancel = %+v, %v", again, err)
	}
	if fin, err := exec.JobCancel(ctx, waiting); err != nil || fin.State != driver.JobStateCancelled {
		t.Errorf("cancel of finalized job = %+v, %v", fin, err)
	}
	if _, err := exec.JobCancel(ctx, driver.JobID{9}); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("cancel of unknown job = %v", err)
	}
}

func TestJobRetry(t *testing.T) {
	t.Parallel()
	ctx, _, exec := setup(t)
	params := insertParams("k", 3)
	params[0].UniqueKey = "u"
	params[2].ScheduledAt = time.Now().Add(time.Hour)
	inserted, err := exec.JobInsertMany(ctx, params, driver.JobInsertOpts{})
	if err != nil {
		t.Fatal(err)
	}
	clientID := register(t, ctx, exec)
	jobs, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 2})
	if err != nil || len(jobs) != 2 {
		t.Fatalf("claim: %v, %d", err, len(jobs))
	}

	// Running: refused.
	if _, err := exec.JobRetry(ctx, jobs[0].ID); !errors.Is(err, driver.ErrJobRunning) {
		t.Errorf("retry of running job = %v", err)
	}
	// Scheduled for later: brought forward.
	job, err := exec.JobRetry(ctx, inserted[2].Job.ID)
	if err != nil || job.State != driver.JobStateAvailable || job.ScheduledAt.After(time.Now().Add(time.Minute)) {
		t.Errorf("retry of scheduled job = %+v, %v", job, err)
	}

	// Discard the unique job with its attempts exhausted, then re-drive it
	// from history: one more attempt allowed, errors kept.
	if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{
		{ID: inserted[0].Job.ID, AttemptedBy: clientID, State: driver.JobStateDiscarded, Error: &driver.AttemptError{Attempt: 1, Error: "x"}},
		{ID: inserted[1].Job.ID, AttemptedBy: clientID, State: driver.JobStateDiscarded, Error: &driver.AttemptError{Attempt: 1, Error: "x"}},
	}}); err != nil {
		t.Fatal(err)
	}
	job, err = exec.JobRetry(ctx, inserted[0].Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != driver.JobStateAvailable || job.Attempt != 1 || job.MaxAttempts != 3 || len(job.Errors) != 1 || job.UniqueKey != "u" {
		t.Errorf("re-driven job = %+v", job)
	}
	if got, err := exec.JobGet(ctx, job.ID); err != nil || got.State != driver.JobStateAvailable || !got.FinalizedAt.IsZero() {
		t.Errorf("JobGet after retry = %+v, %v", got, err)
	}
	// Its unique key is live again, so inserting the same key is a duplicate.
	dup, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{params[0]}, driver.JobInsertOpts{})
	if err != nil || !dup[0].Duplicate {
		t.Errorf("insert after retry = %+v, %v", dup, err)
	}

	// A retry that would violate a unique key is refused and leaves history alone.
	if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{
		{ID: inserted[0].Job.ID, AttemptedBy: clientID, State: driver.JobStateDiscarded},
	}}); err != nil {
		t.Fatal(err)
	}
	// (The job is not running, so finalize did not apply; cancel it instead to get a copy in history.)
	if _, err := exec.JobCancel(ctx, inserted[0].Job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{params[0]}, driver.JobInsertOpts{}); err != nil {
		t.Fatal(err)
	}
	if _, err := exec.JobRetry(ctx, inserted[0].Job.ID); !errors.Is(err, driver.ErrUniqueConflict) {
		t.Errorf("retry with a live duplicate = %v", err)
	}
	if got, err := exec.JobGet(ctx, inserted[0].Job.ID); err != nil || got.State != driver.JobStateCancelled {
		t.Errorf("history row after refused retry = %+v, %v", got, err)
	}

	if _, err := exec.JobRetry(ctx, driver.JobID{9}); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("retry of unknown job = %v", err)
	}
}

func TestJobTTLAndExpiry(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	params := insertParams("k", 3)
	params[0].TTL = time.Hour
	params[1].TTL = time.Millisecond
	inserted, err := exec.JobInsertMany(ctx, params, driver.JobInsertOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if inserted[0].Job.ExpiresAt.IsZero() || !inserted[2].Job.ExpiresAt.IsZero() {
		t.Errorf("expires_at = %s, %s", inserted[0].Job.ExpiresAt, inserted[2].Job.ExpiresAt)
	}
	time.Sleep(20 * time.Millisecond)

	// The expired job is skipped by the claim...
	clientID := register(t, ctx, exec)
	jobs, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 10})
	if err != nil || len(jobs) != 2 {
		t.Fatalf("claim = %d jobs, %v; want 2", len(jobs), err)
	}
	for _, j := range jobs {
		if j.ID == inserted[1].Job.ID {
			t.Error("expired job was claimed")
		}
	}
	// ...and dead-lettered by the leader.
	expired, err := exec.JobDiscardExpired(ctx, 100)
	if err != nil || len(expired) != 1 || expired[0].ID != inserted[1].Job.ID {
		t.Fatalf("expired = %v, %v", expired, err)
	}
	if expired[0].State != driver.JobStateDiscarded || len(expired[0].Errors) != 1 || expired[0].Errors[0].Error != "hopper: expired" {
		t.Errorf("expired job = %+v", expired[0])
	}
	var live int
	if err := d.Pool().QueryRow(ctx, "SELECT count(*) FROM hopper_jobs").Scan(&live); err != nil {
		t.Fatal(err)
	}
	if live != 2 {
		t.Errorf("live = %d", live)
	}

	// COPY sets expires_at too.
	params = insertParams("k", 2)
	params[0].TTL = time.Hour
	copied, err := exec.JobInsertCopy(ctx, params, driver.JobInsertOpts{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := exec.JobGet(ctx, copied[0].Job.ID)
	if err != nil || got.ExpiresAt.IsZero() || !got.ExpiresAt.Equal(copied[0].Job.ExpiresAt) {
		t.Errorf("COPY expires_at = %+v, %v", got, err)
	}
}

func TestJobList(t *testing.T) {
	t.Parallel()
	ctx, _, exec := setup(t)
	params := insertParams("a", 5)
	params[1].Kind, params[3].Kind = "b", "b"
	params[4].Queue = "other"
	inserted, err := exec.JobInsertMany(ctx, params, driver.JobInsertOpts{})
	if err != nil {
		t.Fatal(err)
	}
	clientID := register(t, ctx, exec)
	claimed, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 1})
	if err != nil || len(claimed) != 1 {
		t.Fatal(err)
	}
	if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{
		{ID: claimed[0].ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true},
	}}); err != nil {
		t.Fatal(err)
	}

	list := func(p driver.JobListParams) []driver.JobID {
		t.Helper()
		p.Limit = 100
		jobs, err := exec.JobList(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]driver.JobID, len(jobs))
		for i, j := range jobs {
			ids[i] = j.ID
			if i > 0 && jobs[i-1].ID.String() >= j.ID.String() {
				t.Errorf("not in ID order")
			}
		}
		return ids
	}
	if got := list(driver.JobListParams{}); len(got) != 5 {
		t.Errorf("all = %d, want 5 (live and history)", len(got))
	}
	if got := list(driver.JobListParams{States: []driver.JobState{driver.JobStateCompleted}}); len(got) != 1 || got[0] != claimed[0].ID {
		t.Errorf("completed = %v", got)
	}
	if got := list(driver.JobListParams{Kinds: []string{"b"}}); len(got) != 2 {
		t.Errorf("kind b = %v", got)
	}
	if got := list(driver.JobListParams{Queue: "other"}); len(got) != 1 || got[0] != inserted[4].Job.ID {
		t.Errorf("queue other = %v", got)
	}
	if got := list(driver.JobListParams{States: []driver.JobState{driver.JobStateAvailable}}); len(got) != 4 {
		t.Errorf("available = %v", got)
	}
	// Paging with a cursor.
	first, err := exec.JobList(ctx, driver.JobListParams{Limit: 2})
	if err != nil || len(first) != 2 {
		t.Fatal(err)
	}
	rest := list(driver.JobListParams{After: first[1].ID})
	if len(rest) != 3 || rest[0].String() <= first[1].ID.String() {
		t.Errorf("after cursor = %v", rest)
	}
}

func TestQueuesAndControlNotifications(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	if err := exec.QueueEnsure(ctx, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if err := exec.QueueEnsure(ctx, []string{"a"}); err != nil {
		t.Fatal(err)
	}
	l, err := d.Listener(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close(ctx) //nolint:errcheck // cleanup
	if err := l.Listen(ctx, driver.ChannelControl); err != nil {
		t.Fatal(err)
	}
	expect := func(payload string) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for {
			nctx, cancel := context.WithDeadline(ctx, deadline)
			n, err := l.Next(nctx)
			cancel()
			if err != nil {
				t.Fatalf("no %q notification", payload)
			}
			if n.Payload == payload {
				return
			}
		}
	}

	if err := exec.QueuePause(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	expect("pause:b")
	if err := exec.QueuePause(ctx, "c"); err != nil { // unknown queue: created paused
		t.Fatal(err)
	}
	expect("pause:c")
	queues, err := exec.QueueList(ctx)
	if err != nil || len(queues) != 3 {
		t.Fatalf("queues = %v, %v", queues, err)
	}
	if queues[0].Name != "a" || !queues[0].PausedAt.IsZero() || queues[1].Name != "b" || queues[1].PausedAt.IsZero() || queues[2].PausedAt.IsZero() {
		t.Errorf("queues = %+v %+v %+v", queues[0], queues[1], queues[2])
	}
	res, err := exec.ClientRenew(ctx, driver.ClientRenewParams{ClientID: register(t, ctx, exec), TTL: time.Hour})
	if err != nil || len(res.PausedQueues) != 2 {
		t.Errorf("renew paused queues = %v, %v", res.PausedQueues, err)
	}
	if err := exec.QueueResume(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	expect("resume:b")
	queues, _ = exec.QueueList(ctx)
	if !queues[1].PausedAt.IsZero() {
		t.Error("b still paused after resume")
	}
}

func TestPeriodicInsertOncePerSlot(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	slot := time.Date(2026, 9, 25, 18, 0, 0, 0, time.UTC)
	job := driver.JobInsertParams{Kind: "tick", Queue: "default", Priority: 2, MaxAttempts: 1, Args: json.RawMessage(`{}`)}

	// Two leaders race for the same slot: one inserts.
	type result struct {
		inserted bool
		err      error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			_, inserted, err := exec.PeriodicInsert(ctx, driver.PeriodicInsertParams{Name: "tick", Slot: slot, Job: job})
			results <- result{inserted, err}
		}()
	}
	var inserted int
	for range 2 {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.inserted {
			inserted++
		}
	}
	if inserted != 1 {
		t.Errorf("slot inserted %d times", inserted)
	}
	// An older slot is ignored; a newer one is inserted.
	if _, ins, err := exec.PeriodicInsert(ctx, driver.PeriodicInsertParams{Name: "tick", Slot: slot.Add(-time.Hour), Job: job}); err != nil || ins {
		t.Errorf("older slot inserted = %v, %v", ins, err)
	}
	if _, ins, err := exec.PeriodicInsert(ctx, driver.PeriodicInsertParams{Name: "tick", Slot: slot.Add(time.Hour), Job: job}); err != nil || !ins {
		t.Errorf("newer slot inserted = %v, %v", ins, err)
	}
	var count int
	if err := d.Pool().QueryRow(ctx, "SELECT count(*) FROM hopper_jobs WHERE kind = 'tick'").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("jobs = %d, want 2", count)
	}
	slots, err := exec.PeriodicLastSlots(ctx)
	if err != nil || !slots["tick"].Equal(slot.Add(time.Hour)) {
		t.Errorf("last slots = %v, %v", slots, err)
	}
}

func TestStats(t *testing.T) {
	t.Parallel()
	ctx, _, exec := setup(t)
	params := insertParams("k", 4)
	params[3].Queue = "other"
	params[2].ScheduledAt = time.Now().Add(time.Hour)
	if _, err := exec.JobInsertMany(ctx, params, driver.JobInsertOpts{}); err != nil {
		t.Fatal(err)
	}
	clientID := register(t, ctx, exec)
	claimed, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 1})
	if err != nil || len(claimed) != 1 {
		t.Fatal(err)
	}
	if err := exec.QueueEnsure(ctx, []string{"default", "other"}); err != nil {
		t.Fatal(err)
	}
	if err := exec.QueuePause(ctx, "other"); err != nil {
		t.Fatal(err)
	}
	if _, err := exec.LeaderAttempt(ctx, driver.LeaderParams{ClientID: clientID, TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	// One completion for throughput.
	if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{
		{ID: claimed[0].ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true},
	}}); err != nil {
		t.Fatal(err)
	}
	claimed, err = exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 1})
	if err != nil || len(claimed) != 1 {
		t.Fatal(err)
	}

	stats, err := exec.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	def := stats.Queues["default"]
	if def == nil || def.Available != 0 || def.Scheduled != 1 || def.Running != 1 || def.CompletedLastMinute != 1 || def.Paused {
		t.Errorf("default stats = %+v", def)
	}
	other := stats.Queues["other"]
	if other == nil || other.Available != 1 || !other.Paused || other.OldestAvailable <= 0 {
		t.Errorf("other stats = %+v", other)
	}
	if stats.LiveClients != 1 || stats.Leader != clientID || stats.RunningByClient[clientID] != 1 {
		t.Errorf("cluster stats = %+v", stats)
	}
}

func TestNow(t *testing.T) {
	t.Parallel()
	ctx, _, exec := setup(t)
	now, err := exec.Now(ctx)
	if err != nil || time.Until(now).Abs() > time.Minute {
		t.Errorf("Now = %s, %v", now, err)
	}
}
