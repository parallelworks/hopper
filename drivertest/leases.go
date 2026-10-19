package drivertest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// claimByID claims a job by ID under a one-minute lease.
func claimByID(ctx context.Context, t *testing.T, exec driver.Executor, id driver.JobID) *driver.JobLeaseRow {
	t.Helper()
	l, err := exec.JobClaimByID(ctx, driver.JobClaimByIDParams{ID: id, TTL: time.Minute})
	if err != nil {
		t.Fatalf("claim %s by ID: %v", id, err)
	}
	return l
}

// leaseResult is a result fenced to lease l.
func leaseResult(l *driver.JobLeaseRow, state driver.JobState) driver.JobFinalize {
	lease := l.Lease
	return driver.JobFinalize{ID: l.Job.ID, Lease: &lease, State: state, Archive: true}
}

// testClaimByID claims jobs by ID from every waiting state and checks why
// the others cannot be.
func testClaimByID[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	ps := params("k", 3)
	ps[1].ScheduledAt = time.Now().Add(time.Hour)
	ins := insert(ctx, t, exec, ps)
	available, scheduled, retryable := ins[0].Job.ID, ins[1].Job.ID, ins[2].Job.ID
	f.Exec(ctx, t, d, "UPDATE hopper_jobs SET state = 'retryable', scheduled_at = now() + interval '1 hour' WHERE id = $1", retryable)
	for _, id := range []driver.JobID{available, scheduled, retryable} {
		before := get(ctx, t, exec, id)
		l := claimByID(ctx, t, exec, id)
		if l.Job.State != driver.JobStateRunning || l.Job.AttemptedBy != 0 || l.Job.Attempt != before.Attempt+1 ||
			l.Job.Fence != before.Fence+1 || l.Lease.Fence != l.Job.Fence || l.Lease.Token.IsZero() {
			t.Fatalf("claim %s by ID: %+v, lease %+v", before.State, l.Job, l.Lease)
		}
		now, err := exec.Now(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if left := l.ExpiresAt.Sub(now); left < 55*time.Second || left > 65*time.Second {
			t.Errorf("lease expires in %s, want about a minute", left)
		}
		if j := get(ctx, t, exec, id); !j.LeaseExpiresAt.Equal(l.ExpiresAt) || j.AttemptedBy != 0 {
			t.Errorf("JobGet of a leased job: lease expires %v, attempted by %d; want %v, 0", j.LeaseExpiresAt, j.AttemptedBy, l.ExpiresAt)
		}
		if _, err := exec.JobClaimByID(ctx, driver.JobClaimByIDParams{ID: id, TTL: time.Minute}); !errors.Is(err, driver.ErrJobRunning) {
			t.Errorf("claim of a leased job = %v, want ErrJobRunning", err)
		}
	}

	notClaimable := func(name string, id driver.JobID, want error) {
		t.Helper()
		if _, err := exec.JobClaimByID(ctx, driver.JobClaimByIDParams{ID: id, TTL: time.Minute}); !errors.Is(err, want) {
			t.Errorf("claim of a job %s = %v, want %v", name, err, want)
		}
		if j, err := exec.JobGet(ctx, id); err == nil && j.State == driver.JobStateRunning && !errors.Is(want, driver.ErrJobRunning) {
			t.Errorf("a job %s was claimed", name)
		}
	}
	notClaimable("unknown", driver.JobID{1}, driver.ErrNotFound)

	clientID := register(ctx, t, exec)
	running := insert(ctx, t, exec, params("k", 1))[0].Job.ID
	claimOne(ctx, t, exec, clientID, running)
	notClaimable("a client runs", running, driver.ErrJobRunning)
	finalize(ctx, t, exec, driver.JobFinalize{ID: running, AttemptedBy: clientID, Fence: 1, State: driver.JobStateCompleted, Archive: true})
	notClaimable("finished", running, driver.ErrJobNotClaimable)

	pending := insert(ctx, t, exec, params("k", 1))[0].Job.ID
	f.Exec(ctx, t, d, "UPDATE hopper_jobs SET state = 'pending' WHERE id = $1", pending)
	notClaimable("pending", pending, driver.ErrJobNotClaimable)

	expired := insert(ctx, t, exec, params("k", 1))[0].Job.ID
	f.Exec(ctx, t, d, "UPDATE hopper_jobs SET expires_at = now() - interval '1 second' WHERE id = $1", expired)
	notClaimable("past its TTL", expired, driver.ErrJobNotClaimable)

	keyed := params("k", 2)
	keyed[0].OrderingKey, keyed[1].OrderingKey = "key", "key"
	keyedIDs := insert(ctx, t, exec, keyed)
	notClaimable("behind an older one of its ordering key", keyedIDs[1].Job.ID, driver.ErrJobNotClaimable)
	claimByID(ctx, t, exec, keyedIDs[0].Job.ID)
	notClaimable("whose ordering key runs another", keyedIDs[1].Job.ID, driver.ErrJobNotClaimable)

	fenced := insert(ctx, t, exec, params("k", 1))[0].Job.ID
	if _, err := exec.JobClaimByID(ctx, driver.JobClaimByIDParams{ID: fenced, IfFence: 3, TTL: time.Minute}); !errors.Is(err, driver.ErrJobFenced) {
		t.Errorf("claim at a stale fence = %v, want ErrJobFenced", err)
	}
	claimOne(ctx, t, exec, clientID, fenced)
	finalize(ctx, t, exec, driver.JobFinalize{ID: fenced, AttemptedBy: clientID, Fence: 1, State: driver.JobStateRetryable})
	if l, err := exec.JobClaimByID(ctx, driver.JobClaimByIDParams{ID: fenced, IfFence: 1, TTL: time.Minute}); err != nil || l.Job.Fence != 2 {
		t.Errorf("claim at the current fence = %+v, %v", l, err)
	}

	paused := insert(ctx, t, exec, params("k", 1))[0].Job.ID
	if err := exec.QueuePause(ctx, "default"); err != nil {
		t.Fatal(err)
	}
	notClaimable("in a paused queue", paused, driver.ErrJobNotClaimable)
	if err := exec.QueueResume(ctx, "default"); err != nil {
		t.Fatal(err)
	}
	claimByID(ctx, t, exec, paused)
}

// testClaimByIDRacesClaim claims jobs by ID while a client claims them from
// the queue: each job is claimed once.
func testClaimByIDRacesClaim[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := register(ctx, t, exec)
	const n = 200
	ins := insert(ctx, t, exec, params("k", n))
	var (
		wg            sync.WaitGroup
		mu            sync.Mutex
		byID, byClaim = map[driver.JobID]int{}, map[driver.JobID]int{}
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for _, r := range ins {
			l, err := exec.JobClaimByID(ctx, driver.JobClaimByIDParams{ID: r.Job.ID, TTL: time.Minute})
			if err != nil && !errors.Is(err, driver.ErrJobRunning) {
				t.Error(err)
				return
			}
			if err == nil {
				mu.Lock()
				byID[l.Job.ID]++
				mu.Unlock()
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			res, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 4})
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			for _, j := range res.Jobs {
				byClaim[j.ID]++
			}
			more := len(byID)+len(byClaim) < n
			mu.Unlock()
			if !more {
				return
			}
			if len(res.Jobs) == 0 {
				time.Sleep(time.Millisecond)
			}
		}
	}()
	wg.Wait()
	for _, r := range ins {
		if c := byID[r.Job.ID] + byClaim[r.Job.ID]; c != 1 {
			t.Fatalf("job %s claimed %d times (%d by ID)", r.Job.ID, c, byID[r.Job.ID])
		}
		j := get(ctx, t, exec, r.Job.ID)
		if j.Fence != 1 || (byID[r.Job.ID] == 1) != (j.AttemptedBy == 0) {
			t.Fatalf("job %s: fence %d, attempted by %d", r.Job.ID, j.Fence, j.AttemptedBy)
		}
	}
	if len(byID) == 0 || len(byClaim) == 0 {
		t.Logf("one side claimed every job: %d by ID, %d by the queue", len(byID), len(byClaim))
	}
}

// testLeaseRenew renews a lease, reports a cancel request through it, and
// fences renewals of a lease that is no longer the job's.
func testLeaseRenew[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	id := insert(ctx, t, exec, params("k", 1))[0].Job.ID
	l := claimByID(ctx, t, exec, id)
	f.Exec(ctx, t, d, "UPDATE hopper_job_leases SET renewed_at = renewed_at - interval '30 seconds', expires_at = expires_at - interval '30 seconds' WHERE id = $1", id)
	st, err := exec.JobLeaseRenew(ctx, driver.JobLeaseRenewParams{ID: id, Lease: l.Lease})
	if err != nil || st.CancelRequested || !st.ExpiresAt.After(l.ExpiresAt.Add(-time.Second)) {
		t.Fatalf("renew = %+v, %v; want the expiry a TTL from now", st, err)
	}
	if _, err := exec.JobCancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	if st, err := exec.JobLeaseRenew(ctx, driver.JobLeaseRenewParams{ID: id, Lease: l.Lease}); err != nil || !st.CancelRequested {
		t.Fatalf("renew after a cancel = %+v, %v; want the cancel reported", st, err)
	}
	for name, lease := range map[string]driver.LeaseFence{
		"another token": {Token: driver.JobID{9}, Fence: l.Lease.Fence},
		"another fence": {Token: l.Lease.Token, Fence: l.Lease.Fence + 1},
	} {
		if _, err := exec.JobLeaseRenew(ctx, driver.JobLeaseRenewParams{ID: id, Lease: lease}); !errors.Is(err, driver.ErrJobFenced) {
			t.Errorf("renew with %s = %v, want ErrJobFenced", name, err)
		}
	}
	finalize(ctx, t, exec, leaseResult(l, driver.JobStateCanceled))
	if _, err := exec.JobLeaseRenew(ctx, driver.JobLeaseRenewParams{ID: id, Lease: l.Lease}); !errors.Is(err, driver.ErrJobFenced) {
		t.Errorf("renew after the attempt ended = %v, want ErrJobFenced", err)
	}
	if n := f.QueryText(ctx, t, d, "SELECT count(*)::text FROM hopper_job_leases"); n != "0" {
		t.Errorf("%s lease rows after the attempt ended, want 0", n)
	}
}

// testLeaseRenewConcurrent renews one lease from many goroutines, with a
// stale lease of the same job among them: every renewal of the current
// lease succeeds and none of the stale one does.
func testLeaseRenewConcurrent[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	id := insert(ctx, t, exec, params("k", 1))[0].Job.ID
	stale := claimByID(ctx, t, exec, id)
	finalize(ctx, t, exec, driver.JobFinalize{ID: id, Lease: &stale.Lease, State: driver.JobStateRetryable})
	current := claimByID(ctx, t, exec, id)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease := current.Lease
			if i%2 == 1 {
				lease = stale.Lease
			}
			for range 25 {
				_, err := exec.JobLeaseRenew(ctx, driver.JobLeaseRenewParams{ID: id, Lease: lease})
				if i%2 == 1 && !errors.Is(err, driver.ErrJobFenced) {
					t.Errorf("renew of the stale lease = %v, want ErrJobFenced", err)
					return
				}
				if i%2 == 0 && err != nil {
					t.Errorf("renew of the current lease = %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// testLeaseFinalize ends leased attempts in every way, fenced to the lease.
func testLeaseFinalize[TTx any](t *testing.T, f Fixture[TTx], noBatches bool) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := executor(d, noBatches)
	for _, state := range []driver.JobState{driver.JobStateCompleted, driver.JobStateCanceled, driver.JobStateDiscarded, driver.JobStateRetryable} {
		id := insert(ctx, t, exec, params("k", 1))[0].Job.ID
		l := claimByID(ctx, t, exec, id)
		stale := l.Lease
		stale.Token = driver.JobID{9}
		if applied := finalize(ctx, t, exec, driver.JobFinalize{ID: id, Lease: &stale, State: state, Archive: true}); len(applied) != 0 {
			t.Errorf("%s with another token applied", state)
		}
		if applied := finalize(ctx, t, exec, driver.JobFinalize{ID: id, AttemptedBy: 0, Fence: l.Lease.Fence, State: state, Archive: true}); len(applied) != 0 {
			t.Errorf("%s by no client applied to a leased job", state)
		}
		res := leaseResult(l, state)
		res.Error = &driver.AttemptError{Attempt: 1, Fence: l.Lease.Fence, Error: "x"}
		applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{res}})
		if err != nil || len(applied) != 1 || applied[0] != (driver.JobFinalized{ID: id, Fence: l.Lease.Fence}) {
			t.Fatalf("%s = %v, %v", state, applied, err)
		}
		if j := get(ctx, t, exec, id); j.State != state || !j.LeaseExpiresAt.IsZero() || len(j.Errors) != 1 {
			t.Errorf("after %s: %+v", state, j)
		}
		if applied := finalize(ctx, t, exec, res); len(applied) != 0 {
			t.Errorf("%s applied twice", state)
		}
		if n := f.QueryText(ctx, t, d, "SELECT count(*)::text FROM hopper_job_leases WHERE id = $1", id); n != "0" {
			t.Errorf("after %s, %s lease rows", state, n)
		}
	}

	// A snooze gives the attempt back and keeps the fence; the next claim by
	// ID has a new token and fence, and the old lease is fenced.
	id := insert(ctx, t, exec, params("k", 1))[0].Job.ID
	first := claimByID(ctx, t, exec, id)
	snooze := leaseResult(first, driver.JobStateScheduled)
	snooze.Snooze, snooze.Delay, snooze.SnoozeReason = true, time.Hour, "waiting"
	finalize(ctx, t, exec, snooze)
	if j := get(ctx, t, exec, id); j.State != driver.JobStateScheduled || j.Attempt != 0 || j.Fence != 1 || j.SnoozeReason != "waiting" {
		t.Fatalf("snoozed: %+v", j)
	}
	second := claimByID(ctx, t, exec, id)
	if second.Job.Attempt != 1 || second.Lease.Fence != 2 || second.Lease.Token == first.Lease.Token {
		t.Fatalf("claim after the snooze: %+v, lease %+v", second.Job, second.Lease)
	}
	if applied := finalize(ctx, t, exec, leaseResult(first, driver.JobStateCompleted)); len(applied) != 0 {
		t.Error("the snoozed lease's result applied to the next")
	}
	if p, err := exec.JobRecordProgress(ctx, driver.JobRecordProgressParams{ID: id, Lease: &first.Lease, Step: "a"}); err != nil || p != nil {
		t.Errorf("the snoozed lease's progress = %+v, %v; want fenced", p, err)
	}
	if applied := finalize(ctx, t, exec, leaseResult(second, driver.JobStateCompleted)); len(applied) != 1 {
		t.Error("the current lease's result did not apply")
	}

	// In a transaction.
	id = insert(ctx, t, exec, params("k", 1))[0].Job.ID
	l := claimByID(ctx, t, exec, id)
	tx, _, rollback := f.Begin(ctx, t, d)
	if applied := finalize(ctx, t, d.UnwrapTx(tx), leaseResult(l, driver.JobStateCompleted)); len(applied) != 1 {
		t.Fatal("finalize in a transaction did not apply")
	}
	if err := rollback(); err != nil {
		t.Fatal(err)
	}
	if j := get(ctx, t, exec, id); j.State != driver.JobStateRunning {
		t.Fatalf("after a rollback: %s", j.State)
	}
	tx, commit, _ := f.Begin(ctx, t, d)
	finalize(ctx, t, d.UnwrapTx(tx), leaseResult(l, driver.JobStateCompleted))
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	if j := get(ctx, t, exec, id); j.State != driver.JobStateCompleted {
		t.Fatalf("after a commit: %s", j.State)
	}

	if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{
		leaseResult(l, driver.JobStateCompleted), {ID: id, AttemptedBy: 1, Fence: 1, State: driver.JobStateCompleted},
	}}); err == nil {
		t.Error("a finalize of leased and other jobs together succeeded")
	}
}

// testLeaseProgress records a leased attempt's progress, fenced to the
// lease.
func testLeaseProgress[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	id := insert(ctx, t, exec, params("k", 1))[0].Job.ID
	l := claimByID(ctx, t, exec, id)
	for _, noWait := range []bool{false, true} {
		p, err := exec.JobRecordProgress(ctx, driver.JobRecordProgressParams{ID: id, Lease: &l.Lease, Step: "a", Cursor: []byte(`1`), NoWait: noWait})
		if err != nil || p == nil || p.Step != "a" || string(p.Cursor) != "1" {
			t.Fatalf("lease progress (no wait %v) = %+v, %v", noWait, p, err)
		}
		stale := l.Lease
		stale.Token = driver.JobID{9}
		if p, err := exec.JobRecordProgress(ctx, driver.JobRecordProgressParams{ID: id, Lease: &stale, Step: "b", NoWait: noWait}); err != nil || p != nil {
			t.Errorf("progress with another token (no wait %v) = %+v, %v; want fenced", noWait, p, err)
		}
		if p, err := exec.JobRecordProgress(ctx, driver.JobRecordProgressParams{ID: id, AttemptedBy: 0, Fence: l.Lease.Fence, Attempt: 1, Step: "b", NoWait: noWait}); err != nil || p != nil {
			t.Errorf("progress by no client (no wait %v) = %+v, %v; want fenced", noWait, p, err)
		}
	}
	tx, _, rollback := f.Begin(ctx, t, d)
	if p, err := d.UnwrapTx(tx).JobRecordProgress(ctx, driver.JobRecordProgressParams{ID: id, Lease: &l.Lease, Step: "c"}); err != nil || p == nil {
		t.Fatalf("lease progress in a transaction = %+v, %v", p, err)
	}
	if _, err := exec.JobRecordProgress(ctx, driver.JobRecordProgressParams{ID: id, Lease: &l.Lease, Step: "d", NoWait: true}); !errors.Is(err, driver.ErrJobLocked) {
		t.Errorf("progress past a held row with NoWait = %v, want ErrJobLocked", err)
	}
	if err := rollback(); err != nil {
		t.Fatal(err)
	}
	if j := get(ctx, t, exec, id); j.Progress == nil || j.Progress.Step != "a" {
		t.Errorf("progress after the rollback = %+v", j.Progress)
	}
}

// testLeaseFencesClients checks that no client's claim, finalize, progress
// write or rescue touches a leased job, even with no client alive.
func testLeaseFencesClients[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	id := insert(ctx, t, exec, params("k", 1))[0].Job.ID
	claimByID(ctx, t, exec, id)
	clientID := register(ctx, t, exec)
	if jobs := claim(ctx, t, exec, clientID, 10); len(jobs) != 0 {
		t.Errorf("a client claimed a leased job")
	}
	f.ExpireLease(ctx, t, d, clientID)
	cands, err := exec.JobRescueCandidates(ctx, driver.JobRescueParams{StuckAfter: time.Nanosecond, Limit: 10})
	if err != nil || len(cands) != 0 {
		t.Errorf("rescue candidates = %+v, %v; want none", cands, err)
	}
}

// testExpireLeases expires leases: a job with attempts left is retryable
// with the expiry recorded, one on its last attempt is discarded, and one
// with a cancel requested is canceled. A lease not yet expired is left.
func testExpireLeases[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	ps := params("k", 4)
	ps[1].MaxAttempts = 1
	ins := insert(ctx, t, exec, ps)
	retried, last, canceled, live := ins[0].Job.ID, ins[1].Job.ID, ins[2].Job.ID, ins[3].Job.ID
	leases := map[driver.JobID]*driver.JobLeaseRow{}
	for _, r := range ins {
		leases[r.Job.ID] = claimByID(ctx, t, exec, r.Job.ID)
	}
	if _, err := exec.JobCancel(ctx, canceled); err != nil {
		t.Fatal(err)
	}
	for _, id := range []driver.JobID{retried, last, canceled} {
		f.ExpireJobLease(ctx, t, d, id)
	}
	jobs, err := exec.JobExpireLeases(ctx, 10)
	if err != nil || len(jobs) != 3 {
		t.Fatalf("expire = %d jobs, %v; want 3", len(jobs), err)
	}
	for id, want := range map[driver.JobID]driver.JobState{retried: driver.JobStateRetryable, last: driver.JobStateDiscarded, canceled: driver.JobStateCanceled, live: driver.JobStateRunning} {
		j := get(ctx, t, exec, id)
		if j.State != want {
			t.Errorf("job %s, want %s", j.State, want)
			continue
		}
		if want == driver.JobStateRunning {
			continue
		}
		if len(j.Errors) != 1 || j.Errors[0].Rescue != driver.RescueLeaseExpired || j.Errors[0].Fence != 1 || j.Errors[0].Attempt != 1 {
			t.Errorf("%s job's errors = %+v", want, j.Errors)
		}
		if _, err := exec.JobLeaseRenew(ctx, driver.JobLeaseRenewParams{ID: id, Lease: leases[id].Lease}); !errors.Is(err, driver.ErrJobFenced) {
			t.Errorf("renew of an expired lease = %v, want ErrJobFenced", err)
		}
		if applied := finalize(ctx, t, exec, leaseResult(leases[id], driver.JobStateCompleted)); len(applied) != 0 {
			t.Errorf("the result of an expired lease applied")
		}
	}
	if j := get(ctx, t, exec, retried); j.Attempt != 1 || j.ScheduledAt.After(time.Now().Add(time.Minute)) {
		t.Errorf("retried job: attempt %d, scheduled %v; want it claimable now", j.Attempt, j.ScheduledAt)
	}
	if jobs, err := exec.JobExpireLeases(ctx, 10); err != nil || len(jobs) != 0 {
		t.Errorf("second expire = %d, %v", len(jobs), err)
	}
}

// testExpireRacesRenew expires a lease whose renewal holds the lease row:
// the renewal wins once it commits, and the job stays leased.
func testExpireRacesRenew[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	id := insert(ctx, t, exec, params("k", 1))[0].Job.ID
	l := claimByID(ctx, t, exec, id)
	f.ExpireJobLease(ctx, t, d, id)
	tx, commit, rollback := f.Begin(ctx, t, d)
	ended := false
	defer func() {
		if !ended {
			_ = rollback()
		}
	}()
	if _, err := d.UnwrapTx(tx).JobLeaseRenew(ctx, driver.JobLeaseRenewParams{ID: id, Lease: l.Lease}); err != nil {
		t.Fatal(err)
	}
	done := make(chan []*driver.JobRow, 1)
	go func() {
		jobs, err := exec.JobExpireLeases(ctx, 10)
		if err != nil {
			t.Error(err)
		}
		done <- jobs
	}()
	waitForLockWait(ctx, t, f, d)
	ended = true
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	if jobs := <-done; len(jobs) != 0 {
		t.Errorf("expire past a renewal = %d jobs, want none", len(jobs))
	}
	if j := get(ctx, t, exec, id); j.State != driver.JobStateRunning || j.LeaseExpiresAt.IsZero() {
		t.Errorf("after the renewal won: %s, lease expires %v", j.State, j.LeaseExpiresAt)
	}
	if _, err := exec.JobLeaseRenew(ctx, driver.JobLeaseRenewParams{ID: id, Lease: l.Lease}); err != nil {
		t.Errorf("renew after the race = %v", err)
	}
}

// testExpireRacesFinalize expires a lease whose job's result is being
// written in a transaction: the expiry skips the job, and the result
// applies. A result written after an expiry is fenced.
func testExpireRacesFinalize[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	id := insert(ctx, t, exec, params("k", 1))[0].Job.ID
	l := claimByID(ctx, t, exec, id)
	f.ExpireJobLease(ctx, t, d, id)
	tx, commit, _ := f.Begin(ctx, t, d)
	if applied := finalize(ctx, t, d.UnwrapTx(tx), leaseResult(l, driver.JobStateCompleted)); len(applied) != 1 {
		t.Fatal("finalize in a transaction did not apply")
	}
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	jobs, err := exec.JobExpireLeases(sctx, 10)
	cancel()
	if err != nil || len(jobs) != 0 {
		t.Errorf("expire of a job being finalized = %d jobs, %v; want none, at once", len(jobs), err)
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	if j := get(ctx, t, exec, id); j.State != driver.JobStateCompleted || len(j.Errors) != 0 {
		t.Errorf("after the finalize won: %+v", j)
	}

	id = insert(ctx, t, exec, params("k", 1))[0].Job.ID
	l = claimByID(ctx, t, exec, id)
	f.ExpireJobLease(ctx, t, d, id)
	if jobs, err := exec.JobExpireLeases(ctx, 10); err != nil || len(jobs) != 1 {
		t.Fatalf("expire = %d, %v", len(jobs), err)
	}
	if applied := finalize(ctx, t, exec, leaseResult(l, driver.JobStateCompleted)); len(applied) != 0 {
		t.Error("a result after the expiry applied")
	}
	if j := get(ctx, t, exec, id); j.State != driver.JobStateRetryable {
		t.Errorf("after the expiry won: %s", j.State)
	}
}

// testLeasesOutsideLimits checks that leased jobs neither obey a queue's
// global and partition limits nor count toward them.
func testLeasesOutsideLimits[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
	if err := exec.QueueSetLimits(ctx, driver.QueueLimits{Name: "default", GlobalLimit: 1, PartitionLimit: 1}); err != nil {
		t.Fatal(err)
	}
	p := params("k", 4)
	for i := range p {
		p[i].PartitionKey = "a"
	}
	ins := insert(ctx, t, exec, p)
	claimByID(ctx, t, exec, ins[0].Job.ID)
	claimByID(ctx, t, exec, ins[1].Job.ID)
	clientID := register(ctx, t, exec)
	if got := claimLimited(ctx, t, exec, clientID, 10); len(got.Jobs) != 1 {
		t.Fatalf("claimed %d beside two leased jobs, want the limit of 1", len(got.Jobs))
	}
	if got := claimLimited(ctx, t, exec, clientID, 10); len(got.Jobs) != 0 {
		t.Fatalf("claimed %d over the limit", len(got.Jobs))
	}
	claimByID(ctx, t, exec, ins[3].Job.ID)
}
