package hopper_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
)

type remoteJob struct {
	N int `json:"n"`
}

func (remoteJob) Kind() string { return "remote" }

// remoteQueue is a queue no client works, whose jobs run on remote workers
// under job leases.
const remoteQueue = "remote"

// leaseClient returns a started client that works only the default queue,
// with cfg's other settings: its workers, if any, are registered but run
// nothing, since no job of theirs is in the default queue.
func leaseClient(h *harness, cfg *hopper.Config) *hopper.Client[pgxTx] {
	h.t.Helper()
	cfg.Queues = map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}}
	if cfg.Workers == nil {
		cfg.Workers = hopper.NewWorkers()
	}
	c := h.client(cfg)
	if err := c.Start(context.Background()); err != nil {
		h.t.Fatal(err)
	}
	return c
}

func insertRemote(t *testing.T, c *hopper.Client[pgxTx], n, maxAttempts int) hopper.JobID {
	t.Helper()
	res, err := c.Insert(context.Background(), remoteJob{N: n}, &hopper.InsertOpts{Queue: remoteQueue, MaxAttempts: maxAttempts})
	if err != nil {
		t.Fatal(err)
	}
	return res.Job.ID
}

// TestLeaseLifecycle claims a job by ID, renews its lease, records its
// progress and completes it with an output that Await returns.
func TestLeaseLifecycle(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	c := leaseClient(h, &hopper.Config{})
	id := insertRemote(t, c, 7, 3)
	if _, err := c.Leases().Claim(ctx, id, 1, nil); !errors.Is(err, hopper.ErrJobFenced) {
		t.Errorf("claim of a job never claimed at fence 1 = %v, want ErrJobFenced", err)
	}
	l, err := c.Leases().Claim(ctx, id, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if l.Job.State != hopper.JobStateRunning || l.Job.Attempt != 1 || l.Token.JobID() != id || l.Token.Fence() != 1 {
		t.Fatalf("claim = %+v, token %v", l.Job, l.Token)
	}
	text, err := json.Marshal(struct{ Token hopper.LeaseToken }{l.Token})
	if err != nil {
		t.Fatal(err)
	}
	var carried struct{ Token hopper.LeaseToken }
	if err := json.Unmarshal(text, &carried); err != nil || carried.Token != l.Token {
		t.Fatalf("token through JSON = %v, %v; want %v", carried.Token, err, l.Token)
	}
	if parsed, err := hopper.ParseLeaseToken(l.Token.String()); err != nil || parsed != l.Token {
		t.Fatalf("parsed token = %v, %v", parsed, err)
	}
	for _, bad := range []string{"", "x", l.Token.String() + "0", l.Token.String() + "00", id.String() + ".0." + l.Token.String()[len(id.String())+3:]} {
		if _, err := hopper.ParseLeaseToken(bad); err == nil {
			t.Errorf("ParseLeaseToken(%q) succeeded", bad)
		}
	}

	st, err := c.Leases().Renew(ctx, carried.Token)
	if err != nil || st.CancelRequested || st.ExpiresAt.Before(l.ExpiresAt) {
		t.Fatalf("renew = %+v, %v", st, err)
	}
	if p, err := c.Leases().RecordStep(ctx, carried.Token, "placed", map[string]string{"host": "a"}); err != nil || p.Step != "placed" {
		t.Fatalf("record step = %+v, %v", p, err)
	}
	if job, err := c.JobGet(ctx, id); err != nil || job.LeaseExpiresAt.IsZero() || job.Progress.Step != "placed" {
		t.Fatalf("leased job = %+v, %v", job, err)
	}

	type output struct{ Result string }
	done := make(chan error, 1)
	go func() {
		out, err := hopper.Await[output](ctx, c, id)
		if err == nil && out.Result != "ok" {
			err = fmt.Errorf("output %+v", out)
		}
		done <- err
	}()
	if err := c.Leases().Finish(ctx, carried.Token, hopper.LeaseResult{Output: output{"ok"}}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Await = %v", err)
	}
	if err := c.Leases().Finish(ctx, carried.Token, hopper.LeaseResult{}); !errors.Is(err, hopper.ErrJobFenced) {
		t.Errorf("second Finish = %v, want ErrJobFenced", err)
	}
	if _, err := c.Leases().Renew(ctx, carried.Token); !errors.Is(err, hopper.ErrJobFenced) {
		t.Errorf("Renew after Finish = %v, want ErrJobFenced", err)
	}
	if _, err := c.Leases().RecordCursor(ctx, carried.Token, 1); !errors.Is(err, hopper.ErrJobFenced) {
		t.Errorf("RecordCursor after Finish = %v, want ErrJobFenced", err)
	}
}

// TestLeaseFinishOutcomes ends leased attempts as a worker's return values
// would: an error retries the job through Config.OnError and the retry
// policy, a snooze gives the attempt back, Cancel discards, and an error
// after JobCancel cancels.
func TestLeaseFinishOutcomes(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var mu sync.Mutex
	var onError []string
	c := leaseClient(h, &hopper.Config{
		OnError: func(_ context.Context, _ *hopper.JobRow, err error) hopper.ErrorOutcome {
			mu.Lock()
			onError = append(onError, err.Error())
			mu.Unlock()
			return hopper.ErrorOutcome{}
		},
	})
	claim := func(id hopper.JobID) hopper.LeaseToken {
		t.Helper()
		l, err := c.Leases().Claim(ctx, id, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		return l.Token
	}

	failed := insertRemote(t, c, 1, 3)
	if err := c.Leases().Finish(ctx, claim(failed), hopper.LeaseResult{Err: errors.New("host unreachable")}); err != nil {
		t.Fatal(err)
	}
	job, err := c.JobGet(ctx, failed)
	if err != nil || job.State != hopper.JobStateRetryable || len(job.Errors) != 1 || job.Errors[0].Error != "host unreachable" || job.Errors[0].Fence != 1 {
		t.Fatalf("failed: %+v, %v", job, err)
	}
	mu.Lock()
	if len(onError) != 1 || onError[0] != "host unreachable" {
		t.Errorf("OnError calls = %v", onError)
	}
	mu.Unlock()

	snoozed := insertRemote(t, c, 2, 3)
	if err := c.Leases().Finish(ctx, claim(snoozed), hopper.LeaseResult{Err: hopper.Snooze(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if job, err := c.JobGet(ctx, snoozed); err != nil || job.State != hopper.JobStateScheduled || job.Attempt != 0 || job.Fence != 1 {
		t.Fatalf("snoozed: %+v, %v", job, err)
	}
	// The snoozed job waits for its worker, and is claimed early.
	if l, err := c.Leases().Claim(ctx, snoozed, 1, nil); err != nil || l.Job.Attempt != 1 || l.Token.Fence() != 2 {
		t.Fatalf("claim of the snoozed job = %+v, %v", l, err)
	}

	discarded := insertRemote(t, c, 3, 3)
	if err := c.Leases().Finish(ctx, claim(discarded), hopper.LeaseResult{Err: hopper.Cancel(errors.New("bad input"))}); err != nil {
		t.Fatal(err)
	}
	if job, err := c.JobGet(ctx, discarded); err != nil || job.State != hopper.JobStateDiscarded {
		t.Fatalf("discarded: %+v, %v", job, err)
	}

	canceled := insertRemote(t, c, 4, 3)
	token := claim(canceled)
	if _, err := c.JobCancel(ctx, canceled); err != nil {
		t.Fatal(err)
	}
	if st, err := c.Leases().Renew(ctx, token); err != nil || !st.CancelRequested {
		t.Fatalf("renew after JobCancel = %+v, %v", st, err)
	}
	if err := c.Leases().Finish(ctx, token, hopper.LeaseResult{Err: errors.New("stopped")}); err != nil {
		t.Fatal(err)
	}
	if job, err := c.JobGet(ctx, canceled); err != nil || job.State != hopper.JobStateCanceled {
		t.Fatalf("canceled: %+v, %v", job, err)
	}
}

// TestLeaseFinishUsesWorkerNextRetry finishes a leased attempt of a kind
// whose worker, registered on the client, has its own NextRetry: the retry
// waits as long as the worker's own failed attempt would.
func TestLeaseFinishUsesWorkerNextRetry(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.AddWorker(workers, nextRetryWorker{})
	c := leaseClient(h, &hopper.Config{Workers: workers})
	res, err := c.Insert(ctx, nextRetryArgs{}, &hopper.InsertOpts{Queue: remoteQueue})
	if err != nil {
		t.Fatal(err)
	}
	l, err := c.Leases().Claim(ctx, res.Job.ID, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Leases().Finish(ctx, l.Token, hopper.LeaseResult{Err: errors.New("host unreachable")}); err != nil {
		t.Fatal(err)
	}
	job, err := c.JobGet(ctx, res.Job.ID)
	if err != nil || job.State != hopper.JobStateRetryable {
		t.Fatalf("failed: %+v, %v", job, err)
	}
	if d := h.retryIn(job); d < 3*time.Hour-time.Minute || d > 3*time.Hour {
		t.Errorf("retried in %s, want the worker's 3h", d)
	}
}

// TestLeaseFinishUsesAddedQueue finishes a leased job of a queue added
// at runtime with the queue's own RetryPolicy.
func TestLeaseFinishUsesAddedQueue(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	c := leaseClient(h, &hopper.Config{})
	if err := c.Queues().Add(ctx, remoteQueue, hopper.QueueConfig{MaxWorkers: 1, RetryPolicy: fixedRetry(3 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	res, err := c.Insert(ctx, remoteJob{N: 1}, &hopper.InsertOpts{Queue: remoteQueue, ScheduledAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	l, err := c.Leases().Claim(ctx, res.Job.ID, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Leases().Finish(ctx, l.Token, hopper.LeaseResult{Err: errors.New("host unreachable")}); err != nil {
		t.Fatal(err)
	}
	job, err := c.JobGet(ctx, res.Job.ID)
	if err != nil || job.State != hopper.JobStateRetryable {
		t.Fatalf("failed: %+v, %v", job, err)
	}
	if d := h.retryIn(job); d < 3*time.Hour-time.Minute || d > 3*time.Hour {
		t.Errorf("retried in %s, want the added queue's 3h", d)
	}
}

// TestLeaseExpiry lets a lease lapse: the leader makes the job retryable
// with the expiry in its errors, and the holder's calls are fenced. A new
// claim gets a new token, and the old one stays fenced.
func TestLeaseExpiry(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	c := leaseClient(h, &hopper.Config{})
	id := insertRemote(t, c, 1, 2)
	first, err := c.Leases().Claim(ctx, id, 0, &hopper.LeaseOpts{TTL: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	job := waitForJob(t, c, id, hopper.JobStateRetryable)
	if len(job.Errors) != 1 || job.Errors[0].Rescue != hopper.RescueLeaseExpired || job.Errors[0].Fence != first.Token.Fence() {
		t.Fatalf("expired job's errors = %+v", job.Errors)
	}
	if _, err := c.Leases().Renew(ctx, first.Token); !errors.Is(err, hopper.ErrJobFenced) {
		t.Errorf("renew of the expired lease = %v, want ErrJobFenced", err)
	}
	second, err := c.Leases().Claim(ctx, id, first.Token.Fence(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Leases().Finish(ctx, first.Token, hopper.LeaseResult{}); !errors.Is(err, hopper.ErrJobFenced) {
		t.Errorf("finish with the expired lease's token = %v, want ErrJobFenced", err)
	}
	if _, err := c.Leases().RecordStep(ctx, first.Token, "x", nil); !errors.Is(err, hopper.ErrJobFenced) {
		t.Errorf("record with the expired lease's token = %v, want ErrJobFenced", err)
	}
	if err := c.Leases().Finish(ctx, second.Token, hopper.LeaseResult{}); err != nil {
		t.Errorf("finish with the current token = %v", err)
	}
	if job, err := c.JobGet(ctx, id); err != nil || job.State != hopper.JobStateCompleted {
		t.Fatalf("after the current lease finished: %+v, %v", job, err)
	}

	// On its last attempt, an expiry discards the job.
	last := insertRemote(t, c, 2, 1)
	if _, err := c.Leases().Claim(ctx, last, 0, &hopper.LeaseOpts{TTL: 300 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	waitForJob(t, c, last, hopper.JobStateDiscarded)
}

// TestLeasesRaceTheExpiry has workers that renew late at random, so that
// some leases expire while their holders renew and finish them. Every claim
// ends exactly once: by its holder's Finish or by an expiry recorded with
// its fence, never both; once a holder is fenced it stays fenced; and no
// expiry is recorded before the expiry its holder was last given, by
// database time.
func TestLeasesRaceTheExpiry(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	c := leaseClient(h, &hopper.Config{})
	const jobs, ttl = 20, 250 * time.Millisecond
	ids := make([]hopper.JobID, jobs)
	for i := range ids {
		ids[i] = insertRemote(t, c, i, 1000)
	}
	type claimEnd struct {
		fence     int64
		finished  bool
		expiresAt time.Time
	}
	var (
		mu   sync.Mutex
		ends = map[hopper.JobID][]claimEnd{}
	)
	deadline := time.Now().Add(4 * time.Second)
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var fence int64
			for time.Now().Before(deadline) {
				l, err := c.Leases().Claim(ctx, id, fence, &hopper.LeaseOpts{TTL: ttl})
				if errors.Is(err, hopper.ErrJobRunning) {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				if err != nil {
					t.Error(err)
					return
				}
				fence = l.Token.Fence()
				fenced, expiresAt := false, l.ExpiresAt
				for range 3 {
					time.Sleep(time.Duration(rand.Int64N(int64(2 * ttl))))
					st, err := c.Leases().Renew(ctx, l.Token)
					if err == nil {
						expiresAt = st.ExpiresAt
					}
					switch {
					case errors.Is(err, hopper.ErrJobFenced):
						fenced = true
					case err != nil:
						t.Error(err)
						return
					case fenced:
						t.Errorf("job %s fence %d: renewed after it was fenced", id, l.Token.Fence())
					}
				}
				err = c.Leases().Finish(ctx, l.Token, hopper.LeaseResult{Err: errors.New("again")})
				if err != nil && !errors.Is(err, hopper.ErrJobFenced) {
					t.Error(err)
					return
				}
				if err == nil && fenced {
					t.Errorf("job %s fence %d: finished after it was fenced", id, l.Token.Fence())
				}
				mu.Lock()
				ends[id] = append(ends[id], claimEnd{fence: l.Token.Fence(), finished: err == nil, expiresAt: expiresAt})
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	expiries, finishes := 0, 0
	for _, id := range ids {
		job := waitForJob(t, c, id, hopper.JobStateRetryable)
		expired := map[int64]int{}
		failed := map[int64]int{}
		expiredAt := map[int64]time.Time{}
		for _, e := range job.Errors {
			if e.Rescue == hopper.RescueLeaseExpired {
				expired[e.Fence]++
				expiredAt[e.Fence] = e.At
			} else {
				failed[e.Fence]++
			}
		}
		for _, end := range ends[id] {
			if expired[end.fence]+failed[end.fence] != 1 {
				t.Errorf("job %s fence %d: %d expiries and %d finishes recorded, want one in all", id, end.fence, expired[end.fence], failed[end.fence])
			}
			if end.finished != (failed[end.fence] == 1) {
				t.Errorf("job %s fence %d: Finish returned %v, but %d finishes recorded", id, end.fence, end.finished, failed[end.fence])
			}
			if at, ok := expiredAt[end.fence]; ok && at.Before(end.expiresAt) {
				t.Errorf("job %s fence %d: expired at %v, before the expiry %v its holder was given", id, end.fence, at, end.expiresAt)
			}
			if end.finished {
				finishes++
			} else {
				expiries++
			}
		}
	}
	if expiries == 0 || finishes == 0 {
		t.Logf("%d expiries and %d finishes: the race was not run both ways", expiries, finishes)
	}
}

// TestLeasesOutlastARestart stops the whole cluster while remote workers
// hold job leases, for longer than their TTL, so that no worker can renew.
// Once a client starts again, the workers that come back within the grace
// keep their leases, and the lease of one that never comes back expires no
// earlier than the grace after the restart, not after the start of a client
// that crashed before it.
func TestLeasesOutlastARestart(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	const ttl, grace = time.Second, 4 * time.Second
	first := leaseClient(h, &hopper.Config{JobLeaseGrace: grace})
	ids := make([]hopper.JobID, 4)
	tokens := make([]hopper.LeaseToken, len(ids))
	for i := range ids {
		ids[i] = insertRemote(t, first, i, 3)
		l, err := first.Leases().Claim(ctx, ids[i], 0, &hopper.LeaseOpts{TTL: ttl})
		if err != nil {
			t.Fatal(err)
		}
		tokens[i] = l.Token
	}
	// A client that started after the claims and crashed with the cluster:
	// its row stays, its lease lapsed, until the next leader prunes it. The
	// cluster stays down for longer than the grace from that start.
	if _, err := h.pool.Exec(ctx, "INSERT INTO hopper_clients (hostname, expires_at) VALUES ('crashed', now() + interval '2s')"); err != nil {
		t.Fatal(err)
	}
	if err := first.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(grace + ttl/2)
	second := leaseClient(h, &hopper.Config{JobLeaseGrace: grace})
	// The restart is the second client's start, by database time, as the
	// expiry's error entry is.
	var restarted time.Time
	if err := h.pool.QueryRow(ctx, "SELECT max(started_at) FROM hopper_clients").Scan(&restarted); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * fastTuning.LeaderInterval)

	lost, back := ids[0], tokens[1:]
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for _, token := range back {
		wg.Go(func() {
			for {
				if _, err := second.Leases().Renew(ctx, token); err != nil {
					t.Errorf("job %s: renew after the restart = %v", token.JobID(), err)
					return
				}
				select {
				case <-stop:
					return
				case <-time.After(ttl / 5):
				}
			}
		})
	}
	job := waitForJob(t, second, lost, hopper.JobStateRetryable)
	close(stop)
	wg.Wait()
	if len(job.Errors) != 1 || job.Errors[0].Rescue != hopper.RescueLeaseExpired {
		t.Fatalf("the lost worker's job's errors = %+v", job.Errors)
	}
	if after := job.Errors[0].At.Sub(restarted); after < grace {
		t.Errorf("the lost worker's lease expired %s after the restart, before the grace of %s", after, grace)
	}
	for _, token := range back {
		if j, err := second.JobGet(ctx, token.JobID()); err != nil || j.State != hopper.JobStateRunning {
			t.Errorf("job of a worker that came back: %+v, %v", j, err)
		}
	}
}
