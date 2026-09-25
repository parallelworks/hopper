package hopper_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
)

// blocker is a worker that reports when it starts and then waits until it is
// released or its context ends.
type blocker struct {
	hopper.WorkerDefaults[noop]
	started chan hopper.JobID
	release chan struct{}
	rec     *recorder
}

func newBlocker() *blocker {
	return &blocker{started: make(chan hopper.JobID, 100), release: make(chan struct{}), rec: newRecorder()}
}

func (b *blocker) Work(ctx context.Context, job *hopper.Job[noop]) error {
	b.rec.record(job.ID)
	b.started <- job.ID
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *blocker) Timeout(*hopper.Job[noop]) time.Duration { return -1 }

func TestUniqueKey(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 25, 18, 45, 0, 0, time.UTC)
	cases := []struct {
		name  string
		opts  hopper.UniqueOpts
		args  hopper.JobArgs
		enc   string
		queue string
		want  string
	}{
		{"kind only", hopper.UniqueOpts{}, noop{N: 1}, `{"n":1}`, "q", "*"},
		{"tagged fields", hopper.UniqueOpts{ByArgs: true}, tagged{UserID: 42, Tmpl: "x"}, `{"user_id":42,"tmpl":"x"}`, "q", `a={"user_id":42}`},
		{"embedded tags", hopper.UniqueOpts{ByArgs: true}, embedded{tagged{7, "y"}, "e"}, `{}`, "q", `a={"Extra":"e","user_id":7}`},
		{"pointer args", hopper.UniqueOpts{ByArgs: true}, &tagged{UserID: 3}, `{}`, "q", `a={"user_id":3}`},
		{"whole args canonical", hopper.UniqueOpts{ByArgs: true}, noop{}, `{"z": 1, "a": [1, 2]}`, "q", `a={"a":[1,2],"z":1}`},
		{"queue and period", hopper.UniqueOpts{ByQueue: true, ByPeriod: time.Hour}, noop{}, `{}`, "email", "q=email\np=2026-09-25T18:00:00Z"},
		{"everything", hopper.UniqueOpts{ByArgs: true, ByQueue: true, ByPeriod: 24 * time.Hour}, tagged{UserID: 1}, `{}`, "email", "a={\"user_id\":1}\nq=email\np=2026-09-25T00:00:00Z"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := hopper.UniqueKey(&tc.opts, tc.args, []byte(tc.enc), tc.queue, now)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("key = %q, want %q", got, tc.want)
			}
		})
	}
	long := strings.Repeat("x", 2000)
	if _, err := hopper.UniqueKey(&hopper.UniqueOpts{ByArgs: true}, noop{}, []byte(`"`+long+`"`), "q", now); err == nil {
		t.Error("oversized key accepted")
	}
}

type tagged struct {
	UserID int    `json:"user_id" hopper:"unique"`
	Tmpl   string `json:"tmpl"`
}

func (tagged) Kind() string { return "tagged" }

type embedded struct {
	tagged
	Extra string `hopper:"unique"`
}

func (embedded) Kind() string { return "embedded" }

func TestUniqueInsertConcurrent(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	c := h.client(&hopper.Config{})

	// Many goroutines insert the same unique job; exactly one row exists.
	const n = 32
	var wg sync.WaitGroup
	results := make([]*hopper.InsertResult, n)
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() {
			results[i], errs[i] = c.Insert(ctx, noop{N: i}, &hopper.InsertOpts{Unique: &hopper.UniqueOpts{}})
		})
	}
	wg.Wait()
	var inserted int
	for i := range n {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if !results[i].Duplicate {
			inserted++
		}
		if results[i].Job.ID != results[0].Job.ID {
			t.Errorf("insert %d got a different job", i)
		}
	}
	if inserted != 1 || h.count("SELECT count(*) FROM hopper_jobs") != 1 {
		t.Errorf("inserted = %d, rows = %d", inserted, h.count("SELECT count(*) FROM hopper_jobs"))
	}

	// Duplicates within one batch fold onto the first, in input order.
	batch, err := c.InsertMany(ctx, []hopper.InsertParams{
		{Args: noop{N: 1}, Opts: &hopper.InsertOpts{Unique: &hopper.UniqueOpts{ByArgs: true}}},
		{Args: noop{N: 2}, Opts: &hopper.InsertOpts{Unique: &hopper.UniqueOpts{ByArgs: true}}},
		{Args: noop{N: 1}, Opts: &hopper.InsertOpts{Unique: &hopper.UniqueOpts{ByArgs: true}}},
		{Args: noop{N: 3}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if batch[0].Duplicate || batch[1].Duplicate || !batch[2].Duplicate || batch[3].Duplicate {
		t.Errorf("batch duplicates = %v %v %v %v", batch[0].Duplicate, batch[1].Duplicate, batch[2].Duplicate, batch[3].Duplicate)
	}
	if batch[2].Job.ID != batch[0].Job.ID || string(batch[3].Job.Args) != `{"n": 3}` {
		t.Errorf("batch results = %+v", batch)
	}

	// Replace pushes the existing job back and updates its args.
	future := time.Now().Add(time.Hour)
	rep, err := c.Insert(ctx, noop{N: 9}, &hopper.InsertOpts{
		ScheduledAt: future,
		Unique:      &hopper.UniqueOpts{OnConflict: hopper.UniqueReplace},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Duplicate || rep.Job.ID != results[0].Job.ID || string(rep.Job.Args) != `{"n": 9}` || rep.Job.State != hopper.JobStateScheduled {
		t.Errorf("replace = %+v", rep.Job)
	}
}

func TestRescueAfterClientCrash(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	w := newBlocker()
	workers := hopper.NewWorkers()
	hopper.AddWorker(workers, w)

	victim := h.started(workers, 2)
	res, err := victim.Insert(ctx, noop{N: 1}, &hopper.InsertOpts{MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	exhausted, err := victim.Insert(ctx, noop{N: 2}, &hopper.InsertOpts{MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	<-w.started
	<-w.started

	// The victim dies mid-job. Its lease expires, and the surviving client,
	// once leader, rescues the job and runs it again.
	victim.Abandon()
	close(w.release)
	survivor := h.started(workers, 2)

	job := waitForJob(t, survivor, res.Job.ID, hopper.JobStateCompleted)
	if job.Attempt != 2 || len(job.Errors) != 1 || job.Errors[0].Error != "hopper: client lost" || job.Errors[0].Attempt != 1 {
		t.Errorf("rescued job = %+v", job)
	}
	if job.AttemptedBy != survivor.ClientID() {
		t.Errorf("rescued job finished by %d, want the survivor %d", job.AttemptedBy, survivor.ClientID())
	}
	w.rec.mu.Lock()
	runs := w.rec.seen[res.Job.ID]
	w.rec.mu.Unlock()
	if runs != 2 {
		t.Errorf("job ran %d times, want 2", runs)
	}

	// A lost job with no attempts left is dead-lettered instead.
	job = waitForJob(t, survivor, exhausted.Job.ID, hopper.JobStateDiscarded)
	if len(job.Errors) != 1 || job.Errors[0].Error != "hopper: client lost" {
		t.Errorf("exhausted job = %+v", job)
	}
	if h.count("SELECT count(*) FROM hopper_clients") != 1 {
		t.Error("crashed client's lease row was not pruned")
	}
}

func TestFencedClientCancelsJobsAndReregisters(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	w := newBlocker()
	workers := hopper.NewWorkers()
	hopper.AddWorker(workers, w)
	c := h.started(workers, 1)
	oldID := c.ClientID()

	res, err := c.Insert(ctx, noop{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	<-w.started

	// The lease expires behind the client's back, as after a long pause.
	if _, err := h.pool.Exec(ctx, "UPDATE hopper_clients SET expires_at = now() - interval '1s' WHERE id = $1", oldID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return c.ClientID() != oldID })

	// The running job's context was cancelled, and the job runs again under
	// the new lease.
	waitFor(t, func() bool {
		job, err := c.JobGet(ctx, res.Job.ID)
		return err == nil && (job.State != hopper.JobStateRunning || job.AttemptedBy != oldID)
	})
	waitFor(t, func() bool { w.rec.mu.Lock(); defer w.rec.mu.Unlock(); return w.rec.seen[res.Job.ID] == 2 })
	close(w.release)
	job := waitForJob(t, c, res.Job.ID, hopper.JobStateCompleted)
	if job.AttemptedBy != c.ClientID() || job.Attempt != 2 || len(job.Errors) != 1 {
		t.Errorf("job after fencing = %+v", job)
	}
}

func TestLeaderFailover(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	workers := hopper.NewWorkers()
	a := h.started(workers, 1)
	waitFor(t, a.IsLeader)
	b := h.started(workers, 1)
	time.Sleep(fastTuning.LeaderInterval * 2)
	if b.IsLeader() || !a.IsLeader() {
		t.Fatal("two leaders, or the wrong one")
	}
	if h.count("SELECT count(*) FROM hopper_leader WHERE client_id = $1", a.ClientID()) != 1 {
		t.Fatal("leader row does not name a")
	}

	// Graceful stop resigns, and b takes over without waiting for the TTL.
	if err := a.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	waitFor(t, b.IsLeader)
	if took := time.Since(start); took > fastTuning.LeaderTTL {
		t.Errorf("failover after resign took %s, longer than the TTL", took)
	}

	// A crashed leader is replaced within the TTL.
	c := h.started(workers, 1)
	b.Abandon()
	start = time.Now()
	waitFor(t, c.IsLeader)
	if took := time.Since(start); took > fastTuning.LeaderTTL+2*fastTuning.LeaderInterval {
		t.Errorf("failover after crash took %s", took)
	}
}

func TestLeaderMaintainsHistoryPartitions(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	workers := hopper.NewWorkers()
	c := h.started(workers, 1)
	waitFor(t, c.IsLeader)
	// On election the leader creates the partitions for the periods ahead.
	next := "hopper_job_history_completed_" + time.Now().UTC().Add(time.Hour).Format("2006010215")
	waitFor(t, func() bool { return h.count("SELECT count(*) FROM pg_tables WHERE tablename = $1", next) == 1 })
	nextDay := "hopper_job_history_failed_" + time.Now().UTC().Add(24*time.Hour).Format("20060102")
	if h.count("SELECT count(*) FROM pg_tables WHERE tablename = $1", nextDay) != 1 {
		t.Errorf("daily partition %s missing", nextDay)
	}
}

func TestNotificationsWakeOtherClients(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	started := make(chan time.Time, 10)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error {
		started <- time.Now()
		return nil
	})
	// A slow poll makes it obvious whether the notification did the work.
	worker := h.client(&hopper.Config{
		Queues:       map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers:      workers,
		PollInterval: 5 * time.Second,
	})
	if err := worker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, worker.Listening)
	inserter := h.client(&hopper.Config{})

	pickup := func(insert func() error) time.Duration {
		t.Helper()
		t0 := time.Now()
		if err := insert(); err != nil {
			t.Fatal(err)
		}
		select {
		case t1 := <-started:
			return t1.Sub(t0)
		case <-time.After(10 * time.Second):
			t.Fatal("job not picked up")
			return 0
		}
	}

	// Pool insert from another client: coalesced notification.
	if d := pickup(func() error { _, err := inserter.Insert(ctx, noop{}, nil); return err }); d > time.Second {
		t.Errorf("pickup after pool insert took %s", d)
	}
	// Transactional insert: notified on commit.
	if d := pickup(func() error {
		tx, err := h.pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := inserter.InsertTx(ctx, tx, noop{}, nil); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}); d > time.Second {
		t.Errorf("pickup after transactional insert took %s", d)
	}
	// A large COPY batch notifies too.
	params := make([]hopper.InsertParams, 300)
	for i := range params {
		params[i] = hopper.InsertParams{Args: noop{N: i}}
	}
	if d := pickup(func() error { _, err := inserter.InsertMany(ctx, params); return err }); d > time.Second {
		t.Errorf("pickup after COPY insert took %s", d)
	}
}

func TestListenerReconnectsAfterDisconnect(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var ran atomic.Int32
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error {
		ran.Add(1)
		return nil
	})
	c := h.client(&hopper.Config{
		Queues:       map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers:      workers,
		PollInterval: 300 * time.Millisecond,
	})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, c.Listening)

	// Kill the listener's backend. Work continues by polling, and the
	// listener comes back.
	var pid int
	if err := h.pool.QueryRow(ctx, "SELECT pid FROM pg_stat_activity WHERE application_name = 'hopper-listener:' || current_schema()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	if _, err := h.pool.Exec(ctx, "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return !c.Listening() })
	inserter := h.client(&hopper.Config{})
	if _, err := inserter.Insert(ctx, noop{}, nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return ran.Load() == 1 })
	waitFor(t, c.Listening)
	if h.count("SELECT count(*) FROM pg_stat_activity WHERE application_name = 'hopper-listener:' || current_schema()") != 1 {
		t.Error("expected exactly one listener backend after reconnect")
	}
}

func TestRescueStuckAfter(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	w := newBlocker()
	workers := hopper.NewWorkers()
	hopper.AddWorker(workers, w)
	c := h.client(&hopper.Config{
		Queues:           map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers:          workers,
		RescueStuckAfter: time.Second,
	})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	<-w.started
	// The worker ignores its context; after a second the leader rescues the
	// job, out of attempts, so it is discarded with the reason recorded.
	job := waitForJob(t, c, res.Job.ID, hopper.JobStateDiscarded)
	if len(job.Errors) != 1 || !strings.Contains(job.Errors[0].Error, "RescueStuckAfter") {
		t.Errorf("stuck job = %+v", job)
	}
	close(w.release)
}

func TestInsertOnlyClientNotifiesWorkers(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	done := make(chan struct{}, 1)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error {
		done <- struct{}{}
		return nil
	})
	worker := h.client(&hopper.Config{
		Queues:       map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers:      workers,
		PollInterval: 5 * time.Second,
	})
	if err := worker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, worker.Listening)

	// An insert-only client that is stopped right after inserting still
	// gets its notification out.
	inserter := h.client(&hopper.Config{})
	if _, err := inserter.Insert(ctx, noop{}, nil); err != nil {
		t.Fatal(err)
	}
	if err := inserter.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker not notified within 2s (poll is 5s)")
	}
	if err := errors.Join(); err != nil {
		t.Fatal(err)
	}
	_ = fmt.Sprint()
}
