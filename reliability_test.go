package hopper_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/drivertest"
	"github.com/parallelworks/hopper/internal/testdb"
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

// A unique job that counts finished duplicates is not inserted again once
// its duplicate has completed, through every insert call.
func TestUniqueFinished(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error { return nil })
	c := h.started(workers, 2)
	once := func(n int) hopper.InsertParams {
		return hopper.InsertParams{Args: noop{N: n}, Opts: &hopper.InsertOpts{
			Await:  true,
			Unique: &hopper.UniqueOpts{ByArgs: true, Finished: []hopper.JobState{hopper.JobStateCompleted, hopper.JobStateCompleted}},
		}}
	}
	first, err := c.Insert(ctx, once(1).Args, once(1).Opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hopper.Await[struct{}](ctx, c, first.Job.ID); err != nil {
		t.Fatal(err)
	}

	again, err := c.Insert(ctx, once(1).Args, once(1).Opts)
	if err != nil {
		t.Fatal(err)
	}
	if !again.Duplicate || again.Job.ID != first.Job.ID || again.Job.State != hopper.JobStateCompleted {
		t.Errorf("insert after completion = %+v", again.Job)
	}
	batch, err := c.InsertMany(ctx, []hopper.InsertParams{once(2), once(1)})
	if err != nil {
		t.Fatal(err)
	}
	if batch[0].Duplicate || !batch[1].Duplicate || batch[1].Job.ID != first.Job.ID {
		t.Errorf("batch = %+v, %+v", batch[0], batch[1])
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // committed on the happy path
	inTx, err := c.InsertTx(ctx, tx, once(1).Args, once(1).Opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if !inTx.Duplicate || inTx.Job.ID != first.Job.ID {
		t.Errorf("insert in tx after completion = %+v", inTx.Job)
	}

	// A job without Finished ignores history, also after one with the same
	// key in its batch that counts it.
	mixed, err := c.InsertMany(ctx, []hopper.InsertParams{
		once(1),
		{Args: noop{N: 1}, Opts: &hopper.InsertOpts{Unique: &hopper.UniqueOpts{ByArgs: true}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !mixed[0].Duplicate || mixed[0].Job.ID != first.Job.ID {
		t.Errorf("batch input counting history = %+v", mixed[0].Job)
	}
	if mixed[1].Duplicate {
		t.Errorf("history counted without Finished: %+v", mixed[1].Job)
	}
	if _, err := c.Insert(ctx, noop{N: 3}, &hopper.InsertOpts{Unique: &hopper.UniqueOpts{Finished: []hopper.JobState{hopper.JobStateRunning}}}); err == nil {
		t.Error("Finished accepted a state that is not finished")
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
	// The leader prunes the crashed client's lease row after the rescue, in
	// the same pass but a few statements later.
	waitFor(t, func() bool { return h.count("SELECT count(*) FROM hopper_clients") == 1 })
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

	// The running job's context was canceled, and the job runs again under
	// the new lease.
	waitFor(t, func() bool {
		job, err := c.JobGet(ctx, res.Job.ID)
		return err == nil && (job.State != hopper.JobStateRunning || job.AttemptedBy != oldID)
	})
	waitFor(t, func() bool { w.rec.mu.Lock(); defer w.rec.mu.Unlock(); return w.rec.seen[res.Job.ID] >= 2 })
	close(w.release)
	// The job completes on its second or later attempt with one error per
	// earlier one (the canceled attempt's own error, or the leader's
	// rescue, whichever lands first). Which client ID finishes it depends
	// on timing: the re-claim may happen under the new lease, or under the
	// lapsing ID before this client notices, which fencing allows since
	// that attempt still owns the row; in the latter case the leader may
	// rescue once more first.
	job := waitForJob(t, c, res.Job.ID, hopper.JobStateCompleted)
	if (job.AttemptedBy != c.ClientID() && job.AttemptedBy != oldID) || job.Attempt < 2 || len(job.Errors) < 1 || len(job.Errors) != job.Attempt-1 {
		t.Errorf("job after fencing = %+v", job)
	}
}

// lastAttemptWorker reports when it starts, waits to be released, and records
// whether its context was canceled while it waited.
type lastAttemptWorker struct {
	hopper.WorkerDefaults[noop]
	started  chan struct{}
	release  chan struct{}
	runs     atomic.Int64
	canceled atomic.Bool
}

func (w *lastAttemptWorker) Work(ctx context.Context, _ *hopper.Job[noop]) error {
	w.runs.Add(1)
	w.started <- struct{}{}
	select {
	case <-w.release:
		return nil
	case <-ctx.Done():
		w.canceled.Store(true)
		return ctx.Err()
	}
}

func (w *lastAttemptWorker) Timeout(*hopper.Job[noop]) time.Duration { return -1 }

func TestFencedClientLetsALastAttemptFinish(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	w := &lastAttemptWorker{started: make(chan struct{}, 10), release: make(chan struct{})}
	workers := hopper.NewWorkers()
	hopper.AddWorker(workers, w)
	c := h.started(workers, 1)
	oldID := c.ClientID()

	res, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	<-w.started

	if _, err := h.pool.Exec(ctx, "UPDATE hopper_clients SET expires_at = now() - interval '1s' WHERE id = $1", oldID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return c.ClientID() != oldID })

	// The job has no attempt left, so nothing will run it again and the
	// fenced client leaves it running. Whether its result or the leader's
	// rescue lands first decides the recorded state, so only the run itself
	// is asserted.
	close(w.release)
	job := waitForJob(t, c, res.Job.ID, hopper.JobStateCompleted, hopper.JobStateDiscarded)
	if w.canceled.Load() {
		t.Errorf("the last attempt was canceled when the lease was lost; job = %+v", job)
	}
	if runs := w.runs.Load(); runs != 1 {
		t.Errorf("job ran %d times, want 1", runs)
	}
}

func TestNeverLeadClientWorksJobsWithoutLeading(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()

	worked := make(chan hopper.JobID, 1)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[noop]) error {
		worked <- job.ID
		return nil
	})
	follower := h.client(&hopper.Config{
		Queues:    map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers:   workers,
		NeverLead: true,
	})
	if err := follower.Start(ctx); err != nil {
		t.Fatal(err)
	}

	res, err := follower.Insert(ctx, noop{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-worked:
		if id != res.Job.ID {
			t.Fatalf("worked %s, want %s", id, res.Job.ID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the job was never worked")
	}

	// Several leader intervals pass without it standing.
	time.Sleep(500 * time.Millisecond)
	if follower.IsLeader() {
		t.Fatal("a NeverLead client became leader")
	}
	if n := h.count("SELECT count(*) FROM hopper_leader"); n != 0 {
		t.Errorf("leader rows = %d, want 0", n)
	}

	leader := h.started(hopper.NewWorkers(), 1)
	waitFor(t, leader.IsLeader)
	if follower.IsLeader() {
		t.Error("the NeverLead client became leader alongside another")
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
	waitFor(t, func() bool {
		return h.count("SELECT count(*) FROM pg_tables WHERE schemaname = current_schema() AND tablename = $1", next) == 1
	})
	nextDay := "hopper_job_history_failed_" + time.Now().UTC().Add(24*time.Hour).Format("20060102")
	waitFor(t, func() bool {
		return h.count("SELECT count(*) FROM pg_tables WHERE schemaname = current_schema() AND tablename = $1", nextDay) == 1
	})
}

// TestLeaderRetriesDeferredPartitions holds off the attach of the
// partitions ahead with a transaction that writes to the DEFAULT history
// partition, as an open CompleteTx does. The leader defers them, and creates
// them on a later pass once the transaction ends, without waiting for the
// next maintenance interval.
func TestLeaderRetriesDeferredPartitions(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "LOCK TABLE hopper_job_history_completed_default IN ROW EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	c := h.started(hopper.NewWorkers(), 1)
	waitFor(t, c.IsLeader)
	next := "hopper_job_history_completed_" + time.Now().UTC().Add(time.Hour).Format("2006010215")
	time.Sleep(3 * fastTuning.LeaderInterval)
	if h.count("SELECT count(*) FROM pg_tables WHERE schemaname = current_schema() AND tablename = $1", next) != 0 {
		t.Fatal("partition attached while the DEFAULT partition was in use")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return h.count("SELECT count(*) FROM pg_tables WHERE schemaname = current_schema() AND tablename = $1", next) == 1
	})
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

// TestLeaderKeepsLeaseWhileIndexingHistory has the leader build the history
// indexes an upgrade declares (the DEFAULT partitions of a fresh schema
// have none) while a transaction elsewhere in the database holds an old
// snapshot, which a concurrent index build waits for. The leader keeps
// renewing its lease meanwhile: no other client takes over, so leadership,
// and what fires once per term, does not repeat.
func TestLeaderKeepsLeaseWhileIndexingHistory(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	held, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Rollback(ctx) }()
	if _, err := held.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		t.Fatal(err)
	}
	if _, err := held.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	var elected atomic.Int64
	ectx, cancel := context.WithCancel(ctx)
	defer cancel()
	cfg := func() *hopper.Config {
		return &hopper.Config{Queues: map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}}, Workers: hopper.NewWorkers()}
	}
	clients := []*hopper.Client[pgxTx]{h.client(cfg()), h.client(cfg())}
	for _, c := range clients {
		go func() {
			for range c.Events(ectx, hopper.EventLeaderElected) {
				elected.Add(1)
			}
		}()
	}
	for _, c := range clients {
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// The build is under way, waiting for the old snapshot, before the
	// lease is watched through it.
	waitFor(t, func() bool {
		return h.count(`SELECT count(*) FROM pg_stat_progress_create_index
			WHERE relid IN (to_regclass('hopper_job_history_completed_default'), to_regclass('hopper_job_history_failed_default'))`) > 0
	})
	time.Sleep(3 * fastTuning.LeaderTTL)
	if n := elected.Load(); n != 1 {
		t.Errorf("%d elections while the leader indexed history, want 1", n)
	}
	if err := held.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	const unindexed = `SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		CROSS JOIN (VALUES ('_ordering'), ('_unique')) AS s(suffix)
		WHERE i.inhparent IN ('hopper_job_history_completed'::regclass, 'hopper_job_history_failed'::regclass)
		  AND NOT EXISTS (SELECT 1 FROM pg_index x JOIN pg_inherits xi ON xi.inhrelid = x.indexrelid
		    WHERE x.indrelid = c.oid AND xi.inhparent = to_regclass(i.inhparent::regclass::text || s.suffix))`
	// A concurrent build also waits for every older snapshot elsewhere in
	// the database, such as other packages' tests queued for the migration
	// lock, so it can take longer than waitFor allows.
	for deadline := time.Now().Add(2 * time.Minute); h.count(unindexed) != 0; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("history partitions still without an attached index")
		}
	}
}

// TestLeaderBacksOffTimedOutIndexBuilds runs the index loop on sessions
// whose statement_timeout cuts every build short, the builds waiting for a
// held snapshot. Each partition's build times out on its own, the pass going
// on to the next, and is retried at doubling intervals.
func TestLeaderBacksOffTimedOutIndexBuilds(t *testing.T) {
	t.Parallel()
	defaults := []string{"hopper_job_history_completed_default", "hopper_job_history_failed_default"}
	t.Run("pass", func(t *testing.T) {
		t.Parallel()
		// One pass only: the next is a minute away.
		logs := timedOutBuilds(t, time.Minute)
		waitFor(t, func() bool { return logs.count(defaults[0]) > 0 && logs.count(defaults[1]) > 0 })
	})
	t.Run("backoff", func(t *testing.T) {
		t.Parallel()
		logs := timedOutBuilds(t, 250*time.Millisecond)
		waitFor(t, func() bool { return logs.count(defaults[0]) > 0 && logs.count(defaults[1]) > 0 })
		// Without backoff each partition would time out on every pass, about
		// eight times in three seconds; with it, after waits of 0.5, 1 and 2
		// seconds.
		time.Sleep(3 * time.Second)
		for _, p := range defaults {
			if n := logs.count(p); n < 2 || n > 4 {
				t.Errorf("%s timed out %d times in three seconds, want 2 to 4", p, n)
			}
		}
	})
}

// TestLeaderReportsInvalidHistoryIndex leaves an outcome table's index
// invalid with every partition indexed, as an earlier release's retention
// could. The leader repairs it where Postgres's re-attach counts the attached
// indexes again, and otherwise warns, at doubling intervals capped at eight
// maintenance intervals, so that a server updated to a release that repairs
// it is not left waiting for the longer backoff of a timed-out build.
func TestLeaderReportsInvalidHistoryIndex(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	exec := func(query string) {
		t.Helper()
		if _, err := h.pool.Exec(ctx, query); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	repairs := drivertest.ReattachRevalidates(exec, func(query string) string {
		t.Helper()
		var v string
		if err := h.pool.QueryRow(ctx, query).Scan(&v); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return v
	})
	if _, err := h.d.Executor().HistoryIndex(ctx, driver.HistoryIndexParams{}); err != nil {
		t.Fatal(err)
	}
	const index = "hopper_job_history_failed_ordering"
	exec("UPDATE pg_index SET indisvalid = false WHERE indexrelid = '" + index + "'::regclass")
	logs := &timeoutLog{counts: map[string]int{}, message: "stays invalid", key: "index"}
	c, err := hopper.NewClient(h.d, &hopper.Config{
		Queues: map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}}, Workers: hopper.NewWorkers(),
		Logger: slog.New(logs),
	})
	if err != nil {
		t.Fatal(err)
	}
	tun := fastTuning
	tun.Maintenance = 100 * time.Millisecond
	c.SetTuning(tun)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	if repairs {
		waitFor(t, func() bool {
			return h.count("SELECT indisvalid::int FROM pg_index WHERE indexrelid = '"+index+"'::regclass") == 1
		})
		if n := logs.count(index); n != 0 {
			t.Errorf("repaired index reported %d times", n)
		}
		return
	}
	waitFor(t, func() bool { return logs.count(index) > 0 })
	// After waits of 0.2, 0.4 and then 0.8 seconds: 8 reports in five
	// seconds, against 5 if the waits went on doubling.
	time.Sleep(5 * time.Second)
	if n := logs.count(index); n < 6 || n > 9 {
		t.Errorf("%s reported %d times in five seconds, want 6 to 9", index, n)
	}
}

// TestLeaderBacksOffDeferredIndexRepair leaves an outcome table's index
// invalid with every partition indexed, and holds the re-attach that would
// repair it off with a transaction that read history. Each pass waits longer
// for the lock, as deferred partition work does, and the leader warns from
// the fifth deferred pass in a row on.
func TestLeaderBacksOffDeferredIndexRepair(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	// The partitions ahead exist before the reader holds their attaches
	// off, so that the leader's first maintenance, which the index loop
	// waits for, completes.
	if _, err := h.d.Executor().HistoryMaintain(ctx, driver.HistoryMaintainParams{}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.d.Executor().HistoryIndex(ctx, driver.HistoryIndexParams{}); err != nil {
		t.Fatal(err)
	}
	const index = "hopper_job_history_failed_ordering"
	if _, err := h.pool.Exec(ctx, "UPDATE pg_index SET indisvalid = false WHERE indexrelid = '"+index+"'::regclass"); err != nil {
		t.Fatal(err)
	}
	reader, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Rollback(ctx) })
	if _, err := reader.Exec(ctx, "SELECT count(*) FROM hopper_job_history WHERE ordering_key = 'lane'"); err != nil {
		t.Fatal(err)
	}
	logs := &timeoutLog{counts: map[string]int{}, message: "attach deferred", key: "passes", level: slog.LevelWarn}
	c, err := hopper.NewClient(h.d, &hopper.Config{
		Queues: map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}}, Workers: hopper.NewWorkers(),
		Logger: slog.New(logs),
	})
	if err != nil {
		t.Fatal(err)
	}
	tun := fastTuning
	tun.Maintenance = 100 * time.Millisecond
	c.SetTuning(tun)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	waitFor(t, func() bool { return logs.count("5") > 0 })
	if n := logs.count("4"); n != 0 {
		t.Errorf("warned after 4 deferred passes")
	}
	if v := h.count("SELECT indisvalid::int FROM pg_index WHERE indexrelid = '" + index + "'::regclass"); v != 0 {
		t.Errorf("%s repaired behind the reader", index)
	}
}

// TestLeaderWarnsAboutKeptPartition makes the last partition an outcome
// table's index lacks pass retention while its build keeps running past the
// session's statement_timeout. Retention keeps the partition on every pass,
// and the leader warns from the fifth pass in a row on.
func TestLeaderWarnsAboutKeptPartition(t *testing.T) {
	t.Parallel()
	logs := &timeoutLog{counts: map[string]int{}, message: "kept until their index is built", key: "passes", level: slog.LevelWarn}
	timedOutBuildsLogging(t, 100*time.Millisecond, logs, func(h *harness) {
		const old = "hopper_job_history_failed_20000101"
		for _, query := range []string{
			"DROP INDEX hopper_job_history_failed_ordering",
			"CREATE TABLE " + old + " (LIKE hopper_job_history INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
			"ALTER TABLE hopper_job_history_failed ATTACH PARTITION " + old + " FOR VALUES FROM ('2000-01-01Z') TO ('2000-01-02Z')",
			"CREATE INDEX hopper_job_history_failed_ordering ON ONLY hopper_job_history_failed (ordering_key, id) WHERE ordering_key IS NOT NULL",
		} {
			if _, err := h.pool.Exec(context.Background(), query); err != nil {
				t.Fatalf("%s: %v", query, err)
			}
		}
		if _, err := h.d.Executor().HistoryIndex(context.Background(), driver.HistoryIndexParams{Skip: []string{old}}); err != nil {
			t.Fatal(err)
		}
	}, func(cfg *hopper.Config) { cfg.FailedRetention = 48 * time.Hour })
	waitFor(t, func() bool { return logs.count("5") > 0 })
	if n := logs.count("4"); n != 0 {
		t.Errorf("warned after 4 passes")
	}
}

// TestLeaderDoesNotWarnAboutPartitionKeptForBuild keeps the last partition
// an outcome table's index lacks past retention while a build holds it, for
// more passes than the warning waits for: a long build is reported at Info,
// and does not count towards the warning.
func TestLeaderDoesNotWarnAboutPartitionKeptForBuild(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	const old = "hopper_job_history_failed_20000101"
	for _, query := range []string{
		"DROP INDEX hopper_job_history_failed_ordering",
		"CREATE TABLE " + old + " (LIKE hopper_job_history INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
		"ALTER TABLE hopper_job_history_failed ATTACH PARTITION " + old + " FOR VALUES FROM ('2000-01-01Z') TO ('2000-01-02Z')",
		"CREATE INDEX hopper_job_history_failed_ordering ON ONLY hopper_job_history_failed (ordering_key, id) WHERE ordering_key IS NOT NULL",
	} {
		if _, err := h.pool.Exec(ctx, query); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	if _, err := h.d.Executor().HistoryIndex(ctx, driver.HistoryIndexParams{Skip: []string{old}}); err != nil {
		t.Fatal(err)
	}
	// The partition's build lock (HistoryIndexLockKey), held as a build
	// holds it.
	build, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = build.Rollback(ctx) })
	if _, err := build.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext(current_schema() || '.' || $1), 1752133737)", old); err != nil {
		t.Fatal(err)
	}
	kept := &timeoutLog{counts: map[string]int{}, message: "kept while their index is built", key: "kept"}
	counted := &timeoutLog{counts: map[string]int{}, message: "kept until their index is built", key: "passes"}
	c := h.client(&hopper.Config{
		Queues: map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}}, Workers: hopper.NewWorkers(),
		Logger: slog.New(teeHandler{kept, counted}), FailedRetention: 48 * time.Hour,
	})
	tun := fastTuning
	tun.Maintenance = 100 * time.Millisecond
	c.SetTuning(tun)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return kept.count("["+old+"]") > 6 })
	if n := counted.count("1"); n != 0 {
		t.Errorf("a partition kept for a build under way counted towards the warning %d times", n)
	}
}

// teeHandler hands each record to every one of its handlers.
type teeHandler []slog.Handler

func (t teeHandler) Enabled(context.Context, slog.Level) bool { return true }
func (t teeHandler) WithAttrs([]slog.Attr) slog.Handler       { return t }
func (t teeHandler) WithGroup(string) slog.Handler            { return t }

func (t teeHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, h := range t {
		if err := h.Handle(ctx, r.Clone()); err != nil {
			return err
		}
	}
	return nil
}

// timedOutBuilds starts a leader whose sessions have a statement_timeout
// that every index build overruns, waiting for a snapshot held open, and
// counts the timed-out builds it logs.
func timedOutBuilds(t *testing.T, maintenance time.Duration) *timeoutLog {
	t.Helper()
	logs := &timeoutLog{counts: map[string]int{}, message: "statement_timeout", key: "partition"}
	timedOutBuildsLogging(t, maintenance, logs, func(*harness) {}, func(*hopper.Config) {})
	return logs
}

// timedOutBuildsLogging is timedOutBuilds with the leader logging to logs,
// setup run on the harness before the snapshot is taken, and configure
// applied to the client's configuration.
func timedOutBuildsLogging(t *testing.T, maintenance time.Duration, logs *timeoutLog, setup func(*harness), configure func(*hopper.Config)) {
	t.Helper()
	h := newHarness(t)
	setup(h)
	ctx := context.Background()
	held, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Rollback(ctx) })
	if _, err := held.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		t.Fatal(err)
	}
	if _, err := held.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	pcfg := h.pool.Config()
	pcfg.ConnConfig.RuntimeParams["statement_timeout"] = "200ms"
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	cfg := &hopper.Config{
		Queues: map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}}, Workers: hopper.NewWorkers(),
		Logger: slog.New(logs),
	}
	configure(cfg)
	c, err := hopper.NewClient(testdb.Driver(pool), cfg)
	if err != nil {
		t.Fatal(err)
	}
	tun := fastTuning
	tun.Maintenance = maintenance
	c.SetTuning(tun)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
}

// timeoutLog counts the index loop's records whose message contains
// message, by their key attribute: timed-out builds per partition,
// unrepairable indexes, or the passes a warning repeats over. Records below
// level (Info unless set) are left out.
type timeoutLog struct {
	mu           sync.Mutex
	counts       map[string]int
	message, key string
	level        slog.Level
}

func (l *timeoutLog) Enabled(context.Context, slog.Level) bool { return true }
func (l *timeoutLog) WithAttrs([]slog.Attr) slog.Handler       { return l }
func (l *timeoutLog) WithGroup(string) slog.Handler            { return l }

func (l *timeoutLog) Handle(_ context.Context, r slog.Record) error {
	if !strings.Contains(r.Message, l.message) || r.Level < l.level {
		return nil
	}
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == l.key {
			l.mu.Lock()
			l.counts[a.Value.String()]++
			l.mu.Unlock()
		}
		return true
	})
	return nil
}

func (l *timeoutLog) count(name string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.counts[name]
}
