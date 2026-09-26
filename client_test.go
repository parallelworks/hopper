package hopper_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver/hopperpgx"
	"github.com/parallelworks/hopper/internal/testdb"
)

// fastTuning shortens internal timings so tests finish quickly.
var fastTuning = hopper.Tuning{
	LeaseTTL:         2 * time.Second,
	LeaseRenew:       500 * time.Millisecond,
	ClaimCooldown:    5 * time.Millisecond,
	FinalizeInterval: 5 * time.Millisecond,
	FinalizeBatch:    500,
	StopGrace:        500 * time.Millisecond,
	CopyThreshold:    256,
}

func testLogger() *slog.Logger {
	if testing.Verbose() {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.DiscardHandler)
}

// harness is a test database with a client factory.
type harness struct {
	t    *testing.T
	pool *pgxpool.Pool
	d    *hopperpgx.Driver
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	pool := testdb.Pool(t)
	return &harness{t: t, pool: pool, d: hopperpgx.New(pool)}
}

// client returns a client with fast tuning. It is stopped at test end.
func (h *harness) client(cfg *hopper.Config) *hopper.Client[pgxTx] {
	h.t.Helper()
	if cfg.Logger == nil {
		cfg.Logger = testLogger()
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	c, err := hopper.NewClient(h.d, cfg)
	if err != nil {
		h.t.Fatalf("NewClient: %v", err)
	}
	c.SetTuning(fastTuning)
	h.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.Stop(ctx); err != nil && !errors.Is(err, hopper.ErrClientStopped) {
			h.t.Errorf("Stop: %v", err)
		}
	})
	return c
}

// started returns a started client working queue "default" with the given
// workers.
func (h *harness) started(workers *hopper.Workers, maxWorkers int) *hopper.Client[pgxTx] {
	h.t.Helper()
	c := h.client(&hopper.Config{
		Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: maxWorkers}},
		Workers: workers,
	})
	if err := c.Start(context.Background()); err != nil {
		h.t.Fatalf("Start: %v", err)
	}
	return c
}

func (h *harness) count(query string, args ...any) int {
	h.t.Helper()
	var n int
	if err := h.pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		h.t.Fatalf("%s: %v", query, err)
	}
	return n
}

// waitFor polls cond until it is true or the deadline passes.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

// waitForJob polls until the job reaches one of the states.
func waitForJob(t *testing.T, c *hopper.Client[pgxTx], id hopper.JobID, states ...hopper.JobState) *hopper.JobRow {
	t.Helper()
	var job *hopper.JobRow
	waitFor(t, func() bool {
		var err error
		job, err = c.JobGet(context.Background(), id)
		if err != nil {
			return false
		}
		for _, s := range states {
			if job.State == s {
				return true
			}
		}
		return false
	})
	return job
}

// Job kinds used by the tests.

type noop struct {
	N int `json:"n"`
}

func (noop) Kind() string { return "noop" }

type failing struct {
	Msg string `json:"msg"`
}

func (failing) Kind() string { return "failing" }

type withDefaults struct{}

func (withDefaults) Kind() string { return "with_defaults" }
func (withDefaults) InsertOpts() hopper.InsertOpts {
	return hopper.InsertOpts{Queue: "email", Priority: hopper.PriorityHigh, MaxAttempts: 3}
}

type unregistered struct{}

func (unregistered) Kind() string { return "unregistered" }

// recorder counts executions per job ID.
type recorder struct {
	mu    sync.Mutex
	seen  map[hopper.JobID]int
	total atomic.Int64
}

func newRecorder() *recorder { return &recorder{seen: map[hopper.JobID]int{}} }

func (r *recorder) record(id hopper.JobID) {
	r.mu.Lock()
	r.seen[id]++
	r.mu.Unlock()
	r.total.Add(1)
}

func TestNewClientValidation(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	workers := hopper.NewWorkers()
	cases := map[string]*hopper.Config{
		"queue without workers":  {Queues: map[string]hopper.QueueConfig{"q": {MaxWorkers: 1}}},
		"zero max workers":       {Queues: map[string]hopper.QueueConfig{"q": {}}, Workers: workers},
		"empty queue name":       {Queues: map[string]hopper.QueueConfig{"": {MaxWorkers: 1}}, Workers: workers},
		"negative attempts":      {MaxAttempts: -1},
		"negative timeout":       {JobTimeout: -1},
		"strict without workers": {StrictKinds: true},
	}
	for name, cfg := range cases {
		if _, err := hopper.NewClient(h.d, cfg); err == nil {
			t.Errorf("%s: NewClient succeeded", name)
		}
	}
	if _, err := hopper.NewClient[pgxTx](nil, nil); err == nil {
		t.Error("nil driver accepted")
	}
	if _, err := hopper.NewClient(h.d, nil); err != nil {
		t.Errorf("nil config rejected: %v", err)
	}
}

func TestWorkersRejectDuplicateKind(t *testing.T) {
	t.Parallel()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error { return nil })
	defer func() {
		if recover() == nil {
			t.Fatal("registering a kind twice did not panic")
		}
	}()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error { return nil })
}

func TestInsertDefaultsAndOverrides(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error { return nil })
	c := h.client(&hopper.Config{Workers: workers, MaxAttempts: 7}) // insert-only

	res, err := c.Insert(ctx, noop{N: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	j := res.Job
	if j.Queue != hopper.QueueDefault || j.Priority != int(hopper.PriorityNormal) || j.MaxAttempts != 7 ||
		j.State != hopper.JobStateAvailable || string(j.Args) != `{"n": 1}` || string(j.Metadata) != `{}` {
		t.Errorf("defaults: %+v", j)
	}

	// Kind-level defaults, then per-call overrides on top.
	res, err = c.Insert(ctx, withDefaults{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if j := res.Job; j.Queue != "email" || j.Priority != 1 || j.MaxAttempts != 3 {
		t.Errorf("kind defaults: %+v", j)
	}
	future := time.Now().Add(time.Hour)
	res, err = c.Insert(ctx, withDefaults{}, &hopper.InsertOpts{
		Queue: "other", MaxAttempts: 9, ScheduledAt: future, Metadata: []byte(`{"trace":"abc"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if j := res.Job; j.Queue != "other" || j.Priority != 1 || j.MaxAttempts != 9 ||
		j.State != hopper.JobStateScheduled || string(j.Metadata) != `{"trace": "abc"}` {
		t.Errorf("overrides: %+v", j)
	}

	// Validation.
	for name, p := range map[string]hopper.InsertParams{
		"nil args":         {},
		"bad priority":     {Args: noop{}, Opts: &hopper.InsertOpts{Priority: 5}},
		"bad max attempts": {Args: noop{}, Opts: &hopper.InsertOpts{MaxAttempts: -1}},
		"bad metadata":     {Args: noop{}, Opts: &hopper.InsertOpts{Metadata: []byte(`{`)}},
	} {
		if _, err := c.Insert(ctx, p.Args, p.Opts); err == nil {
			t.Errorf("%s: insert succeeded", name)
		}
	}
	// A kind without a worker is allowed by default...
	if _, err := c.Insert(ctx, unregistered{}, nil); err != nil {
		t.Errorf("unregistered kind rejected: %v", err)
	}
	// ...and rejected with StrictKinds.
	strict := h.client(&hopper.Config{Workers: workers, StrictKinds: true})
	var unknown *hopper.UnknownKindError
	if _, err := strict.Insert(ctx, unregistered{}, nil); !errors.As(err, &unknown) || unknown.Kind != "unregistered" {
		t.Errorf("StrictKinds: err = %v", err)
	}

	// An empty batch is a no-op.
	if results, err := c.InsertMany(ctx, nil); err != nil || len(results) != 0 {
		t.Errorf("empty InsertMany = %v, %v", results, err)
	}
}

func TestInsertTxCommitsWithTransaction(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	c := h.client(&hopper.Config{})

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.InsertTx(ctx, tx, noop{N: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.JobGet(ctx, res.Job.ID); !errors.Is(err, hopper.ErrNotFound) {
		t.Errorf("job survived rollback: %v", err)
	}

	tx, err = h.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	results, err := c.InsertManyTx(ctx, tx, []hopper.InsertParams{{Args: noop{N: 1}}, {Args: noop{N: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if j, err := c.JobGet(ctx, results[1].Job.ID); err != nil || string(j.Args) != `{"n": 2}` {
		t.Errorf("committed job: %+v, %v", j, err)
	}
}

func TestClientWorksJobs(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	rec := newRecorder()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[noop]) error {
		rec.record(job.ID)
		return nil
	})
	c := h.started(workers, 10)

	// Jobs inserted before and after start, singly and in a COPY batch.
	const n = 600
	params := make([]hopper.InsertParams, n-1)
	for i := range params {
		params[i] = hopper.InsertParams{Args: noop{N: i}}
	}
	results, err := c.InsertMany(ctx, params)
	if err != nil {
		t.Fatal(err)
	}
	single, err := c.Insert(ctx, noop{N: n}, nil)
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { return rec.total.Load() >= n })
	waitFor(t, func() bool { return h.count("SELECT count(*) FROM hopper_jobs") == 0 })

	if got := h.count("SELECT count(*) FROM hopper_job_history WHERE state = 'completed'"); got != n {
		t.Errorf("completed in history = %d, want %d", got, n)
	}
	for _, r := range results {
		if rec.seen[r.Job.ID] != 1 {
			t.Errorf("job %s ran %d times", r.Job.ID, rec.seen[r.Job.ID])
		}
	}
	job, err := c.JobGet(ctx, single.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != hopper.JobStateCompleted || job.Attempt != 1 || job.FinalizedAt.IsZero() || job.AttemptedBy != c.ClientID() {
		t.Errorf("finalized job = %+v", job)
	}
}

func TestClientHonorsPriority(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var mu sync.Mutex
	var order []int
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[noop]) error {
		mu.Lock()
		order = append(order, job.Args.N)
		mu.Unlock()
		return nil
	})
	c := h.client(&hopper.Config{
		Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers: workers,
	})
	// Insert before starting so the queue is already populated.
	for i, p := range []hopper.Priority{hopper.PriorityLowest, hopper.PriorityLow, hopper.PriorityHigh, hopper.PriorityNormal, hopper.PriorityHigh} {
		if _, err := c.Insert(ctx, noop{N: i}, &hopper.InsertOpts{Priority: p}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(order) == 5 })
	if want := "[2 4 3 1 0]"; fmt.Sprint(order) != want {
		t.Errorf("run order = %v, want %s", order, want)
	}
}

type retryWorker struct {
	hopper.WorkerDefaults[failing]
	rec *recorder
}

func (w *retryWorker) Work(_ context.Context, job *hopper.Job[failing]) error {
	w.rec.record(job.ID)
	return errors.New(job.Args.Msg)
}

// NextRetry retries immediately so the test does not wait for backoff.
func (w *retryWorker) NextRetry(*hopper.Job[failing]) time.Time { return time.Now() }

func TestRetryUntilDiscarded(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	rec := newRecorder()
	workers := hopper.NewWorkers()
	hopper.AddWorker(workers, &retryWorker{rec: rec})
	c := h.started(workers, 2)

	res, err := c.Insert(ctx, failing{Msg: "nope"}, &hopper.InsertOpts{MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	job := waitForJob(t, c, res.Job.ID, hopper.JobStateDiscarded)
	if rec.seen[res.Job.ID] != 3 || job.Attempt != 3 || len(job.Errors) != 3 {
		t.Fatalf("ran %d times, job = %+v", rec.seen[res.Job.ID], job)
	}
	for i, e := range job.Errors {
		if e.Attempt != i+1 || e.Error != "nope" || e.At.IsZero() || e.Trace != "" {
			t.Errorf("error %d = %+v", i, e)
		}
	}
	if job.FinalizedAt.IsZero() || h.count("SELECT count(*) FROM hopper_job_history_failed") != 1 {
		t.Error("discarded job not in the failed history partition")
	}
}

func TestRetryUsesBackoff(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[failing]) error { return errors.New("x") })
	c := h.started(workers, 1)

	res, err := c.Insert(ctx, failing{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	job := waitForJob(t, c, res.Job.ID, hopper.JobStateRetryable)
	var dbNow time.Time
	if err := h.pool.QueryRow(ctx, "SELECT now()").Scan(&dbNow); err != nil {
		t.Fatal(err)
	}
	// attempt 1: 1^4 + 5 = 6s ± 10%.
	if d := job.ScheduledAt.Sub(dbNow); d < 5*time.Second || d > 7*time.Second {
		t.Errorf("first retry in %s, want ~6s", d)
	}
	if job.AttemptedBy != 0 || job.Attempt != 1 {
		t.Errorf("retryable job = %+v", job)
	}
}

func TestSnoozeKeepsAttempt(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var calls atomic.Int32
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[noop]) error {
		if calls.Add(1) == 1 {
			return hopper.Snooze(0)
		}
		return nil
	})
	c := h.started(workers, 1)

	res, err := c.Insert(ctx, noop{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	job := waitForJob(t, c, res.Job.ID, hopper.JobStateCompleted)
	if calls.Load() != 2 || job.Attempt != 1 || len(job.Errors) != 0 {
		t.Errorf("calls = %d, job = %+v", calls.Load(), job)
	}
}

func TestCancelDiscardsImmediately(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var calls atomic.Int32
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error {
		calls.Add(1)
		return hopper.Cancel(errors.New("bad input"))
	})
	c := h.started(workers, 1)

	res, err := c.Insert(ctx, noop{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	job := waitForJob(t, c, res.Job.ID, hopper.JobStateDiscarded)
	if calls.Load() != 1 || job.Attempt != 1 || len(job.Errors) != 1 || !strings.Contains(job.Errors[0].Error, "bad input") {
		t.Errorf("calls = %d, job = %+v", calls.Load(), job)
	}
}

func TestPanicIsRecorded(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error {
		panic("kaboom")
	})
	c := h.started(workers, 1)

	res, err := c.Insert(ctx, noop{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	job := waitForJob(t, c, res.Job.ID, hopper.JobStateRetryable)
	if len(job.Errors) != 1 || !strings.Contains(job.Errors[0].Error, "kaboom") || !strings.Contains(job.Errors[0].Trace, "client_test.go") {
		t.Errorf("job = %+v", job)
	}
}

type slowWorker struct {
	hopper.WorkerDefaults[noop]
	timeout time.Duration
	err     chan error
}

func (w *slowWorker) Work(ctx context.Context, _ *hopper.Job[noop]) error {
	<-ctx.Done()
	err := context.Cause(ctx)
	w.err <- err
	return err
}

func (w *slowWorker) Timeout(*hopper.Job[noop]) time.Duration { return w.timeout }

func TestTimeoutCancelsJob(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	w := &slowWorker{timeout: 50 * time.Millisecond, err: make(chan error, 10)}
	hopper.AddWorker(workers, w)
	c := h.started(workers, 1)

	res, err := c.Insert(ctx, noop{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cause := <-w.err; !errors.Is(cause, hopper.ErrJobTimeout) {
		t.Errorf("context cause = %v, want ErrJobTimeout", cause)
	}
	job := waitForJob(t, c, res.Job.ID, hopper.JobStateRetryable)
	if len(job.Errors) != 1 || !strings.Contains(job.Errors[0].Error, "timed out after 50ms") {
		t.Errorf("job = %+v", job)
	}
}

func TestUnknownKindFailsTheJob(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	c := h.started(hopper.NewWorkers(), 1)

	res, err := c.Insert(ctx, unregistered{}, &hopper.InsertOpts{MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	job := waitForJob(t, c, res.Job.ID, hopper.JobStateDiscarded)
	if len(job.Errors) != 1 || !strings.Contains(job.Errors[0].Error, `no worker registered for kind "unregistered"`) {
		t.Errorf("job = %+v", job)
	}
}

func TestDeleteCompletedSkipsHistory(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error { return nil })
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[failing]) error { return hopper.Cancel(nil) })
	c := h.client(&hopper.Config{
		Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 2, DeleteCompleted: true}},
		Workers: workers,
	})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ok, err := c.Insert(ctx, noop{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	bad, err := c.Insert(ctx, failing{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return h.count("SELECT count(*) FROM hopper_jobs") == 0 })
	if _, err := c.JobGet(ctx, ok.Job.ID); !errors.Is(err, hopper.ErrNotFound) {
		t.Errorf("completed job was archived: %v", err)
	}
	// Failures are always archived: history is the dead-letter queue.
	if job, err := c.JobGet(ctx, bad.Job.ID); err != nil || job.State != hopper.JobStateDiscarded {
		t.Errorf("discarded job = %+v, %v", job, err)
	}
}

func TestStopDrainsRunningJobs(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	started := make(chan struct{}, 1)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, _ *hopper.Job[noop]) error {
		started <- struct{}{}
		select {
		case <-time.After(300 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	c := h.started(workers, 1)
	res, err := c.Insert(ctx, noop{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	<-started

	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	job, err := c.JobGet(ctx, res.Job.ID)
	if err != nil || job.State != hopper.JobStateCompleted {
		t.Errorf("job after graceful stop = %+v, %v", job, err)
	}
	if h.count("SELECT count(*) FROM hopper_clients") != 0 {
		t.Error("lease row left behind after Stop")
	}
	if err := c.Start(ctx); !errors.Is(err, hopper.ErrClientStopped) {
		t.Errorf("Start after Stop = %v", err)
	}
}

func TestStopDeadlineCancelsJobs(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	started := make(chan struct{}, 1)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, _ *hopper.Job[noop]) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})
	c := h.started(workers, 1)
	res, err := c.Insert(ctx, noop{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	<-started

	stopCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := c.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v, want DeadlineExceeded", err)
	}
	// The interrupted job was flushed as retryable, to run again right away.
	job, err := c.JobGet(ctx, res.Job.ID)
	if err != nil || job.State != hopper.JobStateRetryable || len(job.Errors) != 1 {
		t.Errorf("job after hard stop = %+v, %v", job, err)
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error { return nil })
	c := h.client(&hopper.Config{
		Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers: workers,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	waitFor(t, func() bool { return h.count("SELECT count(*) FROM hopper_clients") == 1 })
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}
}

func TestLeaseIsRenewedAndReregisteredWhenLost(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	c := h.started(hopper.NewWorkers(), 1)
	id := c.ClientID()

	// The lease outlives its TTL because it is renewed.
	time.Sleep(fastTuning.LeaseTTL + fastTuning.LeaseRenew)
	if h.count("SELECT count(*) FROM hopper_clients WHERE id = $1 AND expires_at > now()", id) != 1 {
		t.Fatal("lease was not renewed")
	}

	// Expire it behind the client's back; the client notices and re-registers.
	if _, err := h.pool.Exec(ctx, "UPDATE hopper_clients SET expires_at = now() - interval '1s' WHERE id = $1", id); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return c.ClientID() != id })
	if h.count("SELECT count(*) FROM hopper_clients WHERE expires_at > now()") != 1 {
		t.Error("expected exactly one live lease after re-registration")
	}
}

// TestConcurrentClientsFinalizeExactlyOnce is the M1 concurrency gate: N
// clients working M jobs, every job runs and is finalized exactly once when
// nothing crashes.
func TestConcurrentClientsFinalizeExactlyOnce(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	rec := newRecorder()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[noop]) error {
		rec.record(job.ID)
		return nil
	})

	const clients, jobs = 4, 2000
	for range clients {
		h.started(workers, 8)
	}
	inserter := h.client(&hopper.Config{})
	params := make([]hopper.InsertParams, jobs)
	for i := range params {
		params[i] = hopper.InsertParams{Args: noop{N: i}, Opts: &hopper.InsertOpts{Priority: hopper.Priority(i%4 + 1)}}
	}
	if _, err := inserter.InsertMany(ctx, params); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { return h.count("SELECT count(*) FROM hopper_jobs") == 0 })
	if got := rec.total.Load(); got != jobs {
		t.Errorf("executions = %d, want %d", got, jobs)
	}
	for id, n := range rec.seen {
		if n != 1 {
			t.Errorf("job %s ran %d times", id, n)
		}
	}
	if got := h.count("SELECT count(*) FROM hopper_job_history WHERE state = 'completed' AND attempt = 1"); got != jobs {
		t.Errorf("completed once in history = %d, want %d", got, jobs)
	}
	// Work was spread across clients.
	if got := h.count("SELECT count(DISTINCT attempted_by) FROM hopper_job_history"); got != clients {
		t.Errorf("distinct clients in history = %d, want %d", got, clients)
	}
}

func TestDefaultBackoff(t *testing.T) {
	t.Parallel()
	for attempt, want := range map[int]time.Duration{1: 6 * time.Second, 2: 21 * time.Second, 5: 630 * time.Second} {
		for range 20 {
			got := hopper.DefaultBackoff(attempt)
			lo, hi := time.Duration(float64(want)*0.9), time.Duration(float64(want)*1.1)
			if got < lo || got > hi {
				t.Errorf("backoff(%d) = %s, want %s ±10%%", attempt, got, want)
			}
		}
	}
	if got := hopper.DefaultBackoff(30); got != 24*time.Hour {
		t.Errorf("backoff(30) = %s, want 24h cap", got)
	}
}
