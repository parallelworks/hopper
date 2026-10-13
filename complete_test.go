package hopper_test

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/parallelworks/hopper"
)

type ledger struct {
	N int `json:"n"`
}

func (ledger) Kind() string { return "ledger" }

// ledgerWorker writes a ledger row for each attempt and completes the job in
// the same transaction. fail picks attempts that misbehave after the
// completion: "rollback" rolls back and returns an error, "error" commits
// and returns an error anyway.
type ledgerWorker struct {
	hopper.WorkerDefaults[ledger]
	pool *pgxpool.Pool
	fail func(job *hopper.Job[ledger]) string

	mu   sync.Mutex
	errs []error
}

func (w *ledgerWorker) Work(ctx context.Context, job *hopper.Job[ledger]) error {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, "INSERT INTO ledger (job_id, attempt) VALUES ($1, $2)", job.ID.String(), job.Attempt); err != nil {
		return err
	}
	if err := hopper.SetOutput(ctx, map[string]int{"n": job.Args.N}); err != nil {
		return err
	}
	if err := hopper.CompleteTx(ctx, tx, job); err != nil {
		w.mu.Lock()
		w.errs = append(w.errs, err)
		w.mu.Unlock()
		return err
	}
	mode := ""
	if w.fail != nil {
		mode = w.fail(job)
	}
	switch mode {
	case "rollback":
		return errors.New("rolled back after completing")
	case "error":
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return errors.New("failed after committing")
	}
	return tx.Commit(ctx)
}

func (w *ledgerWorker) NextRetry(*hopper.Job[ledger]) time.Time { return time.Now() }

func (w *ledgerWorker) Timeout(*hopper.Job[ledger]) time.Duration { return -1 }

func createLedger(t *testing.T, h *harness) {
	t.Helper()
	if _, err := h.pool.Exec(context.Background(), "CREATE TABLE ledger (job_id text NOT NULL, attempt int NOT NULL)"); err != nil {
		t.Fatal(err)
	}
}

// recordingHandler keeps every log record's message at Warn or above.
type recordingHandler struct {
	mu       sync.Mutex
	messages []string
}

func (h *recordingHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelWarn }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messages = append(h.messages, r.Message)
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingHandler) contains(sub string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range h.messages {
		if strings.Contains(m, sub) {
			return true
		}
	}
	return false
}

func TestCompleteTx(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	createLedger(t, h)
	w := &ledgerWorker{pool: h.pool}
	workers := hopper.NewWorkers()
	hopper.AddWorker(workers, w)
	logs := &recordingHandler{}
	c := h.client(&hopper.Config{
		Queues: map[string]hopper.QueueConfig{
			hopper.QueueDefault: {MaxWorkers: 2},
			"transient":         {MaxWorkers: 2, DeleteCompleted: true},
		},
		Workers: workers,
		Logger:  slog.New(logs),
	})
	events := make(chan hopper.Event, 10)
	ectx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		for ev := range c.Events(ectx, hopper.EventJobCompleted) {
			events <- ev
		}
	}()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}

	res, err := c.Insert(ctx, ledger{N: 7}, &hopper.InsertOpts{Await: true})
	if err != nil {
		t.Fatal(err)
	}
	actx, acancel := context.WithTimeout(ctx, 10*time.Second)
	defer acancel()
	out, err := hopper.Await[map[string]int](actx, c, res.Job.ID)
	if err != nil || out["n"] != 7 {
		t.Fatalf("Await = %v, %v", out, err)
	}
	job, err := c.JobGet(ctx, res.Job.ID)
	if err != nil || job.State != hopper.JobStateCompleted || job.Attempt != 1 || len(job.Errors) != 0 || job.FinalizedAt.IsZero() {
		t.Errorf("job = %+v, %v", job, err)
	}
	if n := h.count("SELECT count(*) FROM ledger WHERE job_id = $1", res.Job.ID.String()); n != 1 {
		t.Errorf("ledger rows = %d, want 1", n)
	}
	select {
	case ev := <-events:
		if ev.Job.ID != res.Job.ID {
			t.Errorf("completed event for %s, want %s", ev.Job.ID, res.Job.ID)
		}
	case <-time.After(5 * time.Second):
		t.Error("no completed event")
	}

	// On a DeleteCompleted queue the job leaves no history.
	res, err = c.Insert(ctx, ledger{N: 8}, &hopper.InsertOpts{Queue: "transient"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return h.count("SELECT count(*) FROM ledger WHERE job_id = $1", res.Job.ID.String()) == 1 })
	waitFor(t, func() bool {
		_, err := c.JobGet(ctx, res.Job.ID)
		return errors.Is(err, hopper.ErrNotFound)
	})

	// The client did not finalize the jobs again: a second finalize would
	// have been fenced and logged.
	time.Sleep(50 * time.Millisecond)
	if logs.contains("result dropped") {
		t.Errorf("the client finalized a job CompleteTx had completed: %v", logs.messages)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.errs) != 0 {
		t.Errorf("CompleteTx errors: %v", w.errs)
	}
}

func TestCompleteTxWhenTheTransactionDoesNotCommit(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	createLedger(t, h)
	w := &ledgerWorker{pool: h.pool, fail: func(job *hopper.Job[ledger]) string {
		switch {
		case job.Args.N == 1 && job.Attempt == 1:
			return "rollback"
		case job.Args.N == 2:
			return "error"
		}
		return ""
	}}
	workers := hopper.NewWorkers()
	hopper.AddWorker(workers, w)
	c := h.started(workers, 2)

	// A rolled-back completion leaves the attempt to fail as usual, and the
	// retry completes.
	rolled, err := c.Insert(ctx, ledger{N: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	job := waitForJob(t, c, rolled.Job.ID, hopper.JobStateCompleted)
	if job.Attempt != 2 || len(job.Errors) != 1 || job.Errors[0].Error != "rolled back after completing" {
		t.Errorf("rolled-back job = %+v", job)
	}
	if n := h.count("SELECT count(*) FROM ledger WHERE job_id = $1", rolled.Job.ID.String()); n != 1 {
		t.Errorf("ledger rows = %d, want only the second attempt's", n)
	}

	// An error after a committed completion changes nothing: the job is
	// already finished.
	committed, err := c.Insert(ctx, ledger{N: 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForJob(t, c, committed.Job.ID, hopper.JobStateCompleted)
	time.Sleep(100 * time.Millisecond)
	if job, err = c.JobGet(ctx, committed.Job.ID); err != nil || job.State != hopper.JobStateCompleted || job.Attempt != 1 || len(job.Errors) != 0 {
		t.Errorf("committed job = %+v, %v", job, err)
	}
}

func TestCompleteTxIsFenced(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	other := h.client(&hopper.Config{Queues: map[string]hopper.QueueConfig{"elsewhere": {MaxWorkers: 1}}, Workers: hopper.NewWorkers()})
	if err := other.Start(ctx); err != nil {
		t.Fatal(err)
	}
	type result struct {
		fenced, wrongJob, wrongTx error
	}
	results := make(chan result, 1)
	started, release := make(chan hopper.JobID, 1), make(chan struct{})
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[noop]) error {
		started <- job.ID
		<-release
		var r result
		tx, err := h.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		r.wrongJob = hopper.CompleteTx(ctx, tx, &hopper.Job[noop]{JobRow: &hopper.JobRow{ID: hopper.JobID{1}}})
		r.wrongTx = hopper.CompleteTx(ctx, (*sql.Tx)(nil), job)
		r.fenced = hopper.CompleteTx(ctx, tx, job)
		results <- r
		return r.fenced
	})
	c := h.started(workers, 1)
	res, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	id := <-started
	// The attempt loses the job, as when it is rescued and claimed by
	// another client.
	if _, err := h.pool.Exec(ctx, "UPDATE hopper_jobs SET attempted_by = $2 WHERE id = $1", res.Job.ID, other.ClientID()); err != nil {
		t.Fatal(err)
	}
	close(release)
	r := <-results
	if !errors.Is(r.fenced, hopper.ErrJobFenced) {
		t.Errorf("CompleteTx of a lost job = %v, want ErrJobFenced", r.fenced)
	}
	if r.wrongJob == nil || r.wrongTx == nil {
		t.Errorf("CompleteTx of another job = %v; with another transaction type = %v", r.wrongJob, r.wrongTx)
	}
	job, err := c.JobGet(ctx, id)
	if err != nil || job.State != hopper.JobStateRunning || job.AttemptedBy != other.ClientID() {
		t.Errorf("job after fenced CompleteTx = %+v, %v", job, err)
	}

	if err := hopper.CompleteTx(ctx, (*sql.Tx)(nil), &hopper.Job[noop]{}); !errors.Is(err, hopper.ErrNotInJob) {
		t.Errorf("CompleteTx outside a job = %v", err)
	}
}

func TestCompleteTxReleasesDependents(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	createLedger(t, h)
	var (
		mu  sync.Mutex
		ran []string
	)
	workers := hopper.NewWorkers()
	hopper.AddWorker(workers, &ledgerWorker{pool: h.pool})
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[reportDone]) error {
		mu.Lock()
		defer mu.Unlock()
		ran = append(ran, "callback")
		return nil
	})
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[wfStep]) error {
		mu.Lock()
		defer mu.Unlock()
		ran = append(ran, job.Args.Name)
		return nil
	})
	c := h.started(workers, 4)

	wf := hopper.NewWorkflow("complete-tx", &hopper.WorkflowOpts{OnSuccess: reportDone{RunID: 1}})
	first := wf.Add(ledger{N: 1}, nil)
	wf.Add(wfStep{Name: "after"}, hopper.After(first))
	if _, err := c.InsertWorkflow(ctx, wf); err != nil {
		t.Fatal(err)
	}
	b := c.NewBatch(hopper.BatchOpts{OnSuccess: reportDone{RunID: 2}})
	b.Add(ledger{N: 2}, nil)
	b.Add(ledger{N: 3}, nil)
	if _, err := b.Insert(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		callbacks, after := 0, false
		for _, r := range ran {
			if r == "callback" {
				callbacks++
			}
			after = after || r == "after"
		}
		return callbacks == 2 && after
	})
}

// TestCompleteTxExactlyOnceAcrossCrash crashes a client after its jobs
// committed their work but before anything it buffered for finalizing was
// written. Jobs completed with CompleteTx are finished with their work, so
// the survivor runs none of them again.
func TestCompleteTxExactlyOnceAcrossCrash(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	createLedger(t, h)
	workers := hopper.NewWorkers()
	hopper.AddWorker(workers, &ledgerWorker{pool: h.pool})

	slowFinalizer := fastTuning
	slowFinalizer.FinalizeInterval = time.Hour
	slowFinalizer.FinalizeBatch = 10000
	victim := h.startedWith(workers, 8, slowFinalizer)
	const n = 40
	params := make([]hopper.InsertParams, n)
	for i := range params {
		params[i] = hopper.InsertParams{Args: ledger{N: i}}
	}
	if _, err := victim.InsertMany(ctx, params); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return h.count("SELECT count(*) FROM ledger") == n })
	victim.Abandon()

	// A job the victim left running would be rescued and run again here.
	h.started(workers, 8)
	waitFor(t, func() bool { return h.count("SELECT count(*) FROM hopper_jobs") == 0 })
	if runs := h.count("SELECT count(*) FROM ledger"); runs != n {
		t.Errorf("ledger has %d rows for %d jobs: some ran twice", runs, n)
	}
	if done := h.count("SELECT count(*) FROM hopper_job_history WHERE state = 'completed' AND attempt = 1"); done != n {
		t.Errorf("%d jobs completed on their first attempt, want %d", done, n)
	}
}
