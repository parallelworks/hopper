package hoppersqlite_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/hoppersqlite"
)

// longWrite is a write that runs for seconds, for a short context to
// interrupt.
const longWrite = `WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 100000000)
	INSERT INTO t SELECT x + 100 FROM c`

// interrupt runs longWrite in tx under a context that ends while it runs,
// which makes SQLite roll the whole transaction back.
func interrupt(t *testing.T, tx *sql.Tx) {
	t.Helper()
	short, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := tx.ExecContext(short, longWrite); err == nil {
		t.Fatal("the long write was not interrupted")
	}
}

// TestInterruptedTransaction checks a transaction that SQLite rolled back
// itself, after a write its context interrupted: its later statements, the
// executor's included, are refused rather than run on their own; Commit
// returns ErrTransactionAborted and Rollback nothing; nothing it wrote is
// left; and the write lock is free again.
func TestInterruptedTransaction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d := open(t)
	db := d.DB()
	if _, err := db.ExecContext(ctx, "CREATE TABLE t (x INTEGER)"); err != nil {
		t.Fatal(err)
	}
	count := func() (rows, jobs int) {
		t.Helper()
		if err := db.QueryRowContext(ctx, "SELECT (SELECT count(*) FROM t), (SELECT count(*) FROM hopper_jobs)").Scan(&rows, &jobs); err != nil {
			t.Fatal(err)
		}
		return rows, jobs
	}
	for _, end := range []string{"commit", "rollback"} {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO t VALUES (1)"); err != nil {
			t.Fatal(err)
		}
		interrupt(t, tx)
		if _, err := tx.ExecContext(ctx, "INSERT INTO t VALUES (2)"); !errors.Is(err, hoppersqlite.ErrTransactionAborted) {
			t.Errorf("a statement after the interrupted one = %v, want ErrTransactionAborted", err)
		}
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(new(int)); !errors.Is(err, hoppersqlite.ErrTransactionAborted) {
			t.Errorf("a query after the interrupted statement = %v, want ErrTransactionAborted", err)
		}
		_, err = d.UnwrapTx(tx).JobInsertMany(ctx, []driver.JobInsertParams{{Kind: "k", Queue: "default", Priority: 2, MaxAttempts: 1}}, driver.JobInsertOpts{})
		if !errors.Is(err, hoppersqlite.ErrTransactionAborted) {
			t.Errorf("an insert through the transaction = %v, want ErrTransactionAborted", err)
		}
		if end == "commit" {
			if err := tx.Commit(); !errors.Is(err, hoppersqlite.ErrTransactionAborted) {
				t.Errorf("Commit = %v, want ErrTransactionAborted", err)
			}
		} else if err := tx.Rollback(); err != nil {
			t.Errorf("Rollback = %v", err)
		}
		if rows, jobs := count(); rows != 0 || jobs != 0 {
			t.Errorf("after %s: %d rows and %d jobs left of the transaction", end, rows, jobs)
		}
		wctx, cancel := context.WithTimeout(ctx, time.Second)
		if _, err := db.ExecContext(wctx, "INSERT INTO t VALUES (3); DELETE FROM t"); err != nil {
			t.Errorf("a write after %s: %v", end, err)
		}
		cancel()
	}

	// An error that SQLite undoes only the statement of, such as a
	// constraint's, leaves the transaction open, as on Postgres it would
	// not; and so does a read that is interrupted.
	if _, err := db.ExecContext(ctx, "CREATE UNIQUE INDEX t_x ON t (x)"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //nolint:errcheck // committed below
	if _, err := tx.ExecContext(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO t VALUES (1)"); err == nil || errors.Is(err, hoppersqlite.ErrTransactionAborted) {
		t.Errorf("a duplicate = %v, want a constraint error", err)
	}
	short, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if err := tx.QueryRowContext(short, `WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 100000000) SELECT count(*) FROM c`).Scan(new(int)); err == nil {
		t.Error("the long read was not interrupted")
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO t VALUES (2)"); err != nil {
		t.Errorf("a statement after a constraint error and an interrupted read = %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if rows, _ := count(); rows != 2 {
		t.Errorf("t has %d rows, want 2", rows)
	}
}

// TestInterruptedFinalizeInTx finalizes a job in a worker's transaction
// after a statement of the transaction was interrupted: the finalize is
// refused, and the job stays running once the transaction rolls back,
// rather than completing in a write of its own.
func TestInterruptedFinalizeInTx(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d := open(t)
	x := d.Executor()
	clientID, err := x.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "test", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := x.JobInsertMany(ctx, []driver.JobInsertParams{{Kind: "k", Queue: "default", Priority: 2, MaxAttempts: 3}}, driver.JobInsertOpts{}); err != nil {
		t.Fatal(err)
	}
	claimed, err := x.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 1})
	if err != nil || len(claimed.Jobs) != 1 {
		t.Fatalf("claim = %+v, %v", claimed, err)
	}
	j := claimed.Jobs[0]
	db := d.DB()
	if _, err := db.ExecContext(ctx, "CREATE TABLE t (x INTEGER)"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	interrupt(t, tx)
	applied, err := d.UnwrapTx(tx).JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{{
		ID: j.ID, AttemptedBy: clientID, Fence: j.Fence, State: driver.JobStateCompleted, Archive: true,
	}}})
	if !errors.Is(err, hoppersqlite.ErrTransactionAborted) || len(applied) != 0 {
		t.Errorf("finalize in the transaction = %d applied, %v; want ErrTransactionAborted", len(applied), err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	got, err := x.JobGet(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != driver.JobStateRunning {
		t.Errorf("the job is %s after its transaction rolled back, want running", got.State)
	}
}

// TestStaleFinalizeAfterRescue checks fencing once a job is rescued: the
// result of the client that lost it applies neither before the job is
// claimed again nor after, nor does a result with the old fence from the
// client that claimed it again; that client's own result does.
func TestStaleFinalizeAfterRescue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d := open(t)
	x := d.Executor()
	register := func(host string) int64 {
		t.Helper()
		id, err := x.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: host, TTL: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	a, b := register("a"), register("b")
	if _, err := x.JobInsertMany(ctx, []driver.JobInsertParams{{Kind: "k", Queue: "default", Priority: 2, MaxAttempts: 5}}, driver.JobInsertOpts{}); err != nil {
		t.Fatal(err)
	}
	ca, err := x.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: a, Limit: 1})
	if err != nil || len(ca.Jobs) != 1 {
		t.Fatalf("claim by a = %+v, %v", ca, err)
	}
	ja := ca.Jobs[0]
	if _, err := d.DB().ExecContext(ctx, "UPDATE hopper_clients SET expires_at = 0 WHERE id = ?", a); err != nil {
		t.Fatal(err)
	}
	candidates, err := x.JobRescueCandidates(ctx, driver.JobRescueParams{Limit: 10})
	if err != nil || len(candidates) != 1 {
		t.Fatalf("rescue candidates = %+v, %v", candidates, err)
	}
	c := candidates[0].Job
	applied, err := x.JobFinalizeMany(ctx, driver.JobFinalizeParams{SkipLocked: true, Jobs: []driver.JobFinalize{{
		ID: c.ID, AttemptedBy: c.AttemptedBy, Fence: c.Fence, State: driver.JobStateRetryable, Archive: true,
		Error: &driver.AttemptError{Attempt: c.Attempt, Fence: c.Fence, Error: "client lost"},
	}}})
	if err != nil || len(applied) != 1 {
		t.Fatalf("rescue = %d applied, %v", len(applied), err)
	}
	finalize := func(f driver.JobFinalize) int {
		t.Helper()
		applied, err := x.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{f}})
		if err != nil {
			t.Fatal(err)
		}
		return len(applied)
	}
	stale := driver.JobFinalize{ID: ja.ID, AttemptedBy: a, Fence: ja.Fence, State: driver.JobStateCompleted, Archive: true}
	if n := finalize(stale); n != 0 {
		t.Error("the lost client's result applied before the job was claimed again")
	}
	if _, err := d.DB().ExecContext(ctx, "UPDATE hopper_jobs SET scheduled_at = 0"); err != nil {
		t.Fatal(err)
	}
	cb, err := x.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: b, Limit: 1})
	if err != nil || len(cb.Jobs) != 1 || cb.Jobs[0].Fence != ja.Fence+1 {
		t.Fatalf("claim by b = %+v, %v", cb, err)
	}
	if n := finalize(stale); n != 0 {
		t.Error("the lost client's result applied after the job was claimed again")
	}
	stale.AttemptedBy = b
	if n := finalize(stale); n != 0 {
		t.Error("a result of b with the old fence applied")
	}
	if n := finalize(driver.JobFinalize{ID: ja.ID, AttemptedBy: b, Fence: cb.Jobs[0].Fence, State: driver.JobStateCompleted, Archive: true}); n != 1 {
		t.Error("b's result did not apply")
	}
}

type selfArgs struct{}

func (selfArgs) Kind() string { return "self" }

// syncBuffer is a bytes.Buffer that a client's logger and the test share.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// TestSelfWait runs a worker that writes on the pool while its own
// transaction holds the write lock, which would wait for itself until its
// context ended, and every write of the process with it: the write fails
// at once with ErrLockHeldByOwner, naming the job, and the client keeps
// its lease.
func TestSelfWait(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d := open(t)
	var (
		took   time.Duration
		recErr error
		done   = make(chan struct{})
	)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[selfArgs]) error {
		defer close(done)
		tx, err := d.DB().BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback() //nolint:errcheck // committed below
		if _, err := tx.ExecContext(ctx, "UPDATE hopper_queues SET updated_at = updated_at"); err != nil {
			return err
		}
		start := time.Now()
		recErr = hopper.RecordStep(ctx, job, "a", nil)
		took = time.Since(start)
		return tx.Commit()
	})
	logs := &syncBuffer{}
	c, err := hopper.NewClient(d, &hopper.Config{
		Queues:     map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers:    workers,
		JobTimeout: 10 * time.Second, // what a write that waited for itself would wait
		LeaseTTL:   3 * time.Second,
		Logger:     slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Stop(ctx) //nolint:errcheck // cleanup
	res, err := c.Insert(ctx, selfArgs{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	<-done
	if !errors.Is(recErr, hoppersqlite.ErrLockHeldByOwner) || !strings.Contains(recErr.Error(), res.Job.ID.String()) || took > time.Second {
		t.Errorf("RecordStep in the worker's transaction = %v after %s, want ErrLockHeldByOwner naming the job at once", recErr, took)
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		j, err := c.JobGet(ctx, res.Job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if j.State == hopper.JobStateCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the job is %s, want completed", j.State)
		}
	}
	if strings.Contains(logs.String(), "lease") {
		t.Errorf("the client's lease suffered:\n%s", logs)
	}
}

// TestJSONRefused checks that JSON Postgres's jsonb refuses (a \u0000
// escape, text that is not UTF-8, a number beyond numeric's range) is
// refused on insert too, as it would be on Postgres.
func TestJSONRefused(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d := open(t)
	for _, args := range []string{`{"a":"\u0000"}`, "{\"a\":\"\xff\"}", `{"a":"\ud800"}`, `{"a":1e200000}`} {
		_, err := d.Executor().JobInsertMany(ctx, []driver.JobInsertParams{{Kind: "k", Queue: "default", Priority: 2, MaxAttempts: 1, Args: []byte(args)}}, driver.JobInsertOpts{})
		if err == nil {
			t.Errorf("args %q inserted", args)
		}
	}
	var n int
	if err := d.DB().QueryRowContext(ctx, "SELECT count(*) FROM hopper_jobs").Scan(&n); err != nil || n != 0 {
		t.Errorf("jobs = %d, %v", n, err)
	}
}

// TestLogTruncated checks that JobsMaintain cuts back a write-ahead log
// that grew while a reader held up checkpoints, once the reader is done.
func TestLogTruncated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "hopper.db")
	d := openAt(t, path)
	x := d.Executor()
	size := func() int64 {
		st, err := os.Stat(path + "-wal")
		if err != nil {
			t.Fatal(err)
		}
		return st.Size()
	}
	args := []byte(`{"pad":"` + strings.Repeat("x", 4000) + `"}`)
	p := make([]driver.JobInsertParams, 100)
	for i := range p {
		p[i] = driver.JobInsertParams{Kind: "k", Queue: "default", Priority: 2, MaxAttempts: 1, Args: args}
	}
	ro, err := d.DB().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := ro.QueryRowContext(ctx, "SELECT count(*) FROM hopper_jobs").Scan(new(int)); err != nil {
		t.Fatal(err)
	}
	for range 40 {
		if _, err := x.JobInsertMany(ctx, p, driver.JobInsertOpts{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := ro.Rollback(); err != nil {
		t.Fatal(err)
	}
	grown := size()
	if grown < 16<<20 {
		t.Fatalf("the log grew to %d bytes only", grown)
	}
	if _, err := x.JobsMaintain(ctx); err != nil {
		t.Fatal(err)
	}
	if after := size(); after > 1<<20 {
		t.Errorf("the log is %d bytes after JobsMaintain, from %d", after, grown)
	}
}

// TestQueryThatWrites checks that a statement that writes, run on its own
// with Query (INSERT ... RETURNING), waits for the write lock in turn, as
// one run with Exec does, saying who holds it when its context ends.
func TestQueryThatWrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d := open(t)
	db := d.DB()
	if _, err := db.ExecContext(ctx, "CREATE TABLE t (x INTEGER)"); err != nil {
		t.Fatal(err)
	}
	// Open the connections the test needs beforehand, for its short
	// context to be spent waiting for the lock.
	conns := make([]*sql.Conn, 3)
	for i := range conns {
		c, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		conns[i] = c
	}
	for _, c := range conns {
		_ = c.Close()
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	err = db.QueryRowContext(short, "INSERT INTO t VALUES (2) RETURNING x").Scan(new(int))
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "write lock") {
		t.Errorf("INSERT ... RETURNING while a transaction writes = %v, want it to wait for the write lock", err)
	}
	done := make(chan error, 1)
	go func() {
		var x int
		err := db.QueryRowContext(ctx, "-- a comment\n  insert INTO t VALUES (3) RETURNING x").Scan(&x)
		done <- err
	}()
	for hoppersqlite.LockWaiters(d) == 0 {
		time.Sleep(time.Millisecond)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&n); err != nil || n != 2 {
		t.Errorf("rows = %d, %v; want 2", n, err)
	}
}

// TestOpenContext checks that OpenContext gives up when its context ends.
func TestOpenContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d, err := hoppersqlite.OpenContext(ctx, filepath.Join(t.TempDir(), "hopper.db"), nil)
	if err == nil {
		_ = d.Close()
		t.Fatal("OpenContext with an ended context succeeded")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("OpenContext with an ended context = %v", err)
	}
}

type fanArgs struct{}

func (fanArgs) Kind() string { return "fan" }

type leafArgs struct{ N int }

func (leafArgs) Kind() string { return "leaf" }

// TestConcurrentWritesOfOneAttempt runs a worker that writes on the pool
// from eight goroutines at once, with no transaction of its own open, and
// then records a step while it inserts: every write waits its turn and
// succeeds, as on Postgres, rather than failing with ErrLockHeldByOwner
// because another write of the same attempt holds the lock.
func TestConcurrentWritesOfOneAttempt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d := open(t)
	var (
		mu     sync.Mutex
		errs   []error
		worked = make(chan struct{})
	)
	note := func(err error) {
		if err != nil {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}
	}
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[fanArgs]) error {
		defer close(worked)
		c, _ := hopper.ClientFromContext[*sql.Tx](ctx)
		var wg sync.WaitGroup
		for g := range 8 {
			wg.Go(func() {
				for i := range 25 {
					_, err := c.Insert(ctx, leafArgs{N: g*100 + i}, nil)
					note(err)
				}
			})
		}
		wg.Wait()
		wg.Go(func() { note(hopper.RecordStep(ctx, job, "a", nil)) })
		wg.Go(func() { _, err := c.Insert(ctx, leafArgs{N: -1}, nil); note(err) })
		wg.Wait()
		return nil
	})
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[leafArgs]) error { return nil })
	c, err := hopper.NewClient(d, &hopper.Config{
		Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers: workers,
		Logger:  slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Stop(ctx) //nolint:errcheck // cleanup
	if _, err := c.Insert(ctx, fanArgs{}, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-worked:
	case <-time.After(time.Minute):
		t.Fatal("the worker did not finish")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(errs) > 0 {
		t.Errorf("%d of 202 writes of one attempt failed, the first: %v", len(errs), errs[0])
	}
	var leaves int
	if err := d.DB().QueryRowContext(ctx, "SELECT (SELECT count(*) FROM hopper_jobs WHERE kind = 'leaf') + (SELECT count(*) FROM hopper_job_history WHERE kind = 'leaf')").Scan(&leaves); err != nil || leaves != 201 {
		t.Errorf("leaf jobs = %d, %v; want 201", leaves, err)
	}
}
