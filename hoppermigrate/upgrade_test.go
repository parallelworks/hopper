package hoppermigrate_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/hoppermigrate"
	"github.com/parallelworks/hopper/internal/testdb"
)

type upgradeJob struct {
	N int `json:"n"`
}

func (upgradeJob) Kind() string { return "upgrade" }

// TestUpgradeUnderTraffic migrates from the previous schema version to the
// latest while jobs wait in every state, a producer keeps inserting, a
// claim keeps polling and a producer's transaction stays open on
// hopper_jobs, and checks that the migration never holds claims up and that
// a client then works every job. Every schema change ships with this test
// covering it, because migrations that touch hopper_jobs must not block
// claims for longer than one statement.
//
// This release's statements need the latest schema, so the previous
// release's side, before and during the migration, is played by the SQL
// producer contract and by statements every schema version accepts
// (docs/PLAN.md §12).
func TestUpgradeUnderTraffic(t *testing.T) {
	t.Parallel()
	if hoppermigrate.Latest() < 2 {
		t.Skip("only one schema version")
	}
	ctx := context.Background()
	pool := testdb.EmptyPool(t, 0)
	d := testdb.Driver(pool)
	opts := &hoppermigrate.Options{Logger: slog.New(slog.DiscardHandler)}
	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Target: hoppermigrate.Latest() - 1, Logger: opts.Logger}); err != nil {
		t.Fatal(err)
	}
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	count := func(query string) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, query).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// Jobs in every state, as the previous release leaves them: available,
	// scheduled, retryable, running on a process of that release, and
	// finalized in history. Jobs with n%10 = 1 keep failing.
	exec(`SELECT hopper_insert('upgrade', jsonb_build_object('n', n), jsonb_build_object('max_attempts', 3) ||
	        CASE WHEN n % 10 = 3 THEN jsonb_build_object('scheduled_at', now() + interval '1 hour') ELSE '{}' END)
	      FROM generate_series(0, 99) n`)
	previous, err := d.Executor().ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "previous-release", TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	exec(`UPDATE hopper_jobs SET state = 'running', attempt = 1, attempted_at = now(), attempted_by = $1
	      WHERE (args->>'n')::int % 10 = 2`, previous)
	exec(`UPDATE hopper_jobs SET state = 'retryable', attempt = 1, errors = '[{"attempt": 1, "error": "flaky"}]'
	      WHERE (args->>'n')::int % 10 = 1`)
	exec(`UPDATE hopper_jobs SET state = 'completed', attempt = 1 WHERE (args->>'n')::int % 10 = 4`)
	// The columns are named because hopper_jobs gains columns that history
	// does not have, such as snooze_reason.
	const historyColumns = `id, seq, kind, queue, state, priority, attempt, max_attempts, scheduled_at, attempted_at,
	  attempted_by, args, metadata, errors, unique_key, ordering_key, partition_key, batch_id, expires_at,
	  cancel_requested_at, await, created_at`
	exec(`WITH j AS (DELETE FROM hopper_jobs WHERE state = 'completed' RETURNING *)
	      INSERT INTO hopper_job_history (` + historyColumns + `, finalized_at, output)
	      SELECT ` + historyColumns + `, now(), NULL FROM j`)

	// A batch of five jobs as the previous release inserts it, with a
	// callback, two of which that release has finalized and counted down in
	// pending.
	var batch string
	if err := pool.QueryRow(ctx, `INSERT INTO hopper_batches (pending, total, on_complete)
	    VALUES (5, 5, '{"kind": "upgrade", "args": {"n": 300000}}') RETURNING id::text`).Scan(&batch); err != nil {
		t.Fatal(err)
	}
	exec(`SELECT hopper_insert('upgrade', jsonb_build_object('n', n), '{"max_attempts": 3}') FROM generate_series(300010, 300050, 10) n`)
	exec(`UPDATE hopper_jobs SET batch_id = $1::uuid WHERE (args->>'n')::int BETWEEN 300010 AND 300050`, batch)
	if n := count(`SELECT count(*) FROM hopper_jobs WHERE batch_id IS NOT NULL`); n != 5 {
		t.Fatalf("%d jobs in the batch, want 5", n)
	}
	exec(`DELETE FROM hopper_jobs WHERE (args->>'n')::int IN (300010, 300020)`)
	exec(`UPDATE hopper_batches SET pending = 3 WHERE id = $1::uuid`, batch)

	// A producer's transaction that has inserted a job stays open across
	// the start of the migration, as an InsertTx inside a longer unit of
	// work would. A migration that queues for a lock behind it holds up
	// every claim for as long.
	held, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Rollback(ctx) //nolint:errcheck // committed below
	if _, err := held.Exec(ctx, `SELECT hopper_insert('upgrade', '{"n": 100000}', '{"max_attempts": 3}')`); err != nil {
		t.Fatal(err)
	}
	const holdFor = 2 * time.Second

	var (
		stop    = make(chan struct{})
		wg      sync.WaitGroup
		slowest atomic.Int64
	)
	wg.Go(func() {
		for i := 200; ; i += 10 {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			if _, err := pool.Exec(ctx, `SELECT hopper_insert('upgrade', jsonb_build_object('n', n), '{"max_attempts": 3}')
			                             FROM generate_series($1::int, $1::int + 9) n`, i); err != nil {
				t.Errorf("insert during the migration: %v", err)
				return
			}
		}
	})
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
			}
			start := time.Now()
			// The claim's lock on hopper_jobs, in a form every version accepts.
			if _, err := pool.Exec(ctx, `SELECT id FROM hopper_jobs WHERE state IN ('available', 'scheduled', 'retryable')
			                             AND scheduled_at <= now() ORDER BY priority, scheduled_at, seq LIMIT 8 FOR UPDATE SKIP LOCKED`); err != nil {
				t.Errorf("claim during the migration: %v", err)
				return
			}
			if took := int64(time.Since(start)); took > slowest.Load() {
				slowest.Store(took)
			}
		}
	})
	wg.Go(func() {
		time.Sleep(holdFor)
		if err := held.Commit(ctx); err != nil {
			t.Errorf("commit the held transaction: %v", err)
		}
	})

	// The migration gives up on its own only after 10 minutes, too long
	// for a test; a loop that never gets the lock fails here instead. How
	// long it takes otherwise depends on the load on the host, so only the
	// claims' wait is bounded tightly.
	upCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	start := time.Now()
	res, err := hoppermigrate.Up(upCtx, d, opts)
	took := time.Since(start)
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatalf("Up under traffic: %v", err)
	}
	if res.Version != hoppermigrate.Latest() || len(res.Applied) != 1 {
		t.Errorf("Up = %+v", res)
	}
	// A migration that locks hopper_jobs waits for the held transaction,
	// which shows the test reached its lock; one that leaves the table
	// alone has nothing to wait for.
	if latest := hoppermigrate.Migrations()[hoppermigrate.Latest()-1]; namesJobs(latest.Up) && took < holdFor*9/10 {
		t.Errorf("migration took %s, less than the held transaction's %s: it was not held up", took, holdFor)
	}
	if s := time.Duration(slowest.Load()); s > holdFor/2 {
		t.Errorf("a claim waited %s during the migration", s)
	}

	// This release's client works every job: the waiting ones, those
	// inserted during the migration, its own, and, once the previous
	// release's process has gone, the jobs it was running.
	var completed atomic.Int64
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[upgradeJob]) error {
		if job.Args.N%10 == 1 {
			return errors.New("flaky")
		}
		completed.Add(1)
		return nil
	})
	client, err := hopper.NewClient(d, &hopper.Config{
		Queues:       map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 8}},
		Workers:      workers,
		Logger:       opts.Logger,
		PollInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer client.Stop(ctx) //nolint:errcheck // cleanup
	exec(`UPDATE hopper_clients SET expires_at = now() - interval '1s' WHERE id = $1`, previous)
	params := make([]hopper.InsertParams, 50)
	for i := range params {
		params[i] = hopper.InsertParams{Args: upgradeJob{N: 200000 + 10*i}}
	}
	if _, err := client.InsertMany(ctx, params); err != nil {
		t.Fatal(err)
	}

	// Left waiting: the failing jobs and the ones scheduled for later.
	const unfinished = `SELECT count(*) FROM hopper_jobs
	                    WHERE (args->>'n')::int % 10 <> 1 AND scheduled_at < now() + interval '30 minutes'`
	deadline := time.Now().Add(15 * time.Second)
	for count(unfinished) > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := count(unfinished); n > 0 {
		t.Errorf("%d jobs did not finish after the migration (%d completed)", n, completed.Load())
	}
	if n := count(`SELECT count(*) FROM hopper_job_history WHERE state = 'completed' AND (args->>'n')::int % 10 = 2
	               AND (args->>'n')::int < 100`); n != 10 {
		t.Errorf("%d of the previous release's running jobs completed, want 10", n)
	}
	// The previous release's batch completed once this release had
	// finalized the rest of its jobs, and ran its callback once.
	deadline = time.Now().Add(15 * time.Second)
	for count(`SELECT count(*) FROM hopper_job_history WHERE (args->>'n')::int = 300000`) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if b, err := client.BatchGet(ctx, mustParse(t, batch)); err != nil || b.CompletedAt.IsZero() || b.Pending != 0 || b.Failed != 0 {
		t.Errorf("the previous release's batch = %+v, %v", b, err)
	}
	if n := count(`SELECT count(*) FROM (SELECT id FROM hopper_jobs WHERE (args->>'n')::int = 300000
	               UNION ALL SELECT id FROM hopper_job_history WHERE (args->>'n')::int = 300000) c`); n != 1 {
		t.Errorf("the batch's callback ran %d times, want 1", n)
	}
	v, err := hoppermigrate.Version(ctx, d)
	if err != nil || v != hoppermigrate.Latest() {
		t.Errorf("Version = %d, %v", v, err)
	}
}

// namesJobs reports whether a migration script names hopper_jobs outside
// its comments, so that a comment mentioning the table does not count.
func namesJobs(script string) bool {
	for line := range strings.Lines(script) {
		if code, _, _ := strings.Cut(line, "--"); strings.Contains(code, "hopper_jobs") {
			return true
		}
	}
	return false
}

func mustParse(t *testing.T, s string) hopper.JobID {
	t.Helper()
	id, err := hopper.ParseJobID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
