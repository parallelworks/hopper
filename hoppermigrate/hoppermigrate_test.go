package hoppermigrate_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/hoppermigrate"
	"github.com/parallelworks/hopper/internal/testdb"
)

func TestMigrationsAreWellFormed(t *testing.T) {
	t.Parallel()
	ms := hoppermigrate.Migrations()
	if len(ms) == 0 {
		t.Fatal("no migrations")
	}
	for i, m := range ms {
		if m.Version != i+1 || m.Name == "" || m.Up == "" || m.Down == "" {
			t.Errorf("migration %d malformed: %+v", i, m)
		}
	}
	if hoppermigrate.Latest() != ms[len(ms)-1].Version {
		t.Errorf("Latest() = %d, want %d", hoppermigrate.Latest(), ms[len(ms)-1].Version)
	}
	// Every migration changes this line, so two branches that each add the
	// next version conflict here instead of both merging.
	const latest = 17
	if hoppermigrate.Latest() != latest {
		t.Errorf("Latest() = %d; a new migration updates the latest version here to %d", hoppermigrate.Latest(), hoppermigrate.Latest())
	}
}

func TestUpDownUp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.EmptyPool(t, 0)
	d := testdb.Driver(pool)
	opts := &hoppermigrate.Options{Logger: slog.New(slog.DiscardHandler)}

	v, err := hoppermigrate.Version(ctx, d)
	if err != nil || v != 0 {
		t.Fatalf("Version on empty schema = %d, %v; want 0, nil", v, err)
	}

	res, err := hoppermigrate.Up(ctx, d, opts)
	if err != nil {
		t.Fatalf("Up: %v", err)
	}
	if res.Version != hoppermigrate.Latest() || len(res.Applied) != hoppermigrate.Latest() {
		t.Fatalf("Up result = %+v", res)
	}

	// A second Up is a no-op.
	res, err = hoppermigrate.Up(ctx, d, opts)
	if err != nil || len(res.Applied) != 0 || res.Version != hoppermigrate.Latest() {
		t.Fatalf("second Up = %+v, %v", res, err)
	}

	for _, table := range []string{"hopper_jobs", "hopper_job_history", "hopper_clients", "hopper_schema"} {
		var exists bool
		if err := pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Errorf("table %s missing after Up", table)
		}
	}

	// The database generates v7 IDs.
	var version int
	if err := pool.QueryRow(ctx, "SELECT get_byte(uuid_send(hopper_uuidv7()), 6) >> 4").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 7 {
		t.Errorf("hopper_uuidv7() version nibble = %d, want 7", version)
	}
	var variant int
	if err := pool.QueryRow(ctx, "SELECT get_byte(uuid_send(hopper_uuidv7()), 8) >> 6").Scan(&variant); err != nil {
		t.Fatal(err)
	}
	if variant != 2 {
		t.Errorf("hopper_uuidv7() variant bits = %b, want 10", variant)
	}
	// IDs generated in sequence sort in time order.
	var ordered bool
	if err := pool.QueryRow(ctx, "SELECT (SELECT hopper_uuidv7()) < (SELECT hopper_uuidv7() FROM pg_sleep(0.002))").Scan(&ordered); err != nil {
		t.Fatal(err)
	}
	if !ordered {
		t.Error("hopper_uuidv7() is not time-ordered")
	}

	res, err = hoppermigrate.Down(ctx, d, opts)
	if err != nil {
		t.Fatalf("Down: %v", err)
	}
	if res.Version != 0 {
		t.Fatalf("Down result = %+v", res)
	}
	v, err = hoppermigrate.Version(ctx, d)
	if err != nil || v != 0 {
		t.Fatalf("Version after Down = %d, %v; want 0, nil", v, err)
	}
	var leftover int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM pg_tables WHERE schemaname = current_schema()").Scan(&leftover); err != nil {
		t.Fatal(err)
	}
	if leftover != 0 {
		t.Errorf("%d tables left after Down", leftover)
	}

	if _, err := hoppermigrate.Up(ctx, d, opts); err != nil {
		t.Fatalf("Up after Down: %v", err)
	}
}

func TestUpConcurrent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// A small pool reproduces replicas contending for connections while they
	// wait on the migration lock.
	pool := testdb.EmptyPool(t, 2)
	d := testdb.Driver(pool)
	opts := &hoppermigrate.Options{Logger: slog.New(slog.DiscardHandler)}

	var wg sync.WaitGroup
	errs := make([]error, 6)
	results := make([]*hoppermigrate.Result, 6)
	for i := range errs {
		wg.Go(func() {
			results[i], errs[i] = hoppermigrate.Up(ctx, d, opts)
		})
	}
	wg.Wait()
	applied := 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent Up %d: %v", i, err)
		}
		applied += len(results[i].Applied)
	}
	if applied != hoppermigrate.Latest() {
		t.Fatalf("migrations applied %d times across replicas, want %d", applied, hoppermigrate.Latest())
	}

	v, err := hoppermigrate.Version(ctx, d)
	if err != nil || v != hoppermigrate.Latest() {
		t.Fatalf("Version = %d, %v", v, err)
	}
}

func TestUpRejectsUnknownTarget(t *testing.T) {
	t.Parallel()
	pool := testdb.EmptyPool(t, 0)
	_, err := hoppermigrate.Up(context.Background(), testdb.Driver(pool), &hoppermigrate.Options{Target: hoppermigrate.Latest() + 1})
	if err == nil {
		t.Fatal("Up to an unknown version succeeded")
	}
}

// TestDownFoldsBatchProgress goes back below v11, whose release counts a
// batch down on pending alone: progress rows are folded into it, and a batch
// that had nothing left completes with its callback.
func TestDownFoldsBatchProgress(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.EmptyPool(t, 0)
	d := testdb.Driver(pool)
	opts := &hoppermigrate.Options{Logger: slog.New(slog.DiscardHandler)}
	if _, err := hoppermigrate.Up(ctx, d, opts); err != nil {
		t.Fatal(err)
	}
	var open, ready string
	if err := pool.QueryRow(ctx, `INSERT INTO hopper_batches (pending, total) VALUES (3, 3) RETURNING id::text`).Scan(&open); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO hopper_batches (pending, total, on_complete)
	    VALUES (2, 2, '{"kind": "done"}') RETURNING id::text`).Scan(&ready); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO hopper_batch_progress (batch_id, done, failed)
	    VALUES ($1::uuid, 1, 1), ($1::uuid, 1, 0), ($2::uuid, 1, 0), ($2::uuid, 1, 0)`, open, ready); err != nil {
		t.Fatal(err)
	}
	if _, err := hoppermigrate.Down(ctx, d, &hoppermigrate.Options{Logger: opts.Logger, Target: 10}); err != nil {
		t.Fatal(err)
	}
	var (
		pending, failed int
		completed       bool
	)
	if err := pool.QueryRow(ctx, `SELECT pending, failed, completed_at IS NOT NULL FROM hopper_batches WHERE id = $1::uuid`, open).
		Scan(&pending, &failed, &completed); err != nil {
		t.Fatal(err)
	}
	if pending != 1 || failed != 1 || completed {
		t.Errorf("open batch after down: pending = %d, failed = %d, completed = %t; want 1, 1, false", pending, failed, completed)
	}
	if err := pool.QueryRow(ctx, `SELECT pending, completed_at IS NOT NULL FROM hopper_batches WHERE id = $1::uuid`, ready).
		Scan(&pending, &completed); err != nil {
		t.Fatal(err)
	}
	var callbacks int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM hopper_jobs WHERE kind = 'done' AND metadata->>'batch_id' = $1`, ready).Scan(&callbacks); err != nil {
		t.Fatal(err)
	}
	if pending != 0 || !completed || callbacks != 1 {
		t.Errorf("batch with nothing left after down: pending = %d, completed = %t, callbacks = %d", pending, completed, callbacks)
	}
}

// TestCanceledStateRename moves a job canceled under v11 across v12 and back:
// the rename carries the stored row and the history partition with it.
func TestCanceledStateRename(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.EmptyPool(t, 0)
	d := testdb.Driver(pool)
	logger := slog.New(slog.DiscardHandler)
	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: logger, Target: 11}); err != nil {
		t.Fatal(err)
	}
	var id string
	if err := pool.QueryRow(ctx, `SELECT hopper_insert('rename', '{}')::text`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	const columns = `id, seq, kind, queue, state, priority, attempt, max_attempts, scheduled_at, attempted_at,
	  attempted_by, args, metadata, errors, unique_key, ordering_key, partition_key, batch_id, expires_at,
	  cancel_requested_at, await, created_at`
	if _, err := pool.Exec(ctx, `WITH j AS (DELETE FROM hopper_jobs WHERE id = $1::uuid RETURNING *)
	    INSERT INTO hopper_job_history (`+columns+`, finalized_at)
	    SELECT `+strings.ReplaceAll(columns, "state", "'cancelled'")+`, now() FROM j`, id); err != nil {
		t.Fatal(err)
	}
	state := func() (state, partition string) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT state::text, tableoid::regclass::text FROM hopper_job_history WHERE id = $1::uuid`, id).
			Scan(&state, &partition); err != nil {
			t.Fatal(err)
		}
		return state, partition
	}

	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: logger, Target: 12}); err != nil {
		t.Fatal(err)
	}
	if s, p := state(); s != "canceled" || !strings.HasPrefix(p, "hopper_job_history_failed") {
		t.Errorf("after v12: state %q in %s, want canceled in the failed partition", s, p)
	}
	if _, err := hoppermigrate.Down(ctx, d, &hoppermigrate.Options{Logger: logger, Target: 11}); err != nil {
		t.Fatal(err)
	}
	if s, _ := state(); s != "cancelled" {
		t.Errorf("after down to v11: state %q, want cancelled", s)
	}
}

// TestUpAfterHistoryOrderingAs12 upgrades a database migrated while the
// history ordering index was numbered 12: it recorded version 12 with the
// index declared and the canceled-state rename never run.
func TestUpAfterHistoryOrderingAs12(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.EmptyPool(t, 0)
	d := testdb.Driver(pool)
	logger := slog.New(slog.DiscardHandler)
	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: logger, Target: 11}); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"CREATE INDEX hopper_job_history_completed_ordering ON ONLY hopper_job_history_completed (ordering_key, id) WHERE ordering_key IS NOT NULL",
		"CREATE INDEX hopper_job_history_failed_ordering ON ONLY hopper_job_history_failed (ordering_key, id) WHERE ordering_key IS NOT NULL",
		"INSERT INTO hopper_schema (version) VALUES (12)",
	} {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	labels := func() string {
		t.Helper()
		var s string
		if err := pool.QueryRow(ctx, "SELECT string_agg(e.enumlabel, ',' ORDER BY e.enumsortorder) FROM pg_enum e WHERE e.enumtypid = 'hopper_job_state'::regtype").Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	indexes := func() int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM pg_class WHERE relnamespace = current_schema()::regnamespace AND relname LIKE 'hopper_job_history%ordering'").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: logger}); err != nil {
		t.Fatalf("Up: %v", err)
	}
	if l := labels(); !strings.Contains(l, ",canceled,") || strings.Contains(l, "cancelled") {
		t.Errorf("states after Up = %s, want canceled", l)
	}
	if n := indexes(); n != 2 {
		t.Errorf("%d ordering indexes after Up, want 2", n)
	}
	if _, err := hoppermigrate.Down(ctx, d, &hoppermigrate.Options{Logger: logger, Target: 11}); err != nil {
		t.Fatalf("Down: %v", err)
	}
	if l := labels(); !strings.Contains(l, ",cancelled,") {
		t.Errorf("states after Down = %s, want cancelled", l)
	}
	if n := indexes(); n != 0 {
		t.Errorf("%d ordering indexes after Down, want 0", n)
	}
}

// TestDownDropsUnattachedHistoryIndexes downgrades past v14 and v13 with a
// partition's unique-key and ordering-key indexes built but never attached,
// as an interrupted build leaves them.
func TestDownDropsUnattachedHistoryIndexes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.EmptyPool(t, 0)
	d := testdb.Driver(pool)
	logger := slog.New(slog.DiscardHandler)
	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: logger}); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"CREATE INDEX hopper_job_history_completed_default_ordering ON hopper_job_history_completed_default (ordering_key, id) WHERE ordering_key IS NOT NULL",
		"CREATE INDEX hopper_job_history_completed_default_unique ON hopper_job_history_completed_default (kind, unique_key, finalized_at) WHERE unique_key IS NOT NULL",
	} {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	indexes := func(suffix string) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM pg_class WHERE relnamespace = current_schema()::regnamespace AND relname LIKE 'hopper_job_history%' || $1", suffix).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if _, err := hoppermigrate.Down(ctx, d, &hoppermigrate.Options{Logger: logger, Target: 13}); err != nil {
		t.Fatalf("Down: %v", err)
	}
	if u, o := indexes("unique"), indexes("ordering"); u != 0 || o != 3 {
		t.Errorf("after Down to v13: %d unique-key indexes, want 0, and %d ordering-key indexes, want 3", u, o)
	}
	if _, err := hoppermigrate.Down(ctx, d, &hoppermigrate.Options{Logger: logger, Target: 12}); err != nil {
		t.Fatalf("Down: %v", err)
	}
	if n := indexes("ordering"); n != 0 {
		t.Errorf("%d ordering indexes after Down, want 0", n)
	}
}

// TestTagsUpgrade upgrades a database with jobs, live and in history, to
// v17: they have no tags, a tagged job's tags rows follow it from the live
// table to history, and a downgrade then drops the tags tables, triggers
// and functions with the column.
func TestTagsUpgrade(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.EmptyPool(t, 0)
	d := testdb.Driver(pool)
	logger := slog.New(slog.DiscardHandler)
	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: logger, Target: 16}); err != nil {
		t.Fatal(err)
	}
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, q, args...); err != nil {
			t.Fatal(err)
		}
	}
	const archive = `INSERT INTO hopper_job_history (id, seq, kind, queue, state, priority, attempt, max_attempts, scheduled_at,
		  args, metadata, errors, await, created_at, finalized_at%[1]s)
		SELECT id, seq, kind, queue, 'completed', priority, attempt, max_attempts, scheduled_at,
		  args, metadata, errors, await, created_at, now()%[1]s FROM hopper_jobs WHERE %[2]s`
	// A time partition the leader made before the upgrade, named for the
	// current hour, and wide enough that the test does not leave it.
	hour := time.Now().UTC().Truncate(time.Hour)
	partition := "hopper_job_history_completed_" + hour.Format("2006010215")
	exec(fmt.Sprintf("CREATE TABLE %s PARTITION OF hopper_job_history_completed FOR VALUES FROM ('%s') TO ('%s')",
		partition, hour.Add(-time.Hour).Format(time.RFC3339), hour.Add(2*time.Hour).Format(time.RFC3339)))
	// One of a period past, which no job is finalized into any more.
	past := "hopper_job_history_completed_" + hour.Add(-5*time.Hour).Format("2006010215")
	exec(fmt.Sprintf("CREATE TABLE %s PARTITION OF hopper_job_history_completed FOR VALUES FROM ('%s') TO ('%s')",
		past, hour.Add(-5*time.Hour).Format(time.RFC3339), hour.Add(-4*time.Hour).Format(time.RFC3339)))
	exec("INSERT INTO hopper_jobs (kind) SELECT 'k' FROM generate_series(1, 100)")
	exec(fmt.Sprintf(archive, "", "true"))
	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: logger}); err != nil {
		t.Fatal(err)
	}
	count := func(q string, args ...any) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, q, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := count("SELECT count(*) FROM hopper_jobs WHERE tags IS NULL") + count("SELECT count(*) FROM hopper_job_history WHERE tags IS NULL"); n != 200 {
		t.Errorf("%d jobs without tags after the upgrade, want 200", n)
	}
	var id string
	if err := pool.QueryRow(ctx, `SELECT hopper_insert('k', '{}', '{"tags": ["a", "b"]}')::text`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if n := count("SELECT count(*) FROM hopper_job_tags WHERE id = $1::uuid", id); n != 2 {
		t.Errorf("%d live tags rows for the tagged job, want 2", n)
	}
	exec(fmt.Sprintf(archive, ", tags", "id = '"+id+"'"))
	exec("DELETE FROM hopper_jobs WHERE id = $1::uuid", id)
	if l, h := count("SELECT count(*) FROM hopper_job_tags"), count("SELECT count(*) FROM hopper_job_history_tags WHERE id = $1::uuid AND state = 'completed'", id); l != 0 || h != 2 {
		t.Errorf("after the move to history: %d live tags rows, want 0, and %d history tags rows, want 2", l, h)
	}
	// The upgrade gave history's time partition its tags partition, where
	// the job's tags rows went with it.
	if n := count("SELECT count(*) FROM "+strings.Replace(partition, "history_", "history_tags_", 1)+" WHERE id = $1::uuid", id); n != 2 {
		t.Errorf("%d tags rows beside the job's history partition, want 2", n)
	}
	// Tags compare by their bytes, whatever the database's collation.
	if n := count(`SELECT count(*) FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
		WHERE c.relnamespace = current_schema()::regnamespace AND c.relname LIKE 'hopper\_job%tags%' AND a.attname = 'tag'
		  AND a.attcollation <> 'pg_catalog."C"'::regcollation`); n != 0 {
		t.Errorf("%d tags tables' tag columns use a collation other than C", n)
	}
	if n := count("SELECT count(*) FROM pg_class WHERE relnamespace = current_schema()::regnamespace AND relname = $1",
		strings.Replace(past, "history_", "history_tags_", 1)); n != 0 {
		t.Errorf("the past period's history partition got %d tags partitions, want 0", n)
	}
	if _, err := hoppermigrate.Down(ctx, d, &hoppermigrate.Options{Logger: logger, Target: 16}); err != nil {
		t.Fatalf("Down: %v", err)
	}
	if n := count(`SELECT count(*) FROM pg_class WHERE relnamespace = current_schema()::regnamespace AND relname LIKE 'hopper\_job%tags%'`) +
		count(`SELECT count(*) FROM pg_proc WHERE pronamespace = current_schema()::regnamespace AND proname LIKE 'hopper\_%tags%'`) +
		count(`SELECT count(*) FROM pg_trigger g JOIN pg_class c ON c.oid = g.tgrelid WHERE c.relnamespace = current_schema()::regnamespace AND g.tgname LIKE 'hopper\_%tags%'`) +
		count("SELECT count(*) FROM pg_attribute WHERE attname = 'tags' AND attrelid IN ('hopper_jobs'::regclass, 'hopper_job_history'::regclass)"); n != 0 {
		t.Errorf("%d tags objects left after Down", n)
	}
	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: logger}); err != nil {
		t.Fatalf("Up again: %v", err)
	}
}

// TestTagsUpgradeLocksOnlyInItsRetryLoop upgrades to v17 while another
// schema's history time partition is held, as its leader holds one while it
// drops it, and while a session briefly holds this schema's hopper_schema.
// Index scans are off, so that the listing of history's time partitions is
// planned as a scan of the whole catalog, as Postgres may plan it. Listing
// them takes no lock on any partition, and only the retry loop gives up on
// a lock after its lock_timeout: the rest of the migration waits for one.
func TestTagsUpgradeLocksOnlyInItsRetryLoop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	other := testdb.EmptyPool(t, 0)
	if _, err := other.Exec(ctx, `CREATE TABLE hopper_job_history_completed (finalized_at timestamptz) PARTITION BY RANGE (finalized_at);
		CREATE TABLE hopper_job_history_completed_p PARTITION OF hopper_job_history_completed FOR VALUES FROM ('2026-01-01') TO ('2026-01-02')`); err != nil {
		t.Fatal(err)
	}
	dropping, err := other.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer dropping.Rollback(ctx) //nolint:errcheck // cleanup
	if _, err := dropping.Exec(ctx, "LOCK TABLE hopper_job_history_completed_p IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}

	pool := testdb.EmptyPool(t, 0)
	logger := slog.New(slog.DiscardHandler)
	if _, err := hoppermigrate.Up(ctx, testdb.Driver(pool), &hoppermigrate.Options{Logger: logger, Target: 16}); err != nil {
		t.Fatal(err)
	}
	hour := time.Now().UTC().Truncate(time.Hour)
	partition := "hopper_job_history_completed_" + hour.Format("2006010215")
	// Two hours wide, so that the period has not ended by the time the
	// upgrade copies it, should the test straddle the hour.
	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s PARTITION OF hopper_job_history_completed FOR VALUES FROM ('%s') TO ('%s')",
		partition, hour.Format(time.RFC3339), hour.Add(2*time.Hour).Format(time.RFC3339))); err != nil {
		t.Fatal(err)
	}
	held, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := held.Exec(ctx, "LOCK TABLE hopper_schema IN SHARE MODE"); err != nil {
		_ = held.Rollback(ctx)
		t.Fatal(err)
	}
	// The commit is the goroutine's alone, and the test waits for it on any
	// way out, so that the transaction is never used from two goroutines.
	committed := make(chan struct{})
	go func() {
		defer close(committed)
		time.Sleep(500 * time.Millisecond)
		_ = held.Commit(ctx)
	}()
	defer func() { <-committed }()
	cfg := pool.Config().Copy()
	cfg.ConnConfig.RuntimeParams["enable_indexscan"] = "off"
	cfg.ConnConfig.RuntimeParams["enable_bitmapscan"] = "off"
	scans, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer scans.Close()
	uctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := hoppermigrate.Up(uctx, testdb.Driver(scans), &hoppermigrate.Options{Logger: logger}); err != nil {
		t.Fatalf("Up while another schema's partition and this schema's hopper_schema were held: %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM pg_class WHERE relname = $1 AND relnamespace = current_schema()::regnamespace",
		"hopper_job_history_tags_completed_"+hour.Format("2006010215")).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("history's time partition has %d tags partitions, want 1", n)
	}
}

// TestMigrationCancelsIndexBuild runs a migration while the leader builds a
// history index (a fresh schema's DEFAULT partitions have none), held up by
// an old snapshot elsewhere. The migration cancels the build instead of
// waiting on it, which would never end: the build holds a lock the
// migration needs and, in its last phase, waits for the migration's
// statement. The index is built afterwards.
func TestMigrationCancelsIndexBuild(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.EmptyPool(t, 0)
	d := testdb.Driver(pool)
	logger := slog.New(slog.DiscardHandler)
	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: logger}); err != nil {
		t.Fatal(err)
	}
	held, built := startHeldBuild(ctx, t, pool, d)
	// Well short of the 10 minutes a migration would wait on the build, and
	// long enough for the migration lock, which the database's other
	// schemas' migrations take too.
	mctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	if _, err := hoppermigrate.Down(mctx, d, &hoppermigrate.Options{Logger: logger, Target: 16}); err != nil {
		t.Fatalf("Down while an index builds: %v", err)
	}
	if err := <-built; err == nil {
		t.Error("the canceled build reported no error")
	}
	if err := held.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: logger}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Executor().HistoryIndex(ctx, driver.HistoryIndexParams{}); err != nil {
		t.Fatal(err)
	}
	var valid bool
	if err := pool.QueryRow(ctx, "SELECT indisvalid FROM pg_index WHERE indexrelid = 'hopper_job_history_completed_default_ordering'::regclass").Scan(&valid); err != nil || !valid {
		t.Errorf("hopper_job_history_completed_default_ordering valid = %t, %v", valid, err)
	}
}

// TestMigrationCancelsBuildStartedWhileItWaits holds the indexing lock, as
// the leader does while it drops an invalid index before building it again,
// so that a migration starts looking for builds to cancel before there is
// one, and then starts the build. The migration cancels it, though the
// server shows a transaction the activity it first read for the rest of it.
func TestMigrationCancelsBuildStartedWhileItWaits(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.EmptyPool(t, 0)
	d := testdb.Driver(pool)
	logger := slog.New(slog.DiscardHandler)
	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: logger}); err != nil {
		t.Fatal(err)
	}
	// HistoryIndexLockKey ("hopi"), which the leader holds while it indexes,
	// and HistoryIndexCancelKey ("hopc"), which the migration takes once it
	// looks for builds.
	const indexLockKey, cancelKey int32 = 0x686f7069, 0x686f7063
	indexing, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = indexing.Rollback(ctx) })
	if _, err := indexing.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext(current_schema()), $1)", indexLockKey); err != nil {
		t.Fatal(err)
	}
	mctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	migrated := make(chan error, 1)
	go func() {
		_, err := hoppermigrate.Down(mctx, d, &hoppermigrate.Options{Logger: logger, Target: 16})
		migrated <- err
	}()
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(10 * time.Millisecond) {
		var looking bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted
			AND classid = hashtext(current_schema())::oid AND objid = $1::oid AND objsubid = 2)`, cancelKey).Scan(&looking); err != nil {
			t.Fatal(err)
		}
		if looking {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the migration did not start looking for builds")
		}
	}
	// A few of the migration's passes see no build.
	time.Sleep(300 * time.Millisecond)

	// The build waits for a snapshot held open, as the leader's does for a
	// long transaction.
	held, err := pool.Begin(ctx)
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
	built := make(chan error, 1)
	go func() {
		_, err := pool.Exec(ctx, `CREATE INDEX CONCURRENTLY hopper_job_history_completed_default_ordering
			ON hopper_job_history_completed_default (ordering_key, id) WHERE ordering_key IS NOT NULL`)
		built <- err
	}()
	select {
	case err := <-built:
		if err == nil || !strings.Contains(err.Error(), "57014") {
			t.Errorf("the build started while the migration waited = %v, want it canceled (57014)", err)
		}
	case <-time.After(30 * time.Second):
		// The build can be waiting for the migration's transaction too,
		// which waits for the lock: let both go.
		t.Error("the migration did not cancel a build started while it waited")
		_ = held.Rollback(ctx)
		_ = indexing.Rollback(ctx)
		<-built
	}
	_ = indexing.Rollback(ctx)
	if err := <-migrated; err != nil {
		t.Errorf("Down: %v", err)
	}
}

// TestMigrationFailsOnBuildItMayNotCancel runs a migration as a role that
// may not cancel the leader's build, a superuser's, while the build is held
// up. The migration fails at once, saying why, and leaves the build to
// finish: waiting for it could deadlock with migrations queued for the
// migration lock, whose snapshots the build waits for.
func TestMigrationFailsOnBuildItMayNotCancel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.EmptyPool(t, 0)
	var (
		owner string
		super bool
	)
	if err := pool.QueryRow(ctx, "SELECT rolname, rolsuper FROM pg_roles WHERE rolname = current_user").Scan(&owner, &super); err != nil {
		t.Fatal(err)
	}
	if !super {
		t.Skip("the test database's role is not a superuser, whose build another role may not cancel")
	}
	d := testdb.Driver(pool)
	logger := slog.New(slog.DiscardHandler)
	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: logger}); err != nil {
		t.Fatal(err)
	}
	// A role with the owner's privileges, so that it may migrate, but not a
	// superuser's.
	role := pgx.Identifier{pool.Config().ConnConfig.RuntimeParams["search_path"] + "_migrator"}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE ROLE "+role+" NOLOGIN; GRANT "+pgx.Identifier{owner}.Sanitize()+" TO "+role); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(ctx, "REASSIGN OWNED BY "+role+" TO current_user; DROP OWNED BY "+role+"; DROP ROLE "+role); err != nil {
			t.Errorf("drop role: %v", err)
		}
	})
	cfg := pool.Config().Copy()
	cfg.ConnConfig.RuntimeParams["role"] = strings.Trim(role, `"`)
	limited, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer limited.Close()

	held, built := startHeldBuild(ctx, t, pool, d)
	mctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, err = hoppermigrate.Down(mctx, testdb.Driver(limited), &hoppermigrate.Options{Logger: logger, Target: 16})
	if err == nil || !strings.Contains(err.Error(), "may not cancel") {
		t.Fatalf("Down with a build it may not cancel = %v, want the error saying so", err)
	}
	select {
	case err := <-built:
		t.Fatalf("the build ended while held: %v", err)
	default:
	}
	if err := held.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-built; err != nil {
		t.Errorf("the build: %v", err)
	}
	if _, err := hoppermigrate.Down(mctx, testdb.Driver(limited), &hoppermigrate.Options{Logger: logger, Target: 16}); err != nil {
		t.Errorf("Down after the build: %v", err)
	}
}

// TestMigrationLeavesOperatorBuilds runs a migration while the leader holds
// the indexing lock and an operator builds an index of their own,
// concurrently, on a history partition the leader indexes, held up by an old
// snapshot. The migration cancels only the leader's builds, so it waits for
// the lock and leaves the operator's build alone.
func TestMigrationLeavesOperatorBuilds(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.EmptyPool(t, 0)
	d := testdb.Driver(pool)
	logger := slog.New(slog.DiscardHandler)
	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: logger}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Executor().HistoryIndex(ctx, driver.HistoryIndexParams{}); err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := pool.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	// HistoryIndexLockKey ("hopi"), which the leader holds while it
	// indexes.
	indexing, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = indexing.Rollback(ctx) }()
	if _, err := indexing.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1), $2)", schema, int32(0x686f7069)); err != nil {
		t.Fatal(err)
	}
	held, err := pool.Begin(ctx)
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
	built := make(chan error, 1)
	go func() {
		_, err := pool.Exec(ctx, "CREATE INDEX CONCURRENTLY operator_finalized ON hopper_job_history_completed_default (finalized_at)")
		built <- err
	}()
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(10 * time.Millisecond) {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_progress_create_index p JOIN pg_class c ON c.oid = p.relid
			WHERE c.relnamespace = current_schema()::regnamespace`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the operator's build did not start")
		}
	}
	mctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	migrated := make(chan error, 1)
	go func() {
		_, err := hoppermigrate.Down(mctx, d, &hoppermigrate.Options{Logger: logger, Target: 16})
		migrated <- err
	}()
	select {
	case err := <-built:
		t.Fatalf("the operator's build ended while the migration waited for the indexing lock: %v", err)
	case err := <-migrated:
		t.Fatalf("the migration ran while the indexing lock was held: %v", err)
	case <-time.After(time.Second):
	}
	// The operator's build would finish only once every older snapshot in
	// the database had gone, other tests' held ones included, while the
	// migration holds the lock their migrations queue for; it is canceled
	// here instead, before the migration goes on.
	if _, err := pool.Exec(ctx, `SELECT pg_cancel_backend(p.pid) FROM pg_stat_progress_create_index p
		JOIN pg_class c ON c.oid = p.relid WHERE c.relnamespace = current_schema()::regnamespace`); err != nil {
		t.Fatal(err)
	}
	if err := <-built; err == nil {
		t.Error("the operator's build finished while held")
	}
	if _, err := pool.Exec(ctx, "DROP INDEX operator_finalized"); err != nil {
		t.Fatal(err)
	}
	if err := held.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := indexing.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-migrated; err != nil {
		t.Errorf("Down: %v", err)
	}
}

// startHeldBuild starts the leader's history index build, held up in its
// wait for older snapshots by one that held keeps, and returns once the build
// is under way. built yields the build's result.
func startHeldBuild(ctx context.Context, t *testing.T, pool *pgxpool.Pool, d driver.Driver[pgx.Tx]) (held pgx.Tx, built chan error) {
	t.Helper()
	held, err := pool.Begin(ctx)
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
	built = make(chan error, 1)
	go func() {
		_, err := d.Executor().HistoryIndex(ctx, driver.HistoryIndexParams{})
		built <- err
	}()
	for deadline := time.Now().Add(time.Minute); ; {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_progress_create_index p JOIN pg_class c ON c.oid = p.relid
			WHERE c.relnamespace = current_schema()::regnamespace`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			break
		}
		select {
		case err := <-built:
			t.Fatalf("the index pass ended before its build was seen: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("no index build started")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return held, built
}

// TestMigrationLeavesOtherDatabasesBuilds migrates a database made from
// another as its template, whose relations share the other's OIDs, while the
// other's leader builds a history index and this one's holds the indexing
// lock, so that the migration looks for builds to cancel. It cancels none of
// the other database's.
func TestMigrationLeavesOtherDatabasesBuilds(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := slog.New(slog.DiscardHandler)
	const schema = "hopper_template"
	open := func(dbURL string) *pgxpool.Pool {
		t.Helper()
		cfg, err := pgxpool.ParseConfig(dbURL)
		if err != nil {
			t.Fatal(err)
		}
		cfg.ConnConfig.RuntimeParams["search_path"] = schema
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pool
	}
	templateURL := testdb.Database(t, "")
	pool := open(templateURL)
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	if _, err := hoppermigrate.Up(ctx, testdb.Driver(pool), &hoppermigrate.Options{Logger: logger}); err != nil {
		t.Fatal(err)
	}
	pool.Close()
	template := pool.Config().ConnConfig.Database
	copied := open(testdb.Database(t, "TEMPLATE "+pgx.Identifier{template}.Sanitize()))
	pool = open(templateURL)

	// HistoryIndexLockKey ("hopi"), which the copy's leader holds while it
	// indexes.
	indexing, err := copied.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = indexing.Rollback(ctx) }()
	if _, err := indexing.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1), $2)", schema, int32(0x686f7069)); err != nil {
		t.Fatal(err)
	}
	held, built := startHeldBuild(ctx, t, pool, testdb.Driver(pool))
	mctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	migrated := make(chan error, 1)
	go func() {
		_, err := hoppermigrate.Down(mctx, testdb.Driver(copied), &hoppermigrate.Options{Logger: logger, Target: 16})
		migrated <- err
	}()
	select {
	case err := <-built:
		t.Fatalf("the template's build ended while its copy migrated: %v", err)
	case err := <-migrated:
		t.Fatalf("the copy migrated while its indexing lock was held: %v", err)
	case <-time.After(time.Second):
	}
	if err := indexing.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-migrated; err != nil {
		t.Fatalf("Down on the copy: %v", err)
	}
	if err := held.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-built; err != nil {
		t.Errorf("the template's build: %v", err)
	}
}

// TestTagsUpgradeCopiesBoundsInAnyDateStyle upgrades to v17 from a session
// whose DateStyle writes a timestamp's zone as an abbreviation that
// timezone_abbreviations reads as another zone's (Asia/Shanghai's CST as US
// Central's, before Postgres 18). The tags partition v17 gives history's time
// partition covers the same period.
func TestTagsUpgradeCopiesBoundsInAnyDateStyle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.EmptyPool(t, 0)
	logger := slog.New(slog.DiscardHandler)
	if _, err := hoppermigrate.Up(ctx, testdb.Driver(pool), &hoppermigrate.Options{Logger: logger, Target: 16}); err != nil {
		t.Fatal(err)
	}
	hour := time.Now().UTC().Truncate(time.Hour)
	partition := "hopper_job_history_completed_" + hour.Format("2006010215")
	// Two hours wide, so that the period has not ended by the time the
	// upgrade copies it, should the test straddle the hour.
	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s PARTITION OF hopper_job_history_completed FOR VALUES FROM ('%s') TO ('%s')",
		partition, hour.Format(time.RFC3339), hour.Add(2*time.Hour).Format(time.RFC3339))); err != nil {
		t.Fatal(err)
	}
	cfg := pool.Config().Copy()
	cfg.ConnConfig.RuntimeParams["datestyle"] = "SQL, DMY"
	cfg.ConnConfig.RuntimeParams["timezone"] = "Asia/Shanghai"
	styled, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer styled.Close()
	if _, err := hoppermigrate.Up(ctx, testdb.Driver(styled), &hoppermigrate.Options{Logger: logger}); err != nil {
		t.Fatal(err)
	}
	var history, tags string
	if err := pool.QueryRow(ctx, `SELECT pg_get_expr(h.relpartbound, h.oid), coalesce(pg_get_expr(g.relpartbound, g.oid), '')
		FROM pg_class h LEFT JOIN pg_class g ON g.relname = $2 AND g.relnamespace = h.relnamespace
		WHERE h.relname = $1 AND h.relnamespace = current_schema()::regnamespace`,
		partition, strings.Replace(partition, "history_", "history_tags_", 1)).Scan(&history, &tags); err != nil {
		t.Fatal(err)
	}
	if tags != history {
		t.Errorf("tags partition bound %q, want history's %q", tags, history)
	}
}
