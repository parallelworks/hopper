package hoppermigrate_test

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

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
	const latest = 15
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
