package hoppermigrate_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/parallelworks/hopper/driver/hopperpgx"
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
}

func TestUpDownUp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.EmptyPool(t, 0)
	d := hopperpgx.New(pool)
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
	d := hopperpgx.New(pool)
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
	_, err := hoppermigrate.Up(context.Background(), hopperpgx.New(pool), &hoppermigrate.Options{Target: hoppermigrate.Latest() + 1})
	if err == nil {
		t.Fatal("Up to an unknown version succeeded")
	}
}
