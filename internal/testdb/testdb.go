// Package testdb gives integration tests a Postgres database of their own.
//
// Each test gets a fresh schema, so tests run in parallel and leave nothing
// behind. The database comes from HOPPER_TEST_DATABASE_URL; without it,
// integration tests are skipped.
package testdb

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/parallelworks/hopper/driver/hopperpgx"
	"github.com/parallelworks/hopper/hoppermigrate"
)

var counter atomic.Int64

// URL returns HOPPER_TEST_DATABASE_URL or skips the test.
func URL(t testing.TB) string {
	t.Helper()
	url := os.Getenv("HOPPER_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("HOPPER_TEST_DATABASE_URL not set; run `make pg` and `make test`")
	}
	return url
}

// Pool returns a pool whose search_path is a schema created for this test,
// with hopper's schema installed. The schema is dropped when the test ends.
func Pool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	pool := EmptyPool(t, 0)
	opts := &hoppermigrate.Options{Logger: slog.New(slog.DiscardHandler)}
	if _, err := hoppermigrate.Up(context.Background(), hopperpgx.New(pool), opts); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

// EmptyPool is like Pool but leaves the schema empty, for migration tests.
// maxConns of 0 uses the pgxpool default.
func EmptyPool(t testing.TB, maxConns int32) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	url := URL(t)
	schema := fmt.Sprintf("hopper_test_%d_%d", os.Getpid(), counter.Add(1))

	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Errorf("drop schema: %v", err)
		}
		admin.Close(ctx) //nolint:errcheck // test cleanup
	})

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
