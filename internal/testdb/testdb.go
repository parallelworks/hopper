// Package testdb gives integration tests a Postgres database of their own.
//
// Each test gets a fresh schema, so tests run in parallel and leave nothing
// behind, unless the test binary is killed. The database comes from HOPPER_TEST_DATABASE_URL; without it,
// integration tests are skipped.
package testdb

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/url"
	"os"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/parallelworks/hopper/driver/hopperpgx"
	"github.com/parallelworks/hopper/hoppermigrate"
)

var counter atomic.Int64

// run tells this process's schemas from those of an earlier process with the
// same PID, whose schemas a killed test left behind in a database that
// persists between runs.
var run = rand.Uint32()

// URL returns HOPPER_TEST_DATABASE_URL or skips the test.
func URL(t testing.TB) string {
	t.Helper()
	url := os.Getenv("HOPPER_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("HOPPER_TEST_DATABASE_URL not set; run `make devstack` and `make test`")
	}
	return url
}

// Pool returns a pool whose search_path is a schema created for this test,
// with hopper's schema installed. The schema is dropped when the test ends.
func Pool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	return PoolIn(t, URL(t))
}

// PoolIn is like Pool, in the database at dbURL.
func PoolIn(t testing.TB, dbURL string) *pgxpool.Pool {
	t.Helper()
	pool := emptyPool(t, dbURL, 0)
	opts := &hoppermigrate.Options{Logger: slog.New(slog.DiscardHandler)}
	if _, err := hoppermigrate.Up(context.Background(), Driver(pool), opts); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

// EmptyPool is like Pool but leaves the schema empty, for migration tests.
// maxConns of 0 uses the pgxpool default.
func EmptyPool(t testing.TB, maxConns int32) *pgxpool.Pool {
	t.Helper()
	return emptyPool(t, URL(t), maxConns)
}

func emptyPool(t testing.TB, dbURL string, maxConns int32) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	schema := fmt.Sprintf("hopper_test_%d_%08x_%d", os.Getpid(), run, counter.Add(1))

	admin, err := pgx.Connect(ctx, dbURL)
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

	cfg, err := pgxpool.ParseConfig(dbURL)
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

// Database creates a database for this test, with the CREATE DATABASE
// options given, and returns its URL. It is dropped when the test ends.
func Database(t testing.TB, options string) string {
	t.Helper()
	ctx := context.Background()
	base := URL(t)
	name := fmt.Sprintf("hopper_test_%d_%08x_%d", os.Getpid(), run, counter.Add(1))
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer admin.Close(ctx) //nolint:errcheck // test setup
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()+" "+options); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		admin, err := pgx.Connect(ctx, base)
		if err != nil {
			t.Errorf("connect: %v", err)
			return
		}
		defer admin.Close(ctx) //nolint:errcheck // test cleanup
		if _, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Errorf("drop database: %v", err)
		}
	})
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	u.Path = "/" + name
	return u.String()
}

// Driver binds the driver to the isolated schema created by EmptyPool.
func Driver(pool *pgxpool.Pool) *hopperpgx.Driver {
	return hopperpgx.NewWithConfig(pool, &hopperpgx.Config{Schema: pool.Config().ConnConfig.RuntimeParams["search_path"]})
}
