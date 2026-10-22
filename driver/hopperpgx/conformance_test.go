package hopperpgx_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/hopperpgx"
	"github.com/parallelworks/hopper/drivertest"
	"github.com/parallelworks/hopper/internal/testdb"
)

// pgxFixture runs the conformance suite against hopperpgx. Its hooks are
// statements on hopper's schema (drivertest.Postgres), which pgxSQL runs on
// the driver's pool.
type pgxFixture struct{ drivertest.Postgres[pgx.Tx] }

func newFixture() pgxFixture { return pgxFixture{drivertest.Postgres[pgx.Tx]{SQL: pgxSQL{}}} }

func (pgxFixture) NewDriver(t *testing.T) driver.Driver[pgx.Tx] {
	return testdb.Driver(testdb.Pool(t))
}

func (pgxFixture) Begin(ctx context.Context, t *testing.T, d driver.Driver[pgx.Tx]) (pgx.Tx, func() error, func() error) {
	t.Helper()
	tx, err := d.(*hopperpgx.Driver).Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return tx, func() error { return tx.Commit(ctx) }, func() error { return tx.Rollback(ctx) }
}

// pgxSQL runs the suite's statements on a driver's pool.
type pgxSQL struct{}

func (pgxSQL) Exec(ctx context.Context, t *testing.T, d driver.Driver[pgx.Tx], query string, args ...any) {
	t.Helper()
	if _, err := d.(*hopperpgx.Driver).Pool().Exec(ctx, query, args...); err != nil {
		t.Error(err)
	}
}

func (pgxSQL) QueryText(ctx context.Context, t *testing.T, d driver.Driver[pgx.Tx], query string, args ...any) string {
	t.Helper()
	var s string
	if err := d.(*hopperpgx.Driver).Pool().QueryRow(ctx, query, args...).Scan(&s); err != nil {
		t.Error(err)
	}
	return s
}

func TestConformance(t *testing.T) {
	t.Parallel()
	drivertest.Run(t, newFixture())
}

func TestConformancePostgres(t *testing.T) {
	t.Parallel()
	drivertest.RunPostgres(t, newFixture())
}
