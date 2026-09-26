package hopperpgx_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/hopperpgx"
	"github.com/parallelworks/hopper/drivertest"
	"github.com/parallelworks/hopper/internal/testdb"
)

// pgxFixture runs the conformance suite against hopperpgx.
type pgxFixture struct{}

func pool(d driver.Driver[pgx.Tx]) interface {
	Exec(ctx context.Context, sql string, args ...any) (interface{ RowsAffected() int64 }, error)
} {
	return execAdapter{d.(*hopperpgx.Driver)}
}

type execAdapter struct{ d *hopperpgx.Driver }

func (a execAdapter) Exec(ctx context.Context, sql string, args ...any) (interface{ RowsAffected() int64 }, error) {
	return a.d.Pool().Exec(ctx, sql, args...)
}

func (pgxFixture) NewDriver(t *testing.T) driver.Driver[pgx.Tx] {
	return hopperpgx.New(testdb.Pool(t))
}

func (pgxFixture) Begin(ctx context.Context, t *testing.T, d driver.Driver[pgx.Tx]) (pgx.Tx, func() error, func() error) {
	t.Helper()
	tx, err := d.(*hopperpgx.Driver).Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return tx, func() error { return tx.Commit(ctx) }, func() error { return tx.Rollback(ctx) }
}

func (pgxFixture) ExpireLease(ctx context.Context, t *testing.T, d driver.Driver[pgx.Tx], clientID int64) {
	t.Helper()
	if _, err := pool(d).Exec(ctx, "UPDATE hopper_clients SET expires_at = now() - interval '1s' WHERE id = $1", clientID); err != nil {
		t.Fatal(err)
	}
}

func (pgxFixture) ExpireLeader(ctx context.Context, t *testing.T, d driver.Driver[pgx.Tx]) {
	t.Helper()
	if _, err := pool(d).Exec(ctx, "UPDATE hopper_leader SET expires_at = now() - interval '1s'"); err != nil {
		t.Fatal(err)
	}
}

func (pgxFixture) MakeClaimable(ctx context.Context, t *testing.T, d driver.Driver[pgx.Tx], id driver.JobID) {
	t.Helper()
	if _, err := pool(d).Exec(ctx, "UPDATE hopper_jobs SET scheduled_at = now() WHERE id = $1", [16]byte(id)); err != nil {
		t.Fatal(err)
	}
}

func (pgxFixture) AgeAttempt(ctx context.Context, t *testing.T, d driver.Driver[pgx.Tx], id driver.JobID, age time.Duration) {
	t.Helper()
	if _, err := pool(d).Exec(ctx, "UPDATE hopper_jobs SET attempted_at = now() - make_interval(secs => $2) WHERE id = $1", [16]byte(id), age.Seconds()); err != nil {
		t.Fatal(err)
	}
}

func (pgxFixture) CountLive(ctx context.Context, t *testing.T, d driver.Driver[pgx.Tx]) int {
	return count(ctx, t, d, "hopper_jobs")
}

func (pgxFixture) CountHistory(ctx context.Context, t *testing.T, d driver.Driver[pgx.Tx]) int {
	return count(ctx, t, d, "hopper_job_history")
}

func count(ctx context.Context, t *testing.T, d driver.Driver[pgx.Tx], table string) int {
	t.Helper()
	var n int
	if err := d.(*hopperpgx.Driver).Pool().QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestConformance(t *testing.T) {
	t.Parallel()
	drivertest.Run(t, pgxFixture{})
}
