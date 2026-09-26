package hoppersql_test

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/hoppersql"
	"github.com/parallelworks/hopper/drivertest"
	"github.com/parallelworks/hopper/hoppermigrate"
	"github.com/parallelworks/hopper/internal/testdb"
)

// sqlFixture runs the conformance suite against hoppersql, through pgx's
// database/sql adapter.
type sqlFixture struct{}

// open returns a database/sql handle on the pool's schema.
func open(t *testing.T, pool *pgxpool.Pool) *sql.DB {
	t.Helper()
	db := stdlib.OpenDB(*pool.Config().ConnConfig)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func (sqlFixture) NewDriver(t *testing.T) driver.Driver[*sql.Tx] {
	return hoppersql.New(open(t, testdb.Pool(t)))
}

func (sqlFixture) Begin(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx]) (*sql.Tx, func() error, func() error) {
	t.Helper()
	tx, err := d.(*hoppersql.Driver).DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	return tx, tx.Commit, tx.Rollback
}

func exec(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], query string, args ...any) {
	t.Helper()
	if _, err := d.(*hoppersql.Driver).DB().ExecContext(ctx, query, args...); err != nil {
		t.Fatal(err)
	}
}

func (sqlFixture) ExpireLease(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], clientID int64) {
	exec(ctx, t, d, "UPDATE hopper_clients SET expires_at = now() - interval '1s' WHERE id = $1", clientID)
}

func (sqlFixture) ExpireLeader(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx]) {
	exec(ctx, t, d, "UPDATE hopper_leader SET expires_at = now() - interval '1s'")
}

func (sqlFixture) MakeClaimable(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], id driver.JobID) {
	exec(ctx, t, d, "UPDATE hopper_jobs SET scheduled_at = now() WHERE id = $1", id)
}

func (sqlFixture) AgeAttempt(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], id driver.JobID, age time.Duration) {
	exec(ctx, t, d, "UPDATE hopper_jobs SET attempted_at = now() - make_interval(secs => $2) WHERE id = $1", id, age.Seconds())
}

func (sqlFixture) CountLive(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx]) int {
	return count(ctx, t, d, "hopper_jobs")
}

func (sqlFixture) CountHistory(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx]) int {
	return count(ctx, t, d, "hopper_job_history")
}

func count(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], table string) int {
	t.Helper()
	var n int
	if err := d.(*hoppersql.Driver).DB().QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestConformance(t *testing.T) {
	t.Parallel()
	drivertest.Run(t, sqlFixture{})
}

// TestMigrate applies and reverts the schema through the database/sql
// migrator, which holds its lock on a connection reserved from the pool.
func TestMigrate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d := hoppersql.New(open(t, testdb.EmptyPool(t, 0)))
	opts := &hoppermigrate.Options{Logger: slog.New(slog.DiscardHandler)}
	if _, err := hoppermigrate.Up(ctx, d, opts); err != nil {
		t.Fatalf("up: %v", err)
	}
	v, err := hoppermigrate.Version(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	if v != hoppermigrate.Latest() {
		t.Fatalf("version after up = %d, want %d", v, hoppermigrate.Latest())
	}
	if _, err := hoppermigrate.Down(ctx, d, &hoppermigrate.Options{Logger: opts.Logger, Target: 0}); err != nil {
		t.Fatalf("down: %v", err)
	}
	if v, err := hoppermigrate.Version(ctx, d); err != nil || v != 0 {
		t.Fatalf("version after down = %d, %v; want 0", v, err)
	}
}
