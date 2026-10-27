package hoppersqlite_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/hoppersqlite"
	"github.com/parallelworks/hopper/drivertest"
	"github.com/parallelworks/hopper/hoppermigrate"
)

// open returns a driver on a new database in the test's temporary
// directory, migrated to the latest schema.
func open(t testing.TB) *hoppersqlite.Driver {
	t.Helper()
	return openAt(t, filepath.Join(t.TempDir(), "hopper.db"))
}

// openAt is open on the database at path.
func openAt(t testing.TB, path string) *hoppersqlite.Driver {
	t.Helper()
	d, err := hoppersqlite.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if _, err := hoppermigrate.Up(context.Background(), d, &hoppermigrate.Options{Logger: slog.New(slog.DiscardHandler)}); err != nil {
		t.Fatal(err)
	}
	return d
}

// fixture runs the conformance suite against hoppersqlite. Its hooks are
// statements on the driver's database handle, with the host's clock as the
// database's.
type fixture struct{}

func db(d driver.Driver[*sql.Tx]) *sql.DB { return d.(*hoppersqlite.Driver).DB() }

func now() int64 { return time.Now().UnixMicro() }

func execSQL(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], query string, args ...any) {
	t.Helper()
	if _, err := db(d).ExecContext(ctx, query, args...); err != nil {
		t.Error(err)
	}
}

func queryText(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], query string, args ...any) string {
	t.Helper()
	var s string
	if err := db(d).QueryRowContext(ctx, query, args...).Scan(&s); err != nil {
		t.Error(err)
	}
	return s
}

func count(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], query string) int {
	t.Helper()
	n, err := strconv.Atoi(queryText(ctx, t, d, query))
	if err != nil {
		t.Error(err)
	}
	return n
}

func (fixture) NewDriver(t *testing.T) driver.Driver[*sql.Tx] { return open(t) }

func (fixture) Begin(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx]) (*sql.Tx, func() error, func() error) {
	t.Helper()
	tx, err := db(d).BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	return tx, tx.Commit, tx.Rollback
}

func (fixture) ExpireLease(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], clientID int64) {
	execSQL(ctx, t, d, "UPDATE hopper_clients SET expires_at = ? WHERE id = ?", now()-1e6, clientID)
}

func (fixture) ExpireLeader(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx]) {
	execSQL(ctx, t, d, "UPDATE hopper_leader SET expires_at = ?", now()-1e6)
}

func (fixture) MakeClaimable(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], id driver.JobID) {
	execSQL(ctx, t, d, "UPDATE hopper_jobs SET scheduled_at = ? WHERE id = ?", now(), id[:])
}

func (fixture) AgeAttempt(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], id driver.JobID, age time.Duration) {
	execSQL(ctx, t, d, "UPDATE hopper_jobs SET attempted_at = ? WHERE id = ? AND attempted_at IS NOT NULL", now()-age.Microseconds(), id[:])
}

func (fixture) CountLive(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx]) int {
	return count(ctx, t, d, "SELECT count(*) FROM hopper_jobs")
}

func (fixture) CountHistory(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx]) int {
	return count(ctx, t, d, "SELECT count(*) FROM hopper_job_history")
}

func (fixture) ExpireJob(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], id driver.JobID) {
	execSQL(ctx, t, d, "UPDATE hopper_jobs SET expires_at = ? WHERE id = ?", now()-1e6, id[:])
}

func (fixture) StaleWake(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], id driver.JobID) {
	execSQL(ctx, t, d, "UPDATE hopper_jobs SET wake_requested_at = attempted_at - 1000000 WHERE id = ?", id[:])
}

func (fixture) MakePending(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], id driver.JobID) {
	execSQL(ctx, t, d, "UPDATE hopper_jobs SET state = 'pending' WHERE id = ?", id[:])
}

func (fixture) SnoozeReason(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], id driver.JobID) string {
	return queryText(ctx, t, d, "SELECT coalesce(snooze_reason, '') FROM hopper_jobs WHERE id = ?", id[:])
}

func (fixture) SetSnoozeReason(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], id driver.JobID, reason string) {
	execSQL(ctx, t, d, "UPDATE hopper_jobs SET snooze_reason = ? WHERE id = ?", reason, id[:])
}

func (fixture) SetProgress(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], id driver.JobID, value string) {
	execSQL(ctx, t, d, "UPDATE hopper_jobs SET progress = ? WHERE id = ?", value, id[:])
}

func (fixture) ExpireJobLease(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], id driver.JobID) {
	execSQL(ctx, t, d, "UPDATE hopper_job_leases SET expires_at = ?, renewed_at = ? - ttl WHERE id = ?", now()-1e6, now()-1e6, id[:])
}

func (fixture) CountJobLeases(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx]) int {
	return count(ctx, t, d, "SELECT count(*) FROM hopper_job_leases")
}

func (fixture) CountEdges(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx]) int {
	return count(ctx, t, d, "SELECT count(*) FROM hopper_job_deps")
}

func (fixture) WaitForLockWait(_ context.Context, t *testing.T, d driver.Driver[*sql.Tx]) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); hoppersqlite.LockWaiters(d.(*hoppersqlite.Driver)) == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("no statement started waiting for the write lock")
		}
	}
}

func TestConformance(t *testing.T) {
	t.Parallel()
	drivertest.Run(t, fixture{})
}

// A client's registrations go with its row, whether it stops or the leader
// prunes its expired lease, and none are recorded for a client already gone.
func TestPeriodicRegistrationsGoWithTheirClient(t *testing.T) {
	ctx := context.Background()
	d := open(t)
	exec := d.Executor()
	regs := []driver.PeriodicRegistration{{Name: "j", Definition: json.RawMessage(`{}`), Digest: "d"}}
	register := func() int64 {
		id, err := exec.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "h", TTL: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		if err := exec.PeriodicRegister(ctx, driver.PeriodicRegisterParams{ClientID: id, Registrations: regs}); err != nil {
			t.Fatal(err)
		}
		return id
	}
	rows := func() int { return count(ctx, t, d, "SELECT count(*) FROM hopper_periodic_registrations") }

	expired, deleted := register(), register()
	fixture{}.ExpireLease(ctx, t, d, expired)
	if _, err := exec.ClientPruneExpired(ctx); err != nil {
		t.Fatal(err)
	}
	if n := rows(); n != 1 {
		t.Errorf("%d registrations after one of two clients was pruned, want 1", n)
	}
	if err := exec.ClientDelete(ctx, deleted); err != nil {
		t.Fatal(err)
	}
	if err := exec.PeriodicRegister(ctx, driver.PeriodicRegisterParams{ClientID: deleted, Registrations: regs}); err != nil {
		t.Fatal(err)
	}
	if n := rows(); n != 0 {
		t.Errorf("%d registrations after both clients were gone", n)
	}

	execSQL(ctx, t, d, `INSERT INTO hopper_periodic_registrations (client_id, name, definition, digest) VALUES (-1, 'left', '{}', 'd')`)
	register()
	if n := rows(); n != 1 {
		t.Errorf("%d registrations, want only the live client's: a gone client's survived another client's write", n)
	}
}
