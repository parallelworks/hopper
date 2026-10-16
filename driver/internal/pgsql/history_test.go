package pgsql

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// probeConn answers reattachRevalidates' statements and counts the probes
// it runs.
type probeConn struct {
	Conn
	probes *int
	execs  *[]string
}

func (c probeConn) Begin(context.Context) (Tx, error) { *c.probes++; return probeTx{c}, nil }

func (c probeConn) Exec(_ context.Context, sql string, _ ...any) (int64, error) {
	if c.execs != nil {
		*c.execs = append(*c.execs, sql)
	}
	return 0, nil
}

func (c probeConn) QueryRow(context.Context, string, ...any) Row {
	return probeRow(func(dest ...any) error {
		switch d := dest[0].(type) {
		case *sql.NullString:
			*d = sql.NullString{String: "hopper_job_history_failed_2000010100_ordering", Valid: true}
		case *bool:
			*d = true
		}
		return nil
	})
}

type probeTx struct{ probeConn }

func (probeTx) Commit(context.Context) error   { return nil }
func (probeTx) Rollback(context.Context) error { return nil }

type probeRow func(dest ...any) error

func (r probeRow) Scan(dest ...any) error { return r(dest...) }

// The server is asked again on each pass, rather than once per process,
// so that one updated in place, or a fork that backports the fix under the
// same version, gets its index repaired without a restart.
func TestReattachRevalidatesAsksEachTime(t *testing.T) {
	var probes int
	e := &Executor{Conn: probeConn{probes: &probes}, Schema: NewSchema("")}
	for range 2 {
		if ok, err := e.reattachRevalidates(context.Background(), "200ms"); err != nil || !ok {
			t.Fatalf("reattachRevalidates = %v, %v", ok, err)
		}
	}
	if probes != 2 {
		t.Errorf("%d probes for two passes, want 2", probes)
	}
}

// A probe that fails is reported, rather than leave the repair to an attach
// that queues the partition's queries behind it on a server that may not
// repair.
func TestRevalidateIndexReportsProbeFailure(t *testing.T) {
	var probes int
	var execs []string
	e := &Executor{Conn: probeConn{probes: &probes, execs: &execs}, Schema: NewSchema("")}
	failed := func() (bool, error) { return false, errors.New("probe statement rejected") }
	invalid, deferred, err := e.revalidateIndex(context.Background(), "hopper_job_history_failed_ordering", "200ms", failed)
	if err == nil || invalid || deferred {
		t.Fatalf("revalidateIndex after a failed probe = %v, %v, %v", invalid, deferred, err)
	}
	if slices.ContainsFunc(execs, func(q string) bool { return strings.Contains(q, "ATTACH PARTITION") }) {
		t.Errorf("attach after a failed probe: %q", execs)
	}
}

// A probe whose wait for another session's probe ends at the lock timeout
// defers the repair, as the attach's own lock waits do, rather than fail it.
func TestRevalidateIndexDefersHeldProbe(t *testing.T) {
	var probes int
	var execs []string
	e := &Executor{Conn: probeConn{probes: &probes, execs: &execs}, Schema: NewSchema("")}
	held := func() (bool, error) { return false, codedError("55P03") }
	invalid, deferred, err := e.revalidateIndex(context.Background(), "hopper_job_history_failed_ordering", "200ms", held)
	if err != nil || invalid || !deferred {
		t.Fatalf("revalidateIndex behind a held probe = %v, %v, %v", invalid, deferred, err)
	}
	if slices.ContainsFunc(execs, func(q string) bool { return strings.Contains(q, "ATTACH PARTITION") }) {
		t.Errorf("attach after a held probe: %q", execs)
	}
}

// codedError is a server error with a SQLSTATE.
type codedError string

func (e codedError) Error() string    { return "server error " + string(e) }
func (e codedError) SQLState() string { return string(e) }

// attachConn runs createPartition's statements, failing those that contain
// fail with code: the partition does not exist yet, and every lock is free.
type attachConn struct {
	Conn
	fail, code string
}

func (c attachConn) Begin(context.Context) (Tx, error) { return attachTx{c}, nil }

func (c attachConn) Exec(_ context.Context, sql string, _ ...any) (int64, error) {
	if strings.Contains(sql, c.fail) {
		return 0, codedError(c.code)
	}
	return 0, nil
}

func (c attachConn) QueryRow(_ context.Context, sql string, _ ...any) Row {
	return probeRow(func(dest ...any) error {
		*dest[0].(*bool) = !strings.Contains(sql, "to_regclass")
		return nil
	})
}

type attachTx struct{ attachConn }

func (attachTx) Commit(context.Context) error   { return nil }
func (attachTx) Rollback(context.Context) error { return nil }

// A statement_timeout that ends a partition's ATTACH, which scans the
// DEFAULT partition, is the attach's work running long, not a wait for its
// locks: it fails the pass rather than defer the partition to the next
// leader pass, which would scan again. One that ends a wait for the locks,
// taken before the attach, defers it, as a lock_timeout in the attach does.
func TestCreatePartitionDefersOnlyLockWaits(t *testing.T) {
	ctx := context.Background()
	g := historyGroups[1]
	from := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		fail, code string
		deferred   bool
	}{
		{"ATTACH PARTITION", "57014", false},
		{"ATTACH PARTITION", "55P03", true},
		{"LOCK TABLE ONLY", "57014", true},
		{"LOCK TABLE ONLY", "55P03", true},
	} {
		e := &Executor{Conn: attachConn{fail: c.fail, code: c.code}, Schema: NewSchema("")}
		created, deferred, err := e.createPartition(ctx, g, g.name(from), from, from.Add(g.period), "200ms")
		if created || deferred != c.deferred || (err == nil) != c.deferred {
			t.Errorf("createPartition with %s failing %s = %v, %v, %v; want deferred %v", c.fail, c.code, created, deferred, err, c.deferred)
		}
	}
}

// buildConn runs a partition's build, whose CREATE INDEX fails with code: the
// partition exists, and has no index yet.
type buildConn struct {
	Conn
	code string
}

func (c buildConn) Exec(_ context.Context, sql string, _ ...any) (int64, error) {
	if strings.Contains(sql, "CREATE INDEX") {
		return 0, codedError(c.code)
	}
	return 0, nil
}

func (buildConn) QueryRow(_ context.Context, sql string, _ ...any) Row {
	return probeRow(func(dest ...any) error {
		*dest[0].(*bool) = strings.Contains(sql, "IS NOT NULL")
		return nil
	})
}

// A build the deadlock detector cancels in favor of retention's drop of its
// partition, which has not committed when the build returns, is deferred
// like a build held off by a lock, rather than fail the pass.
func TestDeadlockedBuildIsDeferred(t *testing.T) {
	e := &Executor{Conn: buildConn{code: "40P01"}, Schema: NewSchema("")}
	x := historyIndexes[0]
	if err := e.indexHistoryPartition(context.Background(), x, x.parent+"_2000010100", "200ms"); !errors.Is(err, errBuildDeferred) {
		t.Errorf("deadlocked build = %v, want errBuildDeferred", err)
	}
	e.Conn = buildConn{code: "XX000"}
	if err := e.indexHistoryPartition(context.Background(), x, x.parent+"_2000010100", "200ms"); err == nil || errors.Is(err, errBuildDeferred) {
		t.Errorf("failed build = %v, want an error that is not errBuildDeferred", err)
	}
}

// maintainConn runs HistoryMaintain's statements against a schema whose
// failed table has one partition past retention, and fails the read of its
// indexes. It records the statements it runs.
type maintainConn struct {
	Conn
	now   time.Time
	execs *[]string
}

func (c maintainConn) Begin(context.Context) (Tx, error) { return maintainTx{c}, nil }

func (c maintainConn) Exec(_ context.Context, sql string, _ ...any) (int64, error) {
	*c.execs = append(*c.execs, sql)
	return 0, nil
}

func (c maintainConn) QueryRow(_ context.Context, sql string, _ ...any) Row {
	return probeRow(func(dest ...any) error {
		switch d := dest[0].(type) {
		case *time.Time:
			*d = c.now
		case *bool:
			*d = !strings.Contains(sql, "to_regclass")
		}
		return nil
	})
}

func (c maintainConn) Query(_ context.Context, sql string, args ...any) (Rows, error) {
	switch {
	case strings.Contains(sql, "NOT x.indisvalid"):
		return nil, codedError("57014")
	case strings.Contains(sql, "pg_inherits") && args[0] == NewSchema("").Table("hopper_job_history_failed"):
		return &stringRows{rows: []string{"hopper_job_history_failed_20000101"}}, nil
	}
	return &stringRows{}, nil
}

type maintainTx struct{ maintainConn }

func (maintainTx) Commit(context.Context) error   { return nil }
func (maintainTx) Rollback(context.Context) error { return nil }

type stringRows struct {
	rows []string
	at   int
}

func (r *stringRows) Next() bool { r.at++; return r.at <= len(r.rows) }

func (r *stringRows) Scan(dest ...any) error { *dest[0].(*string) = r.rows[r.at-1]; return nil }

func (*stringRows) Err() error { return nil }

func (*stringRows) Close() {}

// A failed read of a group's indexes, which retention needs before it drops a
// partition, leaves that partition to the next pass, but neither the group's
// DEFAULT prune nor the groups after it: their partitions ahead are created.
func TestHistoryMaintainGoesPastIndexReadError(t *testing.T) {
	var execs []string
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	e := &Executor{Conn: maintainConn{now: now, execs: &execs}, Schema: NewSchema("")}
	res, err := e.HistoryMaintain(context.Background(), driver.HistoryMaintainParams{FailedRetention: time.Hour})
	if sqlState(err) != "57014" {
		t.Errorf("HistoryMaintain error = %v, want the index read's", err)
	}
	if len(res.Dropped) != 0 || slices.ContainsFunc(execs, func(q string) bool { return strings.Contains(q, "DETACH") || strings.Contains(q, "DROP TABLE") }) {
		t.Errorf("a partition was dropped without reading its indexes: %+v", res)
	}
	if !slices.ContainsFunc(execs, func(q string) bool {
		return strings.Contains(q, "DELETE FROM") && strings.Contains(q, "hopper_job_history_failed_default")
	}) {
		t.Error("the failed table's DEFAULT partition was not pruned")
	}
	if !slices.Contains(res.Created, historyGroups[2].name(now.Truncate(historyGroups[2].period).Add(historyGroups[2].period))) {
		t.Errorf("the stream group's partitions ahead were not created: %+v", res)
	}
}
