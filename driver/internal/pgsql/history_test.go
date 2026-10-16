package pgsql

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
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
