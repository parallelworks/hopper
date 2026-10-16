package pgsql

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"testing"
)

// probeConn answers reattachRevalidates' statements and counts the probes
// it runs.
type probeConn struct {
	Conn
	version string
	probes  *int
	execs   *[]string
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
		case *string:
			*d = c.version
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

func TestReattachRevalidatesOncePerBuild(t *testing.T) {
	ctx := context.Background()
	versions := []string{"PostgreSQL 18.4 probe-test-a", "PostgreSQL 18.4 probe-test-a", "PostgreSQL 18.5 probe-test-a"}
	// The answers outlive the test in the package's cache, so a run before
	// this one in the same process (go test -count) would have kept them.
	for _, version := range versions {
		revalidates.Delete(version)
	}
	var probes int
	for _, version := range versions {
		e := &Executor{Conn: probeConn{version: version, probes: &probes}, Schema: NewSchema("")}
		if ok, err := e.reattachRevalidates(ctx); err != nil || !ok {
			t.Fatalf("reattachRevalidates on %s = %v, %v", version, ok, err)
		}
	}
	if probes != 2 {
		t.Errorf("%d probes for two server builds, want 2", probes)
	}
}

// A probe that fails, for any reason, leaves the repair to the attach
// itself, rather than end the pass before the indexes after this one.
func TestRevalidateIndexAttachesWhenProbeFails(t *testing.T) {
	var probes int
	var execs []string
	e := &Executor{Conn: probeConn{probes: &probes, execs: &execs}, Schema: NewSchema("")}
	failed := func() (bool, error) { return false, errors.New("probe statement rejected") }
	invalid, deferred, err := e.revalidateIndex(context.Background(), "hopper_job_history_failed_ordering", "200ms", failed)
	if err != nil || invalid || deferred {
		t.Fatalf("revalidateIndex after a failed probe = %v, %v, %v", invalid, deferred, err)
	}
	if !slices.ContainsFunc(execs, func(q string) bool { return strings.Contains(q, "ATTACH PARTITION") }) {
		t.Errorf("no attach after a failed probe: %q", execs)
	}
}
