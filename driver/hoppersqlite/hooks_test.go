package hoppersqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	_ "modernc.org/sqlite" // the "sqlite" driver, unwrapped, to compare with

	"github.com/parallelworks/hopper/driver/hoppersqlite"
)

// hookCase is a statement run in a transaction after an insert, on a
// table made by table; full limits the database's size first, so that the
// statement fills it.
type hookCase struct {
	name, table, stmt string
	full              bool
}

// rawEndsTransaction runs c on modernc's own driver and reports whether
// SQLite still had the transaction open after the statement.
func rawEndsTransaction(t *testing.T, c hookCase) bool {
	t.Helper()
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "raw.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	exec := func(q string) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec(c.table)
	var pages int
	if err := conn.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pages); err != nil {
		t.Fatal(err)
	}
	exec("BEGIN IMMEDIATE")
	exec("INSERT INTO t VALUES (1)")
	if c.full {
		exec("PRAGMA max_page_count = " + strconv.Itoa(pages+3))
	}
	_, _ = conn.ExecContext(ctx, c.stmt)
	_, err = conn.ExecContext(ctx, "ROLLBACK")
	if err != nil && !strings.Contains(err.Error(), "no transaction is active") {
		t.Fatalf("ROLLBACK: %v", err)
	}
	return err != nil
}

// TestHooksMatchSQLite runs statements that end a transaction, or leave it
// open, in one of the driver's transactions and on modernc's own
// connection: the driver refuses the transaction's next statement exactly
// when SQLite ended the transaction, with ErrTransactionAborted after an
// error and sql.ErrTxDone after a ROLLBACK, COMMIT or END, and nothing the
// refused statement would have written is left.
func TestHooksMatchSQLite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	big := "INSERT INTO t VALUES (randomblob(200000))"
	rows := "INSERT INTO t SELECT randomblob(3000) FROM (WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 200) SELECT x FROM c)"
	trigger := func(action string) string {
		return "CREATE TABLE t (x); CREATE TRIGGER tr BEFORE INSERT ON t WHEN NEW.x = 9 BEGIN SELECT RAISE(" + action + ", 'no'); END"
	}
	for _, c := range []hookCase{
		{"a full disk, one row", "CREATE TABLE t (x)", big, true},
		{"a full disk, many rows", "CREATE TABLE t (x)", rows, true},
		{"a full disk, many rows, a CHECK", "CREATE TABLE t (x CHECK (x IS NOT NULL))", rows, true},
		{"a full disk, an UPDATE", "CREATE TABLE t (x)", "UPDATE t SET x = randomblob(300000)", true},
		{"INSERT OR ROLLBACK", "CREATE TABLE t (x UNIQUE)", "INSERT OR ROLLBACK INTO t VALUES (1)", false},
		{"RAISE(ROLLBACK)", trigger("ROLLBACK"), "INSERT INTO t VALUES (9)", false},
		{"RAISE(ABORT)", trigger("ABORT"), "INSERT INTO t VALUES (9)", false},
		{"RAISE(FAIL)", trigger("FAIL"), "INSERT INTO t VALUES (9)", false},
		{"a unique constraint", "CREATE TABLE t (x UNIQUE)", "INSERT INTO t VALUES (1)", false},
		{"a savepoint rolled back to and released", "CREATE TABLE t (x)", "SAVEPOINT a; INSERT INTO t VALUES (5); ROLLBACK TO a; RELEASE a", false},
		{"ROLLBACK TO no savepoint", "CREATE TABLE t (x)", "ROLLBACK TO nosuch", false},
		{"ROLLBACK", "CREATE TABLE t (x)", "ROLLBACK", false},
		{"COMMIT", "CREATE TABLE t (x)", "COMMIT", false},
		{"END", "CREATE TABLE t (x)", "END", false},
	} {
		ended := rawEndsTransaction(t, c)
		d := open(t)
		db := d.DB()
		if _, err := db.ExecContext(ctx, c.table+"; CREATE TABLE side (x)"); err != nil {
			t.Fatal(err)
		}
		var pages int
		if err := db.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pages); err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO t VALUES (1)"); err != nil {
			t.Fatal(err)
		}
		if c.full {
			if _, err := tx.ExecContext(ctx, "PRAGMA max_page_count = "+strconv.Itoa(pages+3)); err != nil {
				t.Fatal(err)
			}
		}
		stmtErr := func() error { _, err := tx.ExecContext(ctx, c.stmt); return err }()
		_, err = tx.ExecContext(ctx, "INSERT INTO side VALUES (1)")
		aborted, done := errors.Is(err, hoppersqlite.ErrTransactionAborted), errors.Is(err, sql.ErrTxDone)
		switch {
		case ended && stmtErr != nil && !aborted:
			t.Errorf("%s: SQLite rolled the transaction back after %v; the next statement = %v, want ErrTransactionAborted", c.name, stmtErr, err)
		case ended && stmtErr == nil && !done:
			t.Errorf("%s: the statement ended the transaction; the next statement = %v, want sql.ErrTxDone", c.name, err)
		case !ended && err != nil:
			t.Errorf("%s: SQLite kept the transaction open; the next statement = %v", c.name, err)
		}
		if c.full {
			_, _ = tx.ExecContext(ctx, "PRAGMA max_page_count = 1073741823")
		}
		_ = tx.Rollback()
		var side int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM side").Scan(&side); err != nil || side != 0 {
			t.Errorf("%s: %d rows of side left after Rollback, %v", c.name, side, err)
		}
	}
}

// TestStatementEndsTransaction ends a transaction with a statement of its
// own: after COMMIT, what it wrote is committed and Commit returns nil;
// after ROLLBACK, nothing is and Commit returns sql.ErrTxDone. Either way
// the write lock is free once the transaction ends.
func TestStatementEndsTransaction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d := open(t)
	db := d.DB()
	if _, err := db.ExecContext(ctx, "CREATE TABLE t (x)"); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		stmt string
		rows int
		err  error
	}{{"COMMIT", 1, nil}, {"ROLLBACK", 0, sql.ErrTxDone}} {
		if _, err := db.ExecContext(ctx, "DELETE FROM t"); err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO t VALUES (1)"); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, c.stmt); err != nil {
			t.Fatalf("%s: %v", c.stmt, err)
		}
		if err := tx.Commit(); !errors.Is(err, c.err) || (c.err == nil && err != nil) {
			t.Errorf("Commit after %s = %v, want %v", c.stmt, err, c.err)
		}
		var n int
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&n); err != nil || n != c.rows {
			t.Errorf("after %s: %d rows, %v; want %d", c.stmt, n, err, c.rows)
		}
	}
}
