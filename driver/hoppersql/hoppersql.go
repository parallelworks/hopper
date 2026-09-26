// Package hoppersql is hopper's PostgreSQL driver for database/sql, for
// applications that already use it with lib/pq or pgx's stdlib adapter.
//
//	db, err := sql.Open("pgx", url)
//	client, err := hopper.NewClient(hoppersql.New(db), cfg)
//
// It shares its SQL with hopperpgx and passes the same conformance suite.
// What database/sql cannot do, it does without: there is no COPY path (bulk
// inserts use the single-statement path) and no LISTEN, so clients poll at
// Config.PollInterval and the driver reports it once. Use hopperpgx for
// lowest latency.
package hoppersql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/internal/pgsql"
)

// Driver implements driver.Driver[*sql.Tx] on a *sql.DB.
type Driver struct {
	db *sql.DB
}

var _ driver.Driver[*sql.Tx] = (*Driver)(nil)

// New returns a driver on db. The database handle is shared with the
// application; the driver does not close it.
func New(db *sql.DB) *Driver {
	if db == nil {
		panic("hoppersql: nil db")
	}
	return &Driver{db: db}
}

// DB returns the underlying database handle.
func (d *Driver) DB() *sql.DB { return d.db }

// Executor implements driver.Driver.
func (d *Driver) Executor() driver.Executor {
	return &pgsql.Executor{Conn: &dbConn{base: base{q: d.db}, db: d.db}}
}

// UnwrapTx implements driver.Driver.
func (d *Driver) UnwrapTx(tx *sql.Tx) driver.Executor {
	return &pgsql.Executor{Conn: &txConn{base: base{q: tx}, tx: tx}, InTx: true}
}

// Capabilities implements driver.Driver.
func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{}
}

// Listener implements driver.Driver: database/sql has no LISTEN.
func (d *Driver) Listener(context.Context) (driver.Listener, error) {
	return nil, driver.ErrNotSupported
}

// Migrator implements driver.Driver.
func (d *Driver) Migrator() driver.Migrator {
	return &migrator{db: d.db}
}

// queryer is what *sql.DB, *sql.Tx and *sql.Conn share.
type queryer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// base implements the statement methods of pgsql.Conn over a queryer.
type base struct {
	q queryer
}

// literals renders slice parameters as array literals: database/sql
// drivers cannot encode slices, and a text literal works with any of them.
func literals(args []any) []any {
	for i, a := range args {
		if s, ok := pgsql.ArrayLiteral(a); ok {
			args[i] = s
		}
	}
	return args
}

func (b *base) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := b.q.ExecContext(ctx, query, literals(args)...)
	if err != nil {
		return 0, wrapErr(err)
	}
	n, err := res.RowsAffected()
	return n, wrapErr(err)
}

func (b *base) Query(ctx context.Context, query string, args ...any) (pgsql.Rows, error) {
	rows, err := b.q.QueryContext(ctx, query, literals(args)...) //nolint:rowserrcheck // the caller iterates and checks Err
	if err != nil {
		return nil, wrapErr(err)
	}
	return sqlRows{rows}, nil
}

func (b *base) QueryRow(ctx context.Context, query string, args ...any) pgsql.Row {
	return sqlRow{b.q.QueryRowContext(ctx, query, literals(args)...)}
}

// dbConn is the pool-scoped connection.
type dbConn struct {
	base
	db *sql.DB
}

func (c *dbConn) Begin(ctx context.Context) (pgsql.Tx, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, wrapErr(err)
	}
	return &txConn{base: base{q: tx}, tx: tx}, nil
}

// QueryExec runs both statements in a transaction: database/sql cannot
// pipeline, and it cannot run a second statement on a transaction while a
// result set is open, so the query's rows are buffered first.
func (c *dbConn) QueryExec(ctx context.Context, query string, queryArgs []any, exec string, execArgs []any) (pgsql.Rows, error) {
	tx, err := c.Begin(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryExec(ctx, query, queryArgs, exec, execArgs)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback(context.WithoutCancel(ctx)))
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return rows, nil
}

// txConn is a transaction, or a savepoint inside one.
type txConn struct {
	base
	tx        *sql.Tx
	savepoint string
}

var savepoints atomic.Int64

func (c *txConn) Begin(ctx context.Context) (pgsql.Tx, error) {
	name := fmt.Sprintf("hopper_sp_%d", savepoints.Add(1))
	if _, err := c.tx.ExecContext(ctx, "SAVEPOINT "+name); err != nil {
		return nil, wrapErr(err)
	}
	return &txConn{base: base{q: c.tx}, tx: c.tx, savepoint: name}, nil
}

func (c *txConn) Commit(ctx context.Context) error {
	if c.savepoint != "" {
		_, err := c.tx.ExecContext(ctx, "RELEASE SAVEPOINT "+c.savepoint)
		return wrapErr(err)
	}
	return wrapErr(c.tx.Commit())
}

func (c *txConn) Rollback(ctx context.Context) error {
	if c.savepoint != "" {
		_, err := c.tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+c.savepoint)
		return wrapErr(err)
	}
	err := c.tx.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return wrapErr(err)
}

func (c *txConn) QueryExec(ctx context.Context, query string, queryArgs []any, exec string, execArgs []any) (pgsql.Rows, error) {
	rows, err := c.tx.QueryContext(ctx, query, literals(queryArgs)...)
	if err != nil {
		return nil, wrapErr(err)
	}
	buffered, err := bufferRows(rows)
	if err != nil {
		return nil, wrapErr(err)
	}
	if _, err := c.tx.ExecContext(ctx, exec, literals(execArgs)...); err != nil {
		return nil, wrapErr(err)
	}
	return buffered, nil
}

// sqlRows adapts *sql.Rows.
type sqlRows struct {
	*sql.Rows
}

func (r sqlRows) Scan(dest ...any) error { return wrapErr(r.Rows.Scan(dest...)) }
func (r sqlRows) Err() error             { return wrapErr(r.Rows.Err()) }
func (r sqlRows) Close()                 { _ = r.Rows.Close() }

type sqlRow struct {
	row *sql.Row
}

func (r sqlRow) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	if errors.Is(err, sql.ErrNoRows) {
		return pgsql.ErrNoRows
	}
	return wrapErr(err)
}

// sqlStateError lets the shared code recognize error classes. Both pgx's
// and lib/pq's errors report their SQLSTATE.
type sqlStateError struct {
	err   error
	state string
}

func (e *sqlStateError) Error() string         { return e.err.Error() }
func (e *sqlStateError) Unwrap() error         { return e.err }
func (e *sqlStateError) SQLState() string      { return e.state }
func (e *sqlStateError) UniqueViolation() bool { return e.state == "23505" }

func wrapErr(err error) error {
	var coded interface{ SQLState() string }
	if errors.As(err, &coded) {
		return &sqlStateError{err: err, state: coded.SQLState()}
	}
	return err
}

type migrator struct {
	db *sql.DB
}

// Lock implements driver.Migrator. The lock is held on a connection
// reserved from the pool for the duration of the migration.
func (m *migrator) Lock(ctx context.Context) (driver.MigrationExecutor, error) {
	c, err := m.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("hoppersql: connection for migration lock: %w", err)
	}
	if _, err := c.ExecContext(ctx, "SELECT pg_advisory_lock($1)", pgsql.MigrationLockKey); err != nil {
		return nil, errors.Join(fmt.Errorf("hoppersql: acquire migration lock: %w", err), c.Close())
	}
	return &migrationExecutor{conn: c}, nil
}

type migrationExecutor struct {
	conn *sql.Conn
}

// connConn adapts a dedicated *sql.Conn to pgsql.Conn.
type connConn struct {
	base
	c *sql.Conn
}

func (c *connConn) Begin(ctx context.Context) (pgsql.Tx, error) {
	tx, err := c.c.BeginTx(ctx, nil)
	if err != nil {
		return nil, wrapErr(err)
	}
	return &txConn{base: base{q: tx}, tx: tx}, nil
}

func (c *connConn) QueryExec(ctx context.Context, query string, queryArgs []any, exec string, execArgs []any) (pgsql.Rows, error) {
	tx, err := c.Begin(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryExec(ctx, query, queryArgs, exec, execArgs)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback(context.WithoutCancel(ctx)))
	}
	return rows, tx.Commit(ctx)
}

func (m *migrationExecutor) Versions(ctx context.Context) ([]int, error) {
	return pgsql.MigrationVersions(ctx, &connConn{base: base{q: m.conn}, c: m.conn})
}

func (m *migrationExecutor) Apply(ctx context.Context, version int, script string, up bool) error {
	return pgsql.MigrationApply(ctx, &connConn{base: base{q: m.conn}, c: m.conn}, version, script, up)
}

func (m *migrationExecutor) Close(ctx context.Context) error {
	_, err := m.conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", pgsql.MigrationLockKey)
	return errors.Join(err, m.conn.Close())
}
