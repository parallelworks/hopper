// Package hopperpgx is hopper's reference driver: PostgreSQL through pgx v5.
//
//	client, err := hopper.NewClient(hopperpgx.New(pool), cfg)
//
// The driver is generic over pgx.Tx, so InsertTx accepts only a pgx
// transaction. Tables live in the first schema of the connection's
// search_path; set it on the pool to isolate hopper in its own schema.
//
// The SQL and the logic around it are shared with hoppersql; this package
// adds what pgx alone offers: COPY for bulk inserts, LISTEN, and pipelined
// statements.
package hopperpgx

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/internal/pgsql"
)

// Driver implements driver.Driver[pgx.Tx] on a pgxpool.Pool.
type Driver struct {
	pool *pgxpool.Pool
	cfg  Config
}

// Config tunes the driver. The zero value is fine for a pool that connects
// directly to Postgres.
type Config struct {
	// ListenConnConfig is used for the LISTEN connection instead of the
	// pool's connection settings. Set it to a direct Postgres address when
	// the pool goes through a transaction pooler such as PgBouncer, which
	// cannot carry LISTEN.
	ListenConnConfig *pgx.ConnConfig
}

var _ driver.Driver[pgx.Tx] = (*Driver)(nil)

// New returns a driver on pool. The pool is shared with the application; the
// driver does not close it.
func New(pool *pgxpool.Pool) *Driver {
	return NewWithConfig(pool, nil)
}

// NewWithConfig is New with driver settings.
func NewWithConfig(pool *pgxpool.Pool, cfg *Config) *Driver {
	if pool == nil {
		panic("hopperpgx: nil pool")
	}
	d := &Driver{pool: pool}
	if cfg != nil {
		d.cfg = *cfg
	}
	return d
}

// Pool returns the underlying pool.
func (d *Driver) Pool() *pgxpool.Pool { return d.pool }

// Executor implements driver.Driver.
func (d *Driver) Executor() driver.Executor {
	return &executor{Executor: &pgsql.Executor{Conn: conn{db: d.pool}}, pool: d.pool}
}

// UnwrapTx implements driver.Driver.
func (d *Driver) UnwrapTx(tx pgx.Tx) driver.Executor {
	return &executor{Executor: &pgsql.Executor{Conn: conn{db: tx}, InTx: true}, tx: tx}
}

// Capabilities implements driver.Driver.
func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{Copy: true, Listen: true}
}

// Listener implements driver.Driver. It opens a dedicated connection.
func (d *Driver) Listener(ctx context.Context) (driver.Listener, error) {
	cfg := d.cfg.ListenConnConfig
	if cfg == nil {
		cfg = d.pool.Config().ConnConfig
	}
	c, err := pgx.ConnectConfig(ctx, cfg.Copy())
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: connect listener: %w", err)
	}
	// Name the connection so operators (and tests) can tell it apart in
	// pg_stat_activity: "hopper-listener:<schema>".
	if _, err := c.Exec(ctx, "SELECT set_config('application_name', 'hopper-listener:' || current_schema(), false)"); err != nil {
		return nil, errors.Join(fmt.Errorf("hopperpgx: name listener: %w", err), c.Close(context.WithoutCancel(ctx)))
	}
	return &listener{conn: c}, nil
}

type listener struct {
	conn *pgx.Conn
}

func (l *listener) Listen(ctx context.Context, channels ...string) error {
	for _, ch := range channels {
		if _, err := l.conn.Exec(ctx, "LISTEN "+pgx.Identifier{ch}.Sanitize()); err != nil {
			return fmt.Errorf("hopperpgx: listen %s: %w", ch, err)
		}
	}
	return nil
}

func (l *listener) Next(ctx context.Context) (driver.Notification, error) {
	n, err := l.conn.WaitForNotification(ctx)
	if err != nil {
		return driver.Notification{}, err
	}
	return driver.Notification{Channel: n.Channel, Payload: n.Payload}, nil
}

func (l *listener) Close(ctx context.Context) error {
	return l.conn.Close(ctx)
}

// Migrator implements driver.Driver.
func (d *Driver) Migrator() driver.Migrator {
	return &migrator{pool: d.pool}
}

// executor is the shared implementation plus the COPY path, which needs
// the pool or the transaction itself.
type executor struct {
	*pgsql.Executor
	// Exactly one of pool and tx is set.
	pool *pgxpool.Pool
	tx   pgx.Tx
}

var _ driver.Executor = (*executor)(nil)

// dbtx is the subset of pgx shared by *pgxpool.Pool, *pgxpool.Conn,
// *pgx.Conn and pgx.Tx.
type dbtx interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	Begin(ctx context.Context) (pgx.Tx, error)
}

// conn adapts a dbtx to pgsql.Conn.
type conn struct {
	db dbtx
}

func (c conn) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	tag, err := c.db.Exec(ctx, sql, args...)
	if err != nil {
		return 0, wrapErr(err)
	}
	return tag.RowsAffected(), nil
}

func (c conn) Query(ctx context.Context, sql string, args ...any) (pgsql.Rows, error) {
	rows, err := c.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, wrapErr(err)
	}
	return pgxRows{rows}, nil
}

func (c conn) QueryRow(ctx context.Context, sql string, args ...any) pgsql.Row {
	return pgxRow{c.db.QueryRow(ctx, sql, args...)}
}

// QueryExec pipelines both statements in one round trip. Outside an
// explicit transaction a batch runs in an implicit one, so the statement's
// failure also undoes the query; the rows report it from Err once they are
// exhausted.
func (c conn) QueryExec(ctx context.Context, query string, queryArgs []any, exec string, execArgs []any) (pgsql.Rows, error) {
	b := &pgx.Batch{}
	b.Queue(query, queryArgs...)
	b.Queue(exec, execArgs...)
	br := c.db.SendBatch(ctx, b)
	rows, err := br.Query()
	if err != nil {
		return nil, errors.Join(wrapErr(err), br.Close())
	}
	return &batchRows{Rows: rows, br: br}, nil
}

func (c conn) Begin(ctx context.Context) (pgsql.Tx, error) {
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return nil, wrapErr(err)
	}
	return pgxTx{conn: conn{db: tx}, tx: tx}, nil
}

// pgxTx is a transaction, or a savepoint inside one.
type pgxTx struct {
	conn
	tx pgx.Tx
}

func (t pgxTx) Commit(ctx context.Context) error { return wrapErr(t.tx.Commit(ctx)) }

func (t pgxTx) Rollback(ctx context.Context) error {
	err := t.tx.Rollback(ctx)
	if errors.Is(err, pgx.ErrTxClosed) {
		return nil
	}
	return wrapErr(err)
}

// pgxRows adapts pgx.Rows.
type pgxRows struct {
	pgx.Rows
}

func (r pgxRows) Scan(dest ...any) error { return wrapErr(r.Rows.Scan(dest...)) }
func (r pgxRows) Err() error             { return wrapErr(r.Rows.Err()) }

// batchRows is the query result of a batch, which finishes the batch (the
// trailing statement) once the rows are consumed or closed.
type batchRows struct {
	pgx.Rows
	br   pgx.BatchResults
	err  error
	done bool
}

func (r *batchRows) Next() bool {
	if r.done {
		return false
	}
	if r.Rows.Next() {
		return true
	}
	r.finish()
	return false
}

func (r *batchRows) Scan(dest ...any) error { return wrapErr(r.Rows.Scan(dest...)) }

func (r *batchRows) Err() error {
	if !r.done {
		return wrapErr(r.Rows.Err())
	}
	return r.err
}

func (r *batchRows) Close() { r.finish() }

func (r *batchRows) finish() {
	if r.done {
		return
	}
	r.done = true
	r.Rows.Close()
	r.err = wrapErr(r.Rows.Err())
	if _, err := r.br.Exec(); err != nil && r.err == nil {
		r.err = wrapErr(err)
	}
	if err := r.br.Close(); err != nil && r.err == nil {
		r.err = wrapErr(err)
	}
}

type pgxRow struct {
	row pgx.Row
}

func (r pgxRow) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	if errors.Is(err, pgx.ErrNoRows) {
		return pgsql.ErrNoRows
	}
	return wrapErr(err)
}

// sqlStateError lets the shared code recognize error classes without
// knowing pgx.
type sqlStateError struct {
	err   error
	state string
}

func (e *sqlStateError) Error() string         { return e.err.Error() }
func (e *sqlStateError) Unwrap() error         { return e.err }
func (e *sqlStateError) SQLState() string      { return e.state }
func (e *sqlStateError) UniqueViolation() bool { return e.state == pgerrUniqueViolation }

const pgerrUniqueViolation = "23505"

func wrapErr(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return &sqlStateError{err: err, state: pgErr.Code}
	}
	return err
}

type migrator struct {
	pool *pgxpool.Pool
}

// Lock implements driver.Migrator. The lock is taken on a dedicated
// connection, outside the pool, so processes waiting for the lock cannot
// starve the migration of pool connections.
func (m *migrator) Lock(ctx context.Context) (driver.MigrationExecutor, error) {
	c, err := pgx.ConnectConfig(ctx, m.pool.Config().ConnConfig.Copy())
	if err != nil {
		return nil, fmt.Errorf("connect for migration lock: %w", err)
	}
	if _, err := c.Exec(ctx, "SELECT pg_advisory_lock($1)", pgsql.MigrationLockKey); err != nil {
		return nil, errors.Join(fmt.Errorf("acquire migration lock: %w", err), c.Close(context.WithoutCancel(ctx)))
	}
	return &migrationExecutor{conn: c}, nil
}

type migrationExecutor struct {
	conn *pgx.Conn
}

func (m *migrationExecutor) Versions(ctx context.Context) ([]int, error) {
	return pgsql.MigrationVersions(ctx, conn{db: m.conn})
}

func (m *migrationExecutor) Apply(ctx context.Context, version int, script string, up bool) error {
	return pgsql.MigrationApply(ctx, conn{db: m.conn}, version, script, up)
}

func (m *migrationExecutor) Close(ctx context.Context) error {
	_, err := m.conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", pgsql.MigrationLockKey)
	return errors.Join(err, m.conn.Close(ctx))
}
