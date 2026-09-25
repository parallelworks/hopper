// Package hopperpgx is hopper's reference driver: PostgreSQL through pgx v5.
//
//	client, err := hopper.NewClient(hopperpgx.New(pool), cfg)
//
// The driver is generic over pgx.Tx, so InsertTx accepts only a pgx
// transaction. Tables live in the first schema of the connection's
// search_path; set it on the pool to isolate hopper in its own schema.
package hopperpgx

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/parallelworks/hopper/driver"
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
	return &executor{db: d.pool, pool: d.pool}
}

// UnwrapTx implements driver.Driver.
func (d *Driver) UnwrapTx(tx pgx.Tx) driver.Executor {
	return &executor{db: tx, tx: tx}
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
	conn, err := pgx.ConnectConfig(ctx, cfg.Copy())
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: connect listener: %w", err)
	}
	// Name the connection so operators (and tests) can tell it apart in
	// pg_stat_activity: "hopper-listener:<schema>".
	if _, err := conn.Exec(ctx, "SELECT set_config('application_name', 'hopper-listener:' || current_schema(), false)"); err != nil {
		return nil, errors.Join(fmt.Errorf("hopperpgx: name listener: %w", err), conn.Close(context.WithoutCancel(ctx)))
	}
	return &listener{conn: conn}, nil
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

// dbtx is the subset of pgx shared by *pgxpool.Pool and pgx.Tx.
type dbtx interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// executor runs operations on either the pool or a transaction.
type executor struct {
	db dbtx
	// Exactly one of pool and tx is set.
	pool *pgxpool.Pool
	tx   pgx.Tx
}

var _ driver.Executor = (*executor)(nil)

// migrationLockKey is the pg_advisory_lock key held while migrating.
const migrationLockKey int64 = 0x686f707065725f6d // "hopper_m"

type migrator struct {
	pool *pgxpool.Pool
}

// Lock implements driver.Migrator. The lock is taken on a dedicated
// connection, outside the pool, so processes waiting for the lock cannot
// starve the migration of pool connections.
func (m *migrator) Lock(ctx context.Context) (driver.MigrationExecutor, error) {
	conn, err := pgx.ConnectConfig(ctx, m.pool.Config().ConnConfig.Copy())
	if err != nil {
		return nil, fmt.Errorf("connect for migration lock: %w", err)
	}
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		return nil, errors.Join(fmt.Errorf("acquire migration lock: %w", err), conn.Close(context.WithoutCancel(ctx)))
	}
	return &migrationExecutor{conn: conn}, nil
}

type migrationExecutor struct {
	conn *pgx.Conn
}

func (m *migrationExecutor) Versions(ctx context.Context) ([]int, error) {
	var exists bool
	if err := m.conn.QueryRow(ctx, "SELECT to_regclass('hopper_schema') IS NOT NULL").Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	rows, err := m.conn.Query(ctx, "SELECT version FROM hopper_schema ORDER BY version")
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int])
}

func (m *migrationExecutor) Apply(ctx context.Context, version int, script string, up bool) (err error) {
	tx, err := m.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback(context.WithoutCancel(ctx)))
		}
	}()
	if up {
		// Exec without arguments uses the simple protocol, which allows a
		// multi-statement script.
		if _, err := tx.Exec(ctx, script); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO hopper_schema (version) VALUES ($1)", version); err != nil {
			return err
		}
	} else {
		// The version row goes first: the down script of version 1 drops
		// hopper_schema itself.
		if _, err := tx.Exec(ctx, "DELETE FROM hopper_schema WHERE version = $1", version); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, script); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (m *migrationExecutor) Close(ctx context.Context) error {
	_, err := m.conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", migrationLockKey)
	return errors.Join(err, m.conn.Close(ctx))
}
