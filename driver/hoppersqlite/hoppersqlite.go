// Package hoppersqlite is hopper's SQLite driver, for an application that
// runs on one machine and wants hopper without a database server: a
// command-line tool that runs a workflow, a single-node service, local
// development. It is pure Go (modernc.org/sqlite, SQLite transpiled to Go),
// so a program that uses it builds with CGO_ENABLED=0.
//
//	d, err := hoppersqlite.Open("/var/lib/app/hopper.db", nil)
//	if _, err := hoppermigrate.Up(ctx, d, nil); err != nil { ... }
//	client, err := hopper.NewClient(d, cfg)
//
// It passes the drivertest conformance suite, as the Postgres drivers do,
// and several processes may share one database. What SQLite's design makes
// different from Postgres:
//
//   - One transaction writes at a time (driver.Capabilities.SingleWriter).
//     A transaction that writes holds the database's write lock until it
//     ends, and every other write waits for it: within this process in
//     turn, for as long as its context allows, and across processes
//     through SQLite's busy handler. A claim is exclusive without SKIP
//     LOCKED, and the transaction that finalizes a job settles its batch
//     and promotes its dependents itself. A finalize with SkipLocked, or a
//     progress write with NoWait, waits 100ms for a write lock held
//     elsewhere, and then skips every job, or returns driver.ErrJobLocked.
//   - Transactions come from DB, whose connections take the write lock
//     when a transaction runs its first statement, not when it begins, so
//     that what it reads is never stale by the time it writes, and so that
//     beginning one never blocks. A transaction begun read-only takes no
//     lock and may not write. Within a transaction, write through it
//     (InsertTx and the like), not through the client: a write on the pool
//     would wait for the transaction's own lock.
//   - The database must be on a local filesystem: Open refuses one on a
//     network filesystem with ErrNetworkFilesystem. Its clock is therefore
//     the host's, which every process that opens it shares, and the driver
//     reads it as database time.
//   - Notifications are rows of a table, delivered on commit to listeners
//     in every process that opens the database: at once in this process,
//     and within ListenInterval in others.
//   - History is one table, and retention deletes its rows in batches; a
//     stream's events commit in the order they are appended, so a
//     position is an event's sequence number (Xid is always 0).
//   - Args, metadata and other JSON values are stored as given, not
//     normalized as Postgres's jsonb normalizes them. A metadata filter
//     matches as jsonb's @> does, and is refused for what jsonb refuses.
//   - There is no COPY: bulk inserts go through the one insert path.
package hoppersqlite

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// Config tunes the SQLite driver. A nil *Config means the defaults.
type Config struct {
	// Synchronous is SQLite's synchronous setting for the database's
	// connections: "FULL" (the default), which makes every commit durable
	// before it returns, as Postgres's default does, or "NORMAL", with which
	// a power failure, not a crash of the process, can lose the last commits
	// before a checkpoint, and commits take less time.
	Synchronous string
	// ListenInterval is how often a listener looks for notifications
	// committed by other processes. Those of this process wake it at once.
	// Zero means 50ms.
	ListenInterval time.Duration
}

// DefaultListenInterval is ListenInterval's default.
const DefaultListenInterval = 50 * time.Millisecond

// lockWaitBrief is how long a finalize with SkipLocked, a progress write
// with NoWait, and the leader's maintenance wait for a write lock another
// transaction holds before they give up: long enough for a statement of
// another client to end, short enough not to hold the caller up behind a
// transaction that stays open.
const lockWaitBrief = 100 * time.Millisecond

// Driver implements driver.Driver[*sql.Tx] on a SQLite database.
type Driver struct {
	sqlDB          *sql.DB
	db             *database
	listenInterval time.Duration
	statements     statements
}

var _ driver.Driver[*sql.Tx] = (*Driver)(nil)

// Open opens the SQLite database at path, creating it if needed, in
// write-ahead log mode. It returns ErrNetworkFilesystem for a path on a
// network filesystem. Close the Driver to close the database.
func Open(path string, cfg *Config) (*Driver, error) {
	var c Config
	if cfg != nil {
		c = *cfg
	}
	sync := strings.ToUpper(c.Synchronous)
	switch sync {
	case "":
		sync = "FULL"
	case "FULL", "NORMAL":
	default:
		return nil, fmt.Errorf("hoppersqlite: Synchronous is %q, not FULL or NORMAL", c.Synchronous)
	}
	if c.ListenInterval < 0 {
		return nil, errors.New("hoppersqlite: ListenInterval is negative")
	}
	if c.ListenInterval == 0 {
		c.ListenInterval = DefaultListenInterval
	}
	if path == "" || path == ":memory:" || strings.HasPrefix(path, "file:") {
		return nil, fmt.Errorf("hoppersqlite: %q is not a file path; the database must be a file, which every connection and process opens", path)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("hoppersqlite: %w", err)
	}
	if err := checkLocal(abs); err != nil {
		return nil, err
	}
	db := openDatabase(abs)
	sqlDB := sql.OpenDB(&connector{db: db, dsn: dsn(abs), synchronous: sync})
	sqlDB.SetMaxIdleConns(16)
	d := &Driver{sqlDB: sqlDB, db: db, listenInterval: c.ListenInterval}
	var mode string
	if err := sqlDB.QueryRowContext(context.Background(), "PRAGMA journal_mode = WAL").Scan(&mode); err != nil {
		_ = d.Close()
		return nil, fmt.Errorf("hoppersqlite: open %s: %w", abs, err)
	}
	if !strings.EqualFold(mode, "wal") {
		_ = d.Close()
		return nil, fmt.Errorf("hoppersqlite: open %s: the database is in journal mode %s and cannot use a write-ahead log", abs, mode)
	}
	return d, nil
}

// dsn is the URI of the database file at an absolute path, for modernc's
// driver, which takes its query parameters out of a plain file name.
func dsn(abs string) string {
	p := filepath.ToSlash(abs)
	if !strings.HasPrefix(p, "/") {
		p = "/" + p // a Windows drive letter
	}
	return (&url.URL{Scheme: "file", OmitHost: true, Path: p}).String()
}

// DB returns the database/sql handle the driver uses. Begin the
// transactions you pass to InsertTx and the like on it: its connections
// take the database's write lock as a transaction's first statement runs.
func (d *Driver) DB() *sql.DB { return d.sqlDB }

// Close closes the database.
func (d *Driver) Close() error {
	err := errors.Join(d.statements.close(), d.sqlDB.Close())
	d.db.close()
	return err
}

// Executor implements driver.Driver.
func (d *Driver) Executor() driver.Executor { return &executor{d: d} }

// UnwrapTx implements driver.Driver. The transaction must come from DB.
func (d *Driver) UnwrapTx(tx *sql.Tx) driver.Executor { return &executor{d: d, tx: tx} }

// Capabilities implements driver.Driver.
func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{Listen: true, SingleWriter: true}
}

// Migrator implements driver.Driver.
func (d *Driver) Migrator() driver.Migrator { return migrator{d} }

// Listener implements driver.Driver.
func (d *Driver) Listener(ctx context.Context) (driver.Listener, error) { return d.newListener(ctx) }

//go:embed migrations/*.sql
var migrationFiles embed.FS

var migrationName = regexp.MustCompile(`^(\d+)_[a-z0-9_]+\.(up|down)\.sql$`)

// scripts are SQLite's migration scripts by version and direction.
var scripts = func() map[int]map[bool]string {
	out := map[int]map[bool]string{}
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		panic(err)
	}
	for _, e := range entries {
		m := migrationName.FindStringSubmatch(e.Name())
		if m == nil {
			panic("hoppersqlite: malformed migration file name " + e.Name())
		}
		version, _ := strconv.Atoi(m[1])
		body, err := fs.ReadFile(migrationFiles, "migrations/"+e.Name())
		if err != nil {
			panic(err)
		}
		if out[version] == nil {
			out[version] = map[bool]string{}
		}
		out[version][m[2] == "up"] = string(body)
	}
	return out
}()

// migrator implements driver.Migrator. Its lock is SQLite's write lock,
// held from Lock to Close on a connection of its own: each version applies
// in a savepoint of that transaction, and Close commits the versions that
// applied.
type migrator struct{ d *Driver }

func (m migrator) Lock(ctx context.Context) (driver.MigrationExecutor, error) {
	conn, err := m.d.sqlDB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err == nil {
		_, err = tx.ExecContext(ctx, "SELECT 1") // takes the write lock
	}
	if err != nil {
		if tx != nil {
			_ = tx.Rollback()
		}
		_ = conn.Close()
		return nil, err
	}
	return &migration{conn: conn, tx: tx}, nil
}

// migration implements driver.MigrationExecutor and driver.MigrationScripts.
type migration struct {
	conn *sql.Conn
	tx   *sql.Tx
}

var _ driver.MigrationScripts = (*migration)(nil)

func (m *migration) Versions(ctx context.Context) ([]int, error) { return schemaVersions(ctx, m.tx) }

// Script implements driver.MigrationScripts.
func (m *migration) Script(version int, up bool) (string, error) {
	script, ok := scripts[version][up]
	if !ok {
		return "", fmt.Errorf("hoppersqlite has no script for schema version %d", version)
	}
	return script, nil
}

func (m *migration) Apply(ctx context.Context, version int, script string, up bool) (err error) {
	if _, err := m.tx.ExecContext(ctx, "SAVEPOINT migration"); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_, rerr := m.tx.ExecContext(context.WithoutCancel(ctx), "ROLLBACK TO migration; RELEASE migration")
			err = errors.Join(err, rerr)
		}
	}()
	if !up {
		// The script of version 1 drops hopper_schema with the rest.
		if _, err := m.tx.ExecContext(ctx, "DELETE FROM hopper_schema WHERE version = ?", version); err != nil {
			return err
		}
	}
	if _, err := m.tx.ExecContext(ctx, script); err != nil {
		return err
	}
	if up {
		if _, err := m.tx.ExecContext(ctx, "INSERT INTO hopper_schema (version, applied_at) VALUES (?, ?)", version, time.Now().UnixMicro()); err != nil {
			return err
		}
	}
	_, err = m.tx.ExecContext(ctx, "RELEASE migration")
	return err
}

func (m *migration) Close(context.Context) error {
	err := m.tx.Commit()
	return errors.Join(err, m.conn.Close())
}

// schemaVersions returns the applied schema versions in ascending order, or
// nil if the schema has never been installed.
func schemaVersions(ctx context.Context, q querier) ([]int, error) {
	var exists bool
	if err := q.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'hopper_schema')").Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	rows, err := q.QueryContext(ctx, "SELECT version FROM hopper_schema ORDER BY version")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var versions []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		versions = append(versions, v)
	}
	return versions, rows.Err()
}
