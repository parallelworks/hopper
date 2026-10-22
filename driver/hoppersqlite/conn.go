package hoppersqlite

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// SQLite runs one writing transaction at a time, which it holds the
// database's write lock for. A transaction that reads first and writes
// later can find, once it gets the lock, that another wrote in between, and
// must fail: what it read is stale. So every transaction that may write
// takes the lock first, before its first statement reads anything:
// BEGIN IMMEDIATE.
//
// The connections of this package's database/sql handle do that for every
// transaction not begun read-only, but lazily: BeginTx sends nothing, and
// the transaction's first statement, whatever it is, takes the lock. A
// caller that begins a transaction while another holds the lock, and only
// then hands the transaction to a goroutine of its own, therefore never
// blocks in BeginTx. Within this process, transactions queue for the lock
// on a channel, in turn and for as long as their statement's context
// allows; across processes, SQLite's busy handler polls for it, in
// quanta of busyQuantum, between which the context is checked.

// busyQuantum is the busy_timeout of every connection: how long SQLite's
// busy handler waits for a lock another process holds before the statement
// fails with SQLITE_BUSY, which this package retries while the statement's
// context allows.
const busyQuantum = 50 * time.Millisecond

// database is what every connection to one database file in this process
// shares: the write lock and the listeners to wake when a transaction that
// notified commits.
type database struct {
	path string
	// lock holds a token while a connection of this process holds
	// SQLite's write lock or waits for another process's to end.
	lock chan struct{}
	// waiting counts the statements waiting for the write lock, here or
	// in another process.
	waiting atomic.Int64
	// refs counts the Drivers open on the database (registry.mu).
	refs int

	listenMu  sync.Mutex
	listeners map[chan struct{}]struct{}
}

// registry holds the databases open in this process by path, so that two
// Drivers on one file share its write lock.
var registry = struct {
	mu  sync.Mutex
	dbs map[string]*database
}{dbs: map[string]*database{}}

func openDatabase(path string) *database {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	db := registry.dbs[path]
	if db == nil {
		db = &database{path: path, lock: make(chan struct{}, 1), listeners: map[chan struct{}]struct{}{}}
		registry.dbs[path] = db
	}
	db.refs++
	return db
}

func (db *database) close() {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if db.refs--; db.refs == 0 {
		delete(registry.dbs, db.path)
	}
}

// acquire takes the write lock for this process, waiting in turn for as
// long as ctx allows.
func (db *database) acquire(ctx context.Context) error {
	select {
	case db.lock <- struct{}{}:
		return nil
	default:
	}
	db.waiting.Add(1)
	defer db.waiting.Add(-1)
	select {
	case db.lock <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (db *database) release() { <-db.lock }

// listen registers a channel to be signaled when a transaction that wrote
// notifications commits; the returned function unregisters it.
func (db *database) listen(ch chan struct{}) func() {
	db.listenMu.Lock()
	defer db.listenMu.Unlock()
	db.listeners[ch] = struct{}{}
	return func() {
		db.listenMu.Lock()
		defer db.listenMu.Unlock()
		delete(db.listeners, ch)
	}
}

// poke signals every listener of this process.
func (db *database) poke() {
	db.listenMu.Lock()
	defer db.listenMu.Unlock()
	for ch := range db.listeners {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// busy reports an error of SQLite's busy family, which a statement that
// waited busyQuantum for another process's lock returns.
func busy(err error) bool {
	var e *sqlite.Error
	return errors.As(err, &e) && e.Code()&0xff == sqlite3.SQLITE_BUSY
}

// retryBusy runs fn again while it fails with SQLITE_BUSY and ctx allows,
// counting itself among the database's waiters meanwhile.
func retryBusy[T any](ctx context.Context, db *database, fn func() (T, error)) (T, error) {
	v, err := fn()
	if !busy(err) {
		return v, err
	}
	db.waiting.Add(1)
	defer db.waiting.Add(-1)
	for busy(err) && ctx.Err() == nil {
		v, err = fn()
	}
	if busy(err) {
		return v, errors.Join(ctx.Err(), err)
	}
	return v, err
}

// innerConn is what a connection of modernc's driver implements.
type innerConn interface {
	driver.Conn
	driver.ConnPrepareContext
	driver.ExecerContext
	driver.QueryerContext
	driver.Pinger
	driver.SessionResetter
	driver.Validator
}

// connector opens the connections of a Driver's database/sql handle.
type connector struct {
	db          *database
	dsn         string
	synchronous string
}

var _ driver.Connector = (*connector)(nil)

// pragmas sets up a connection: the busy handler's quantum, the
// durability of its commits, and foreign keys off, as SQLite has them by
// default and hopper's schema has none.
func (c *connector) pragmas() []string {
	return []string{
		fmt.Sprintf("PRAGMA busy_timeout = %d", busyQuantum.Milliseconds()),
		"PRAGMA synchronous = " + c.synchronous,
		"PRAGMA foreign_keys = OFF",
	}
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	opened, err := sqliteDriver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	inner, ok := opened.(innerConn)
	if !ok {
		_ = opened.Close()
		return nil, fmt.Errorf("hoppersqlite: modernc.org/sqlite's connection is a %T, which lacks a method this driver needs", opened)
	}
	for _, p := range c.pragmas() {
		if _, err := inner.ExecContext(ctx, p, nil); err != nil {
			_ = inner.Close()
			return nil, fmt.Errorf("hoppersqlite: %s: %w", p, err)
		}
	}
	return &conn{inner: inner, db: c.db}, nil
}

func (c *connector) Driver() driver.Driver { return connectorDriver{c} }

// connectorDriver is the driver.Driver a connector reports, which opens
// what the connector does whatever the name.
type connectorDriver struct{ c *connector }

func (d connectorDriver) Open(string) (driver.Conn, error) { return d.c.Connect(context.Background()) }

// conn is a connection whose transactions take the write lock on their
// first statement.
type conn struct {
	inner innerConn
	db    *database
	tx    *tx
}

var (
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.SessionResetter    = (*conn)(nil)
	_ driver.Validator          = (*conn)(nil)
)

// tx is a transaction on a conn.
type tx struct {
	c        *conn
	readOnly bool
	// begun reports that BEGIN has run, and locked that the transaction
	// holds this process's write lock.
	begun, locked bool
	// notified reports that the transaction wrote notifications, so that
	// its commit wakes this process's listeners.
	notified bool
}

func (c *conn) exec(ctx context.Context, query string) error {
	_, err := c.inner.ExecContext(ctx, query, nil)
	return err
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	s, err := c.inner.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	inner, ok := s.(innerStmt)
	if !ok {
		_ = s.Close()
		return nil, fmt.Errorf("hoppersqlite: modernc.org/sqlite's statement is a %T, which lacks a method this driver needs", s)
	}
	return &stmt{inner: inner, c: c, query: query}, nil
}

func (c *conn) Close() error {
	if c.tx != nil {
		_ = c.tx.Rollback()
	}
	return c.inner.Close()
}

func (c *conn) Begin() (driver.Tx, error) { return c.BeginTx(context.Background(), driver.TxOptions{}) }

// BeginTx begins a transaction. One begun read-only reads a snapshot of
// the database and may not write; any other takes the write lock on its
// first statement. Every isolation level is SQLite's, serializable, which
// is at least as strong as any asked for.
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.tx != nil {
		return nil, errors.New("hoppersqlite: a transaction is already open on this connection")
	}
	t := &tx{c: c, readOnly: opts.ReadOnly}
	if opts.ReadOnly {
		if err := c.exec(ctx, "PRAGMA query_only = ON"); err != nil {
			return nil, err
		}
		if _, err := retryBusy(ctx, c.db, func() (struct{}, error) { return struct{}{}, c.exec(ctx, "BEGIN") }); err != nil {
			_ = c.exec(context.Background(), "PRAGMA query_only = OFF")
			return nil, err
		}
		t.begun = true
	}
	c.tx = t
	return t, nil
}

// beginWrite takes the write lock for the open transaction and begins it.
func (c *conn) beginWrite(ctx context.Context) error {
	if err := c.db.acquire(ctx); err != nil {
		return err
	}
	if _, err := retryBusy(ctx, c.db, func() (struct{}, error) { return struct{}{}, c.exec(ctx, "BEGIN IMMEDIATE") }); err != nil {
		c.db.release()
		return err
	}
	c.tx.begun, c.tx.locked = true, true
	return nil
}

// statement prepares the connection to run query: inside a transaction,
// it begins the transaction if this is its first statement, and notes a
// notification written; outside one, a statement that may write (exec)
// takes the write lock for its run, which done releases.
func (c *conn) statement(ctx context.Context, query string, write bool) (done func(), err error) {
	if c.tx == nil {
		if !write {
			return func() {}, nil
		}
		if err := c.db.acquire(ctx); err != nil {
			return nil, err
		}
		return c.db.release, nil
	}
	if !c.tx.begun {
		if err := c.beginWrite(ctx); err != nil {
			return nil, err
		}
	}
	if query == notifySQL {
		c.tx.notified = true
	}
	return func() {}, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	done, err := c.statement(ctx, query, true)
	if err != nil {
		return nil, err
	}
	defer done()
	if c.tx != nil {
		return c.inner.ExecContext(ctx, query, args)
	}
	return retryBusy(ctx, c.db, func() (driver.Result, error) { return c.inner.ExecContext(ctx, query, args) })
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if _, err := c.statement(ctx, query, false); err != nil {
		return nil, err
	}
	if c.tx != nil {
		return c.inner.QueryContext(ctx, query, args)
	}
	return retryBusy(ctx, c.db, func() (driver.Rows, error) { return c.inner.QueryContext(ctx, query, args) })
}

func (c *conn) Ping(ctx context.Context) error { return c.inner.Ping(ctx) }

func (c *conn) ResetSession(ctx context.Context) error {
	if c.tx != nil {
		_ = c.tx.Rollback()
	}
	return c.inner.ResetSession(ctx)
}

func (c *conn) IsValid() bool { return c.inner.IsValid() }

// end ends the transaction's hold on the connection and the lock.
func (t *tx) end() {
	if t.locked {
		t.c.db.release()
	}
	if t.readOnly {
		_ = t.c.exec(context.Background(), "PRAGMA query_only = OFF")
	}
	t.c.tx = nil
}

func (t *tx) Commit() error {
	defer t.end()
	if !t.begun {
		return nil
	}
	if err := t.c.exec(context.Background(), "COMMIT"); err != nil {
		// A COMMIT that failed can leave the transaction open, which
		// database/sql does not expect of a connection it reuses.
		_ = t.rollback()
		return err
	}
	if t.notified {
		t.c.db.poke()
	}
	return nil
}

func (t *tx) Rollback() error {
	defer t.end()
	if !t.begun {
		return nil
	}
	return t.rollback()
}

// rollback rolls the transaction back. SQLite rolls a transaction back
// itself after some errors (a full disk, an I/O error), which leaves
// nothing to roll back here.
func (t *tx) rollback() error {
	err := t.c.exec(context.Background(), "ROLLBACK")
	if err != nil && strings.Contains(err.Error(), "no transaction is active") {
		return nil
	}
	return err
}

// innerStmt is what a statement of modernc's driver implements.
type innerStmt interface {
	driver.Stmt
	driver.StmtExecContext
	driver.StmtQueryContext
}

// stmt is a prepared statement whose runs take part in their connection's
// transaction as its statements do.
type stmt struct {
	inner innerStmt
	c     *conn
	query string
}

var (
	_ driver.StmtExecContext  = (*stmt)(nil)
	_ driver.StmtQueryContext = (*stmt)(nil)
)

func (s *stmt) Close() error  { return s.inner.Close() }
func (s *stmt) NumInput() int { return s.inner.NumInput() }

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), named(args))
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), named(args))
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	done, err := s.c.statement(ctx, s.query, true)
	if err != nil {
		return nil, err
	}
	defer done()
	if s.c.tx != nil {
		return s.inner.ExecContext(ctx, args)
	}
	return retryBusy(ctx, s.c.db, func() (driver.Result, error) { return s.inner.ExecContext(ctx, args) })
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if _, err := s.c.statement(ctx, s.query, false); err != nil {
		return nil, err
	}
	if s.c.tx != nil {
		return s.inner.QueryContext(ctx, args)
	}
	return retryBusy(ctx, s.c.db, func() (driver.Rows, error) { return s.inner.QueryContext(ctx, args) })
}

func named(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, v := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return out
}
