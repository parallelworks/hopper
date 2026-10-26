package hoppersqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	hopperdriver "github.com/parallelworks/hopper/driver"
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
//
// After some errors (an interrupted write, a full disk, an I/O error)
// SQLite rolls the whole transaction back itself, and would run the
// transaction's later statements on their own, each committing at once.
// SQLite's rollback hook tells the connection, which then refuses every
// later statement of the transaction (ErrTransactionAborted), as Postgres
// does those of a transaction an error aborted. A COMMIT, END or ROLLBACK
// statement run in the transaction ends it the same way, which the commit
// and rollback hooks tell too: its later statements are refused
// (sql.ErrTxDone) rather than run on their own.

// busyQuantum is the busy_timeout of every connection: how long SQLite's
// busy handler waits for a lock another process holds before the statement
// fails with SQLITE_BUSY, which this package retries while the statement's
// context allows.
const busyQuantum = 50 * time.Millisecond

// journalSizeLimit is the size SQLite truncates the write-ahead log to
// when a checkpoint has copied it all back and it starts over, so that a
// burst of writes, or a reader that held up checkpoints for a while, does
// not leave a large log behind for good.
const journalSizeLimit = 32 << 20

// ErrTransactionAborted is returned for a statement of a transaction that
// SQLite rolled back itself after an error, such as an interrupted write, a
// full disk or an I/O error: the transaction's earlier statements are
// undone, and its later ones are refused rather than run on their own, as
// Postgres refuses those of a transaction an error aborted. Roll it back;
// Commit rolls it back and returns this error too.
var ErrTransactionAborted = errors.New("hoppersqlite: the transaction was rolled back after an error and runs no more statements; roll it back")

// ErrLockHeldByOwner is returned for a write that would wait for the
// database's write lock while a transaction of the same owner holds it,
// one its caller began on DB and keeps open, which would wait until its
// context ended and hold every other write up meanwhile. The owner is a
// job's attempt (driver.WithOwner), which the worker's context carries:
// the check applies to a worker's transaction begun with that context, or
// one derived from it, and not to one begun with another. Write through
// that transaction instead, as InsertTx and the other Tx methods do, or end
// it first. Writes of one attempt that are not in a transaction of its own,
// from however many goroutines, wait their turn as any others do. A worker
// that writes from two goroutines at once, one of them in its own
// transaction, gets it, where Postgres would run both; give the other
// goroutine a context without the owner (driver.WithOwner(ctx, nil)) to
// have it wait its turn instead.
var ErrLockHeldByOwner = errors.New("hoppersqlite: the write would wait for the database's write lock, which a transaction the same job attempt began holds open; write through that transaction, or end it first")

// database is what every connection to one database file in this process
// shares: the write lock, the clock, and the listeners to wake when a
// transaction that notified commits.
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
	// last is the latest time the clock read, in microseconds.
	last atomic.Int64

	holderMu sync.Mutex
	holder   holder

	listenMu  sync.Mutex
	listeners map[chan struct{}]struct{}
}

// holder is who holds the write lock: the owner its transaction runs for,
// or none, since when, and whether the transaction is one a caller began
// and keeps open while its own code runs, rather than the driver's for one
// operation, or a statement on its own.
type holder struct {
	owner   fmt.Stringer
	since   time.Time
	callers bool
}

func (h holder) String() string {
	if h.since.IsZero() {
		return "a transaction that has just ended"
	}
	who := "a transaction of this process"
	if h.owner != nil {
		who = "a transaction of " + h.owner.String()
	}
	return fmt.Sprintf("%s for %s", who, time.Since(h.since).Round(time.Millisecond))
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

// clock returns the database's time: the host's, in Postgres's precision,
// never earlier than a time it returned before in this process, so that a
// wall clock stepped back (by NTP, say) cannot date a write before the
// writes it follows.
func (db *database) clock() time.Time {
	now := time.Now().UTC().Truncate(time.Microsecond).UnixMicro()
	for {
		last := db.last.Load()
		if now <= last {
			return time.UnixMicro(last).UTC()
		}
		if db.last.CompareAndSwap(last, now) {
			return time.UnixMicro(now).UTC()
		}
	}
}

// acquire takes the write lock for this process for owner, for a
// transaction its caller began (callers) or not, waiting in turn for as
// long as ctx allows. It refuses at once to wait for a lock a transaction
// that a caller of the same owner began holds (ErrLockHeldByOwner), and a
// wait that ctx ends says who held the lock.
func (db *database) acquire(ctx context.Context, owner fmt.Stringer, callers bool) error {
	select {
	case db.lock <- struct{}{}:
		db.held(owner, callers)
		return nil
	default:
	}
	db.holderMu.Lock()
	h := db.holder
	db.holderMu.Unlock()
	if h.callers && sameOwner(h.owner, owner) {
		return fmt.Errorf("%w (%s)", ErrLockHeldByOwner, owner)
	}
	db.waiting.Add(1)
	defer db.waiting.Add(-1)
	start := time.Now()
	select {
	case db.lock <- struct{}{}:
		db.held(owner, callers)
		return nil
	case <-ctx.Done():
		db.holderMu.Lock()
		h := db.holder
		db.holderMu.Unlock()
		return fmt.Errorf("hoppersqlite: waited %s for the database's write lock, held by %s: %w",
			time.Since(start).Round(time.Millisecond), h, ctx.Err())
	}
}

// sameOwner reports whether a and b are one owner. Owners compare by
// identity; one whose type cannot be compared is never the same as
// another.
func sameOwner(a, b fmt.Stringer) bool {
	if a == nil || b == nil {
		return false
	}
	t := reflect.TypeOf(a)
	return t == reflect.TypeOf(b) && t.Comparable() && a == b
}

func (db *database) held(owner fmt.Stringer, callers bool) {
	db.holderMu.Lock()
	db.holder = holder{owner: owner, since: time.Now(), callers: callers}
	db.holderMu.Unlock()
}

// oneOperationKey marks the context of a transaction the driver begins for
// one operation of its own, which never stays open while a caller's code
// runs.
type oneOperationKey struct{}

// oneOperation returns ctx marked as oneOperationKey says.
func oneOperation(ctx context.Context) context.Context {
	return context.WithValue(ctx, oneOperationKey{}, true)
}

func (db *database) release() {
	db.holderMu.Lock()
	db.holder = holder{}
	db.holderMu.Unlock()
	<-db.lock
}

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
	start := time.Now()
	for busy(err) && ctx.Err() == nil {
		v, err = fn()
	}
	if busy(err) {
		return v, fmt.Errorf("hoppersqlite: waited %s for a lock another process holds on the database: %w",
			time.Since(start).Round(time.Millisecond), errors.Join(ctx.Err(), err))
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
	sqlite.HookRegisterer
}

// connector opens the connections of a Driver's database/sql handle.
type connector struct {
	db          *database
	dsn         string
	synchronous string
}

var _ driver.Connector = (*connector)(nil)

// pragmas sets up a connection: the busy handler's quantum; the
// durability of its commits, on macOS with F_FULLFSYNC, without which fsync
// leaves the commit in the disk's cache; the size the write-ahead log is
// cut back to; and foreign keys off, as SQLite has them by default and
// hopper's schema has none. Some read the database's schema, which waits
// for a lock another connection can hold, as one does while it turns a new
// database's write-ahead log on: Connect runs each again while it fails
// with SQLITE_BUSY and its context allows, as the package's statements do.
func (c *connector) pragmas() []string {
	p := []string{
		fmt.Sprintf("PRAGMA busy_timeout = %d", busyQuantum.Milliseconds()),
		"PRAGMA synchronous = " + c.synchronous,
		fmt.Sprintf("PRAGMA journal_size_limit = %d", journalSizeLimit),
		"PRAGMA foreign_keys = OFF",
	}
	if runtime.GOOS == "darwin" && c.synchronous == "FULL" {
		p = append(p, "PRAGMA fullfsync = ON", "PRAGMA checkpoint_fullfsync = ON")
	}
	return p
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
		if _, err := retryBusy(ctx, c.db, func() (driver.Result, error) { return inner.ExecContext(ctx, p, nil) }); err != nil {
			_ = inner.Close()
			return nil, fmt.Errorf("hoppersqlite: %s: %w", p, err)
		}
	}
	cn := &conn{inner: inner, db: c.db}
	// SQLite calls the hooks on the goroutine running the statement that
	// commits or rolls a transaction back, whether by COMMIT or ROLLBACK
	// or, for a rollback, after an error.
	inner.RegisterRollbackHook(func() { cn.rolledBack = true })
	inner.RegisterCommitHook(func() int32 { cn.committed = true; return 0 })
	return cn, nil
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
	// rolledBack and committed are set by SQLite's rollback and commit
	// hooks when a transaction rolls back or commits; a statement resets
	// them before it runs.
	rolledBack, committed bool
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
	// owner is who the transaction runs for (driver.Owner of the context
	// it began with), and oneOperation reports one the driver began for
	// one operation of its own.
	owner        fmt.Stringer
	oneOperation bool
	// begun reports that BEGIN has run, and locked that the transaction
	// holds this process's write lock.
	begun, locked bool
	// read reports that a read-only transaction has run a statement.
	read bool
	// notified reports that the transaction wrote notifications, so that
	// its commit wakes this process's listeners.
	notified bool
	// aborted is the error after which SQLite rolled the transaction back.
	aborted error
	// endedBy names the statement of the caller's that ended the
	// transaction (COMMIT, END or ROLLBACK), and committed reports that it
	// committed it.
	endedBy   string
	committed bool
}

// refused returns the error of a statement of a transaction that has
// ended, or nil.
func (t *tx) refused() error {
	switch {
	case t.aborted != nil:
		return abortedError(t.aborted)
	case t.endedBy != "":
		return fmt.Errorf("hoppersqlite: %s ended the transaction; end it with Commit or Rollback instead: %w", t.endedBy, sql.ErrTxDone)
	}
	return nil
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
	t := &tx{c: c, readOnly: opts.ReadOnly, owner: hopperdriver.Owner(ctx), oneOperation: ctx.Value(oneOperationKey{}) != nil}
	if opts.ReadOnly {
		if err := c.exec(ctx, "PRAGMA query_only = ON"); err != nil {
			return nil, err
		}
		if err := c.exec(ctx, "BEGIN"); err != nil {
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
	if err := c.db.acquire(ctx, c.tx.owner, !c.tx.oneOperation); err != nil {
		return err
	}
	if _, err := retryBusy(ctx, c.db, func() (struct{}, error) { return struct{}{}, c.exec(ctx, "BEGIN IMMEDIATE") }); err != nil {
		c.db.release()
		return err
	}
	c.tx.begun, c.tx.locked = true, true
	return nil
}

// abortedError is the error of a statement refused in an aborted
// transaction. It names the cause but does not wrap it: a statement
// refused after an earlier one was interrupted did not see its own context
// end.
func abortedError(cause error) error {
	return fmt.Errorf("%w (after: %v)", ErrTransactionAborted, cause) //nolint:errorlint // see above
}

// enter prepares the open transaction for a statement: it refuses one
// after the transaction ended (refused), begins the transaction on its
// first statement, notes a notification written, and resets the hooks'
// flags. It reports whether the statement may meet SQLITE_BUSY that can be
// retried: the first of a read-only transaction, which has read no
// snapshot yet.
func (c *conn) enter(ctx context.Context, query string) (retry bool, err error) {
	t := c.tx
	if err := t.refused(); err != nil {
		return false, err
	}
	if !t.begun {
		if err := c.beginWrite(ctx); err != nil {
			return false, err
		}
	}
	if query == notifySQL {
		t.notified = true
	}
	retry = t.readOnly && !t.read
	t.read = true
	c.rolledBack, c.committed = false, false
	return retry, nil
}

// leave notes whether a statement of the open transaction, ending with
// err, ended the transaction: SQLite rolled it back after the statement's
// error, or the statement was a ROLLBACK, a COMMIT or an END.
func (c *conn) leave(err error) {
	t := c.tx
	if t == nil || t.refused() != nil {
		return
	}
	switch {
	case c.rolledBack && err != nil:
		t.aborted = err
	case c.rolledBack:
		t.endedBy = "a ROLLBACK statement"
	case c.committed && err == nil:
		t.endedBy, t.committed = "a COMMIT statement", true
	}
}

// runExec runs a statement that may write: in the open transaction, or on
// its own, holding the write lock while it runs.
func (c *conn) runExec(ctx context.Context, query string, run func() (driver.Result, error)) (driver.Result, error) {
	if c.tx == nil {
		if err := c.db.acquire(ctx, hopperdriver.Owner(ctx), false); err != nil {
			return nil, err
		}
		defer c.db.release()
		return retryBusy(ctx, c.db, run)
	}
	if _, err := c.enter(ctx, query); err != nil {
		return nil, err
	}
	res, err := run()
	c.leave(err)
	return res, err
}

// runQuery runs a query: in the open transaction, or on its own. On its
// own, one that may write (mayWrite) holds the write lock until its rows
// are closed, as a statement that writes in autocommit holds SQLite's
// until it is done.
func (c *conn) runQuery(ctx context.Context, query string, run func() (driver.Rows, error)) (driver.Rows, error) {
	if c.tx == nil {
		if !mayWrite(query) {
			return retryBusy(ctx, c.db, run)
		}
		if err := c.db.acquire(ctx, hopperdriver.Owner(ctx), false); err != nil {
			return nil, err
		}
		r, err := retryBusy(ctx, c.db, run)
		if err != nil {
			c.db.release()
			return nil, err
		}
		return &rows{inner: r, release: c.db.release}, nil
	}
	retry, err := c.enter(ctx, query)
	if err != nil {
		return nil, err
	}
	var r driver.Rows
	if retry {
		r, err = retryBusy(ctx, c.db, run)
	} else {
		r, err = run()
	}
	c.leave(err)
	if err != nil {
		return nil, err
	}
	return &rows{inner: r, c: c, t: c.tx}, nil
}

// writeKeywords are the first words of the statements that may write.
var writeKeywords = []string{"INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE", "DROP", "ALTER", "VACUUM", "REINDEX", "ANALYZE"}

// readPragmas are the pragmas that take an argument in parentheses and
// only read. wal_checkpoint is among them: it waits for writers through
// SQLite's busy handler rather than in turn, as a reader would.
var readPragmas = []string{
	"COLLATION_LIST", "DATABASE_LIST", "FOREIGN_KEY_CHECK", "FOREIGN_KEY_LIST", "FUNCTION_LIST",
	"INDEX_INFO", "INDEX_LIST", "INDEX_XINFO", "INTEGRITY_CHECK", "MODULE_LIST", "PRAGMA_LIST",
	"QUICK_CHECK", "TABLE_INFO", "TABLE_LIST", "TABLE_XINFO", "WAL_CHECKPOINT",
}

// mayWrite reports whether a query, run on its own, may write: whether
// any of its statements does, by its first word; a WITH statement by
// whether INSERT, UPDATE, DELETE or REPLACE INTO is among its words; a
// PRAGMA by whether it sets a value, with "=" or in parentheses, or is
// optimize or incremental_vacuum. Words in string literals, quoted
// identifiers and comments do not count.
func mayWrite(query string) bool {
	for _, stmt := range sqlTokens(query) {
		if statementMayWrite(stmt) {
			return true
		}
	}
	return false
}

func statementMayWrite(t []string) bool {
	if len(t) == 0 {
		return false
	}
	switch t[0] {
	case "WITH":
		for i, w := range t {
			switch {
			case w == "INSERT" || w == "UPDATE" || w == "DELETE":
				return true
			case w == "REPLACE" && i+1 < len(t) && t[i+1] == "INTO":
				return true
			}
		}
		return false
	case "PRAGMA":
		name := ""
		for i, w := range t[1:] {
			switch {
			case w == "=":
				return true
			case w == "(":
				return !slices.Contains(readPragmas, name)
			case w != "." && i < 3:
				name = w // the last of schema.name
			}
		}
		return name == "OPTIMIZE" || name == "INCREMENTAL_VACUUM"
	default:
		return slices.Contains(writeKeywords, t[0])
	}
}

// sqlTokens splits SQL into statements, each a list of its words,
// upper-cased, and of its punctuation, one character each. String and blob
// literals, quoted identifiers, numbers, parameters and comments are left
// out.
func sqlTokens(q string) [][]string {
	var (
		stmts [][]string
		cur   []string
	)
	isWord := func(c byte) bool {
		return c == '_' || c == '$' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
	}
	skipTo := func(i int, end string) int {
		if j := strings.Index(q[i:], end); j >= 0 {
			return i + j + len(end)
		}
		return len(q)
	}
	for i := 0; i < len(q); {
		c := q[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			// A quote doubled inside is the quote itself.
			j := i + 1
			for j < len(q) && (q[j] != c || j+1 < len(q) && q[j+1] == c) {
				if q[j] == c {
					j++
				}
				j++
			}
			i = j + 1
		case c == '[':
			i = skipTo(i, "]")
		case strings.HasPrefix(q[i:], "--"):
			i = skipTo(i, "\n")
		case strings.HasPrefix(q[i:], "/*"):
			i = skipTo(i, "*/")
		case c == ';':
			stmts, cur = append(stmts, cur), nil
			i++
		case c == '?' || c == ':' || c == '@' || c >= '0' && c <= '9' || c == '$':
			// A parameter or a number, which may run into letters.
			j := i + 1
			for j < len(q) && (isWord(q[j]) || q[j] == '.') {
				j++
			}
			i = j
		case isWord(c):
			j := i + 1
			for j < len(q) && isWord(q[j]) {
				j++
			}
			cur = append(cur, strings.ToUpper(q[i:j]))
			i = j
		case c == ' ' || c == '\t' || c == '\n' || c == '\f' || c == '\r':
			// SQLite's whitespace; a vertical tab is not, and fails to prepare.
			i++
		default:
			cur = append(cur, string(c))
			i++
		}
	}
	return append(stmts, cur)
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.runExec(ctx, query, func() (driver.Result, error) { return c.inner.ExecContext(ctx, query, args) })
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.runQuery(ctx, query, func() (driver.Rows, error) { return c.inner.QueryContext(ctx, query, args) })
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

// Commit commits the transaction. If SQLite rolled it back after an error,
// it rolls back what is left of it and returns ErrTransactionAborted; if a
// statement of the caller's ended it, Commit returns nil for a COMMIT and
// sql.ErrTxDone for a ROLLBACK.
func (t *tx) Commit() error {
	defer t.end()
	switch {
	case !t.begun:
		return nil
	case t.aborted != nil:
		_ = t.rollback()
		return abortedError(t.aborted)
	case t.endedBy != "" && !t.committed:
		return t.refused()
	case t.endedBy != "":
		if t.notified {
			t.c.db.poke()
		}
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

// Rollback rolls the transaction back, or does nothing if it has ended.
func (t *tx) Rollback() error {
	defer t.end()
	if !t.begun || t.endedBy != "" {
		return nil
	}
	return t.rollback()
}

// rollback rolls the transaction back. SQLite rolls a transaction back
// itself after some errors (a full disk, an I/O error, an interrupted
// write), which leaves nothing to roll back here.
func (t *tx) rollback() error {
	err := t.c.exec(context.Background(), "ROLLBACK")
	if err != nil && strings.Contains(err.Error(), "no transaction is active") {
		return nil
	}
	return err
}

// rows reads a query's rows. In a transaction, it notes a rollback SQLite
// makes while it steps (leave); on its own, a query that may write holds the write
// lock until it is closed.
type rows struct {
	inner   driver.Rows
	c       *conn
	t       *tx
	release func()
}

var (
	_ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
	_ driver.RowsColumnTypeLength           = (*rows)(nil)
	_ driver.RowsColumnTypeNullable         = (*rows)(nil)
	_ driver.RowsColumnTypePrecisionScale   = (*rows)(nil)
	_ driver.RowsColumnTypeScanType         = (*rows)(nil)
)

func (r *rows) Columns() []string { return r.inner.Columns() }

func (r *rows) Close() error {
	err := r.inner.Close()
	if r.release != nil {
		r.release()
		r.release = nil
	}
	return err
}

func (r *rows) Next(dest []driver.Value) error {
	if r.t == nil {
		return r.inner.Next(dest)
	}
	if err := r.t.refused(); err != nil {
		return err
	}
	err := r.inner.Next(dest)
	if r.c.tx == r.t {
		r.c.leave(err)
	}
	return err
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	if t, ok := r.inner.(driver.RowsColumnTypeDatabaseTypeName); ok {
		return t.ColumnTypeDatabaseTypeName(index)
	}
	return ""
}

func (r *rows) ColumnTypeLength(index int) (int64, bool) {
	if t, ok := r.inner.(driver.RowsColumnTypeLength); ok {
		return t.ColumnTypeLength(index)
	}
	return 0, false
}

func (r *rows) ColumnTypeNullable(index int) (bool, bool) {
	if t, ok := r.inner.(driver.RowsColumnTypeNullable); ok {
		return t.ColumnTypeNullable(index)
	}
	return false, false
}

func (r *rows) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
	if t, ok := r.inner.(driver.RowsColumnTypePrecisionScale); ok {
		return t.ColumnTypePrecisionScale(index)
	}
	return 0, 0, false
}

func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	if t, ok := r.inner.(driver.RowsColumnTypeScanType); ok {
		return t.ColumnTypeScanType(index)
	}
	return reflect.TypeFor[any]()
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
	return s.c.runExec(ctx, s.query, func() (driver.Result, error) { return s.inner.ExecContext(ctx, args) })
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.c.runQuery(ctx, s.query, func() (driver.Rows, error) { return s.inner.QueryContext(ctx, args) })
}

func named(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, v := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return out
}
