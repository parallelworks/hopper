package hoppersqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/internal/jobcodec"
)

// executor implements driver.Executor on the pool, or inside a caller's
// transaction.
type executor struct {
	d  *Driver
	tx *sql.Tx
}

var _ driver.Executor = (*executor)(nil)

// querier is what *sql.DB, *sql.Tx and *sql.Conn share.
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// errLockBusy reports that a write gave up waiting briefly for the write
// lock (writeBrief).
var errLockBusy = errors.New("hoppersqlite: another transaction holds the database's write lock")

// clock returns the database's time (database.clock).
func (e *executor) clock() time.Time { return e.d.db.clock() }

// w is a write in progress: a transaction that holds the write lock, and
// its time, read once the lock was taken, so that every write sees a time
// no earlier than the writes before it.
type w struct {
	cached
	now time.Time
}

// maxStatements bounds the statements a Driver keeps prepared. Listings,
// whose text varies with their filters, may otherwise add one for each
// shape of filter.
const maxStatements = 256

// statements holds a Driver's prepared statements by text. database/sql
// keeps each prepared on every connection it has run on, and binds it to a
// transaction's connection with Tx.StmtContext, so SQLite parses each text
// once a connection, rather than once a run, which cost most of a
// finalize's time.
type statements struct {
	mu sync.Mutex
	m  map[string]*sql.Stmt
}

// prepared returns the prepared statement of a query, preparing it on db
// the first time, or nil for a script of several statements, or once
// maxStatements are kept.
func (s *statements) prepared(ctx context.Context, db *sql.DB, query string) (*sql.Stmt, error) {
	if strings.Contains(query, ";") {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.m[query]; ok {
		return st, nil
	}
	if len(s.m) >= maxStatements {
		return nil, nil
	}
	st, err := db.PrepareContext(ctx, query) //nolint:sqlclosecheck // kept for the Driver's life; Close closes it
	if err != nil {
		return nil, err
	}
	if s.m == nil {
		s.m = map[string]*sql.Stmt{}
	}
	s.m[query] = st
	return st, nil
}

func (s *statements) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	for _, st := range s.m {
		errs = append(errs, st.Close())
	}
	s.m = nil
	return errors.Join(errs...)
}

// cached runs statements on a querier, the pool or a transaction, through
// the Driver's prepared statements.
type cached struct {
	q querier
	d *Driver
}

var _ querier = cached{}

// stmt returns the prepared statement of query bound to the querier, or
// nil to run the text as it is.
func (c cached) stmt(ctx context.Context, query string) (*sql.Stmt, error) {
	st, err := c.d.statements.prepared(ctx, c.d.sqlDB, query)
	if err != nil || st == nil {
		return nil, err
	}
	if tx, ok := c.q.(*sql.Tx); ok {
		return tx.StmtContext(ctx, st), nil
	}
	return st, nil
}

func (c cached) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	st, err := c.stmt(ctx, query) //nolint:sqlclosecheck // the cache's, or bound to a transaction that closes it
	if err != nil {
		return nil, err
	}
	if st == nil {
		return c.q.ExecContext(ctx, query, args...)
	}
	return st.ExecContext(ctx, args...)
}

func (c cached) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	st, err := c.stmt(ctx, query) //nolint:sqlclosecheck // the cache's, or bound to a transaction that closes it
	if err != nil {
		return nil, err
	}
	if st == nil {
		return c.q.QueryContext(ctx, query, args...)
	}
	return st.QueryContext(ctx, args...)
}

// QueryRowContext runs a query that returns at most one row. A statement
// that fails to prepare reports its error through Scan, as the query's
// would.
func (c cached) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	st, err := c.stmt(ctx, query) //nolint:sqlclosecheck // the cache's, or bound to a transaction that closes it
	if err != nil || st == nil {
		return c.q.QueryRowContext(ctx, query, args...)
	}
	return st.QueryRowContext(ctx, args...)
}

// us is the write's time in the unit the schema keeps times in.
func (x *w) us() int64 { return x.now.UnixMicro() }

// at returns the write's time plus d, in the schema's unit.
func (x *w) at(d time.Duration) int64 { return x.now.Add(d).UnixMicro() }

// write runs fn in a transaction that holds the write lock: the caller's,
// inside a savepoint so that an error leaves it as it was, or a new one.
func (e *executor) write(ctx context.Context, fn func(*w) error) error {
	return e.writeLocking(ctx, 0, fn)
}

// writeBrief is write that waits at most lockWaitBrief for a write lock
// another transaction holds, and then returns errLockBusy.
func (e *executor) writeBrief(ctx context.Context, fn func(*w) error) error {
	return e.writeLocking(ctx, lockWaitBrief, fn)
}

func (e *executor) writeLocking(ctx context.Context, wait time.Duration, fn func(*w) error) (err error) {
	// The first statement takes the lock, under its own deadline when the
	// wait is bounded.
	lock := func(q cached, statement string) error {
		lctx := ctx
		if wait > 0 {
			var cancel context.CancelFunc
			lctx, cancel = context.WithTimeout(ctx, wait)
			defer cancel()
		}
		_, err := q.ExecContext(lctx, statement)
		if err != nil && wait > 0 && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
			return errLockBusy
		}
		return err
	}
	if e.tx != nil {
		q := cached{q: e.tx, d: e.d}
		if err := lock(q, "SAVEPOINT hopper"); err != nil {
			return err
		}
		defer func() {
			if err != nil {
				_, rerr := e.tx.ExecContext(context.WithoutCancel(ctx), "ROLLBACK TO hopper; RELEASE hopper")
				err = errors.Join(err, rerr)
			}
		}()
		if err := fn(&w{cached: q, now: e.clock()}); err != nil {
			return err
		}
		_, err := q.ExecContext(ctx, "RELEASE hopper")
		return err
	}
	// A transaction of the driver's own, for this one operation, which may
	// wait for a lock its owner's own transaction holds but never holds
	// one that another write of the owner should not wait for.
	tx, err := e.d.sqlDB.BeginTx(oneOperation(ctx), nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, ignoreDone(tx.Rollback()))
		}
	}()
	q := cached{q: tx, d: e.d}
	if err := lock(q, "SELECT 1"); err != nil {
		return err
	}
	if err := fn(&w{cached: q, now: e.clock()}); err != nil {
		return err
	}
	return tx.Commit()
}

func ignoreDone(err error) error {
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
}

// read returns what a read runs on: the caller's transaction, or the pool,
// on which each statement reads the latest committed state.
func (e *executor) read() querier {
	if e.tx != nil {
		return cached{q: e.tx, d: e.d}
	}
	return cached{q: e.d.sqlDB, d: e.d}
}

// readTx runs fn on one snapshot of the database: the caller's
// transaction, or a read-only one.
func (e *executor) readTx(ctx context.Context, fn func(querier) error) (err error) {
	if e.tx != nil {
		return fn(cached{q: e.tx, d: e.d})
	}
	tx, err := e.d.sqlDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, ignoreDone(tx.Rollback())) }()
	return fn(cached{q: tx, d: e.d})
}

// notifySQL writes a notification. conn.go recognizes the statement by its
// text, to wake this process's listeners when the transaction commits.
const notifySQL = "INSERT INTO hopper_notifications (channel, payload, created_at) VALUES (?, ?, ?)"

// notificationRetention is how long notifications are kept for listeners
// that fall behind; one that falls further behind misses some, and its
// client polls as it would without them.
const notificationRetention = time.Minute

// notify writes notifications, one per payload, delivered when the write
// commits, and deletes those past notificationRetention.
func (x *w) notify(ctx context.Context, channel string, payloads ...string) error {
	if len(payloads) == 0 {
		return nil
	}
	for _, p := range payloads {
		if _, err := x.ExecContext(ctx, notifySQL, channel, p, x.us()); err != nil {
			return fmt.Errorf("hopper: notify: %w", err)
		}
	}
	if _, err := x.ExecContext(ctx, "DELETE FROM hopper_notifications WHERE created_at < ?", x.at(-notificationRetention)); err != nil {
		return fmt.Errorf("hopper: notify: %w", err)
	}
	return nil
}

// Notify implements driver.Executor.
func (e *executor) Notify(ctx context.Context, channel string, payloads []string) error {
	if len(payloads) == 0 {
		return nil
	}
	return e.write(ctx, func(x *w) error { return x.notify(ctx, channel, payloads...) })
}

// Now implements driver.Executor.
func (e *executor) Now(context.Context) (time.Time, error) { return e.clock(), nil }

// SchemaVersion implements driver.Executor.
func (e *executor) SchemaVersion(ctx context.Context) (int, error) {
	versions, err := schemaVersions(ctx, e.read())
	if err != nil {
		return 0, fmt.Errorf("hopper: read schema version: %w", err)
	}
	if len(versions) == 0 {
		return 0, nil
	}
	return slices.Max(versions), nil
}

// Rows.

// jobColumns are hopper_jobs' columns in scan order; historyColumns are
// history's, in the same order, with no snooze reason (a finalized job is
// not waiting), followed by finalized_at and output.
const (
	jobColumns = `id, kind, queue, state, priority, attempt, max_attempts, scheduled_at, attempted_at, attempted_by,
  args, metadata, errors, unique_key, ordering_key, partition_key, batch_id, expires_at, cancel_requested_at, await,
  created_at, snooze_reason, progress, tags, fence`
	historyColumns = `id, kind, queue, state, priority, attempt, max_attempts, scheduled_at, attempted_at, attempted_by,
  args, metadata, errors, unique_key, ordering_key, partition_key, batch_id, expires_at, cancel_requested_at, await,
  created_at, NULL AS snooze_reason, progress, tags, fence, finalized_at, output`
	// liveAsHistory reads live rows in historyColumns' shape.
	liveAsHistory = jobColumns + `, NULL AS finalized_at, NULL AS output`
)

// prefixed qualifies a column list with a table alias.
func prefixed(columns, alias string) string {
	parts := strings.Split(columns, ",")
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if !strings.HasPrefix(p, "NULL") {
			p = alias + "." + p
		}
		parts[i] = p
	}
	return strings.Join(parts, ", ")
}

type scanner interface{ Scan(dest ...any) error }

// micros scans a nullable time column.
type micros struct{ sql.NullInt64 }

func (m micros) time() time.Time {
	if !m.Valid {
		return time.Time{}
	}
	return time.UnixMicro(m.Int64).UTC()
}

// scanJob scans a row of jobColumns, plus finalized_at and output when
// history is true, plus trailing columns into extra.
func scanJob(row scanner, history bool, extra ...any) (*driver.JobRow, error) {
	var (
		j                                                  driver.JobRow
		id, batchID                                        []byte
		state                                              string
		scheduledAt                                        int64
		attemptedAt, expiresAt, cancelAt, createdAt, final micros
		attemptedBy                                        sql.NullInt64
		args, metadata, errs                               string
		uniqueKey, orderingKey, partKey, snoozeReason      sql.NullString
		progress, tags, output                             sql.NullString
		await                                              bool
	)
	dest := []any{
		&id, &j.Kind, &j.Queue, &state, &j.Priority, &j.Attempt, &j.MaxAttempts, &scheduledAt, &attemptedAt, &attemptedBy,
		&args, &metadata, &errs, &uniqueKey, &orderingKey, &partKey, &batchID, &expiresAt, &cancelAt, &await,
		&createdAt, &snoozeReason, &progress, &tags, &j.Fence,
	}
	if history {
		dest = append(dest, &final, &output)
	}
	if err := row.Scan(append(dest, extra...)...); err != nil {
		return nil, err
	}
	if err := j.ID.Scan(id); err != nil {
		return nil, err
	}
	if len(batchID) > 0 {
		if err := j.BatchID.Scan(batchID); err != nil {
			return nil, err
		}
	}
	var err error
	if j.Errors, err = jobcodec.DecodeErrors(errs); err != nil {
		return nil, fmt.Errorf("errors of job %s: %w", j.ID, err)
	}
	j.State = driver.JobState(state)
	j.ScheduledAt = time.UnixMicro(scheduledAt).UTC()
	j.AttemptedAt, j.ExpiresAt, j.CancelRequestedAt = attemptedAt.time(), expiresAt.time(), cancelAt.time()
	j.CreatedAt, j.FinalizedAt = createdAt.time(), final.time()
	j.AttemptedBy = attemptedBy.Int64
	j.Args, j.Metadata = json.RawMessage(args), json.RawMessage(metadata)
	if output.Valid {
		j.Output = json.RawMessage(output.String)
	}
	j.UniqueKey, j.OrderingKey, j.PartitionKey = uniqueKey.String, orderingKey.String, partKey.String
	j.Await = await
	j.Tags = jobcodec.DecodeTags(tags.String)
	// Only a snoozed job is waiting for a reason; one left on a job in
	// another state by a direct write is not reported.
	if j.State == driver.JobStateScheduled {
		j.SnoozeReason = snoozeReason.String
	}
	if progress.Valid {
		j.Progress = jobcodec.DecodeProgress(progress.String)
	}
	return &j, nil
}

// queryAll runs a query and reads every row it returns through scan.
func queryAll[T any](ctx context.Context, q querier, scan func(scanner) (T, error), query string, args ...any) ([]T, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []T
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// scanHistoryJob scans a row in historyColumns' shape.
func scanHistoryJob(r scanner) (*driver.JobRow, error) { return scanJob(r, true) }

// Parameters.

func idParam(id driver.JobID) []byte { return id[:] }

// nullableID is a BLOB parameter for an ID, NULL for the zero ID.
func nullableID(id driver.JobID) any {
	if id.IsZero() {
		return nil
	}
	return id[:]
}

// nullable is a TEXT parameter, NULL for the empty string.
func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// smallint checks a value bound for what Postgres keeps in a smallint, so
// that a value means the same on either engine.
func smallint(v int, column string) (int, error) {
	if v < -32768 || v > 32767 {
		return 0, fmt.Errorf("hopper: %s %d is out of range", column, v)
	}
	return v, nil
}

// jsonValue is a JSON parameter: raw, or def when empty. It refuses what
// Postgres's jsonb refuses (checkJSON).
func jsonValue(raw []byte, def, what string) (string, error) {
	if len(raw) == 0 {
		return def, nil
	}
	if err := checkJSON(raw); err != nil {
		return "", fmt.Errorf("hopper: %s %w: %.40q", what, err, raw)
	}
	return string(raw), nil
}

// tagsJSON is a job's tags column: a JSON array, or NULL for none.
func tagsJSON(tags []string) any {
	if len(tags) == 0 {
		return nil
	}
	b, _ := json.Marshal(tags)
	return string(b)
}

// placeholders returns n comma-separated parameter markers.
func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat("?, ", n-1) + "?"
}
