package pgsql

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// historyGroup describes one time-partitioned table: an outcome partition
// of hopper_job_history, or the stream log.
type historyGroup struct {
	// root is the table whose shape new partitions copy; parent is the
	// partitioned table they attach to (the same, except for history's
	// outcome partitions).
	root, parent string
	// timeColumn is the range key.
	timeColumn string
	period     time.Duration
	// ahead is how many future periods to keep ready, beyond the current one.
	ahead int
	// format names a partition by its period start (in UTC).
	format string
}

// HistoryLockKey is the pg_advisory_xact_lock key that serializes partition
// creation between overlapping leaders. It is combined with the schema, so
// installations sharing a database do not queue behind each other.
const HistoryLockKey int32 = 0x686f7068 // "hoph"

var historyGroups = []historyGroup{
	{root: "hopper_job_history", parent: "hopper_job_history_completed", timeColumn: "finalized_at", period: time.Hour, ahead: 3, format: "2006010215"},
	{root: "hopper_job_history", parent: "hopper_job_history_failed", timeColumn: "finalized_at", period: 24 * time.Hour, ahead: 2, format: "20060102"},
	{root: "hopper_stream_events", parent: "hopper_stream_events", timeColumn: "created_at", period: 24 * time.Hour, ahead: 2, format: "20060102"},
}

func (g historyGroup) name(start time.Time) string {
	return g.parent + "_" + start.UTC().Format(g.format)
}

// parse returns the period start encoded in a partition name, or false for
// names that are not time partitions (the DEFAULT partition).
func (g historyGroup) parse(name string) (time.Time, bool) {
	suffix, ok := strings.CutPrefix(name, g.parent+"_")
	if !ok {
		return time.Time{}, false
	}
	t, err := time.ParseInLocation(g.format, suffix, time.UTC)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// HistoryMaintain implements driver.Executor. It keeps time partitions
// ready for the periods ahead and drops the ones past retention. Only future
// periods are created, so no existing row ever has to move: rows finalized
// before their period's partition existed (the first period after
// installation, or after a long leader outage) stay in the DEFAULT partition
// and are pruned by retention individually.
func (e *Executor) HistoryMaintain(ctx context.Context, params driver.HistoryMaintainParams) (driver.HistoryMaintainResult, error) {
	var res driver.HistoryMaintainResult
	if e.InTx {
		return res, errors.New("hopper: history maintenance must run on the pool, not in a transaction")
	}
	var now time.Time
	if err := e.Conn.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		return res, fmt.Errorf("hopper: history maintenance: %w", err)
	}
	lockTimeout := fmt.Sprintf("%dms", max(cmp.Or(params.LockTimeout, defaultDDLLockTimeout), time.Millisecond).Milliseconds())
	retention := map[string]time.Duration{
		"hopper_job_history_completed": params.CompletedRetention,
		"hopper_job_history_failed":    params.FailedRetention,
		"hopper_stream_events":         params.StreamRetention,
	}
	for _, g := range historyGroups {
		existing, err := e.partitionsOf(ctx, g.parent)
		if err != nil {
			return res, err
		}
		keep := retention[g.parent]

		// Create the periods ahead of the current one. Every attach waits
		// for the same locks (the parent, and the DEFAULT partition it
		// scans), so once one is deferred the rest are deferred untried:
		// each attempt would hold the group's writers up for
		// the lock timeout again.
		start := now.UTC().Truncate(g.period)
		blocked := false
		for i := 1; i <= g.ahead; i++ {
			from := start.Add(time.Duration(i) * g.period)
			name := g.name(from)
			if _, ok := existing[name]; ok {
				continue
			}
			if blocked {
				res.Deferred = append(res.Deferred, name)
				continue
			}
			created, deferred, err := e.createPartition(ctx, g, name, from, from.Add(g.period), lockTimeout)
			if err != nil {
				return res, err
			}
			if created {
				res.Created = append(res.Created, name)
			}
			if deferred {
				blocked = true
				res.Deferred = append(res.Deferred, name)
			}
		}

		// Drop partitions whose whole period is past retention. Every drop
		// waits for the same lock on the parent, so as with the attaches,
		// once one is deferred the rest are deferred untried.
		if keep > 0 {
			cutoff := now.Add(-keep)
			blocked := false
			for name := range existing {
				from, ok := g.parse(name)
				if !ok || !from.Add(g.period).Before(cutoff) {
					continue
				}
				if blocked {
					res.Deferred = append(res.Deferred, name)
					continue
				}
				dropped, err := e.dropPartition(ctx, name, lockTimeout)
				if err != nil {
					return res, err
				}
				if dropped {
					res.Dropped = append(res.Dropped, name)
				} else {
					blocked = true
					res.Deferred = append(res.Deferred, name)
				}
			}
			// Rows in the DEFAULT partition cannot be dropped by period.
			n, err := e.Conn.Exec(ctx,
				fmt.Sprintf("DELETE FROM %s WHERE %s < now() - make_interval(secs => $1::float8)", e.Schema.Table(g.parent+"_default"), g.timeColumn),
				keep.Seconds())
			if err != nil {
				return res, fmt.Errorf("hopper: prune %s_default: %w", g.parent, err)
			}
			res.Pruned += n
		}
	}
	return res, nil
}

// defaultDDLLockTimeout bounds how long a partition's creation or drop
// waits for its lock, unless HistoryMaintainParams.LockTimeout says
// otherwise. While it waits, every statement that needs the same table queues
// behind it, and a transaction that wrote history (a worker's CompleteTx)
// holds it off until that transaction ends; the next pass retries.
const defaultDDLLockTimeout = 200 * time.Millisecond

// lockNotAvailable reports whether err is a lock_timeout expiry.
func lockNotAvailable(err error) bool {
	var coded interface{ SQLState() string }
	return errors.As(err, &coded) && coded.SQLState() == "55P03"
}

// dropPartition drops a partition past retention. DROP takes a brief
// ACCESS EXCLUSIVE lock on the parent for the catalog change, once per
// period. (DETACH CONCURRENTLY would avoid it, but Postgres refuses it while
// a DEFAULT partition exists.) The wait is bounded by lockTimeout: a drop
// that cannot get the lock in time returns false.
func (e *Executor) dropPartition(ctx context.Context, name, lockTimeout string) (bool, error) {
	err := e.withTx(ctx, func(tx Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '"+lockTimeout+"'"); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "DROP TABLE IF EXISTS "+e.Schema.Table(name))
		return err
	})
	if lockNotAvailable(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("hopper: drop partition %s: %w", name, err)
	}
	return true, nil
}

// orderingIndexes pairs each history outcome table with its ON ONLY
// ordering-key index (schema v13).
var orderingIndexes = map[string]string{
	"hopper_job_history_completed": "hopper_job_history_completed_ordering",
	"hopper_job_history_failed":    "hopper_job_history_failed_ordering",
}

// HistoryIndexLockKey is the pg_advisory_xact_lock key held while a history
// partition is indexed, combined with the schema like HistoryLockKey. Two
// concurrent CREATE INDEX CONCURRENTLY on one table can deadlock, so
// overlapping leaders index one at a time; the one that does not get the
// lock leaves the work to the other.
//
// A build on an outcome table's DEFAULT partition also holds the key combined
// with the schema and that table. Partition creation under the table tries it
// without waiting, and leaves the partition to its next pass while the build
// runs: its attach scans the DEFAULT partition, which the build holds, while
// the build waits for the attach's transaction to end. A build elsewhere does
// not conflict with the attach.
//
// The CREATE INDEX CONCURRENTLY cannot run in a transaction, so the locks are
// held by a transaction of their own, which stays open and idle on another
// connection for the build. A session-level lock would not do: behind a
// transaction pooler, the lock, the build and the unlock can each run on a
// different server connection, and the lock would outlive the build.
const HistoryIndexLockKey int32 = 0x686f7069 // "hopi"

// tryHistoryIndexLock tries HistoryIndexLockKey combined with name, for the
// rest of tx.
func tryHistoryIndexLock(ctx context.Context, tx Conn, name string) (bool, error) {
	var locked bool
	err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock(hashtext($1), $2)", name, HistoryIndexLockKey).Scan(&locked)
	return locked, err
}

// HistoryIndex implements driver.Executor. It builds the ordering-key index
// of every history partition that has none attached yet, concurrently so
// that finalizes keep moving rows into history, and attaches it. Partitions
// created since v13 get theirs when they are attached, so this finds work
// only after the upgrade, once per partition. An index a canceled build
// left invalid is dropped and built again. A build canceled by the
// session's statement timeout is reported, and the pass goes on to the next
// partition.
func (e *Executor) HistoryIndex(ctx context.Context, params driver.HistoryIndexParams) (driver.HistoryIndexResult, error) {
	var res driver.HistoryIndexResult
	if e.InTx {
		return res, errors.New("hopper: history indexing must run on the pool, not in a transaction")
	}
	for parent, parentIndex := range orderingIndexes {
		leaves, err := scanStrings(e.Conn.Query(ctx, `
			SELECT c.relname FROM pg_inherits i
			JOIN pg_class c ON c.oid = i.inhrelid
			WHERE i.inhparent = to_regclass($1) AND to_regclass($2) IS NOT NULL
			  AND NOT EXISTS (
			    SELECT 1 FROM pg_index x JOIN pg_inherits xi ON xi.inhrelid = x.indexrelid
			    WHERE x.indrelid = c.oid AND xi.inhparent = to_regclass($2))`,
			e.Schema.Table(parent), e.Schema.Table(parentIndex)))
		if err != nil {
			return res, fmt.Errorf("hopper: list unindexed partitions of %s: %w", parent, err)
		}
		// The outcome table's index can be valid only once every partition
		// has its index, so it is checked only after a pass that left none
		// out.
		complete := true
		for _, leaf := range leaves {
			if slices.Contains(params.Skip, leaf) {
				complete = false
				continue
			}
			done, err := e.indexHistoryPartitionLocked(ctx, parent, leaf, parentIndex)
			if errors.Is(err, errStatementTimeout) {
				res.TimedOut = append(res.TimedOut, leaf)
				complete = false
				continue
			}
			if err != nil || !done {
				return res, err
			}
		}
		if !complete {
			continue
		}
		if err := e.revalidateOrderingIndex(ctx, parentIndex); err != nil {
			return res, err
		}
	}
	return res, nil
}

// errStatementTimeout marks a build the session's statement_timeout
// canceled.
var errStatementTimeout = errors.New("hopper: index build exceeded statement_timeout")

// indexHistoryPartitionLocked indexes one partition under
// HistoryIndexLockKey, and reports false, having done nothing, if another
// session holds it: another leader indexing, or, for a DEFAULT partition,
// partition creation. The lock is held per partition, so that creation waits
// out at most one partition's build.
func (e *Executor) indexHistoryPartitionLocked(ctx context.Context, parent, leaf, parentIndex string) (bool, error) {
	lock, err := e.Conn.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("hopper: lock history indexing: %w", err)
	}
	// Ending the transaction releases the locks.
	defer func() { _ = lock.Rollback(context.WithoutCancel(ctx)) }()
	// The transaction must hold no snapshot while it waits idle for the
	// build, which would otherwise wait for it in turn: read committed keeps
	// none between statements.
	if _, err := lock.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"); err != nil {
		return false, fmt.Errorf("hopper: lock history indexing: %w", err)
	}
	locked, err := tryHistoryIndexLock(ctx, lock, e.Schema.Name)
	if err == nil && locked && leaf == parent+"_default" {
		locked, err = tryHistoryIndexLock(ctx, lock, e.Schema.Name+"."+parent)
	}
	if err != nil {
		return false, fmt.Errorf("hopper: lock history indexing: %w", err)
	}
	if !locked {
		return false, nil
	}
	// A statement of its own lets go of the portal the lock's query left
	// open, whose snapshot the build would wait for, and it keeps the server
	// from ending the idle transaction, and the locks, during a long build.
	if _, err := lock.Exec(ctx, "SET LOCAL idle_in_transaction_session_timeout = 0"); err != nil {
		return false, fmt.Errorf("hopper: lock history indexing: %w", err)
	}
	return true, e.indexHistoryPartition(ctx, leaf, parentIndex)
}

// indexHistoryPartition builds one partition's ordering-key index and
// attaches it to its outcome table's index. A partition dropped by retention
// since it was listed is skipped.
func (e *Executor) indexHistoryPartition(ctx context.Context, leaf, parentIndex string) error {
	index := leaf + "_ordering"
	var (
		exists bool
		valid  sql.NullBool
	)
	if err := e.Conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL, (SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($2))",
		e.Schema.Table(leaf), e.Schema.Table(index)).Scan(&exists, &valid); err != nil {
		return fmt.Errorf("hopper: index partition %s: %w", leaf, err)
	}
	if !exists {
		return nil
	}
	if valid.Valid && !valid.Bool {
		if _, err := e.Conn.Exec(ctx, "DROP INDEX CONCURRENTLY IF EXISTS "+e.Schema.Table(index)); err != nil {
			return fmt.Errorf("hopper: drop invalid index %s: %w", index, err)
		}
	}
	if !valid.Valid || !valid.Bool {
		if _, err := e.Conn.Exec(ctx, "CREATE INDEX CONCURRENTLY IF NOT EXISTS "+quoteIdent(index)+" ON "+e.Schema.Table(leaf)+
			" (ordering_key, id) WHERE ordering_key IS NOT NULL"); err != nil {
			var coded interface{ SQLState() string }
			if ctx.Err() == nil && errors.As(err, &coded) && coded.SQLState() == "57014" {
				// The build left an invalid index behind, which would cost
				// every write to the partition until the next attempt.
				if _, derr := e.Conn.Exec(ctx, "DROP INDEX CONCURRENTLY IF EXISTS "+e.Schema.Table(index)); derr != nil {
					return fmt.Errorf("hopper: drop invalid index %s: %w", index, derr)
				}
				return fmt.Errorf("%w: %s: %w", errStatementTimeout, leaf, err)
			}
			return fmt.Errorf("hopper: index partition %s: %w", leaf, err)
		}
	}
	return e.attachIndex(ctx, parentIndex, index)
}

// attachIndex attaches a partition's index under the history maintenance
// lock, which partition creation (whose attach indexes the new partition)
// also takes. Postgres marks a partitioned index valid in the attach that
// completes it, counting the attached indexes it can see; two attaches
// committing side by side would each miss the other's and leave the parent
// invalid for good.
func (e *Executor) attachIndex(ctx context.Context, parentIndex, index string) (err error) {
	tx, err := e.Conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1), $2)", e.Schema.Name, HistoryLockKey); err != nil {
		return fmt.Errorf("hopper: lock history maintenance: %w", err)
	}
	if _, err = tx.Exec(ctx, "ALTER INDEX "+e.Schema.Table(parentIndex)+" ATTACH PARTITION "+e.Schema.Table(index)); err != nil {
		return fmt.Errorf("hopper: attach index %s: %w", index, err)
	}
	return tx.Commit(ctx)
}

// revalidateOrderingIndex marks an outcome table's index valid when no
// partition lacks its index any more but the attach that would have
// completed it never ran, because retention dropped the last partition
// without one before it was built. Attaching a partition's index again makes
// Postgres count the attached indexes again; a new partition's attach does
// not.
func (e *Executor) revalidateOrderingIndex(ctx context.Context, parentIndex string) error {
	var child sql.NullString
	err := e.Conn.QueryRow(ctx, `
		SELECT (SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		        WHERE i.inhparent = x.indexrelid LIMIT 1)
		FROM pg_index x WHERE x.indexrelid = to_regclass($1) AND NOT x.indisvalid`, e.Schema.Table(parentIndex)).Scan(&child)
	if errors.Is(err, ErrNoRows) || (err == nil && !child.Valid) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("hopper: check index %s: %w", parentIndex, err)
	}
	return e.attachIndex(ctx, parentIndex, child.String)
}

// partitionsOf lists the partitions attached to parent.
func (e *Executor) partitionsOf(ctx context.Context, parent string) (map[string]struct{}, error) {
	names, err := scanStrings(e.Conn.Query(ctx, `
		SELECT c.relname FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = to_regclass($1)`, e.Schema.Table(parent)))
	if err != nil {
		return nil, fmt.Errorf("hopper: list partitions of %s: %w", parent, err)
	}
	out := make(map[string]struct{}, len(names))
	for _, n := range names {
		out[n] = struct{}{}
	}
	return out, nil
}

// createPartition creates a time partition for a future period and attaches
// it. It returns false if another leader created the partition meanwhile,
// and deferred if it could not get its locks within lockTimeout.
func (e *Executor) createPartition(ctx context.Context, g historyGroup, name string, from, to time.Time, lockTimeout string) (created, deferred bool, err error) {
	tx, err := e.Conn.Begin(ctx)
	if err != nil {
		return false, false, err
	}
	defer func() {
		if err != nil || deferred {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	ident := e.Schema.Table(name)
	parent := e.Schema.Table(g.parent)
	fromLit, toLit := literal(from), literal(to)

	// Two leaders can overlap briefly; serialize them on an advisory lock
	// held for this transaction, then check whether the other one has
	// already created the partition.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1), $2)", e.Schema.Name, HistoryLockKey); err != nil {
		return false, false, fmt.Errorf("hopper: lock history maintenance: %w", err)
	}
	// An index being built concurrently on this table's DEFAULT partition
	// (HistoryIndex) holds HistoryIndexLockKey combined with the table, and
	// the attach below would deadlock with it. Partitions are created periods
	// ahead, so this one is deferred to the next pass instead.
	if _, indexed := orderingIndexes[g.parent]; indexed {
		var free bool
		if free, err = tryHistoryIndexLock(ctx, tx, e.Schema.Name+"."+g.parent); err != nil {
			return false, false, fmt.Errorf("hopper: lock history indexing: %w", err)
		}
		if !free {
			return false, true, nil
		}
	}
	var exists bool
	if err = tx.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", e.Schema.Table(name)).Scan(&exists); err != nil {
		return false, false, fmt.Errorf("hopper: check partition %s: %w", name, err)
	}
	if exists {
		return false, false, tx.Rollback(ctx)
	}
	// CREATE TABLE ... PARTITION OF would take ACCESS EXCLUSIVE on the
	// parent and block finalizes; a standalone table plus ATTACH takes only
	// SHARE UPDATE EXCLUSIVE on it. ATTACH does scan the DEFAULT partition
	// for rows belonging to the new period, under an exclusive lock on that
	// small table alone; the period is in the future, so it finds none. A
	// transaction that wrote a row to the DEFAULT partition holds that lock
	// off until it ends, so the wait is bounded.
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout = '"+lockTimeout+"'"); err != nil {
		return false, false, fmt.Errorf("hopper: create partition %s: %w", name, err)
	}
	if _, err = tx.Exec(ctx, "CREATE TABLE "+ident+" (LIKE "+e.Schema.Table(g.root)+" INCLUDING DEFAULTS INCLUDING CONSTRAINTS)"); err != nil {
		if lockNotAvailable(err) {
			return false, true, nil
		}
		return false, false, fmt.Errorf("hopper: create partition %s: %w", name, err)
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM (%s) TO (%s)", parent, ident, fromLit, toLit)); err != nil {
		if lockNotAvailable(err) {
			return false, true, nil
		}
		return false, false, fmt.Errorf("hopper: attach partition %s: %w", name, err)
	}
	if err = tx.Commit(ctx); err != nil {
		return false, false, fmt.Errorf("hopper: create partition %s: %w", name, err)
	}
	return true, false, nil
}

// literal formats a time as a timestamptz literal for DDL, which cannot take
// bind parameters.
func literal(t time.Time) string {
	return "'" + t.UTC().Format("2006-01-02 15:04:05.999999+00") + "'"
}

// quoteIdent double-quotes an identifier.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// Migrations.

// MigrationVersions returns the applied schema versions, or nil if the
// schema has never been installed.
func MigrationVersions(ctx context.Context, conn Conn, schema *Schema) ([]int, error) {
	var exists bool
	if err := conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", schema.Table("hopper_schema")).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return scanInts(conn.Query(ctx, "SELECT version FROM {{schema}}.hopper_schema ORDER BY version"))
}

// SchemaVersion implements driver.Executor.
func (e *Executor) SchemaVersion(ctx context.Context) (int, error) {
	versions, err := MigrationVersions(ctx, e.Conn, e.Schema)
	if err != nil {
		return 0, fmt.Errorf("hopper: read schema version: %w", err)
	}
	if len(versions) == 0 {
		return 0, nil
	}
	return slices.Max(versions), nil
}

var (
	scanStrings = collect(func(r Row) (string, error) {
		var s string
		err := r.Scan(&s)
		return s, err
	})
	scanInts = collect(func(r Row) (int, error) {
		var v int
		err := r.Scan(&v)
		return v, err
	})
)

// MigrationApply runs a migration script and records (up) or removes (down)
// its version, atomically. The script may hold several statements, so the
// transport must run it as a simple query (no parameters).
func MigrationApply(ctx context.Context, conn Conn, schema *Schema, version int, script string, up bool) (err error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback(context.WithoutCancel(ctx)))
		}
	}()
	if up {
		var exists bool
		if err = tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", schema.Name).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			if _, err = tx.Exec(ctx, "CREATE SCHEMA "+quoteIdent(schema.Name)); err != nil {
				return err
			}
		}
	}
	// Migration scripts use unqualified DDL; this setting is transaction-local
	// and never changes the application's pooled connections.
	if _, err = tx.Exec(ctx, "SELECT set_config('search_path', $1, true)", quoteIdent(schema.Name)); err != nil {
		return err
	}
	if up {
		if _, err = tx.Exec(ctx, script); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO {{schema}}.hopper_schema (version) VALUES ($1)", version); err != nil {
			return err
		}
	} else {
		// The version row goes first: the down script of version 1 drops
		// hopper_schema itself.
		if _, err = tx.Exec(ctx, "DELETE FROM {{schema}}.hopper_schema WHERE version = $1", version); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, script); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// MigrationLockKey is the pg_advisory_lock key held while migrating.
const MigrationLockKey int64 = 0x686f707065725f6d // "hopper_m"
