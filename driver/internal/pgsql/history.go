package pgsql

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
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

// HistoryLockKey is the pg_advisory_xact_lock key that serializes changes to
// the history partitions and their indexes: partition creation between
// overlapping leaders, and index attaches. It is
// combined with the schema, so installations sharing a database do not queue
// behind each other.
const HistoryLockKey int32 = 0x686f7068 // "hoph"

// lockHistoryMaintenance takes HistoryLockKey for the rest of tx.
func lockHistoryMaintenance(ctx context.Context, tx Conn, schema string) error {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1), $2)", schema, HistoryLockKey); err != nil {
		return fmt.Errorf("hopper: lock history maintenance: %w", err)
	}
	return nil
}

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
	lockTimeout := lockTimeoutSetting(params.LockTimeout)
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
			lacking, err := e.lackingInvalidIndexes(ctx, g.parent)
			if err != nil {
				return res, err
			}
			blocked := false
			for name := range existing {
				from, ok := g.parse(name)
				if !ok || !from.Add(g.period).Before(cutoff) {
					continue
				}
				if _, ok := lacking[name]; ok {
					res.Kept = append(res.Kept, name)
					continue
				}
				if blocked {
					res.Deferred = append(res.Deferred, name)
					continue
				}
				outcome, err := e.dropPartition(ctx, g, name, lockTimeout)
				if err != nil {
					return res, err
				}
				switch outcome {
				case partitionDropped:
					res.Dropped = append(res.Dropped, name)
				case partitionKept:
					res.Kept = append(res.Kept, name)
				case partitionDeferred:
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

// defaultDDLLockTimeout bounds how long a partition's creation or drop, a
// DEFAULT partition's build's wait for its table's HistoryIndexLockKey, or an
// index's attach
// or repair waits for its lock, unless HistoryMaintainParams or
// HistoryIndexParams.LockTimeout says otherwise. While it waits, every
// statement that needs the same table queues behind it, and a transaction
// that wrote history (a worker's CompleteTx) holds it off until that
// transaction ends; the next pass retries.
const defaultDDLLockTimeout = 200 * time.Millisecond

// lockTimeoutSetting formats d as a lock_timeout value: defaultDDLLockTimeout
// if d is zero, and at least a millisecond.
func lockTimeoutSetting(d time.Duration) string {
	return fmt.Sprintf("%dms", max(cmp.Or(d, defaultDDLLockTimeout), time.Millisecond).Milliseconds())
}

// setLockTimeout bounds the lock waits of the rest of tx by lockTimeout, a
// lockTimeoutSetting value.
func setLockTimeout(ctx context.Context, tx Conn, lockTimeout string) error {
	_, err := tx.Exec(ctx, "SELECT set_config('lock_timeout', $1, true)", lockTimeout)
	return err
}

// sqlState returns err's SQLSTATE, or "" if it carries none.
func sqlState(err error) string {
	var coded interface{ SQLState() string }
	if errors.As(err, &coded) {
		return coded.SQLState()
	}
	return ""
}

// lockNotAvailable reports whether err is a lock_timeout expiry.
func lockNotAvailable(err error) bool { return sqlState(err) == "55P03" }

// waitEnded reports whether err ended a wait bounded by setLockTimeout: a
// lock_timeout expiry, or a session statement_timeout that ran out first,
// which it may since lock_timeout bounds each of a statement's lock waits
// rather than their sum. ctx's own cancel is neither. It is for statements
// that only take locks, or change the catalog once they have them: a
// statement_timeout in one that scans a table may be its work running long.
func waitEnded(ctx context.Context, err error) bool {
	return lockNotAvailable(err) || (ctx.Err() == nil && sqlState(err) == "57014")
}

// deadlocked reports whether err is a deadlock the server broke.
func deadlocked(err error) bool { return sqlState(err) == "40P01" }

// undefinedTable reports whether err names a relation that does not exist.
func undefinedTable(err error) bool { return sqlState(err) == "42P01" }

// partitionOutcome is what dropPartition did with a partition past retention.
type partitionOutcome int

const (
	partitionDropped partitionOutcome = iota
	// partitionKept waits for its outcome table's DEFAULT partition's build.
	partitionKept
	// partitionDeferred could not get its locks, for the next pass to retry.
	partitionDeferred
)

// dropPartition drops a partition past retention. DROP takes a brief
// ACCESS EXCLUSIVE lock on the parent for the catalog change, once per
// period. (DETACH CONCURRENTLY would avoid it, but Postgres refuses it while
// a DEFAULT partition exists.) The wait is bounded by lockTimeout: a drop
// that cannot get the lock in time, or that the deadlock detector picks over
// a concurrent index build waiting for it, is deferred.
//
// A build on the outcome table's DEFAULT partition holds HistoryIndexLockKey
// combined with the table for its whole run. DROP locks the table and then
// the DEFAULT partition, whose new constraint it writes: it would hold every
// query of the table up while it waits for the build, which waits for this
// transaction's snapshot in turn. The partition is kept instead, and the
// key, held in share mode to the end of the transaction, keeps such a build
// from starting before the drop commits.
func (e *Executor) dropPartition(ctx context.Context, g historyGroup, name, lockTimeout string) (partitionOutcome, error) {
	var kept bool
	err := e.withTx(ctx, func(tx Tx) error {
		if err := setLockTimeout(ctx, tx, lockTimeout); err != nil {
			return err
		}
		if hasHistoryIndexes(g.parent) {
			free, err := tryHistoryIndexLockShared(ctx, tx, e.Schema.Name+"."+g.parent)
			if err != nil {
				return fmt.Errorf("hopper: lock history indexing: %w", err)
			}
			if kept = !free; kept {
				return nil
			}
		}
		_, err := tx.Exec(ctx, "DROP TABLE IF EXISTS "+e.Schema.Table(name))
		return err
	})
	switch {
	case waitEnded(ctx, err) || deadlocked(err):
		return partitionDeferred, nil
	case err != nil:
		return 0, fmt.Errorf("hopper: drop partition %s: %w", name, err)
	case kept:
		return partitionKept, nil
	}
	return partitionDropped, nil
}

// lackingInvalidIndexes lists the partitions of the outcome table parent
// that lack one of its invalid indexes, which retention keeps until
// HistoryIndex has attached them. Postgres marks an outcome table's index
// valid only in the attach that completes it, so dropping the last partition
// it lacks first would leave the index invalid for good on releases before
// May 2026 (18.4, 17.10, 16.14, 15.18, 14.23), where attaching an index
// again does not revalidate it. Keeping every partition that lacks one,
// rather than only the last, needs no lock: a partition only ever goes from
// lacking an index to having it attached, so a list read before the drop
// keeps at worst a partition indexed since, which a later pass drops. A
// build runs only on a partition that lacks its index, so none is dropped
// under it. The cost is building, once after an upgrade declared an index,
// the partitions that retention drops next.
func (e *Executor) lackingInvalidIndexes(ctx context.Context, parent string) (map[string]struct{}, error) {
	if !hasHistoryIndexes(parent) {
		return nil, nil
	}
	invalid, err := scanStrings(e.Conn.Query(ctx, `
		SELECT c.relname FROM pg_index x JOIN pg_class c ON c.oid = x.indexrelid
		WHERE x.indrelid = to_regclass($1) AND NOT x.indisvalid`, e.Schema.Table(parent)))
	if err != nil {
		return nil, fmt.Errorf("hopper: check indexes of %s: %w", parent, err)
	}
	lacking := map[string]struct{}{}
	for _, x := range historyIndexes {
		if x.parent != parent || !slices.Contains(invalid, x.name()) {
			continue
		}
		unindexed, err := scanStrings(e.Conn.Query(ctx, unindexedPartitions, e.Schema.Table(parent), e.Schema.Table(x.name())))
		if err != nil {
			return nil, fmt.Errorf("hopper: check index %s: %w", x.name(), err)
		}
		for _, n := range unindexed {
			lacking[n] = struct{}{}
		}
	}
	return lacking, nil
}

// historyIndex is an index declared ON ONLY a history outcome table by a
// schema upgrade, which each partition gets as parent+suffix.
type historyIndex struct {
	parent, suffix, definition string
}

func (x historyIndex) name() string { return x.parent + x.suffix }

// historyIndexes are the ordering-key index (schema v13) and the unique-key
// index (schema v14) of each outcome table.
var historyIndexes = []historyIndex{
	{"hopper_job_history_completed", "_ordering", "(ordering_key, id) WHERE ordering_key IS NOT NULL"},
	{"hopper_job_history_failed", "_ordering", "(ordering_key, id) WHERE ordering_key IS NOT NULL"},
	{"hopper_job_history_completed", "_unique", "(kind, unique_key, finalized_at) WHERE unique_key IS NOT NULL"},
	{"hopper_job_history_failed", "_unique", "(kind, unique_key, finalized_at) WHERE unique_key IS NOT NULL"},
}

// hasHistoryIndexes reports whether an upgrade declared history indexes on
// the outcome table parent.
func hasHistoryIndexes(parent string) bool {
	return slices.ContainsFunc(historyIndexes, func(x historyIndex) bool { return x.parent == parent })
}

// unindexedPartitions lists the partitions of an outcome table ($1) that have
// no index attached to its history index $2, or none if $2 does not exist.
const unindexedPartitions = `
	SELECT c.relname FROM pg_inherits i
	JOIN pg_class c ON c.oid = i.inhrelid
	WHERE i.inhparent = to_regclass($1) AND to_regclass($2) IS NOT NULL
	  AND NOT EXISTS (
	    SELECT 1 FROM pg_index x JOIN pg_inherits xi ON xi.inhrelid = x.indexrelid
	    WHERE x.indrelid = c.oid AND xi.inhparent = to_regclass($2))`

// HistoryIndexLockKey is the pg_advisory_xact_lock key held while a history
// partition is indexed, combined with the schema like HistoryLockKey. Two
// concurrent CREATE INDEX CONCURRENTLY on one table can deadlock, so
// overlapping leaders index one at a time; the one that does not get the
// lock leaves the work to the other.
//
// A build on an outcome table's DEFAULT partition also holds the key
// combined with the schema and that table. Partition
// creation under the table tries it without waiting, and leaves the
// partition to its next pass while the build runs: its attach scans the
// DEFAULT partition, which the build holds, while the build waits for the
// attach's transaction to end. A build elsewhere does not conflict with the
// attach. Retention tries the table's key as well, and keeps the partitions
// past it while it is held: a drop locks the DEFAULT partition too. The
// build waits for the table's key, which creation and retention hold only
// briefly.
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

// tryHistoryIndexLockShared tries HistoryIndexLockKey combined with name in
// share mode, for the rest of tx. Retention and partition creation hold a
// table's key this way, so that they hold off a build, which
// takes it exclusively, without holding off each other: a key found taken
// means a build.
func tryHistoryIndexLockShared(ctx context.Context, tx Conn, name string) (bool, error) {
	var locked bool
	err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock_shared(hashtext($1), $2)", name, HistoryIndexLockKey).Scan(&locked)
	return locked, err
}

// lockHistoryIndex waits for HistoryIndexLockKey combined with name, for the
// rest of tx.
func lockHistoryIndex(ctx context.Context, tx Conn, name string) error {
	_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1), $2)", name, HistoryIndexLockKey)
	return err
}

// HistoryIndex implements driver.Executor. It builds each history index of
// every partition that has none attached yet, concurrently so that
// finalizes keep moving rows into history, and attaches it. Partitions
// created since an index was declared get theirs when they are attached, so
// this finds work only after an upgrade, once per partition and index. An index a canceled build
// left invalid is dropped and built again. A build canceled by the
// session's statement timeout is reported, and the pass goes on to the next
// partition.
func (e *Executor) HistoryIndex(ctx context.Context, params driver.HistoryIndexParams) (driver.HistoryIndexResult, error) {
	var res driver.HistoryIndexResult
	if e.InTx {
		return res, errors.New("hopper: history indexing must run on the pool, not in a transaction")
	}
	skip := slices.Clone(params.Skip)
	lockTimeout := lockTimeoutSetting(params.LockTimeout)
	// Whether the server's re-attach repairs an index is asked at most once
	// a pass, however many indexes need repair.
	probe := sync.OnceValues(func() (bool, error) { return e.reattachRevalidates(ctx, lockTimeout) })
	var repairErr error
	for _, x := range historyIndexes {
		parent, parentIndex := x.parent, x.name()
		leaves, err := scanStrings(e.Conn.Query(ctx, unindexedPartitions, e.Schema.Table(parent), e.Schema.Table(parentIndex)))
		if err != nil {
			return res, errors.Join(repairErr, fmt.Errorf("hopper: list unindexed partitions of %s: %w", parent, err))
		}
		// The outcome table's index can be valid only once every partition
		// has its index, so it is checked only after a pass that left none
		// out.
		complete := true
		for _, leaf := range leaves {
			if slices.Contains(skip, leaf) {
				complete = false
				continue
			}
			done, err := e.indexHistoryPartitionLocked(ctx, x, leaf, lockTimeout)
			switch {
			case errors.Is(err, errStatementTimeout):
				res.TimedOut = append(res.TimedOut, leaf)
				skip = append(skip, leaf)
				complete = false
				continue
			case errors.Is(err, errBuildDeferred), errors.Is(err, errAttachDeferred):
				// The partition's other indexes would wait behind the same
				// holder of its lock, or be built only for their attach to.
				res.Deferred = append(res.Deferred, leaf+x.suffix)
				skip = append(skip, leaf)
				complete = false
				continue
			}
			if err != nil {
				return res, errors.Join(repairErr, err)
			}
			if !done {
				// Another session holds the schema's build lock: another
				// leader's pass, or one that stalled. It is reported, so that
				// a holder that never lets go is warned about.
				res.Busy = leaf + x.suffix
				return res, repairErr
			}
		}
		if !complete || slices.Contains(skip, parentIndex) {
			continue
		}
		invalid, deferred, err := e.revalidateIndex(ctx, parentIndex, lockTimeout, probe)
		switch {
		case err != nil:
			// A repair that fails, its probe included, is reported once the
			// indexes after it are built.
			repairErr = errors.Join(repairErr, err)
		case invalid:
			res.Invalid = append(res.Invalid, parentIndex)
		case deferred:
			res.Deferred = append(res.Deferred, parentIndex)
		}
	}
	return res, repairErr
}

// errStatementTimeout marks a build the session's statement_timeout
// canceled.
var errStatementTimeout = errors.New("hopper: index build exceeded statement_timeout")

// errAttachDeferred marks a built index whose attach could not get its locks
// within the lock timeout. The index stays, valid, and a later pass attaches
// it without building it again.
var errAttachDeferred = errors.New("hopper: index attach deferred")

// errBuildDeferred marks a DEFAULT partition's build that could not get its
// table's HistoryIndexLockKey within the lock timeout; a later pass builds
// it.
var errBuildDeferred = errors.New("hopper: index build deferred")

// indexHistoryPartitionLocked indexes one partition under
// HistoryIndexLockKey, and reports false, having done nothing, if another
// leader holds it, indexing. The lock is held per partition, so that
// creation and retention defer a table's partitions for at most one build
// of its DEFAULT partition.
func (e *Executor) indexHistoryPartitionLocked(ctx context.Context, x historyIndex, leaf, lockTimeout string) (bool, error) {
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
	if err != nil {
		return false, fmt.Errorf("hopper: lock history indexing: %w", err)
	}
	if !locked {
		return false, nil
	}
	if leaf == x.parent+"_default" {
		// Partition creation and retention hold the table's key only while
		// they create or drop a partition of the table, never waiting for a
		// build meanwhile, so the build waits for them. The wait is bounded
		// like a drop's, so that a transaction left open with the key does
		// not stall the pass, and every other leader's, behind the schema's
		// key.
		if err := setLockTimeout(ctx, lock, lockTimeout); err != nil {
			return false, fmt.Errorf("hopper: lock history indexing: %w", err)
		}
		if err := lockHistoryIndex(ctx, lock, e.Schema.Name+"."+x.parent); err != nil {
			if waitEnded(ctx, err) {
				return false, fmt.Errorf("%w: %s: %w", errBuildDeferred, leaf, err)
			}
			return false, fmt.Errorf("hopper: lock history indexing: %w", err)
		}
	}
	// A statement of its own lets go of the portal the lock's query left
	// open, whose snapshot the build would wait for, and it keeps the server
	// from ending the idle transaction, and the locks, during a long build.
	if _, err := lock.Exec(ctx, "SET LOCAL idle_in_transaction_session_timeout = 0"); err != nil {
		return false, fmt.Errorf("hopper: lock history indexing: %w", err)
	}
	return true, e.indexHistoryPartition(ctx, x, leaf, lockTimeout)
}

// indexHistoryPartition builds one partition's index and attaches it to its
// outcome table's index. A partition dropped by retention since it was
// listed, or during the build, is skipped.
func (e *Executor) indexHistoryPartition(ctx context.Context, x historyIndex, leaf, lockTimeout string) error {
	err := e.buildHistoryIndex(ctx, x, leaf, lockTimeout)
	if err == nil || errors.Is(err, errStatementTimeout) || errors.Is(err, errAttachDeferred) {
		return err
	}
	var gone bool
	if qerr := e.Conn.QueryRow(ctx, "SELECT to_regclass($1) IS NULL", e.Schema.Table(leaf)).Scan(&gone); qerr == nil && gone {
		return nil
	}
	return err
}

// buildHistoryIndex builds leaf's index and attaches it. The attach waits
// for its locks no longer than lockTimeout: it queues every query of the
// partition behind it, and a transaction that read the partition since the
// build holds it off until that transaction ends.
func (e *Executor) buildHistoryIndex(ctx context.Context, x historyIndex, leaf, lockTimeout string) error {
	index := leaf + x.suffix
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
			" "+x.definition); err != nil {
			if ctx.Err() == nil && sqlState(err) == "57014" {
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
	if err := e.attachIndex(ctx, x.name(), index, lockTimeout); err != nil {
		if waitEnded(ctx, err) {
			return fmt.Errorf("%w: %s: %w", errAttachDeferred, index, err)
		}
		return err
	}
	return nil
}

// attachIndex attaches a partition's index under the history maintenance
// lock, which partition creation (whose attach indexes the new partition)
// also takes. Postgres marks a partitioned index valid in the attach that
// completes it, counting the attached indexes it can see; two attaches
// committing side by side would each miss the other's and leave the parent
// invalid for good.
func (e *Executor) attachIndex(ctx context.Context, parentIndex, index, lockTimeout string) (err error) {
	tx, err := e.Conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	if err = setLockTimeout(ctx, tx, lockTimeout); err != nil {
		return err
	}
	if err = lockHistoryMaintenance(ctx, tx, e.Schema.Name); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "ALTER INDEX "+e.Schema.Table(parentIndex)+" ATTACH PARTITION "+e.Schema.Table(index)); err != nil {
		return fmt.Errorf("hopper: attach index %s: %w", index, err)
	}
	return tx.Commit(ctx)
}

// revalidateIndex marks an outcome table's index valid when no partition
// lacks its index any more but the attach that would have completed it never
// ran, because an earlier release's retention dropped the last partition
// without one first (lackingInvalidIndexes). Attaching a partition's index again makes
// Postgres count the attached indexes again from its May 2026 releases on;
// earlier ones do not, and it reports invalid when the index stays invalid.
// The attach waits for its locks no longer than lockTimeout, the way a
// partition's creation or drop does, since every query of that partition
// queues behind it; a pass that cannot get them reports the index deferred,
// and one whose partition was dropped since it was picked leaves the index to
// the next. The partition is the furthest ahead, which finalizes do not write
// to yet and retention does not drop, picked by the partition's name rather
// than its index's, which Postgres may have truncated: the attach locks the
// index exclusively, which would hold up the finalizes into the current
// period's partition. probe is the pass's reattachRevalidates.
func (e *Executor) revalidateIndex(ctx context.Context, parentIndex, lockTimeout string, probe func() (bool, error)) (invalid, deferred bool, err error) {
	var child sql.NullString
	err = e.Conn.QueryRow(ctx, `
		SELECT (SELECT c.relname FROM pg_inherits i
		        JOIN pg_class c ON c.oid = i.inhrelid
		        JOIN pg_index ci ON ci.indexrelid = i.inhrelid
		        JOIN pg_class t ON t.oid = ci.indrelid
		        WHERE i.inhparent = x.indexrelid
		        ORDER BY pg_get_expr(t.relpartbound, t.oid) = 'DEFAULT', t.relname DESC LIMIT 1)
		FROM pg_index x WHERE x.indexrelid = to_regclass($1) AND NOT x.indisvalid`, e.Schema.Table(parentIndex)).Scan(&child)
	if errors.Is(err, ErrNoRows) || (err == nil && !child.Valid) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("hopper: check index %s: %w", parentIndex, err)
	}
	// On a release whose attach does not count again, the attach would only
	// queue the partition's queries behind it for nothing. A probe held off
	// by another session's (another leader's, or one that stalled) defers
	// the repair like the attach's own locks.
	repairs, err := probe()
	if err != nil {
		if waitEnded(ctx, err) {
			return false, true, nil
		}
		return false, false, err
	}
	if !repairs {
		return true, false, nil
	}
	if err := e.attachIndex(ctx, parentIndex, child.String, lockTimeout); err != nil {
		switch {
		case waitEnded(ctx, err):
			return false, true, nil
		case undefinedTable(err):
			return false, false, nil
		}
		return false, false, err
	}
	var valid bool
	if err := e.Conn.QueryRow(ctx, "SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)",
		e.Schema.Table(parentIndex)).Scan(&valid); err != nil {
		return false, false, fmt.Errorf("hopper: check index %s: %w", parentIndex, err)
	}
	return !valid, false, nil
}

// reattachRevalidates reports whether the server's attach of an index already
// attached marks its parent valid. It tries on tables of its own in the
// schema, which partition maintenance needs the right to create in anyway,
// in a transaction it rolls back, the case revalidateIndex repairs: a
// partitioned index left invalid by the drop of the one partition that
// lacked it. It asks the server rather than its version, which forks number
// their own way, and asks again on each pass that has an index to repair,
// which the leader backs off: a server updated in place, or a fork that
// backports the change under the same version, is repaired without a
// restart. drivertest's ReattachRevalidates repeats it for the tests'
// expectations. Its tables' names are the same in every session using the
// schema, so a concurrent probe, uncommitted, holds this one's off until it
// ends; the wait is bounded by lockTimeout, so that a leader that stalls
// mid-probe does not stall this one's pass.
func (e *Executor) reattachRevalidates(ctx context.Context, lockTimeout string) (valid bool, err error) {
	tx, err := e.Conn.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("hopper: probe index revalidation: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := setLockTimeout(ctx, tx, lockTimeout); err != nil {
		return false, fmt.Errorf("hopper: probe index revalidation: %w", err)
	}
	table, index := e.Schema.Table("hopper_revalidate_probe"), e.Schema.Table("hopper_revalidate_probe_k")
	for _, q := range []string{
		"CREATE TABLE " + table + " (k int) PARTITION BY LIST (k)",
		"CREATE TABLE " + e.Schema.Table("hopper_revalidate_probe_1") + " PARTITION OF " + table + " FOR VALUES IN (1)",
		"CREATE TABLE " + e.Schema.Table("hopper_revalidate_probe_2") + " PARTITION OF " + table + " FOR VALUES IN (2)",
		"CREATE INDEX hopper_revalidate_probe_k ON ONLY " + table + " (k)",
		"CREATE INDEX hopper_revalidate_probe_1_k ON " + e.Schema.Table("hopper_revalidate_probe_1") + " (k)",
		"ALTER INDEX " + index + " ATTACH PARTITION " + e.Schema.Table("hopper_revalidate_probe_1_k"),
		"DROP TABLE " + e.Schema.Table("hopper_revalidate_probe_2"),
		"ALTER INDEX " + index + " ATTACH PARTITION " + e.Schema.Table("hopper_revalidate_probe_1_k"),
	} {
		if _, err := tx.Exec(ctx, q); err != nil {
			return false, fmt.Errorf("hopper: probe index revalidation: %w", err)
		}
	}
	if err := tx.QueryRow(ctx, "SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)", index).Scan(&valid); err != nil {
		return false, fmt.Errorf("hopper: probe index revalidation: %w", err)
	}
	return valid, nil
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
	// already created the partition. The lock timeout bounds the wait for
	// it too: a leader that stalls while it holds it, creating a partition
	// or attaching an index, would otherwise stall this one's loop, lease
	// renewal included.
	// Partitions are created periods ahead, so this one is deferred instead.
	if err = setLockTimeout(ctx, tx, lockTimeout); err != nil {
		return false, false, fmt.Errorf("hopper: create partition %s: %w", name, err)
	}
	if err = lockHistoryMaintenance(ctx, tx, e.Schema.Name); err != nil {
		if waitEnded(ctx, err) {
			return false, true, nil
		}
		return false, false, err
	}
	// An index being built concurrently on this table's DEFAULT partition
	// (HistoryIndex) holds HistoryIndexLockKey combined with the table, and
	// the attach below would deadlock with it. Partitions are created periods
	// ahead, so this one is deferred to the next pass instead.
	if hasHistoryIndexes(g.parent) {
		var free bool
		if free, err = tryHistoryIndexLockShared(ctx, tx, e.Schema.Name+"."+g.parent); err != nil {
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
	// off until it ends, so the wait is bounded. The attach's locks are
	// taken first, in statements that do nothing else, so that a
	// statement_timeout that ends the ATTACH itself, scanning a DEFAULT
	// partition grown large, fails the pass rather than read as a wait.
	if _, err = tx.Exec(ctx, "CREATE TABLE "+ident+" (LIKE "+e.Schema.Table(g.root)+" INCLUDING DEFAULTS INCLUDING CONSTRAINTS)"); err != nil {
		if waitEnded(ctx, err) {
			return false, true, nil
		}
		return false, false, fmt.Errorf("hopper: create partition %s: %w", name, err)
	}
	for _, q := range []string{
		"LOCK TABLE ONLY " + parent + " IN SHARE UPDATE EXCLUSIVE MODE",
		"LOCK TABLE ONLY " + e.Schema.Table(g.parent+"_default") + " IN ACCESS EXCLUSIVE MODE",
	} {
		if _, err = tx.Exec(ctx, q); err != nil {
			if waitEnded(ctx, err) {
				return false, true, nil
			}
			return false, false, fmt.Errorf("hopper: attach partition %s: %w", name, err)
		}
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
