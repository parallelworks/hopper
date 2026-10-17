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
	// mirrors is, for a table of history's tags rows, the outcome table
	// whose partitions its own follow period for period.
	mirrors string
}

// HistoryLockKey is the pg_advisory_xact_lock key that serializes changes to
// the history partitions and their indexes: partition creation between
// overlapping leaders, and index attaches. It is
// combined with the schema, so installations sharing a database do not queue
// behind each other.
const HistoryLockKey int32 = 0x686f7068 // "hoph"

// lockHistoryMaintenance takes HistoryLockKey for the rest of tx.
func lockHistoryMaintenance(ctx context.Context, tx Conn, schema string) error {
	if err := advisoryXactLock(ctx, tx, schema, HistoryLockKey, false); err != nil {
		return fmt.Errorf("hopper: lock history maintenance: %w", err)
	}
	return nil
}

var historyGroups = []historyGroup{
	{root: "hopper_job_history", parent: "hopper_job_history_completed", timeColumn: "finalized_at", period: time.Hour, ahead: 3, format: "2006010215"},
	{root: "hopper_job_history", parent: "hopper_job_history_failed", timeColumn: "finalized_at", period: 24 * time.Hour, ahead: 2, format: "20060102"},
	{root: "hopper_stream_events", parent: "hopper_stream_events", timeColumn: "created_at", period: 24 * time.Hour, ahead: 2, format: "20060102"},
	// History's tags rows (schema v17), partitioned by outcome as history is
	// and then by day, so that retention drops them with their jobs and a
	// walk of a tag's rows plans a partition a day: by the hour, a month's
	// retention of completed jobs would be 720. A day's tags partition is
	// dropped only once every history partition of that day is gone, in a
	// statement of its own: these groups come after history's, so a pass
	// drops a day's last history partition and its tags partition together.
	{root: "hopper_job_history_tags", parent: "hopper_job_history_tags_completed", timeColumn: "finalized_at", period: 24 * time.Hour, ahead: 2, format: "20060102", mirrors: "hopper_job_history_completed"},
	{root: "hopper_job_history_tags", parent: "hopper_job_history_tags_failed", timeColumn: "finalized_at", period: 24 * time.Hour, ahead: 2, format: "20060102", mirrors: "hopper_job_history_failed"},
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
	// A group whose maintenance fails leaves the others to theirs: the
	// partitions ahead of each, and its retention, are independent.
	var errs []error
	// retained records the groups whose retention ran in full this pass. A
	// tags group drops its time partitions only once the history group it
	// mirrors has pruned its DEFAULT partition: a period whose history
	// partition never existed has its jobs there, and dropping their tags
	// partition before them would leave jobs in history that no listing by
	// tag finds.
	retained := map[string]bool{}
	for _, g := range historyGroups {
		// A tags group keeps its rows as long as the history it mirrors.
		mayDrop := g.mirrors == "" || retained[g.mirrors]
		ok, err := e.maintainGroup(ctx, g, now, retention[cmp.Or(g.mirrors, g.parent)], lockTimeout, mayDrop, &res)
		if err != nil {
			errs = append(errs, err)
		}
		retained[g.parent] = ok
	}
	return res, errors.Join(errs...)
}

// maintainGroup creates g's partitions ahead and applies its retention, keep,
// adding what it did to res. A step that fails leaves the rest of that step
// to the next pass, but not the group's other steps: a failure that lasts,
// such as an attach whose scan of a DEFAULT partition grown large outlasts
// statement_timeout, would otherwise also stop the drops and the DEFAULT
// prune that let the group recover. The errors are reported together. The
// time partitions are dropped only when mayDrop. It reports whether its
// retention ran in full: every expired partition dropped, kept or deferred,
// and the DEFAULT partition pruned, or retention is off.
func (e *Executor) maintainGroup(ctx context.Context, g historyGroup, now time.Time, keep time.Duration, lockTimeout string, mayDrop bool, res *driver.HistoryMaintainResult) (retained bool, err error) {
	existing, err := e.partitionsOf(ctx, g.parent)
	if err != nil {
		// Without the list, neither the partitions ahead nor those past
		// retention are known, but the DEFAULT partition's prune needs none.
		return false, errors.Join(err, e.pruneDefault(ctx, g, keep, res))
	}
	var errs []error

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
			errs = append(errs, err)
			break
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
	var expired []string
	if keep > 0 {
		cutoff := now.Add(-keep)
		for name := range existing {
			if from, ok := g.parse(name); ok && from.Add(g.period).Before(cutoff) {
				expired = append(expired, name)
			}
		}
	}
	dropsFailed := false
	if mayDrop && len(expired) > 0 {
		// Read once, before the first drop: most passes drop nothing.
		lacking := sync.OnceValues(func() (map[string][]string, error) { return e.lackingInvalidIndexes(ctx, g.parent) })
		mirrored, err := e.mirroredPartitions(ctx, g, expired)
		if err != nil {
			errs = append(errs, err)
			dropsFailed = true
			expired = nil
		}
		blocked := false
		for _, name := range expired {
			// A tags partition goes only once its history partition is
			// gone: dropped first, the jobs it indexes would drop out of
			// listings by tag. The two are never dropped together: a
			// finalize locks history before its tags, and a listing by
			// tag the tags before history, so a drop holding one parent
			// while it waits for the other could deadlock with either.
			if mirrored[name] {
				continue
			}
			unindexed, err := lacking()
			if err != nil {
				errs = append(errs, err)
				dropsFailed = true
				break
			}
			needs := unindexed[name]
			if !lackedByDefault(needs, unindexed[g.parent+"_default"]) {
				res.Kept = append(res.Kept, name)
				continue
			}
			if blocked {
				res.Deferred = append(res.Deferred, name)
				continue
			}
			outcome, err := e.dropPartition(ctx, g, name, needs, lockTimeout)
			if err != nil {
				errs = append(errs, err)
				dropsFailed = true
				break
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
	}
	pruneErr := e.pruneDefault(ctx, g, keep, res)
	return mayDrop && !dropsFailed && pruneErr == nil, errors.Join(append(errs, pruneErr)...)
}

// pruneDefault deletes the rows of g's DEFAULT partition past retention,
// keep, which cannot be dropped by period, adding their count to res. A zero
// keep keeps everything. History's delete trigger, a row trigger and so its
// partitions' too, deletes the jobs' tags rows, which are not counted. A tags
// row in a DEFAULT partition can belong to a job in one of history's time
// partitions, which stays until its whole period is past, so the tags' prune
// keeps the rows whose job is still there, and takes those a job left
// without its trigger (a TRUNCATE of a partition).
func (e *Executor) pruneDefault(ctx context.Context, g historyGroup, keep time.Duration, res *driver.HistoryMaintainResult) error {
	if keep <= 0 {
		return nil
	}
	table := e.Schema.Table(g.parent + "_default")
	if g.mirrors != "" {
		if _, err := e.Conn.Exec(ctx, fmt.Sprintf(`
			DELETE FROM %s t WHERE finalized_at < now() - make_interval(secs => $1::float8)
			  AND NOT EXISTS (SELECT 1 FROM %s h WHERE h.state = t.state AND h.finalized_at = t.finalized_at AND h.id = t.id)`,
			table, e.Schema.Table("hopper_job_history")), keep.Seconds()); err != nil {
			return fmt.Errorf("hopper: prune %s_default: %w", g.parent, err)
		}
		return nil
	}
	n, err := e.Conn.Exec(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE %s < now() - make_interval(secs => $1::float8)", table, g.timeColumn),
		keep.Seconds())
	if err != nil {
		return fmt.Errorf("hopper: prune %s_default: %w", g.parent, err)
	}
	res.Pruned += n
	return nil
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
	return lockNotAvailable(err) || statementTimedOut(ctx, err)
}

// statementTimedOut reports whether err is a statement canceled by the
// server, such as by the session's statement_timeout, rather than by ctx.
func statementTimedOut(ctx context.Context, err error) bool {
	return ctx.Err() == nil && sqlState(err) == "57014"
}

// deadlocked reports whether err is a deadlock the server broke.
func deadlocked(err error) bool { return sqlState(err) == "40P01" }

// undefinedTable reports whether err names a relation that does not exist.
func undefinedTable(err error) bool { return sqlState(err) == "42P01" }

// partitionOutcome is what dropPartition did with a partition past retention.
type partitionOutcome int

const (
	partitionDropped partitionOutcome = iota
	// partitionKept waits for its outcome table's DEFAULT partition's
	// build, or for its own, once the DEFAULT partition has an index it
	// lacks.
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
// A build holds HistoryIndexLockKey combined with the outcome table for its
// whole run, exclusively on the DEFAULT partition and in share mode
// elsewhere. DROP locks the table and then the partition and the DEFAULT
// partition, whose new constraint it writes: it would hold every query of
// the table up while it waits for a build on either, which waits for this
// transaction's snapshot in turn. The partition is kept instead. The key is
// tried in share mode for a partition that has every index (needs empty),
// which no build runs on, so that only the DEFAULT partition's build keeps
// it, and exclusively for one that lacks some, which may be the one being
// built. Held to the end of the transaction, it keeps such a build from
// starting before the drop commits.
//
// A partition that lacks invalid indexes (needs) is dropped only while the
// DEFAULT partition still lacks them too, whose attach then completes them:
// the build that attaches it holds the key exclusively, so the check, made
// under the key, holds until the drop commits. It is made at read committed,
// whatever the session's default, so that it sees an attach that committed
// before the key was free.
func (e *Executor) dropPartition(ctx context.Context, g historyGroup, name string, needs []string, lockTimeout string) (partitionOutcome, error) {
	var kept bool
	err := e.withTx(ctx, func(tx Tx) error {
		if len(needs) > 0 {
			if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"); err != nil {
				return err
			}
		}
		if err := setLockTimeout(ctx, tx, lockTimeout); err != nil {
			return err
		}
		if hasHistoryIndexes(g.parent) {
			free, err := tryAdvisoryXactLock(ctx, tx, e.Schema.Name+"."+g.parent, HistoryIndexLockKey, len(needs) == 0)
			if err != nil {
				return fmt.Errorf("hopper: lock history indexing: %w", err)
			}
			if kept = !free; kept {
				return nil
			}
		}
		if len(needs) > 0 {
			indexes := make([]string, len(needs))
			for i, x := range needs {
				indexes[i] = e.Schema.Table(x)
			}
			if err := tx.QueryRow(ctx, `
				SELECT EXISTS (
				  SELECT 1 FROM unnest($2::text[]) n
				  JOIN pg_inherits xi ON xi.inhparent = to_regclass(n)
				  JOIN pg_index x ON x.indexrelid = xi.inhrelid
				  WHERE x.indrelid = to_regclass($1))`,
				e.Schema.Table(g.parent+"_default"), textArray(indexes)).Scan(&kept); err != nil {
				return fmt.Errorf("hopper: check indexes of %s_default: %w", g.parent, err)
			}
			if kept {
				return nil
			}
		}
		if err := e.dropTagsRowsOf(ctx, tx, g, name); err != nil {
			return err
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

func (e *Executor) dropTagsRowsOf(ctx context.Context, tx Tx, g historyGroup, name string) error {
	i := slices.IndexFunc(historyGroups, func(t historyGroup) bool { return t.mirrors == g.parent })
	if i < 0 || historyGroups[i].period <= g.period {
		return nil
	}
	from, ok := g.parse(name)
	if !ok {
		return nil
	}
	var exists bool
	if err := tx.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", e.Schema.Table(name)).Scan(&exists); err != nil || !exists {
		return err
	}
	_, err := tx.Exec(ctx, fmt.Sprintf(`
		DELETE FROM %s t USING %s h, %s.hopper_tags_of(h.tags) o(tag)
		WHERE h.tags IS NOT NULL AND t.tag = o.tag AND t.id = h.id AND t.state = h.state AND t.finalized_at = h.finalized_at
		  AND t.finalized_at >= $1 AND t.finalized_at < $2`,
		e.Schema.Table(historyGroups[i].parent), e.Schema.Table(name), quoteIdent(e.Schema.Name)), from, from.Add(g.period))
	return err
}

// lackingInvalidIndexes maps the partitions of the outcome table parent
// that lack some of its invalid indexes to those indexes. Postgres marks an
// outcome table's index valid only in the attach that completes it, so
// dropping the last partition it lacks first would leave the index invalid
// for good on releases before May 2026 (18.4, 17.10, 16.14, 15.18, 14.23),
// where attaching an index again does not revalidate it. Retention keeps a
// partition that lacks one until HistoryIndex has attached it, unless the
// DEFAULT partition, which retention never drops, lacks it too
// (lackedByDefault, rechecked by dropPartition). Keeping every such
// partition, rather than only the last, needs no lock: a partition only ever
// goes from lacking an index to having it attached, so a list read before
// the drop keeps at worst a partition indexed since, which a later pass
// drops. A build runs only on a partition that lacks its index, so none is
// dropped under it.
func (e *Executor) lackingInvalidIndexes(ctx context.Context, parent string) (map[string][]string, error) {
	if !hasHistoryIndexes(parent) {
		return nil, nil
	}
	invalid, err := scanStrings(e.Conn.Query(ctx, `
		SELECT c.relname FROM pg_index x JOIN pg_class c ON c.oid = x.indexrelid
		WHERE x.indrelid = to_regclass($1) AND NOT x.indisvalid`, e.Schema.Table(parent)))
	if err != nil {
		return nil, fmt.Errorf("hopper: check indexes of %s: %w", parent, err)
	}
	lacking := map[string][]string{}
	for _, x := range historyIndexes {
		if x.parent != parent || !slices.Contains(invalid, x.name()) {
			continue
		}
		unindexed, err := scanStrings(e.Conn.Query(ctx, unindexedPartitions, e.Schema.Table(parent), e.Schema.Table(x.name())))
		if err != nil {
			return nil, fmt.Errorf("hopper: check index %s: %w", x.name(), err)
		}
		for _, n := range unindexed {
			lacking[n] = append(lacking[n], x.name())
		}
	}
	return lacking, nil
}

// lackedByDefault reports whether the DEFAULT partition, lacking the indexes
// byDefault, lacks every one of needs, the indexes a partition lacks.
func lackedByDefault(needs, byDefault []string) bool {
	for _, x := range needs {
		if !slices.Contains(byDefault, x) {
			return false
		}
	}
	return true
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
// They come newest first and the DEFAULT partition last, the order
// HistoryIndex builds them in: while any other partition still lacks the
// index the DEFAULT one does too, so retention drops the partitions that
// expire before the pass reaches them rather than keep them for a build
// (lackingInvalidIndexes).
const unindexedPartitions = `
	SELECT c.relname FROM pg_inherits i
	JOIN pg_class c ON c.oid = i.inhrelid
	WHERE i.inhparent = to_regclass($1) AND to_regclass($2) IS NOT NULL
	  AND NOT EXISTS (
	    SELECT 1 FROM pg_index x JOIN pg_inherits xi ON xi.inhrelid = x.indexrelid
	    WHERE x.indrelid = c.oid AND xi.inhparent = to_regclass($2))
	ORDER BY pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT', c.relname DESC`

// HistoryIndexLockKey is the pg_advisory_xact_lock key held while a history
// partition is indexed, combined with the schema like HistoryLockKey. Two
// concurrent CREATE INDEX CONCURRENTLY on one table can deadlock, so
// overlapping leaders index one at a time; the one that does not get the
// lock leaves the work to the other.
//
// A build also holds the key combined with the schema and its outcome
// table: exclusively on the DEFAULT partition, in share mode elsewhere.
// Partition creation under the table tries it in share mode without
// waiting, and leaves the partition to its next pass while the DEFAULT
// partition's build runs: its attach scans the DEFAULT partition, which the
// build holds, while the build waits for the attach's transaction to end. A
// build elsewhere does not conflict with the attach. Retention tries the
// table's key as well, and keeps the partitions past it while a build holds
// it: a drop locks the DEFAULT partition too, and a drop of the partition
// being built would hold the table locked while it waits for the build. It
// tries the key in share mode for a partition that has every index, which
// no build runs on, and exclusively for one that lacks some. A build waits
// for the table's key, which creation and retention hold only briefly.
//
// The CREATE INDEX CONCURRENTLY cannot run in a transaction, so the locks are
// held by a transaction of their own, which stays open and idle on another
// connection for the build. A session-level lock would not do: behind a
// transaction pooler, the lock, the build and the unlock can each run on a
// different server connection, and the lock would outlive the build.
const HistoryIndexLockKey int32 = 0x686f7069 // "hopi"

// advisoryXactLock waits for the advisory lock key combined with name, in
// share mode if shared, for the rest of tx.
func advisoryXactLock(ctx context.Context, tx Conn, name string, key int32, shared bool) error {
	fn := "pg_advisory_xact_lock"
	if shared {
		fn += "_shared"
	}
	_, err := tx.Exec(ctx, "SELECT "+fn+"(hashtext($1), $2)", name, key)
	return err
}

// tryAdvisoryXactLock tries the advisory lock key combined with name, in
// share mode if shared, for the rest of tx, and reports whether it got it.
func tryAdvisoryXactLock(ctx context.Context, tx Conn, name string, key int32, shared bool) (bool, error) {
	fn := "pg_try_advisory_xact_lock"
	if shared {
		fn += "_shared"
	}
	var locked bool
	err := tx.QueryRow(ctx, "SELECT "+fn+"(hashtext($1), $2)", name, key).Scan(&locked)
	return locked, err
}

// HistoryIndex implements driver.Executor. It builds each history index of
// every partition that has none attached yet, concurrently so that
// finalizes keep moving rows into history, and attaches it. Partitions
// created since an index was declared get theirs when they are attached, so
// this finds work only after an upgrade, once per partition and index. An index a canceled build
// left invalid is dropped and built again. A build canceled by the
// session's statement timeout or by an operator is reported, and the pass
// goes on to the next partition; a migration's cancel ends the pass.
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
			case errors.Is(err, errBuildCanceled):
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
				return res, repairError(repairErr)
			}
		}
		if !complete || slices.Contains(params.SkipRepair, parentIndex) {
			continue
		}
		invalid, deferred, err := e.revalidateIndex(ctx, parentIndex, lockTimeout, probe)
		if err != nil {
			// A repair that fails, its probe included, is reported once the
			// indexes after it are built, and the index, which stays
			// invalid (revalidateIndex reports it so), with the indexes the
			// server cannot repair, so that the leader backs its retries off
			// alike. A failed check of whether the index is invalid at all
			// reports only its error.
			repairErr = errors.Join(repairErr, err)
		}
		switch {
		case invalid:
			res.Invalid = append(res.Invalid, parentIndex)
		case deferred:
			res.Deferred = append(res.Deferred, parentIndex)
		}
	}
	return res, repairError(repairErr)
}

// repairError wraps err, what a pass's failed repairs reported, in
// driver.ErrHistoryIndexRepair, or returns nil if none failed.
func repairError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", driver.ErrHistoryIndexRepair, err)
}

// errBuildCanceled marks a build canceled by the session's statement_timeout
// or by an operator, not by a migration; a later pass builds it again.
var errBuildCanceled = errors.New("hopper: index build canceled")

// errAttachDeferred marks a built index whose attach could not get its locks
// within the lock timeout. The index stays, valid, and a later pass attaches
// it without building it again.
var errAttachDeferred = errors.New("hopper: index attach deferred")

// errBuildDeferred marks a build that did not run: a DEFAULT partition's that
// could not get its table's HistoryIndexLockKey within the lock timeout, or
// one the deadlock detector canceled in favor of retention's drop of its
// partition. A later pass builds it, if the partition is still there.
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
	locked, err := tryAdvisoryXactLock(ctx, lock, e.Schema.Name, HistoryIndexLockKey, false)
	if err != nil {
		return false, fmt.Errorf("hopper: lock history indexing: %w", err)
	}
	if !locked {
		return false, nil
	}
	// Partition creation and retention hold the table's key only while they
	// create or drop a partition of the table, never waiting for a build
	// meanwhile, so the build waits for them. The wait is bounded like a
	// drop's, so that a transaction left open with the key does not stall
	// the pass, and every other leader's, behind the schema's key.
	if err := setLockTimeout(ctx, lock, lockTimeout); err != nil {
		return false, fmt.Errorf("hopper: lock history indexing: %w", err)
	}
	if err := advisoryXactLock(ctx, lock, e.Schema.Name+"."+x.parent, HistoryIndexLockKey, leaf != x.parent+"_default"); err != nil {
		if waitEnded(ctx, err) {
			return false, fmt.Errorf("%w: %s: %w", errBuildDeferred, leaf, err)
		}
		return false, fmt.Errorf("hopper: lock history indexing: %w", err)
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
	if err == nil || errors.Is(err, errBuildCanceled) || errors.Is(err, errAttachDeferred) {
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
			if statementTimedOut(ctx, err) {
				// 57014 is the session's statement_timeout, a cancel by a
				// migration (lockOutIndexBuilds), or an operator's. A migration
				// holds HistoryIndexCancelKey until it commits, and the pass
				// ends so that the leader starts nothing while it runs; the next
				// pass drops what its cancel left. Otherwise the build left an
				// invalid index behind, which would cost every write to the
				// partition until the next attempt, so it is dropped, the
				// partition is reported and backed off, and the pass goes on.
				// A timeout and an operator's cancel are not told apart: only
				// the message, in the server's language, says which it was,
				// and the statement_timeout a pooled connection read need not
				// be the build's.
				free, qerr := tryAdvisoryXactLock(ctx, e.Conn, e.Schema.Name, HistoryIndexCancelKey, true)
				if qerr != nil {
					return fmt.Errorf("hopper: index partition %s: %w", leaf, errors.Join(err, qerr))
				}
				if !free {
					return fmt.Errorf("hopper: index partition %s: %w", leaf, err)
				}
				if _, derr := e.Conn.Exec(ctx, "DROP INDEX CONCURRENTLY IF EXISTS "+e.Schema.Table(index)); derr != nil {
					return fmt.Errorf("hopper: drop invalid index %s: %w", index, derr)
				}
				return fmt.Errorf("%w: %s: %w", errBuildCanceled, leaf, err)
			}
			if deadlocked(err) {
				// The deadlock detector chose the build over another
				// statement, usually retention's drop of the partition, which
				// commits after this returns; a later pass finds the
				// partition gone, or drops the invalid index the build left
				// and builds it again, whatever the other statement was.
				return fmt.Errorf("%w: %s: %w", errBuildDeferred, leaf, err)
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
// period's partition. probe is the pass's reattachRevalidates. A repair that
// fails once the index is found invalid reports it invalid with its error.
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
		return true, false, err
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
		return true, false, err
	}
	var valid bool
	if err := e.Conn.QueryRow(ctx, "SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)",
		e.Schema.Table(parentIndex)).Scan(&valid); err != nil {
		return true, false, fmt.Errorf("hopper: check index %s: %w", parentIndex, err)
	}
	return !valid, false, nil
}

// reattachRevalidates reports whether the server's attach of an index already
// attached marks its parent valid. It tries on tables of its own in the
// schema, which partition maintenance needs the right to create in anyway,
// in a transaction it rolls back, the case revalidateIndex repairs: a
// partitioned index left invalid by the drop of the one partition that
// lacked it. It asks the server rather than its version, which forks number
// their own way, once per server start (Schema.revalidates): the answer
// changes only with the server's code, which a running server keeps until it
// restarts, and the probe's DDL leaves dead catalog rows on every try.
// drivertest's ReattachRevalidates repeats it for the tests' expectations. Its tables' names are the same in every session using the
// schema, so a concurrent probe, uncommitted, holds this one's off until it
// ends; the wait is bounded by lockTimeout, so that a leader that stalls
// mid-probe does not stall this one's pass.
func (e *Executor) reattachRevalidates(ctx context.Context, lockTimeout string) (valid bool, err error) {
	var start string
	// The start time as epoch seconds, which, unlike its text, does not
	// depend on the session's TimeZone or DateStyle.
	if err := e.Conn.QueryRow(ctx, "SELECT extract(epoch FROM pg_postmaster_start_time())::text || ' ' || version()").Scan(&start); err != nil {
		return false, fmt.Errorf("hopper: probe index revalidation: %w", err)
	}
	if v, ok := e.Schema.revalidates.Load(start); ok {
		return v.(bool), nil
	}
	defer func() {
		if err == nil {
			e.Schema.revalidates.Store(start, valid)
		}
	}()
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

// mirroredPartitions reports which of g's expired partitions, names, still
// have a partition of g.mirrors in their period, in one statement for the
// whole group: a tags partition goes only once every history partition of
// its day is gone.
func (e *Executor) mirroredPartitions(ctx context.Context, g historyGroup, names []string) (map[string]bool, error) {
	if g.mirrors == "" || len(names) == 0 {
		return nil, nil
	}
	mirror := historyGroups[slices.IndexFunc(historyGroups, func(m historyGroup) bool { return m.parent == g.mirrors })]
	var owners, mirrors []string
	for _, name := range names {
		from, _ := g.parse(name)
		for at := from; at.Before(from.Add(g.period)); at = at.Add(mirror.period) {
			owners = append(owners, name)
			mirrors = append(mirrors, e.Schema.Table(mirror.name(at)))
		}
	}
	found, err := scanStrings(e.Conn.Query(ctx, `
		SELECT DISTINCT n FROM unnest($1::text[], $2::text[]) AS m(n, mirror) WHERE to_regclass(m.mirror) IS NOT NULL`,
		textArray(owners), textArray(mirrors)))
	if err != nil {
		return nil, fmt.Errorf("hopper: check partitions of %s: %w", g.mirrors, err)
	}
	out := make(map[string]bool, len(found))
	for _, n := range found {
		out[n] = true
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
		if free, err = tryAdvisoryXactLock(ctx, tx, e.Schema.Name+"."+g.parent, HistoryIndexLockKey, true); err != nil {
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
	// Read committed keeps no snapshot between statements, which
	// lockOutIndexBuilds relies on, whatever the server's default.
	if _, err = tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"); err != nil {
		return err
	}
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
	if err = lockOutIndexBuilds(ctx, tx, schema.Name); err != nil {
		return err
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

// lockOutIndexBuilds takes HistoryIndexLockKey for the rest of a migration's
// transaction, so that the leader starts no index build meanwhile, and
// cancels a build under way to get it. A build and a migration would
// otherwise wait on each other unseen: the migration for the table lock the
// build holds, and the build, in its last phase, for the migration's
// statement to end. The leader builds the index again on a later pass. Only
// the leader's own builds are canceled, those of an index historyIndexes
// names on a partition it indexes, so that an operator's build is left
// alone, elsewhere in the schema or on history's partitions alike. A build
// this role may not cancel fails the migration
// (errBuildNotCancelable). The transaction
// keeps no snapshot between these statements, so a build never waits for it
// here.
func lockOutIndexBuilds(ctx context.Context, tx Conn, schema string) error {
	var parents, suffixes []string
	for _, x := range historyIndexes {
		if p := quoteIdent(schema) + "." + quoteIdent(x.parent); !slices.Contains(parents, p) {
			parents = append(parents, p)
		}
		if !slices.Contains(suffixes, x.suffix) {
			suffixes = append(suffixes, x.suffix)
		}
	}
	deadline := time.Now().Add(10 * time.Minute)
	marked := false
	for {
		locked, err := tryAdvisoryXactLock(ctx, tx, schema, HistoryIndexLockKey, false)
		if err != nil || locked {
			return err
		}
		if time.Now().After(deadline) {
			return errors.New("hopper: the leader's history indexing held the schema for 10 minutes, and this role found no build to cancel: " +
				"a role without pg_read_all_stats sees no other role's builds, and the leader also holds the schema while it drops an invalid index " +
				"or attaches one, which can wait for a long transaction; run the migration as a role that sees the build, or once the leader's indexing has finished")
		}
		if !marked {
			if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1), $2)", schema, HistoryIndexCancelKey); err != nil {
				return fmt.Errorf("hopper: cancel index builds: %w", err)
			}
			marked = true
		}
		// The server reads other sessions' activity, the progress view's
		// included, once a transaction and shows the same view for the rest
		// of it, and this loop runs in the migration's transaction: without
		// a fresh view each pass, a build started after the first pass, such
		// as one the leader starts once it has dropped an invalid index,
		// would never be seen, and a backend listed by the first pass would
		// be canceled again on every pass, whatever it ran since.
		if _, err := tx.Exec(ctx, "SELECT pg_stat_clear_snapshot()"); err != nil {
			return fmt.Errorf("hopper: cancel index builds: %w", err)
		}
		// The builds are found and canceled in one statement: a backend
		// listed by one statement and canceled by the next could have
		// finished its build meanwhile, and the cancel would hit whatever
		// it ran next, such as an application's query on the same pool.
		// The view lists every database's builds, and a database made from
		// another as its template shares its relations' OIDs, so only this
		// database's builds are matched. The leader names a partition's
		// index after the partition (indexHistoryPartition), and a
		// concurrent build has committed its index's catalog row before it
		// waits for anything, so the index's name tells the leader's builds
		// from an operator's on the same partitions.
		if _, err := tx.Exec(ctx, `
			SELECT pg_cancel_backend(p.pid) FROM pg_stat_progress_create_index p
			JOIN pg_inherits i ON i.inhrelid = p.relid
			JOIN pg_class l ON l.oid = p.relid
			JOIN pg_class x ON x.oid = p.index_relid
			WHERE p.datid = (SELECT oid FROM pg_database WHERE datname = current_database())
			  AND i.inhparent IN (SELECT to_regclass(n) FROM unnest($1::text[]) n) AND p.command = 'CREATE INDEX CONCURRENTLY'
			  AND x.relname IN (SELECT (l.relname || s)::name FROM unnest($2::text[]) s)`,
			textArray(parents), textArray(suffixes)); err != nil {
			if sqlState(err) == "42501" {
				return fmt.Errorf("%w: %w", errBuildNotCancelable, err)
			}
			return fmt.Errorf("hopper: cancel index builds: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// errBuildNotCancelable fails a migration that cannot cancel a build. It
// fails rather than wait for the build, which may run for hours and, in its
// last phase, waits for every older snapshot, among them those of migrations
// queued for the migration lock this one holds: the wait would never end.
var errBuildNotCancelable = errors.New("hopper: a history index build holds the schema, and this role may not cancel it " +
	"(that takes the privileges of the build's role or of pg_signal_backend, and a superuser's build a superuser); " +
	"run the migration as a role that may, or once the build has finished")

// HistoryIndexCancelKey is the pg_advisory_xact_lock key, combined with the
// schema, that a migration holds from before it cancels the leader's index
// builds until it commits. A build that fails with 57014, which a statement
// timeout and a cancel share, was canceled by a migration if the key is held:
// the message would tell them apart only in the server's language.
const HistoryIndexCancelKey int32 = 0x686f7063 // "hopc"

// MigrationLockKey is the pg_advisory_lock key held while migrating.
const MigrationLockKey int64 = 0x686f707065725f6d // "hopper_m"
