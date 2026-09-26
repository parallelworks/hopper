package pgsql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// historyGroup describes one outcome partition of hopper_job_history and how
// it is split by time.
type historyGroup struct {
	parent string
	period time.Duration
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
	{parent: "hopper_job_history_completed", period: time.Hour, ahead: 3, format: "2006010215"},
	{parent: "hopper_job_history_failed", period: 24 * time.Hour, ahead: 2, format: "20060102"},
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
	retention := map[string]time.Duration{
		"hopper_job_history_completed": params.CompletedRetention,
		"hopper_job_history_failed":    params.FailedRetention,
	}
	for _, g := range historyGroups {
		existing, err := e.partitionsOf(ctx, g.parent)
		if err != nil {
			return res, err
		}
		keep := retention[g.parent]

		// Create the periods ahead of the current one.
		start := now.UTC().Truncate(g.period)
		for i := 1; i <= g.ahead; i++ {
			from := start.Add(time.Duration(i) * g.period)
			name := g.name(from)
			if _, ok := existing[name]; ok {
				continue
			}
			created, err := e.createPartition(ctx, g, name, from, from.Add(g.period))
			if err != nil {
				return res, err
			}
			if created {
				res.Created = append(res.Created, name)
			}
		}

		// Drop partitions whose whole period is past retention.
		if keep > 0 {
			cutoff := now.Add(-keep)
			for name := range existing {
				from, ok := g.parse(name)
				if !ok || !from.Add(g.period).Before(cutoff) {
					continue
				}
				// DROP takes a brief ACCESS EXCLUSIVE lock on the parent for
				// the catalog change, once per period. (DETACH CONCURRENTLY
				// would avoid it, but Postgres refuses it while a DEFAULT
				// partition exists.)
				if _, err := e.Conn.Exec(ctx, "DROP TABLE IF EXISTS "+quoteIdent(name)); err != nil {
					return res, fmt.Errorf("hopper: drop partition %s: %w", name, err)
				}
				res.Dropped = append(res.Dropped, name)
			}
			// Rows in the DEFAULT partition cannot be dropped by period.
			n, err := e.Conn.Exec(ctx,
				fmt.Sprintf("DELETE FROM %s_default WHERE finalized_at < now() - make_interval(secs => $1::float8)", g.parent),
				keep.Seconds())
			if err != nil {
				return res, fmt.Errorf("hopper: prune %s_default: %w", g.parent, err)
			}
			res.Pruned += n
		}
	}
	return res, nil
}

// partitionsOf lists the partitions attached to parent.
func (e *Executor) partitionsOf(ctx context.Context, parent string) (map[string]struct{}, error) {
	names, err := scanStrings(e.Conn.Query(ctx, `
		SELECT c.relname FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = to_regclass($1)`, parent))
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
// it. It returns false if another leader created the partition meanwhile.
func (e *Executor) createPartition(ctx context.Context, g historyGroup, name string, from, to time.Time) (created bool, err error) {
	tx, err := e.Conn.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	ident := quoteIdent(name)
	parent := quoteIdent(g.parent)
	fromLit, toLit := literal(from), literal(to)

	// Two leaders can overlap briefly; serialize them on an advisory lock
	// held for this transaction, then check whether the other one has
	// already created the partition.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext(current_schema()), $1)", HistoryLockKey); err != nil {
		return false, fmt.Errorf("hopper: lock history maintenance: %w", err)
	}
	var exists bool
	if err = tx.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists); err != nil {
		return false, fmt.Errorf("hopper: check partition %s: %w", name, err)
	}
	if exists {
		return false, tx.Rollback(ctx)
	}
	// CREATE TABLE ... PARTITION OF would take ACCESS EXCLUSIVE on the
	// parent and block finalizes; a standalone table plus ATTACH takes only
	// SHARE UPDATE EXCLUSIVE on it. ATTACH does scan the DEFAULT partition
	// for rows belonging to the new period, under an exclusive lock on that
	// small table alone; the period is in the future, so it finds none.
	if _, err = tx.Exec(ctx, "CREATE TABLE "+ident+" (LIKE hopper_job_history INCLUDING DEFAULTS INCLUDING CONSTRAINTS)"); err != nil {
		return false, fmt.Errorf("hopper: create partition %s: %w", name, err)
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM (%s) TO (%s)", parent, ident, fromLit, toLit)); err != nil {
		return false, fmt.Errorf("hopper: attach partition %s: %w", name, err)
	}
	if err = tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("hopper: create partition %s: %w", name, err)
	}
	return true, nil
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
func MigrationVersions(ctx context.Context, conn Conn) ([]int, error) {
	var exists bool
	if err := conn.QueryRow(ctx, "SELECT to_regclass('hopper_schema') IS NOT NULL").Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return scanInts(conn.Query(ctx, "SELECT version FROM hopper_schema ORDER BY version"))
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
func MigrationApply(ctx context.Context, conn Conn, version int, script string, up bool) (err error) {
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
		if _, err = tx.Exec(ctx, script); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO hopper_schema (version) VALUES ($1)", version); err != nil {
			return err
		}
	} else {
		// The version row goes first: the down script of version 1 drops
		// hopper_schema itself.
		if _, err = tx.Exec(ctx, "DELETE FROM hopper_schema WHERE version = $1", version); err != nil {
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
