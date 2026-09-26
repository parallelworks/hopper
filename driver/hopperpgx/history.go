package hopperpgx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

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

// historyLockKey is the pg_advisory_xact_lock key that serializes partition
// creation between overlapping leaders. It is combined with the schema, so
// installations sharing a database do not queue behind each other.
const historyLockKey int32 = 0x686f7068 // "hoph"

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

// HistoryMaintain keeps time partitions ready for the periods ahead and drops
// the ones past retention. Only future periods are created, so no existing
// row ever has to move: rows finalized before their period's partition
// existed (the first period after installation, or after a long leader
// outage) stay in the DEFAULT partition and are pruned by retention
// individually.
func (e *executor) HistoryMaintain(ctx context.Context, params driver.HistoryMaintainParams) (driver.HistoryMaintainResult, error) {
	var res driver.HistoryMaintainResult
	if e.pool == nil {
		return res, errors.New("hopperpgx: history maintenance must run on the pool, not in a transaction")
	}
	var now time.Time
	if err := e.pool.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		return res, fmt.Errorf("hopperpgx: history maintenance: %w", err)
	}
	retention := map[string]time.Duration{
		"hopper_job_history_completed": params.CompletedRetention,
		"hopper_job_history_failed":    params.FailedRetention,
	}

	for _, g := range historyGroups {
		existing, err := partitionsOf(ctx, e.pool, g.parent)
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
			created, err := createPartition(ctx, e.pool, g, name, from, from.Add(g.period))
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
				if err := dropPartition(ctx, e.pool, name); err != nil {
					return res, err
				}
				res.Dropped = append(res.Dropped, name)
			}
			// Rows in the DEFAULT partition cannot be dropped by period.
			tag, err := e.pool.Exec(ctx,
				fmt.Sprintf("DELETE FROM %s_default WHERE finalized_at < now() - make_interval(secs => $1)", g.parent),
				keep.Seconds())
			if err != nil {
				return res, fmt.Errorf("hopperpgx: prune %s_default: %w", g.parent, err)
			}
			res.Pruned += tag.RowsAffected()
		}
	}
	return res, nil
}

// partitionsOf lists the partitions attached to parent.
func partitionsOf(ctx context.Context, pool *pgxpool.Pool, parent string) (map[string]struct{}, error) {
	rows, err := pool.Query(ctx, `
		SELECT c.relname FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = to_regclass($1)`, parent)
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: list partitions of %s: %w", parent, err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: list partitions of %s: %w", parent, err)
	}
	out := make(map[string]struct{}, len(names))
	for _, n := range names {
		out[n] = struct{}{}
	}
	return out, nil
}

// createPartition creates a time partition for a future period and attaches
// it. It returns false if another leader created the partition meanwhile.
func createPartition(ctx context.Context, pool *pgxpool.Pool, g historyGroup, name string, from, to time.Time) (created bool, err error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	ident := pgx.Identifier{name}.Sanitize()
	parent := pgx.Identifier{g.parent}.Sanitize()
	fromLit, toLit := literal(from), literal(to)

	// Two leaders can overlap briefly; serialize them on an advisory lock
	// held for this transaction, then check whether the other one has
	// already created the partition.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext(current_schema()), $1)", historyLockKey); err != nil {
		return false, fmt.Errorf("hopperpgx: lock history maintenance: %w", err)
	}
	var exists bool
	if err = tx.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists); err != nil {
		return false, fmt.Errorf("hopperpgx: check partition %s: %w", name, err)
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
		return false, fmt.Errorf("hopperpgx: create partition %s: %w", name, err)
	}
	_, err = tx.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM (%s) TO (%s)", parent, ident, fromLit, toLit))
	if err != nil {
		return false, fmt.Errorf("hopperpgx: attach partition %s: %w", name, err)
	}
	if err = tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("hopperpgx: create partition %s: %w", name, err)
	}
	return true, nil
}

// dropPartition drops a partition. DROP takes an ACCESS EXCLUSIVE lock on
// the parent for the catalog change, which is brief and happens once per
// period. (DETACH CONCURRENTLY would avoid it, but Postgres refuses it while
// a DEFAULT partition exists.)
func dropPartition(ctx context.Context, pool *pgxpool.Pool, name string) error {
	if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{name}.Sanitize()); err != nil {
		return fmt.Errorf("hopperpgx: drop partition %s: %w", name, err)
	}
	return nil
}

// literal formats a time as a timestamptz literal for DDL, which cannot take
// bind parameters.
func literal(t time.Time) string {
	return "'" + t.UTC().Format("2006-01-02 15:04:05.999999+00") + "'"
}
