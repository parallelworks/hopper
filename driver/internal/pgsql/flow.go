package pgsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// claimLimited claims inside a transaction that locks the queue row, so that
// every client's claims on a limited queue are serialized and the limits
// hold exactly: the budget is the smallest of the free slots, what the
// global limit leaves, and the whole tokens in the rate bucket.
func (e *Executor) claimLimited(ctx context.Context, params driver.JobClaimParams) (driver.JobClaimResult, error) {
	var res driver.JobClaimResult
	err := e.withTx(ctx, func(tx Tx) error {
		var (
			global, burst, partition sql.NullInt64
			rate, tokens             sql.NullFloat64
			refilled                 sql.NullTime
			now                      time.Time
			running                  int
		)
		// Lock first, count second: a statement that waited for the lock
		// keeps the snapshot it started with, so a running count taken in
		// the locking statement would not see the claims that just
		// committed ahead of it.
		err := tx.QueryRow(ctx, `
			SELECT global_limit, rate_per_sec, rate_burst, tokens, refilled_at, partition_limit
			FROM {{schema}}.hopper_queues WHERE name = $1 FOR UPDATE`, params.Queue,
		).Scan(&global, &rate, &burst, &tokens, &refilled, &partition)
		if errors.Is(err, ErrNoRows) {
			// No row, no limits.
			query, limit := claimQuery(jobClaimQueries, params.Limit)
			jobs, err := claimRows(ctx, tx, query, params.Queue, params.ClientID, limit)
			res.Jobs = jobs
			return err
		}
		if err != nil {
			return fmt.Errorf("hopper: lock queue: %w", err)
		}
		if err := tx.QueryRow(ctx, `SELECT now(), (SELECT count(*) FROM {{schema}}.hopper_jobs WHERE queue = $1 AND state = 'running' AND attempted_by IS NOT NULL)`, params.Queue).Scan(&now, &running); err != nil {
			return fmt.Errorf("hopper: count running: %w", err)
		}

		budget := params.Limit
		if global.Valid {
			budget = min(budget, int(global.Int64)-running)
		}
		var available float64
		if rate.Valid && rate.Float64 > 0 {
			capacity := float64(burst.Int64)
			if !burst.Valid || capacity <= 0 {
				capacity = math.Ceil(rate.Float64)
			}
			available = capacity
			if tokens.Valid {
				elapsed := 0.0
				if refilled.Valid {
					elapsed = now.Sub(refilled.Time).Seconds()
				}
				available = min(capacity, tokens.Float64+rate.Float64*elapsed)
			}
			budget = min(budget, int(math.Floor(available)))
			if budget <= 0 {
				res.Wait = time.Duration((1 - available) / rate.Float64 * float64(time.Second))
			}
		}
		if budget <= 0 {
			return nil
		}
		var jobs []*driver.JobRow
		if partition.Valid && partition.Int64 > 0 {
			query, limit := claimQuery(jobClaimPartitionedQueries, budget)
			jobs, err = claimRows(ctx, tx, query, params.Queue, params.ClientID, limit, partition.Int64)
		} else {
			query, limit := claimQuery(jobClaimQueries, budget)
			jobs, err = claimRows(ctx, tx, query, params.Queue, params.ClientID, limit)
		}
		if err != nil {
			return err
		}
		res.Jobs = jobs
		if rate.Valid && rate.Float64 > 0 {
			if _, err := tx.Exec(ctx, `UPDATE {{schema}}.hopper_queues SET tokens = $2, refilled_at = $3 WHERE name = $1`,
				params.Queue, available-float64(len(jobs)), now); err != nil {
				return fmt.Errorf("hopper: update rate bucket: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return driver.JobClaimResult{}, err
	}
	return res, nil
}

// callbackJSON encodes a batch callback for the batch row.
func callbackJSON(p *driver.JobInsertParams) (*string, error) {
	if p == nil {
		return nil, nil
	}
	// The callback's columns are cast and checked when the batch completes,
	// where a value out of range would fail the completing statement (and,
	// in BatchSettle, every other batch's with it). hopper_jobs checks
	// priority BETWEEN 1 AND 4.
	if p.Priority < 1 || p.Priority > 4 {
		return nil, fmt.Errorf("hopper: callback priority %d is out of range 1..4", p.Priority)
	}
	if _, err := smallint(p.MaxAttempts, "callback max_attempts"); err != nil {
		return nil, err
	}
	cb := map[string]any{
		"kind": p.Kind, "queue": p.Queue, "priority": p.Priority, "max_attempts": p.MaxAttempts,
		"args": json.RawMessage(jsonOrEmptyObject(p.Args)), "metadata": json.RawMessage(jsonOrEmptyObject(p.Metadata)),
	}
	if len(p.Tags) > 0 {
		cb["tags"] = p.Tags
	}
	b, err := json.Marshal(cb)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}

// BatchInsert implements driver.Executor.
func (e *Executor) BatchInsert(ctx context.Context, params driver.BatchInsertParams) (driver.JobID, error) {
	onSuccess, err := callbackJSON(params.OnSuccess)
	if err != nil {
		return driver.JobID{}, err
	}
	onFailure, err := callbackJSON(params.OnFailure)
	if err != nil {
		return driver.JobID{}, err
	}
	onComplete, err := callbackJSON(params.OnComplete)
	if err != nil {
		return driver.JobID{}, err
	}
	var id driver.JobID
	err = e.Conn.QueryRow(ctx, `
		INSERT INTO {{schema}}.hopper_batches (pending, total, on_success, on_failure, on_complete, metadata)
		VALUES ($1, $1, $2::jsonb, $3::jsonb, $4::jsonb, $5::jsonb) RETURNING id`,
		params.Total, onSuccess, onFailure, onComplete, jsonOrEmptyObject(params.Metadata)).Scan(&id)
	if err != nil {
		return driver.JobID{}, fmt.Errorf("hopper: insert batch: %w", err)
	}
	return id, nil
}

// batchProgressSelect reads a batch's pending and failed counts, with its
// progress rows (batchProgressSQL) folded in, from a hopper_batches row
// aliased b. A completed batch reads the counts it completed with: the rows
// it has left, which BatchSettle deletes without folding (batchSettleSQL),
// are not counted.
const batchProgressSelect = `CASE WHEN b.completed_at IS NULL THEN greatest(b.pending - p.done, 0) ELSE 0 END AS pending,
  b.failed + CASE WHEN b.completed_at IS NULL THEN p.failed ELSE 0 END AS failed`

// batchSettleSQL folds batches' progress rows into their batch rows and
// completes, with their callbacks, those with nothing remaining: batches
// whose last jobs were finalized by concurrent statements, each of which
// missed the other's uncommitted row. locked selects the batch rows to
// settle, locked FOR UPDATE; an uncommitted progress row is not folded
// until it commits. Whether a batch was open is read from the row as
// locked, which nothing else can change before the fold, rather than
// inferred from completed_at = now(), which a batch completed earlier in the
// same transaction also satisfies.
//
// The rows of a batch that had completed when locked are deleted without
// being folded. They are of jobs re-driven from history (JobRetry) that the
// batch completed without: a retry's own row (done -1, and failed -1 for a
// failed job) that the statement completing the batch missed, the retry
// held open in a caller's transaction (JobRetryTx) or committed while that
// statement ran, and the re-driven job's next finish. The batch completed
// with the job's earlier outcome and ran the callbacks it called for.
// Dropping the rows keeps the batch's counts those its callbacks were
// given, as a retry that finds the batch completed leaves them, rather than
// taking a failure off a batch whose OnFailure has run, or adding one to a
// batch whose OnSuccess has.
func batchSettleSQL(locked string) string {
	return `
WITH locked AS (` + locked + `),
moved AS (
  DELETE FROM {{schema}}.hopper_batch_progress p USING locked l WHERE p.batch_id = l.id
  RETURNING p.batch_id, p.done, p.failed
),
sums AS (
  SELECT m.batch_id, sum(m.done) AS done, sum(m.failed) AS failed
  FROM moved m JOIN locked l ON l.id = m.batch_id
  WHERE l.open
  GROUP BY m.batch_id
),
folded AS (
  UPDATE {{schema}}.hopper_batches b
  SET pending = greatest(b.pending - s.done, 0), failed = b.failed + s.failed,
      completed_at = CASE WHEN b.completed_at IS NULL AND b.pending - s.done <= 0 THEN now() ELSE b.completed_at END
  FROM sums s WHERE b.id = s.batch_id
  RETURNING b.id, b.failed, b.on_success, b.on_failure, b.on_complete, b.completed_at, b.reads
),
batches AS (SELECT f.* FROM folded f JOIN locked l ON l.id = f.id WHERE l.open AND f.completed_at IS NOT NULL),
` + resultsClearedSQL + `,
` + batchCallbacksSQL("$1") + `
SELECT (SELECT count(*) FROM locked), (SELECT count(*) FROM batches), (SELECT id FROM locked ORDER BY id DESC LIMIT 1)`
}

// batchSweepSQL settles, in ID order, up to $2 batches with progress and
// an ID after $3, skipping a batch row another statement holds (that
// statement settles it, or the next pass does), and deletes progress rows
// whose batch no longer exists.
var batchSweepSQL = strings.Replace(batchSettleSQL(`
  SELECT b.id, b.completed_at IS NULL AS open FROM {{schema}}.hopper_batches b
  WHERE b.id IN (SELECT p.batch_id FROM {{schema}}.hopper_batch_progress p WHERE p.batch_id > $3)
  ORDER BY b.id
  LIMIT $2
  FOR UPDATE OF b SKIP LOCKED
`), "\nSELECT (", `,
orphans AS (
  DELETE FROM {{schema}}.hopper_batch_progress p
  WHERE p.batch_id > $3 AND NOT EXISTS (SELECT 1 FROM {{schema}}.hopper_batches b WHERE b.id = p.batch_id)
  RETURNING p.batch_id
)
SELECT (`, 1)

// jobResultsOrphansSQL deletes up to $1 dependency results of workflows
// that have completed or no longer exist, which a client of an earlier
// release, completing a workflow, leaves. It reads the whole table, the
// results of the workflows in flight, so the leader runs it once per
// maintenance interval rather than on every pass. It skips a row another
// transaction holds, as resultsClearedSQL does: a JobRetryTx held open that
// deletes the result the statement completing its workflow skipped, which
// the leader, its every duty and its lease's renewal would otherwise wait
// for. A retry that rolls back leaves the row to the next sweep.
const jobResultsOrphansSQL = `
DELETE FROM {{schema}}.hopper_job_results r WHERE (r.batch_id, r.job_id) IN (
  SELECT o.batch_id, o.job_id FROM {{schema}}.hopper_job_results o
  WHERE NOT EXISTS (SELECT 1 FROM {{schema}}.hopper_batches b WHERE b.id = o.batch_id AND b.completed_at IS NULL)
  LIMIT $1
  FOR UPDATE OF o SKIP LOCKED)`

// batchSettleIDsSQL settles the batches in $2 that have progress, waiting
// for a batch row another statement holds, so that of two clients that
// finalized a batch's last jobs at the same time, the one settling second
// sees the first's progress.
var batchSettleIDsSQL = batchSettleSQL(`
  SELECT b.id, b.completed_at IS NULL AS open FROM {{schema}}.hopper_batches b
  WHERE b.id = ANY($2::uuid[]) AND EXISTS (SELECT 1 FROM {{schema}}.hopper_batch_progress p WHERE p.batch_id = b.id)
  ORDER BY b.id
  FOR UPDATE OF b
`)

// settleLockTimeout bounds how long BatchSettle with IDs waits for a batch
// row. It is below deadlock_timeout's default, so that a settle that would
// deadlock with a finalize completing several batches gives up first, and
// leaves the batch to the leader.
const settleLockTimeout = "500ms"

// BatchSettle implements driver.Executor.
func (e *Executor) BatchSettle(ctx context.Context, params driver.BatchSettleParams) (driver.BatchSettleResult, error) {
	var (
		res  driver.BatchSettleResult
		last nullUUID
	)
	if params.IDs == nil {
		err := e.Conn.QueryRow(ctx, batchSweepSQL, driver.ChannelInsert, max(params.Limit, 1), uuidParam(params.After)).
			Scan(&res.Folded, &res.Completed, &last)
		if err != nil {
			return res, fmt.Errorf("hopper: settle batches: %w", err)
		}
		if params.Results {
			if _, err := e.Conn.Exec(ctx, jobResultsOrphansSQL, max(params.Limit, 1)); err != nil {
				return res, fmt.Errorf("hopper: delete orphaned dependency results: %w", err)
			}
		}
		res.Last = last.ID
		return res, nil
	}
	if len(params.IDs) == 0 {
		return res, nil
	}
	ids := uuidArray(params.IDs)
	if e.InTx {
		if err := e.Conn.QueryRow(ctx, batchSettleIDsSQL, driver.ChannelInsert, ids).Scan(&res.Folded, &res.Completed, &last); err != nil {
			return res, fmt.Errorf("hopper: settle batches: %w", err)
		}
		res.Last = last.ID
		return res, nil
	}
	err := e.withTx(ctx, func(tx Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '"+settleLockTimeout+"'"); err != nil {
			return err
		}
		return tx.QueryRow(ctx, batchSettleIDsSQL, driver.ChannelInsert, ids).Scan(&res.Folded, &res.Completed, &last)
	})
	if err != nil {
		if lockNotAvailable(err) {
			return driver.BatchSettleResult{}, nil
		}
		return driver.BatchSettleResult{}, fmt.Errorf("hopper: settle batches: %w", err)
	}
	res.Last = last.ID
	return res, nil
}

// BatchGet implements driver.Executor.
func (e *Executor) BatchGet(ctx context.Context, id driver.JobID) (*driver.BatchRow, error) {
	var (
		b         driver.BatchRow
		completed sql.NullTime
		metadata  jsonText
	)
	err := e.Conn.QueryRow(ctx, `SELECT id, `+batchProgressSelect+`, total, created_at, completed_at, metadata
		FROM {{schema}}.hopper_batches b, `+batchProgressSQL+` WHERE id = $1`, uuidParam(id)).
		Scan(&b.ID, &b.Pending, &b.Failed, &b.Total, &b.CreatedAt, &completed, &metadata)
	if errors.Is(err, ErrNoRows) {
		return nil, driver.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: get batch: %w", err)
	}
	b.CompletedAt = completed.Time
	b.Metadata = metadata.raw()
	return &b, nil
}
