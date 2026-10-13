package pgsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
		if err := tx.QueryRow(ctx, `SELECT now(), (SELECT count(*) FROM {{schema}}.hopper_jobs WHERE queue = $1 AND state = 'running')`, params.Queue).Scan(&now, &running); err != nil {
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
	// The callback's columns are cast when the batch completes, where a
	// value out of range would fail the completing statement.
	if _, err := smallint(p.Priority, "callback priority"); err != nil {
		return nil, err
	}
	if _, err := smallint(p.MaxAttempts, "callback max_attempts"); err != nil {
		return nil, err
	}
	b, err := json.Marshal(map[string]any{
		"kind": p.Kind, "queue": p.Queue, "priority": p.Priority, "max_attempts": p.MaxAttempts,
		"args": json.RawMessage(jsonOrEmptyObject(p.Args)), "metadata": json.RawMessage(jsonOrEmptyObject(p.Metadata)),
	})
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
// progress rows folded in, from a hopper_batches row aliased b.
const batchProgressSelect = `CASE WHEN b.completed_at IS NULL THEN greatest(b.pending - p.done, 0) ELSE 0 END AS pending,
  b.failed + p.failed AS failed`

// batchSettleSQL folds batches' progress rows into their batch rows and
// completes, with their callbacks, those with nothing remaining: batches
// whose last jobs were finalized by concurrent statements, each of which
// missed the other's uncommitted row. It skips a batch row another
// statement holds; that statement settles it, or the next call does. An
// uncommitted progress row is not folded until it commits.
var batchSettleSQL = `
WITH locked AS (
  SELECT b.id FROM {{schema}}.hopper_batches b
  WHERE b.id IN (SELECT p.batch_id FROM {{schema}}.hopper_batch_progress p)
  ORDER BY b.id
  LIMIT $1
  FOR UPDATE OF b SKIP LOCKED
),
moved AS (
  DELETE FROM {{schema}}.hopper_batch_progress p USING locked l WHERE p.batch_id = l.id
  RETURNING p.batch_id, p.done, p.failed
),
sums AS (SELECT batch_id, sum(done) AS done, sum(failed) AS failed FROM moved GROUP BY batch_id),
folded AS (
  UPDATE {{schema}}.hopper_batches b
  SET pending = b.pending - s.done, failed = b.failed + s.failed,
      completed_at = CASE WHEN b.completed_at IS NULL AND b.pending - s.done <= 0 THEN now() ELSE b.completed_at END
  FROM sums s WHERE b.id = s.batch_id
  RETURNING b.id, b.failed, b.on_success, b.on_failure, b.on_complete, b.completed_at = now() AS completed_now
),
batches AS (SELECT * FROM folded WHERE completed_now),
` + batchCallbacksSQL("$2") + `
SELECT (SELECT count(*) FROM locked), (SELECT count(*) FROM batches)`

// BatchSettle implements driver.Executor.
func (e *Executor) BatchSettle(ctx context.Context, limit int) (driver.BatchSettleResult, error) {
	var res driver.BatchSettleResult
	if err := e.Conn.QueryRow(ctx, batchSettleSQL, max(limit, 1), driver.ChannelInsert).Scan(&res.Folded, &res.Completed); err != nil {
		return res, fmt.Errorf("hopper: settle batches: %w", err)
	}
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
