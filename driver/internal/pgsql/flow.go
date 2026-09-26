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

// batchAccountingSQLWith returns the CTEs that count finalized jobs off
// their batches and insert the callbacks of batches that reached zero, all
// inside the finalizing statement. They expect a CTE named done whose rows
// carry batch_id and a text column final_state, and the insert channel as
// the given parameter. Rows without a batch cost one filtered scan of the
// CTE and nothing else.
func batchAccountingSQLWith(channelParam string) string {
	return `batches AS (
  UPDATE hopper_batches b
  SET pending = b.pending - c.n, failed = b.failed + c.f,
      completed_at = CASE WHEN b.pending - c.n <= 0 THEN now() ELSE b.completed_at END
  FROM (SELECT batch_id, count(*) AS n, count(*) FILTER (WHERE final_state IN ('cancelled', 'discarded')) AS f
        FROM done WHERE batch_id IS NOT NULL GROUP BY batch_id) c
  WHERE b.id = c.batch_id
  RETURNING b.id, b.pending, b.failed, b.on_success, b.on_failure, b.on_complete
),
callbacks AS (
  INSERT INTO hopper_jobs (kind, queue, priority, max_attempts, args, metadata)
  SELECT cb->>'kind', coalesce(cb->>'queue', 'default'), coalesce((cb->>'priority')::smallint, 2),
    coalesce((cb->>'max_attempts')::smallint, 25), coalesce(cb->'args', '{}'),
    coalesce(cb->'metadata', '{}') || jsonb_build_object('batch_id', b.id::text, 'batch_failed', b.failed)
  FROM batches b, LATERAL (VALUES (CASE WHEN b.failed = 0 THEN b.on_success ELSE b.on_failure END), (b.on_complete)) AS v(cb)
  WHERE b.pending <= 0 AND cb IS NOT NULL
  RETURNING pg_notify(` + channelParam + `, queue)
)`
}

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
			FROM hopper_queues WHERE name = $1 FOR UPDATE`, params.Queue,
		).Scan(&global, &rate, &burst, &tokens, &refilled, &partition)
		if errors.Is(err, ErrNoRows) {
			// No row, no limits.
			jobs, err := claimRows(ctx, tx, jobClaimQuery, params.Queue, params.ClientID, params.Limit)
			res.Jobs = jobs
			return err
		}
		if err != nil {
			return fmt.Errorf("hopper: lock queue: %w", err)
		}
		if err := tx.QueryRow(ctx, `SELECT now(), (SELECT count(*) FROM hopper_jobs WHERE queue = $1 AND state = 'running')`, params.Queue).Scan(&now, &running); err != nil {
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
			jobs, err = claimRows(ctx, tx, jobClaimPartitionedQuery, params.Queue, params.ClientID, budget, partition.Int64)
		} else {
			jobs, err = claimRows(ctx, tx, jobClaimQuery, params.Queue, params.ClientID, budget)
		}
		if err != nil {
			return err
		}
		res.Jobs = jobs
		if rate.Valid && rate.Float64 > 0 {
			if _, err := tx.Exec(ctx, `UPDATE hopper_queues SET tokens = $2, refilled_at = $3 WHERE name = $1`,
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
		INSERT INTO hopper_batches (pending, total, on_success, on_failure, on_complete, metadata)
		VALUES ($1, $1, $2::jsonb, $3::jsonb, $4::jsonb, $5::jsonb) RETURNING id`,
		params.Total, onSuccess, onFailure, onComplete, jsonOrEmptyObject(params.Metadata)).Scan(&id)
	if err != nil {
		return driver.JobID{}, fmt.Errorf("hopper: insert batch: %w", err)
	}
	return id, nil
}

// BatchGet implements driver.Executor.
func (e *Executor) BatchGet(ctx context.Context, id driver.JobID) (*driver.BatchRow, error) {
	var (
		b         driver.BatchRow
		completed sql.NullTime
		metadata  jsonText
	)
	err := e.Conn.QueryRow(ctx, `SELECT id, pending, failed, total, created_at, completed_at, metadata FROM hopper_batches WHERE id = $1`, uuidParam(id)).
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
