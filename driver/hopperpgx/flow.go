package hopperpgx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/parallelworks/hopper/driver"
)

// batchAccountingSQL counts finalized jobs off their batches and inserts the
// callbacks of batches that reached zero, all inside the finalizing
// statement. It expects a CTE named done whose rows carry batch_id and a
// text column final_state, and the insert channel as the parameter named by
// batchAccountingSQLWith. Rows without a batch cost one filtered scan of
// the CTE and nothing else.
var batchAccountingSQL = batchAccountingSQLWith("$10")

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
func (e *executor) claimLimited(ctx context.Context, params driver.JobClaimParams) (driver.JobClaimResult, error) {
	var res driver.JobClaimResult
	err := e.withTx(ctx, func(tx pgx.Tx) error {
		var (
			global, burst, partition pgtype.Int4
			rate, tokens             pgtype.Float8
			refilled                 pgtype.Timestamptz
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
		if errors.Is(err, pgx.ErrNoRows) {
			// No row, no limits.
			jobs, err := claimRows(ctx, tx, jobClaimQuery, params.Queue, params.ClientID, params.Limit)
			res.Jobs = jobs
			return err
		}
		if err != nil {
			return fmt.Errorf("hopperpgx: lock queue: %w", err)
		}
		if err := tx.QueryRow(ctx, `SELECT now(), (SELECT count(*) FROM hopper_jobs WHERE queue = $1 AND state = 'running')`, params.Queue).Scan(&now, &running); err != nil {
			return fmt.Errorf("hopperpgx: count running: %w", err)
		}

		budget := params.Limit
		if global.Valid {
			budget = min(budget, int(global.Int32)-running)
		}
		var available float64
		if rate.Valid && rate.Float64 > 0 {
			capacity := float64(burst.Int32)
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
		if partition.Valid && partition.Int32 > 0 {
			jobs, err = claimRows(ctx, tx, jobClaimPartitionedQuery, params.Queue, params.ClientID, budget, partition.Int32)
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
				return fmt.Errorf("hopperpgx: update rate bucket: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return driver.JobClaimResult{}, err
	}
	return res, nil
}

func (e *executor) JobAge(ctx context.Context, queue string, after time.Duration) (int64, error) {
	if after <= 0 {
		return 0, nil
	}
	// Bumped jobs restart their wait, so a job climbs one level per period.
	tag, err := e.db.Exec(ctx, `
		UPDATE hopper_jobs SET priority = priority - 1, scheduled_at = now()
		WHERE queue = $1 AND state IN ('available', 'scheduled', 'retryable') AND priority > 1
		  AND scheduled_at <= now() - make_interval(secs => $2)`, queue, after.Seconds())
	if err != nil {
		return 0, fmt.Errorf("hopperpgx: age jobs: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (e *executor) QueueSetLimits(ctx context.Context, l driver.QueueLimits) error {
	nullable := func(v int) pgtype.Int4 { return pgtype.Int4{Int32: int32(v), Valid: v > 0} } //nolint:gosec // limits are small
	rate := pgtype.Float8{Float64: l.RatePerSec, Valid: l.RatePerSec > 0}
	_, err := e.db.Exec(ctx, `
		INSERT INTO hopper_queues (name, global_limit, rate_per_sec, rate_burst, partition_limit, aging_seconds, tokens, refilled_at)
		VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL)
		ON CONFLICT (name) DO UPDATE SET global_limit = EXCLUDED.global_limit, rate_per_sec = EXCLUDED.rate_per_sec,
		  rate_burst = EXCLUDED.rate_burst, partition_limit = EXCLUDED.partition_limit, aging_seconds = EXCLUDED.aging_seconds,
		  tokens = CASE WHEN hopper_queues.rate_per_sec IS DISTINCT FROM EXCLUDED.rate_per_sec THEN NULL ELSE hopper_queues.tokens END,
		  updated_at = now()`,
		l.Name, nullable(l.GlobalLimit), rate, nullable(l.RateBurst), nullable(l.PartitionLimit), nullable(int(l.Aging/time.Second)))
	if err != nil {
		return fmt.Errorf("hopperpgx: set queue limits: %w", err)
	}
	if _, err := e.db.Exec(ctx, notifySQL, driver.ChannelControl, []string{"limit:" + l.Name}); err != nil {
		return fmt.Errorf("hopperpgx: set queue limits: notify: %w", err)
	}
	return nil
}

// callbackJSON encodes a batch callback for the batch row.
func callbackJSON(p *driver.JobInsertParams) ([]byte, error) {
	if p == nil {
		return nil, nil
	}
	return json.Marshal(map[string]any{
		"kind": p.Kind, "queue": p.Queue, "priority": p.Priority, "max_attempts": p.MaxAttempts,
		"args": json.RawMessage(jsonOrEmptyObject(p.Args)), "metadata": json.RawMessage(jsonOrEmptyObject(p.Metadata)),
	})
}

func (e *executor) BatchInsert(ctx context.Context, params driver.BatchInsertParams) (driver.JobID, error) {
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
	err = e.db.QueryRow(ctx, `
		INSERT INTO hopper_batches (pending, total, on_success, on_failure, on_complete, metadata)
		VALUES ($1, $1, $2, $3, $4, $5) RETURNING id`,
		params.Total, onSuccess, onFailure, onComplete, jsonOrEmptyObject(params.Metadata)).Scan(&id)
	if err != nil {
		return driver.JobID{}, fmt.Errorf("hopperpgx: insert batch: %w", err)
	}
	return id, nil
}

func (e *executor) BatchGet(ctx context.Context, id driver.JobID) (*driver.BatchRow, error) {
	var (
		b         driver.BatchRow
		completed pgtype.Timestamptz
	)
	err := e.db.QueryRow(ctx, `SELECT id, pending, failed, total, created_at, completed_at, metadata FROM hopper_batches WHERE id = $1`, [16]byte(id)).
		Scan(&b.ID, &b.Pending, &b.Failed, &b.Total, &b.CreatedAt, &completed, &b.Metadata)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, driver.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: get batch: %w", err)
	}
	b.CompletedAt = completed.Time
	return &b, nil
}
