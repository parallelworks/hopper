package hoppersqlite

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

// claimLimited claims within a queue's limits: the budget is the smallest
// of the claim's limit, what the global limit leaves, and the whole tokens
// in the rate bucket. The write lock serializes every client's claims on
// the queue, so the limits hold exactly.
func (e *executor) claimLimited(ctx context.Context, params driver.JobClaimParams) (driver.JobClaimResult, error) {
	var res driver.JobClaimResult
	err := e.write(ctx, func(x *w) error {
		var (
			global, burst, partition sql.NullInt64
			rate, tokens             sql.NullFloat64
			refilled                 micros
			running                  int
		)
		err := x.QueryRowContext(ctx, `SELECT global_limit, rate_per_sec, rate_burst, tokens, refilled_at, partition_limit
			FROM hopper_queues WHERE name = ?`, params.Queue).Scan(&global, &rate, &burst, &tokens, &refilled, &partition)
		if errors.Is(err, sql.ErrNoRows) {
			// No row, no limits.
			res.Jobs, err = x.claim(ctx, params.Queue, params.ClientID, params.Limit, 0)
			return err
		}
		if err != nil {
			return fmt.Errorf("hopper: read queue limits: %w", err)
		}
		if err := x.QueryRowContext(ctx, `SELECT count(*) FROM hopper_jobs WHERE queue = ? AND state = 'running' AND attempted_by IS NOT NULL`, params.Queue).Scan(&running); err != nil {
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
					elapsed = x.now.Sub(refilled.time()).Seconds()
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
		if res.Jobs, err = x.claim(ctx, params.Queue, params.ClientID, budget, partition.Int64); err != nil {
			return err
		}
		if rate.Valid && rate.Float64 > 0 {
			if _, err := x.ExecContext(ctx, `UPDATE hopper_queues SET tokens = ?, refilled_at = ? WHERE name = ?`,
				available-float64(len(res.Jobs)), x.us(), params.Queue); err != nil {
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

// callbackJSON encodes a batch callback for the batch row, refusing values
// a job could not be inserted with, which would fail the transaction that
// completes the batch.
func callbackJSON(p *driver.JobInsertParams) (any, error) {
	if p == nil {
		return nil, nil
	}
	if p.Priority < 1 || p.Priority > 4 {
		return nil, fmt.Errorf("hopper: callback priority %d is out of range 1..4", p.Priority)
	}
	if _, err := smallint(p.MaxAttempts, "callback max_attempts"); err != nil {
		return nil, err
	}
	args, err := jsonValue(p.Args, "{}", "callback args")
	if err != nil {
		return nil, err
	}
	metadata, err := jsonValue(p.Metadata, "{}", "callback metadata")
	if err != nil {
		return nil, err
	}
	cb := map[string]any{
		"kind": p.Kind, "queue": p.Queue, "priority": p.Priority, "max_attempts": p.MaxAttempts,
		"args": json.RawMessage(args), "metadata": json.RawMessage(metadata),
	}
	if len(p.Tags) > 0 {
		cb["tags"] = p.Tags
	}
	b, err := json.Marshal(cb)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

// batchRow is a batch's callbacks and metadata, encoded for its row.
type batchRow struct {
	onSuccess, onFailure, onComplete any
	metadata                         string
}

func encodeBatch(onSuccess, onFailure, onComplete *driver.JobInsertParams, metadata []byte) (batchRow, error) {
	var b batchRow
	var err error
	if b.onSuccess, err = callbackJSON(onSuccess); err != nil {
		return b, err
	}
	if b.onFailure, err = callbackJSON(onFailure); err != nil {
		return b, err
	}
	if b.onComplete, err = callbackJSON(onComplete); err != nil {
		return b, err
	}
	b.metadata, err = jsonValue(metadata, "{}", "batch metadata")
	return b, err
}

// insertBatch inserts a batch row expecting total jobs.
func (x *w) insertBatch(ctx context.Context, id driver.JobID, name string, total int, b batchRow, edges, steps any) error {
	_, err := x.ExecContext(ctx, `INSERT INTO hopper_batches (id, name, pending, total, on_success, on_failure, on_complete, metadata, edges, steps, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		idParam(id), nullable(name), total, total, b.onSuccess, b.onFailure, b.onComplete, b.metadata, edges, steps, x.us())
	return err
}

// BatchInsert implements driver.Executor.
func (e *executor) BatchInsert(ctx context.Context, params driver.BatchInsertParams) (driver.JobID, error) {
	b, err := encodeBatch(params.OnSuccess, params.OnFailure, params.OnComplete, params.Metadata)
	if err != nil {
		return driver.JobID{}, err
	}
	var id driver.JobID
	err = e.write(ctx, func(x *w) error {
		id = newID(x.now)
		return x.insertBatch(ctx, id, "", params.Total, b, nil, nil)
	})
	if err != nil {
		return driver.JobID{}, fmt.Errorf("hopper: insert batch: %w", err)
	}
	return id, nil
}

// BatchSettle implements driver.Executor. The transaction that finalizes a
// batch's jobs counts them down on its row and completes it, so there is no
// progress to fold, and no batch a race left open. A sweep deletes the
// dependency results of workflows that completed or no longer exist, which
// a client of an earlier release, completing a workflow, leaves.
func (e *executor) BatchSettle(ctx context.Context, params driver.BatchSettleParams) (driver.BatchSettleResult, error) {
	if params.IDs != nil {
		return driver.BatchSettleResult{}, nil
	}
	const orphans = `SELECT o.job_id FROM hopper_job_results o
		WHERE NOT EXISTS (SELECT 1 FROM hopper_batches b WHERE b.id = o.batch_id AND b.completed_at IS NULL)`
	var found bool
	if err := e.read().QueryRowContext(ctx, `SELECT EXISTS (`+orphans+`)`).Scan(&found); err != nil {
		return driver.BatchSettleResult{}, fmt.Errorf("hopper: settle batches: %w", err)
	}
	if !found {
		return driver.BatchSettleResult{}, nil
	}
	err := e.write(ctx, func(x *w) error {
		_, err := x.ExecContext(ctx, `DELETE FROM hopper_job_results WHERE job_id IN (`+orphans+` LIMIT ?)`, max(params.Limit, 1))
		return err
	})
	if err != nil {
		return driver.BatchSettleResult{}, fmt.Errorf("hopper: settle batches: %w", err)
	}
	return driver.BatchSettleResult{}, nil
}

// batchColumns read a batch's progress as BatchRow has it.
const batchColumns = `id, CASE WHEN completed_at IS NULL THEN max(pending, 0) ELSE 0 END, failed, total, created_at, completed_at, metadata`

func scanBatch(row scanner, extra ...any) (*driver.BatchRow, error) {
	var (
		b                  driver.BatchRow
		id                 []byte
		created, completed micros
		metadata           string
	)
	if err := row.Scan(append([]any{&id, &b.Pending, &b.Failed, &b.Total, &created, &completed, &metadata}, extra...)...); err != nil {
		return nil, err
	}
	if err := b.ID.Scan(id); err != nil {
		return nil, err
	}
	b.CreatedAt, b.CompletedAt, b.Metadata = created.time(), completed.time(), json.RawMessage(metadata)
	return &b, nil
}

// BatchGet implements driver.Executor.
func (e *executor) BatchGet(ctx context.Context, id driver.JobID) (*driver.BatchRow, error) {
	b, err := scanBatch(e.read().QueryRowContext(ctx, `SELECT `+batchColumns+` FROM hopper_batches WHERE id = ?`, idParam(id)))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, driver.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: get batch: %w", err)
	}
	return b, nil
}
