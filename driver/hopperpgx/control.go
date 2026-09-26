package hopperpgx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/parallelworks/hopper/driver"
)

func (e *executor) Now(ctx context.Context) (time.Time, error) {
	var now time.Time
	if err := e.db.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("hopperpgx: now: %w", err)
	}
	return now, nil
}

// withTx runs fn in a transaction: a new one on the pool, or a savepoint
// inside the caller's transaction.
func (e *executor) withTx(ctx context.Context, fn func(tx pgx.Tx) error) (err error) {
	var tx pgx.Tx
	if e.tx != nil {
		tx, err = e.tx.Begin(ctx)
	} else {
		tx, err = e.pool.Begin(ctx)
	}
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback(context.WithoutCancel(ctx)))
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// historyInsertSQL archives rows from a CTE named done, with the given
// state and an error appended. The CTE must return hopper_jobs columns.
func historyInsertSQL(state, errorText string) string {
	return fmt.Sprintf(`
INSERT INTO hopper_job_history (
  id, seq, kind, queue, state, priority, attempt, max_attempts, scheduled_at, attempted_at,
  attempted_by, args, metadata, errors, unique_key, ordering_key, partition_key, batch_id,
  expires_at, cancel_requested_at, await, created_at, finalized_at, output)
SELECT id, seq, kind, queue, '%s', priority, attempt, max_attempts, scheduled_at, attempted_at,
  attempted_by, args, metadata,
  errors || jsonb_build_object('at', now(), 'attempt', attempt, 'error', '%s'),
  unique_key, ordering_key, partition_key, batch_id,
  expires_at, cancel_requested_at, await, created_at, now(), NULL
FROM done`, state, errorText)
}

var (
	jobListLiveSQL    = fmt.Sprintf(`SELECT %s, NULL::timestamptz, NULL::jsonb FROM hopper_jobs`, jobColumns(""))
	jobListHistorySQL = fmt.Sprintf(`SELECT %s, finalized_at, output FROM hopper_job_history`, jobColumns(""))
	jobListFilterSQL  = ` WHERE ($1 = '' OR queue = $1) AND (cardinality($2::text[]) = 0 OR kind = ANY($2))
  AND (cardinality($3::text[]) = 0 OR state::text = ANY($3)) AND id > $4`
)

func (e *executor) JobList(ctx context.Context, params driver.JobListParams) ([]*driver.JobRow, error) {
	live, history := false, false
	for _, s := range params.States {
		if s.Terminal() {
			history = true
		} else {
			live = true
		}
	}
	if len(params.States) == 0 {
		live, history = true, true
	}
	var query string
	switch {
	case live && history:
		query = "(" + jobListLiveSQL + jobListFilterSQL + ") UNION ALL (" + jobListHistorySQL + jobListFilterSQL + ")"
	case live:
		query = jobListLiveSQL + jobListFilterSQL
	default:
		query = jobListHistorySQL + jobListFilterSQL
	}
	query += " ORDER BY id LIMIT $5"
	states := make([]string, len(params.States))
	for i, s := range params.States {
		states[i] = string(s)
	}
	kinds := params.Kinds
	if kinds == nil {
		kinds = []string{}
	}
	rows, err := e.db.Query(ctx, query, params.Queue, kinds, states, [16]byte(params.After), max(params.Limit, 1))
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: list jobs: %w", err)
	}
	jobs, err := collectJobs(rows, true)
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: list jobs: %w", err)
	}
	return jobs, nil
}

var (
	jobCancelRunningSQL = fmt.Sprintf(`
UPDATE hopper_jobs SET cancel_requested_at = coalesce(cancel_requested_at, now())
WHERE id = $1 AND state = 'running'
RETURNING %s`, jobColumns(""))
	jobCancelWaitingSQL = fmt.Sprintf(`
WITH done AS (
  DELETE FROM hopper_jobs WHERE id = $1 AND state <> 'running' RETURNING *, 'cancelled'::text AS final_state,
    CASE WHEN await THEN pg_notify($2, id::text) END AS notified
), archived AS (%s RETURNING %s, finalized_at, output),
%s
SELECT * FROM archived`, historyInsertSQL("cancelled", "hopper: cancelled"), jobColumns(""), batchAccountingSQLWith("$3"))
)

func (e *executor) JobCancel(ctx context.Context, id driver.JobID) (*driver.JobRow, error) {
	var job *driver.JobRow
	err := e.withTx(ctx, func(tx pgx.Tx) error {
		var state string
		err := tx.QueryRow(ctx, "SELECT state FROM hopper_jobs WHERE id = $1 FOR UPDATE", [16]byte(id)).Scan(&state)
		if errors.Is(err, pgx.ErrNoRows) {
			// Not live: finalized already, or unknown.
			job, err = e.JobGet(ctx, id)
			return err
		}
		if err != nil {
			return err
		}
		if driver.JobState(state) == driver.JobStateRunning {
			job, err = scanJob(tx.QueryRow(ctx, jobCancelRunningSQL, [16]byte(id)), false)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, notifySQL, driver.ChannelControl, []string{"cancel:" + id.String()})
			return err
		}
		job, err = scanJob(tx.QueryRow(ctx, jobCancelWaitingSQL, [16]byte(id), driver.ChannelDone, driver.ChannelInsert), true)
		return err
	})
	if err != nil {
		if errors.Is(err, driver.ErrNotFound) {
			return nil, driver.ErrNotFound
		}
		return nil, fmt.Errorf("hopperpgx: cancel job: %w", err)
	}
	return job, nil
}

var (
	jobRetryLiveSQL = fmt.Sprintf(`
UPDATE hopper_jobs SET state = 'available', scheduled_at = now()
WHERE id = $1 AND state <> 'running'
RETURNING %s`, jobColumns(""))
	// jobRetryHistorySQL moves a finalized job back to the live table with
	// its history (attempt count and errors) intact. It gets a fresh seq,
	// as a newly inserted job would, and one more attempt if it had run out.
	jobRetryHistorySQL = fmt.Sprintf(`
WITH h AS (DELETE FROM hopper_job_history WHERE id = $1 RETURNING *)
INSERT INTO hopper_jobs (id, kind, queue, state, priority, attempt, max_attempts, scheduled_at, args, metadata,
  errors, unique_key, ordering_key, partition_key, batch_id, await, created_at)
SELECT id, kind, queue, 'available', priority, attempt,
  CASE WHEN attempt >= max_attempts THEN attempt + 1 ELSE max_attempts END, now(), args, metadata,
  errors, unique_key, ordering_key, partition_key, batch_id, await, created_at
FROM h
RETURNING %s`, jobColumns(""))
)

func (e *executor) JobRetry(ctx context.Context, id driver.JobID) (*driver.JobRow, error) {
	var job *driver.JobRow
	err := e.withTx(ctx, func(tx pgx.Tx) error {
		var state string
		err := tx.QueryRow(ctx, "SELECT state FROM hopper_jobs WHERE id = $1 FOR UPDATE", [16]byte(id)).Scan(&state)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			job, err = scanJob(tx.QueryRow(ctx, jobRetryHistorySQL, [16]byte(id)), false)
			if errors.Is(err, pgx.ErrNoRows) {
				return driver.ErrNotFound
			}
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return driver.ErrUniqueConflict
			}
			return err
		case err != nil:
			return err
		case driver.JobState(state) == driver.JobStateRunning:
			return driver.ErrJobRunning
		}
		job, err = scanJob(tx.QueryRow(ctx, jobRetryLiveSQL, [16]byte(id)), false)
		return err
	})
	if err != nil {
		if errors.Is(err, driver.ErrNotFound) || errors.Is(err, driver.ErrJobRunning) || errors.Is(err, driver.ErrUniqueConflict) {
			return nil, err
		}
		return nil, fmt.Errorf("hopperpgx: retry job: %w", err)
	}
	return job, nil
}

var jobDiscardExpiredSQL = fmt.Sprintf(`
WITH done AS (
  DELETE FROM hopper_jobs j
  WHERE j.id IN (
    SELECT id FROM hopper_jobs
    WHERE state IN ('available', 'scheduled', 'retryable') AND expires_at <= now()
    LIMIT $1 FOR UPDATE SKIP LOCKED
  )
  RETURNING j.*, 'discarded'::text AS final_state, CASE WHEN j.await THEN pg_notify($2, j.id::text) END AS notified
), archived AS (%s RETURNING %s, finalized_at, output),
%s
SELECT * FROM archived`, historyInsertSQL("discarded", "hopper: expired"), jobColumns(""), batchAccountingSQLWith("$3"))

func (e *executor) JobDiscardExpired(ctx context.Context, limit int) ([]*driver.JobRow, error) {
	rows, err := e.db.Query(ctx, jobDiscardExpiredSQL, max(limit, 1), driver.ChannelDone, driver.ChannelInsert)
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: discard expired jobs: %w", err)
	}
	jobs, err := collectJobs(rows, true)
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: discard expired jobs: %w", err)
	}
	return jobs, nil
}

func (e *executor) ClientList(ctx context.Context) ([]*driver.ClientRow, error) {
	rows, err := e.db.Query(ctx, `SELECT id, hostname, started_at, expires_at, info FROM hopper_clients ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: list clients: %w", err)
	}
	clients, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*driver.ClientRow, error) {
		var c driver.ClientRow
		err := row.Scan(&c.ID, &c.Hostname, &c.StartedAt, &c.ExpiresAt, &c.Info)
		return &c, err
	})
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: list clients: %w", err)
	}
	return clients, nil
}

func (e *executor) QueueEnsure(ctx context.Context, names []string) error {
	if len(names) == 0 {
		return nil
	}
	_, err := e.db.Exec(ctx, `INSERT INTO hopper_queues (name) SELECT unnest($1::text[]) ON CONFLICT (name) DO NOTHING`, names)
	if err != nil {
		return fmt.Errorf("hopperpgx: ensure queues: %w", err)
	}
	return nil
}

func (e *executor) QueuePause(ctx context.Context, name string) error {
	return e.setPaused(ctx, name, true)
}

func (e *executor) QueueResume(ctx context.Context, name string) error {
	return e.setPaused(ctx, name, false)
}

func (e *executor) setPaused(ctx context.Context, name string, paused bool) error {
	b := &pgx.Batch{}
	action := "resume:"
	if paused {
		action = "pause:"
		b.Queue(`INSERT INTO hopper_queues (name, paused_at) VALUES ($1, now())
			ON CONFLICT (name) DO UPDATE SET paused_at = coalesce(hopper_queues.paused_at, now()), updated_at = now()`, name)
	} else {
		b.Queue(`INSERT INTO hopper_queues (name) VALUES ($1)
			ON CONFLICT (name) DO UPDATE SET paused_at = NULL, updated_at = now()`, name)
	}
	b.Queue(notifySQL, driver.ChannelControl, []string{action + name})
	if err := e.db.SendBatch(ctx, b).Close(); err != nil {
		return fmt.Errorf("hopperpgx: %s queue: %w", action[:len(action)-1], err)
	}
	return nil
}

func (e *executor) QueueList(ctx context.Context) ([]*driver.QueueRow, error) {
	rows, err := e.db.Query(ctx, `SELECT name, paused_at, updated_at, global_limit, rate_per_sec, rate_burst, partition_limit, aging_seconds
		FROM hopper_queues ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: list queues: %w", err)
	}
	queues, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*driver.QueueRow, error) {
		var (
			q                               driver.QueueRow
			paused                          pgtype.Timestamptz
			global, burst, partition, aging pgtype.Int4
			rate                            pgtype.Float8
		)
		if err := row.Scan(&q.Name, &paused, &q.UpdatedAt, &global, &rate, &burst, &partition, &aging); err != nil {
			return nil, err
		}
		q.PausedAt = paused.Time
		q.Limits = driver.QueueLimits{
			Name: q.Name, GlobalLimit: int(global.Int32), RatePerSec: rate.Float64, RateBurst: int(burst.Int32),
			PartitionLimit: int(partition.Int32), Aging: time.Duration(aging.Int32) * time.Second,
		}
		return &q, nil
	})
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: list queues: %w", err)
	}
	return queues, nil
}

// periodicSlotSQL advances a periodic job's last slot only if the slot is
// newer, so the job for a slot is inserted at most once, whichever leader
// gets there.
const periodicSlotSQL = `
INSERT INTO hopper_periodic (name, last_slot) VALUES ($1, $2)
ON CONFLICT (name) DO UPDATE SET last_slot = EXCLUDED.last_slot
WHERE hopper_periodic.last_slot < EXCLUDED.last_slot
RETURNING name`

func (e *executor) PeriodicInsert(ctx context.Context, params driver.PeriodicInsertParams) (*driver.JobRow, bool, error) {
	var (
		job      *driver.JobRow
		inserted bool
	)
	err := e.withTx(ctx, func(tx pgx.Tx) error {
		var name string
		err := tx.QueryRow(ctx, periodicSlotSQL, params.Name, params.Slot).Scan(&name)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		results, err := (&executor{db: tx, tx: tx}).JobInsertMany(ctx, []driver.JobInsertParams{params.Job}, driver.JobInsertOpts{Notify: true})
		if err != nil {
			return err
		}
		job, inserted = results[0].Job, !results[0].Duplicate
		return nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("hopperpgx: insert periodic job %s: %w", params.Name, err)
	}
	return job, inserted, nil
}

func (e *executor) PeriodicLastSlots(ctx context.Context) (map[string]time.Time, error) {
	rows, err := e.db.Query(ctx, `SELECT name, last_slot FROM hopper_periodic`)
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: periodic slots: %w", err)
	}
	out := map[string]time.Time{}
	var (
		name string
		slot time.Time
	)
	if _, err := pgx.ForEachRow(rows, []any{&name, &slot}, func() error {
		out[name] = slot
		return nil
	}); err != nil {
		return nil, fmt.Errorf("hopperpgx: periodic slots: %w", err)
	}
	return out, nil
}

// statsSQL gathers everything in one round trip: depth by queue and state
// from the live table, throughput from the history index per known queue,
// and cluster state.
const statsSQL = `
SELECT
  (SELECT coalesce(jsonb_agg(jsonb_build_object('queue', queue, 'state', state, 'count', count, 'oldest', oldest)), '[]')
   FROM (
     SELECT queue, state::text AS state, count(*) AS count,
            extract(epoch FROM now() - min(scheduled_at) FILTER (WHERE state IN ('available', 'scheduled', 'retryable') AND scheduled_at <= now())) AS oldest
     FROM hopper_jobs GROUP BY queue, state
   ) d),
  (SELECT coalesce(jsonb_agg(jsonb_build_object('queue', q.name, 'paused', q.paused_at IS NOT NULL, 'completed',
     (SELECT count(*) FROM hopper_job_history h WHERE h.queue = q.name AND h.state = 'completed' AND h.finalized_at > now() - interval '1 minute'))), '[]')
   FROM hopper_queues q),
  (SELECT count(*) FROM hopper_clients WHERE expires_at > now()),
  (SELECT coalesce((SELECT client_id FROM hopper_leader WHERE name = 'default' AND expires_at > now()), 0)),
  (SELECT coalesce(jsonb_object_agg(attempted_by, count), '{}') FROM (
     SELECT attempted_by, count(*) AS count FROM hopper_jobs WHERE state = 'running' AND attempted_by IS NOT NULL GROUP BY attempted_by) r)`

func (e *executor) Stats(ctx context.Context) (*driver.Stats, error) {
	var (
		depths []struct {
			Queue  string   `json:"queue"`
			State  string   `json:"state"`
			Count  int      `json:"count"`
			Oldest *float64 `json:"oldest"`
		}
		queues []struct {
			Queue     string `json:"queue"`
			Paused    bool   `json:"paused"`
			Completed int    `json:"completed"`
		}
		running map[string]int
		stats   = &driver.Stats{Queues: map[string]*driver.QueueStats{}, RunningByClient: map[int64]int{}}
	)
	if err := e.db.QueryRow(ctx, statsSQL).Scan(&depths, &queues, &stats.LiveClients, &stats.Leader, &running); err != nil {
		return nil, fmt.Errorf("hopperpgx: stats: %w", err)
	}
	queue := func(name string) *driver.QueueStats {
		q := stats.Queues[name]
		if q == nil {
			q = &driver.QueueStats{}
			stats.Queues[name] = q
		}
		return q
	}
	for _, d := range depths {
		q := queue(d.Queue)
		switch driver.JobState(d.State) {
		case driver.JobStateAvailable:
			q.Available = d.Count
		case driver.JobStateScheduled:
			q.Scheduled = d.Count
		case driver.JobStateRetryable:
			q.Retryable = d.Count
		case driver.JobStateRunning:
			q.Running = d.Count
		case driver.JobStatePending, driver.JobStateCompleted, driver.JobStateCancelled, driver.JobStateDiscarded:
		}
		if d.Oldest != nil {
			if age := time.Duration(*d.Oldest * float64(time.Second)); age > q.OldestAvailable {
				q.OldestAvailable = age
			}
		}
	}
	for _, r := range queues {
		q := queue(r.Queue)
		q.Paused = r.Paused
		q.CompletedLastMinute = r.Completed
	}
	for id, n := range running {
		var clientID int64
		if _, err := fmt.Sscan(id, &clientID); err == nil {
			stats.RunningByClient[clientID] = n
		}
	}
	return stats, nil
}
