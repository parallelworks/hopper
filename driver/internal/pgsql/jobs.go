package pgsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// JobColumns lists hopper_jobs columns in scan order. prefix qualifies them
// with a table alias.
func JobColumns(prefix string) string {
	cols := []string{
		"id", "kind", "queue", "state", "priority", "attempt", "max_attempts",
		"scheduled_at", "attempted_at", "attempted_by", "args", "metadata", "errors",
		"unique_key", "ordering_key", "partition_key", "batch_id", "expires_at", "cancel_requested_at", "await", "created_at",
	}
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = prefix + c
	}
	return strings.Join(out, ", ")
}

// scanJob scans a row of JobColumns, plus finalized_at and output when
// history is true, plus a trailing duplicate flag when dup is non-nil.
func scanJob(row Row, history bool, dup *bool) (*driver.JobRow, error) {
	var (
		j                                driver.JobRow
		state                            string
		priority, attempt, maxAttempts   int
		attemptedAt, expiresAt, cancelAt sql.NullTime
		finalizedAt                      sql.NullTime
		attemptedBy                      sql.NullInt64
		uniqueKey, orderingKey, partKey  sql.NullString
		args, metadata, output           jsonText
		errs                             attemptErrors
		batchID                          nullUUID
	)
	dest := []any{
		&j.ID, &j.Kind, &j.Queue, &state, &priority, &attempt, &maxAttempts,
		&j.ScheduledAt, &attemptedAt, &attemptedBy, &args, &metadata, &errs,
		&uniqueKey, &orderingKey, &partKey, &batchID, &expiresAt, &cancelAt, &j.Await, &j.CreatedAt,
	}
	if history {
		dest = append(dest, &finalizedAt, &output)
	}
	if dup != nil {
		dest = append(dest, dup)
	}
	if err := row.Scan(dest...); err != nil {
		return nil, err
	}
	j.State = driver.JobState(state)
	j.Priority, j.Attempt, j.MaxAttempts = priority, attempt, maxAttempts
	j.AttemptedAt, j.ExpiresAt, j.CancelRequestedAt, j.FinalizedAt = attemptedAt.Time, expiresAt.Time, cancelAt.Time, finalizedAt.Time
	j.AttemptedBy = attemptedBy.Int64
	j.Args, j.Metadata, j.Output = args.raw(), metadata.raw(), output.raw()
	j.Errors = []driver.AttemptError(errs)
	j.UniqueKey, j.OrderingKey, j.PartitionKey = uniqueKey.String, orderingKey.String, partKey.String
	if batchID.Valid {
		j.BatchID = batchID.ID
	}
	return &j, nil
}

var (
	liveJobs    = collect(func(r Row) (*driver.JobRow, error) { return scanJob(r, false, nil) })
	historyJobs = collect(func(r Row) (*driver.JobRow, error) { return scanJob(r, true, nil) })
)

var scanInsertResults = collect(func(r Row) (driver.JobInsertResult, error) {
	var dup bool
	j, err := scanJob(r, false, &dup)
	return driver.JobInsertResult{Job: j, Duplicate: dup}, err
})

var scanIDs = collect(func(r Row) (driver.JobID, error) {
	var id driver.JobID
	err := r.Scan(&id)
	return id, err
})

// jobInsertSQL inserts a batch in one statement. On a unique-key conflict a
// DO UPDATE (rather than DO NOTHING) makes RETURNING include the existing
// row, and xmax <> 0 marks those rows as duplicates. For ConflictSkip the
// update is a no-op; for ConflictReplace it replaces the job unless it is
// running. RETURNING emits rows in the order the SELECT produced them, which
// is input order.
const jobInsertSQL = `
WITH p AS (
  SELECT * FROM unnest(
    $1::text[], $2::text[], $3::smallint[], $4::smallint[], $5::timestamptz[], $6::jsonb[], $7::jsonb[], $8::text[],
    $9::float8[], $10::boolean[], $11::text[], $12::text[], $13::uuid[]
  ) AS p(kind, queue, priority, max_attempts, scheduled_at, args, metadata, unique_key, ttl, await, ordering_key, partition_key, batch_id)
)
INSERT INTO hopper_jobs (kind, queue, state, priority, max_attempts, scheduled_at, args, metadata, unique_key, expires_at, await, ordering_key, partition_key, batch_id)
SELECT p.kind, p.queue,
       CASE WHEN p.scheduled_at > now() THEN 'scheduled' ELSE 'available' END::hopper_job_state,
       p.priority, p.max_attempts, coalesce(p.scheduled_at, now()), p.args, p.metadata, p.unique_key,
       CASE WHEN p.ttl > 0 THEN now() + make_interval(secs => p.ttl) END, p.await, p.ordering_key, p.partition_key, p.batch_id
FROM p
ON CONFLICT (kind, unique_key) WHERE unique_key IS NOT NULL DO UPDATE SET ` + "%s" + `
RETURNING ` + "%s" + `, (xmax <> 0) AS duplicate`

const (
	conflictSkipSet    = `kind = EXCLUDED.kind`
	conflictReplaceSet = `
  args         = CASE WHEN hopper_jobs.state = 'running' THEN hopper_jobs.args         ELSE EXCLUDED.args         END,
  metadata     = CASE WHEN hopper_jobs.state = 'running' THEN hopper_jobs.metadata     ELSE EXCLUDED.metadata     END,
  priority     = CASE WHEN hopper_jobs.state = 'running' THEN hopper_jobs.priority     ELSE EXCLUDED.priority     END,
  max_attempts = CASE WHEN hopper_jobs.state = 'running' THEN hopper_jobs.max_attempts ELSE EXCLUDED.max_attempts END,
  scheduled_at = CASE WHEN hopper_jobs.state = 'running' THEN hopper_jobs.scheduled_at ELSE EXCLUDED.scheduled_at END,
  state        = CASE WHEN hopper_jobs.state = 'running' THEN hopper_jobs.state        ELSE EXCLUDED.state        END`
)

var (
	jobInsertSkipQuery    = fmt.Sprintf(jobInsertSQL, conflictSkipSet, JobColumns(""))
	jobInsertReplaceQuery = fmt.Sprintf(jobInsertSQL, conflictReplaceSet, JobColumns(""))
)

// NotifySQL sends one notification per payload. Postgres delivers them on
// commit and collapses identical ones within a transaction.
const NotifySQL = `SELECT pg_notify($1, p) FROM unnest($2::text[]) AS p`

// Notify implements driver.Executor.
func (e *Executor) Notify(ctx context.Context, channel string, payloads []string) error {
	if len(payloads) == 0 {
		return nil
	}
	if _, err := e.Conn.Exec(ctx, NotifySQL, channel, textArray(payloads)); err != nil {
		return fmt.Errorf("hopper: notify: %w", err)
	}
	return nil
}

func distinctQueues(params []driver.JobInsertParams) []string {
	seen := make(map[string]struct{}, 4)
	var out []string
	for _, p := range params {
		if _, ok := seen[p.Queue]; !ok {
			seen[p.Queue] = struct{}{}
			out = append(out, p.Queue)
		}
	}
	return out
}

// JobInsertMany implements driver.Executor.
func (e *Executor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams, opts driver.JobInsertOpts) ([]driver.JobInsertResult, error) {
	if len(params) == 0 {
		return nil, nil
	}
	n := len(params)
	var (
		kinds, queues, uniqueKeys, ordering, partitions = make([]string, n), make([]string, n), make([]string, n), make([]string, n), make([]string, n)
		priorities, maxAttempts                         = make([]int, n), make([]int, n)
		scheduledAt                                     = make([]time.Time, n)
		args, metadata                                  = make([][]byte, n), make([][]byte, n)
		ttls                                            = make([]float64, n)
		awaits                                          = make([]bool, n)
		batches                                         = make([]driver.JobID, n)
	)
	for i, p := range params {
		var err error
		if priorities[i], err = smallint(p.Priority, "priority"); err != nil {
			return nil, err
		}
		if maxAttempts[i], err = smallint(p.MaxAttempts, "max_attempts"); err != nil {
			return nil, err
		}
		kinds[i], queues[i], uniqueKeys[i] = p.Kind, p.Queue, p.UniqueKey
		ordering[i], partitions[i], batches[i] = p.OrderingKey, p.PartitionKey, p.BatchID
		scheduledAt[i] = p.ScheduledAt
		args[i] = []byte(jsonOrEmptyObject(p.Args))
		metadata[i] = []byte(jsonOrEmptyObject(p.Metadata))
		ttls[i] = max(p.TTL, 0).Seconds()
		awaits[i] = p.Await
	}
	query := jobInsertSkipQuery
	if opts.OnConflict == driver.ConflictReplace {
		query = jobInsertReplaceQuery
	}
	queryArgs := []any{
		textArray(kinds), textArray(queues), intArray(priorities), intArray(maxAttempts), timeArray(scheduledAt),
		jsonArray(args), jsonArray(metadata), nullableTextArray(uniqueKeys), floatArray(ttls), boolArray(awaits),
		nullableTextArray(ordering), nullableTextArray(partitions), uuidArray(batches),
	}
	var (
		results []driver.JobInsertResult
		err     error
	)
	if opts.Notify {
		// The notify rides in the same pipelined batch, so the insert and
		// its notification cost one round trip and commit together.
		results, err = scanInsertResults(e.Conn.QueryExec(ctx, query, queryArgs, NotifySQL, []any{driver.ChannelInsert, textArray(distinctQueues(params))}))
	} else {
		results, err = scanInsertResults(e.Conn.Query(ctx, query, queryArgs...))
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: insert jobs: %w", err)
	}
	if len(results) != n {
		return nil, fmt.Errorf("hopper: insert jobs: inserted %d rows for %d inputs", len(results), n)
	}
	return results, nil
}

// JobInsertCopy implements driver.Executor for transports without COPY.
func (e *Executor) JobInsertCopy(context.Context, []driver.JobInsertParams, driver.JobInsertOpts) ([]driver.JobInsertResult, error) {
	return nil, driver.ErrNotSupported
}

// claimEligibleSQL is the WHERE clause of a claim, over an alias cand. A job
// with an ordering key is claimable only if it is the oldest waiting job of
// its key and no job of that key is running; jobs without a key pay nothing
// for the check. The unique index on running ordering keys is the backstop
// for two clients passing the check at once, in which case one claim fails
// and is retried.
const claimEligibleSQL = `
    cand.queue = $1 AND cand.state IN ('available', 'scheduled', 'retryable')
      AND cand.scheduled_at <= now()
      AND (cand.expires_at IS NULL OR cand.expires_at > now())
      AND (cand.ordering_key IS NULL OR (
        NOT EXISTS (SELECT 1 FROM hopper_jobs r
                    WHERE r.queue = cand.queue AND r.ordering_key = cand.ordering_key AND r.state = 'running')
        AND cand.seq = (SELECT min(o.seq) FROM hopper_jobs o
                        WHERE o.queue = cand.queue AND o.ordering_key = cand.ordering_key
                          AND o.state IN ('available', 'scheduled', 'retryable'))))`

// jobClaimSQL is the hot path. The inner SELECT walks the claim index in
// claim order and locks its rows, skipping any locked by other clients. The
// outer SELECT restores claim order, which UPDATE ... RETURNING does not
// guarantee.
const jobClaimSQL = `
WITH claimed AS (
  UPDATE hopper_jobs j
  SET state = 'running', attempt = j.attempt + 1, attempted_at = now(), attempted_by = $2
  FROM (
    SELECT cand.id FROM hopper_jobs cand
    WHERE ` + claimEligibleSQL + `
    ORDER BY cand.priority, cand.scheduled_at, cand.seq
    LIMIT $3
    FOR UPDATE SKIP LOCKED
  ) c
  WHERE j.id = c.id
  RETURNING j.seq, ` + "%s" + `
)
SELECT ` + "%s" + ` FROM claimed ORDER BY priority, scheduled_at, seq`

// jobClaimPartitionedSQL ranks candidates within their partition key and
// admits only those whose rank, counting the key's running jobs, fits under
// the limit. The ranking scans the queue's waiting jobs, so only
// partition-limited queues pay for it.
const jobClaimPartitionedSQL = `
WITH ranked AS (
  SELECT cand.id, cand.partition_key,
    row_number() OVER (PARTITION BY cand.partition_key ORDER BY cand.priority, cand.scheduled_at, cand.seq)
      + coalesce((SELECT count(*) FROM hopper_jobs pr
                  WHERE pr.queue = cand.queue AND pr.partition_key = cand.partition_key AND pr.state = 'running'), 0) AS slot
  FROM hopper_jobs cand
  WHERE ` + claimEligibleSQL + `
),
claimed AS (
  UPDATE hopper_jobs j
  SET state = 'running', attempt = j.attempt + 1, attempted_at = now(), attempted_by = $2
  FROM (
    SELECT cand.id FROM hopper_jobs cand
    WHERE cand.id IN (SELECT id FROM ranked WHERE partition_key IS NULL OR slot <= $4)
    ORDER BY cand.priority, cand.scheduled_at, cand.seq
    LIMIT $3
    FOR UPDATE SKIP LOCKED
  ) c
  WHERE j.id = c.id
  RETURNING j.seq, ` + "%s" + `
)
SELECT ` + "%s" + ` FROM claimed ORDER BY priority, scheduled_at, seq`

var (
	jobClaimQuery            = fmt.Sprintf(jobClaimSQL, JobColumns("j."), JobColumns(""))
	jobClaimPartitionedQuery = fmt.Sprintf(jobClaimPartitionedSQL, JobColumns("j."), JobColumns(""))
)

// JobClaim implements driver.Executor.
func (e *Executor) JobClaim(ctx context.Context, params driver.JobClaimParams) (driver.JobClaimResult, error) {
	if params.Limit <= 0 {
		return driver.JobClaimResult{}, nil
	}
	if params.Limited {
		return e.claimLimited(ctx, params)
	}
	jobs, err := claimRows(ctx, e.Conn, jobClaimQuery, params.Queue, params.ClientID, params.Limit)
	if err != nil {
		return driver.JobClaimResult{}, err
	}
	return driver.JobClaimResult{Jobs: jobs}, nil
}

func claimRows(ctx context.Context, conn Conn, query string, args ...any) ([]*driver.JobRow, error) {
	jobs, err := liveJobs(conn.Query(ctx, query, args...))
	if err != nil {
		return nil, fmt.Errorf("hopper: claim jobs: %w", err)
	}
	return jobs, nil
}

// jobFinalizeSQL applies a batch of results in one statement. Every
// transition checks the prior state (running) and the owning client, so a
// stale result from a fenced client never overwrites a newer attempt.
// Retries and snoozes are updated in place; terminal outcomes go through
// finalizeTailSQL: history, batches, and workflow dependents. Error
// timestamps come from database time and awaited jobs announce themselves
// on the done channel.
var jobFinalizeSQL = `
WITH RECURSIVE r AS (
  SELECT * FROM unnest(
    $1::uuid[], $2::bigint[], $3::text[], $4::float8[], $5::boolean[], $6::jsonb[], $7::jsonb[], $8::boolean[]
  ) AS r(id, attempted_by, state, delay, snooze, error, output, archive)
),
retry AS (
  UPDATE hopper_jobs j
  SET state = r.state::hopper_job_state,
      scheduled_at = now() + make_interval(secs => r.delay),
      attempt = CASE WHEN r.snooze THEN j.attempt - 1 ELSE j.attempt END,
      errors = CASE WHEN r.error IS NULL THEN j.errors
                    ELSE j.errors || jsonb_set(r.error, '{at}', to_jsonb(now())) END,
      attempted_by = NULL
  FROM r
  WHERE j.id = r.id AND j.state = 'running' AND j.attempted_by = r.attempted_by
    AND r.state IN ('retryable', 'scheduled')
  RETURNING j.id
),
finished AS (
  DELETE FROM hopper_jobs j
  USING r
  WHERE j.id = r.id AND j.state = 'running' AND j.attempted_by = r.attempted_by
    AND r.state IN ('completed', 'cancelled', 'discarded')
  RETURNING ` + finishedColumns(`CASE WHEN r.error IS NULL THEN j.errors
         ELSE j.errors || jsonb_set(r.error, '{at}', to_jsonb(now())) END`, "r.output", "r.archive", "r.state", "$9") + `
),
` + finalizeTailSQL("$9", "$10") + `
SELECT id FROM retry UNION ALL SELECT id FROM finished`

// JobFinalizeMany implements driver.Executor.
func (e *Executor) JobFinalizeMany(ctx context.Context, params driver.JobFinalizeParams) ([]driver.JobID, error) {
	if len(params.Jobs) == 0 {
		return nil, nil
	}
	n := len(params.Jobs)
	var (
		ids         = make([]driver.JobID, n)
		attemptedBy = make([]int64, n)
		states      = make([]string, n)
		delays      = make([]float64, n)
		snoozes     = make([]bool, n)
		errs        = make([][]byte, n)
		outputs     = make([][]byte, n)
		archives    = make([]bool, n)
	)
	for i, f := range params.Jobs {
		switch f.State {
		case driver.JobStateCompleted, driver.JobStateCancelled, driver.JobStateDiscarded,
			driver.JobStateRetryable, driver.JobStateScheduled:
		default:
			return nil, fmt.Errorf("hopper: finalize jobs: %s is not a finalize state", f.State)
		}
		ids[i], attemptedBy[i], states[i] = f.ID, f.AttemptedBy, string(f.State)
		delays[i], snoozes[i] = max(f.Delay, 0).Seconds(), f.Snooze
		if f.Error != nil {
			b, err := marshalAttemptError(f.Error)
			if err != nil {
				return nil, fmt.Errorf("hopper: finalize jobs: %w", err)
			}
			errs[i] = b
		}
		outputs[i] = f.Output
		// Only completed jobs may skip history; the failed partitions are the
		// dead-letter queue.
		archives[i] = f.Archive || f.State != driver.JobStateCompleted
	}
	applied, err := scanIDs(e.Conn.Query(ctx, jobFinalizeSQL,
		uuidArray(ids), int64Array(attemptedBy), textArray(states), floatArray(delays), boolArray(snoozes),
		jsonArray(errs), jsonArray(outputs), boolArray(archives), driver.ChannelDone, driver.ChannelInsert,
	))
	if err != nil {
		return nil, fmt.Errorf("hopper: finalize jobs: %w", err)
	}
	return applied, nil
}

// marshalAttemptError encodes an error record without its timestamp, which
// the finalize statement fills in from database time.
func marshalAttemptError(e *driver.AttemptError) ([]byte, error) {
	return json.Marshal(struct {
		Attempt int    `json:"attempt"`
		Error   string `json:"error"`
		Trace   string `json:"trace,omitempty"`
	}{e.Attempt, e.Error, e.Trace})
}

var jobGetQuery = fmt.Sprintf(`
SELECT %s, NULL::timestamptz AS finalized_at, NULL::jsonb AS output FROM hopper_jobs WHERE id = $1
UNION ALL
SELECT %s, finalized_at, output FROM hopper_job_history WHERE id = $1
LIMIT 1`, JobColumns(""), JobColumns(""))

// JobGet implements driver.Executor.
func (e *Executor) JobGet(ctx context.Context, id driver.JobID) (*driver.JobRow, error) {
	job, err := scanJob(e.Conn.QueryRow(ctx, jobGetQuery, uuidParam(id)), true, nil)
	if errors.Is(err, ErrNoRows) {
		return nil, driver.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: get job: %w", err)
	}
	return job, nil
}

var (
	jobListLiveSQL    = fmt.Sprintf(`SELECT %s, NULL::timestamptz, NULL::jsonb FROM hopper_jobs`, JobColumns(""))
	jobListHistorySQL = fmt.Sprintf(`SELECT %s, finalized_at, output FROM hopper_job_history`, JobColumns(""))
	jobListFilterSQL  = ` WHERE ($1 = '' OR queue = $1) AND (cardinality($2::text[]) = 0 OR kind = ANY($2::text[]))
  AND (cardinality($3::text[]) = 0 OR state::text = ANY($3::text[])) AND id > $4::uuid`
)

// JobList implements driver.Executor.
func (e *Executor) JobList(ctx context.Context, params driver.JobListParams) ([]*driver.JobRow, error) {
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
	jobs, err := historyJobs(e.Conn.Query(ctx, query, params.Queue, textArray(params.Kinds), textArray(states), uuidParam(params.After), max(params.Limit, 1)))
	if err != nil {
		return nil, fmt.Errorf("hopper: list jobs: %w", err)
	}
	return jobs, nil
}

var (
	jobCancelRunningSQL = fmt.Sprintf(`
UPDATE hopper_jobs SET cancel_requested_at = coalesce(cancel_requested_at, now())
WHERE id = $1 AND state = 'running'
RETURNING %s`, JobColumns(""))
	// jobCancelWaitingSQL finalizes a waiting job as cancelled, with
	// everything that entails for its batch and its dependents.
	jobCancelWaitingSQL = `
WITH RECURSIVE finished AS (
  DELETE FROM hopper_jobs j WHERE j.id = $1 AND j.state <> 'running'
  RETURNING ` + finishedColumns(appendErrorSQL("hopper: cancelled"), "NULL::jsonb", "true", "'cancelled'::text", "$2") + `
),
` + finalizeTailSQL("$2", "$3") + `
SELECT ` + finishedSelectColumns + ` FROM finished`
)

// JobCancel implements driver.Executor.
func (e *Executor) JobCancel(ctx context.Context, id driver.JobID) (*driver.JobRow, error) {
	var job *driver.JobRow
	err := e.withTx(ctx, func(tx Tx) error {
		var state string
		err := tx.QueryRow(ctx, "SELECT state FROM hopper_jobs WHERE id = $1 FOR UPDATE", uuidParam(id)).Scan(&state)
		if errors.Is(err, ErrNoRows) {
			// Not live: finalized already, or unknown.
			job, err = e.JobGet(ctx, id)
			return err
		}
		if err != nil {
			return err
		}
		if driver.JobState(state) == driver.JobStateRunning {
			job, err = scanJob(tx.QueryRow(ctx, jobCancelRunningSQL, uuidParam(id)), false, nil)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, NotifySQL, driver.ChannelControl, textArray([]string{"cancel:" + id.String()}))
			return err
		}
		job, err = scanJob(tx.QueryRow(ctx, jobCancelWaitingSQL, uuidParam(id), driver.ChannelDone, driver.ChannelInsert), true, nil)
		return err
	})
	if err != nil {
		if errors.Is(err, driver.ErrNotFound) {
			return nil, driver.ErrNotFound
		}
		return nil, fmt.Errorf("hopper: cancel job: %w", err)
	}
	return job, nil
}

var (
	jobRetryLiveSQL = fmt.Sprintf(`
UPDATE hopper_jobs SET state = 'available', scheduled_at = now()
WHERE id = $1 AND state <> 'running'
RETURNING %s`, JobColumns(""))
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
RETURNING %s`, JobColumns(""))
)

// IsUniqueViolation reports a unique_violation error from the transport.
// Transports wrap their error type so that this can tell.
type uniqueViolation interface{ UniqueViolation() bool }

func isUniqueViolation(err error) bool {
	var uv uniqueViolation
	return errors.As(err, &uv) && uv.UniqueViolation()
}

// JobRetry implements driver.Executor.
func (e *Executor) JobRetry(ctx context.Context, id driver.JobID) (*driver.JobRow, error) {
	var job *driver.JobRow
	err := e.withTx(ctx, func(tx Tx) error {
		var state string
		err := tx.QueryRow(ctx, "SELECT state FROM hopper_jobs WHERE id = $1 FOR UPDATE", uuidParam(id)).Scan(&state)
		switch {
		case errors.Is(err, ErrNoRows):
			job, err = scanJob(tx.QueryRow(ctx, jobRetryHistorySQL, uuidParam(id)), false, nil)
			if errors.Is(err, ErrNoRows) {
				return driver.ErrNotFound
			}
			if isUniqueViolation(err) {
				return driver.ErrUniqueConflict
			}
			return err
		case err != nil:
			return err
		case driver.JobState(state) == driver.JobStateRunning:
			return driver.ErrJobRunning
		}
		job, err = scanJob(tx.QueryRow(ctx, jobRetryLiveSQL, uuidParam(id)), false, nil)
		return err
	})
	if err != nil {
		if errors.Is(err, driver.ErrNotFound) || errors.Is(err, driver.ErrJobRunning) || errors.Is(err, driver.ErrUniqueConflict) {
			return nil, err
		}
		return nil, fmt.Errorf("hopper: retry job: %w", err)
	}
	return job, nil
}

var jobDiscardExpiredSQL = `
WITH RECURSIVE finished AS (
  DELETE FROM hopper_jobs j
  WHERE j.id IN (
    SELECT id FROM hopper_jobs
    WHERE state IN ('available', 'scheduled', 'retryable') AND expires_at <= now()
    LIMIT $1 FOR UPDATE SKIP LOCKED
  )
  RETURNING ` + finishedColumns(appendErrorSQL("hopper: expired"), "NULL::jsonb", "true", "'discarded'::text", "$2") + `
),
` + finalizeTailSQL("$2", "$3") + `
SELECT ` + finishedSelectColumns + ` FROM finished`

// JobDiscardExpired implements driver.Executor.
func (e *Executor) JobDiscardExpired(ctx context.Context, limit int) ([]*driver.JobRow, error) {
	jobs, err := historyJobs(e.Conn.Query(ctx, jobDiscardExpiredSQL, max(limit, 1), driver.ChannelDone, driver.ChannelInsert))
	if err != nil {
		return nil, fmt.Errorf("hopper: discard expired jobs: %w", err)
	}
	return jobs, nil
}

// jobRescueSQL finds running jobs whose client has no live lease, plus jobs
// running longer than a stuck threshold. It walks the running index, which is
// bounded by total concurrency.
var jobRescueQuery = fmt.Sprintf(`
SELECT %s FROM hopper_jobs j
WHERE j.state = 'running'
  AND (NOT EXISTS (SELECT 1 FROM hopper_clients c WHERE c.id = j.attempted_by AND c.expires_at > now())
       OR ($1::float8 > 0 AND j.attempted_at < now() - make_interval(secs => $1::float8)))
ORDER BY j.attempted_at
LIMIT $2`, JobColumns("j."))

// JobRescueCandidates implements driver.Executor.
func (e *Executor) JobRescueCandidates(ctx context.Context, params driver.JobRescueParams) ([]*driver.JobRow, error) {
	jobs, err := liveJobs(e.Conn.Query(ctx, jobRescueQuery, max(params.StuckAfter, 0).Seconds(), params.Limit))
	if err != nil {
		return nil, fmt.Errorf("hopper: rescue candidates: %w", err)
	}
	return jobs, nil
}

// JobAge implements driver.Executor.
func (e *Executor) JobAge(ctx context.Context, queue string, after time.Duration) (int64, error) {
	if after <= 0 {
		return 0, nil
	}
	// Bumped jobs restart their wait, so a job climbs one level per period.
	n, err := e.Conn.Exec(ctx, `
		UPDATE hopper_jobs SET priority = priority - 1, scheduled_at = now()
		WHERE queue = $1 AND state IN ('available', 'scheduled', 'retryable') AND priority > 1
		  AND scheduled_at <= now() - make_interval(secs => $2::float8)`, queue, after.Seconds())
	if err != nil {
		return 0, fmt.Errorf("hopper: age jobs: %w", err)
	}
	return n, nil
}

// Now implements driver.Executor.
func (e *Executor) Now(ctx context.Context) (time.Time, error) {
	var now time.Time
	if err := e.Conn.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("hopper: now: %w", err)
	}
	return now, nil
}
