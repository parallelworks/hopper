package pgsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/parallelworks/hopper/driver"
)

var jobColumns = []string{
	"id", "kind", "queue", "state", "priority", "attempt", "max_attempts",
	"scheduled_at", "attempted_at", "attempted_by", "args", "metadata", "errors",
	"unique_key", "ordering_key", "partition_key", "batch_id", "expires_at", "cancel_requested_at", "await", "created_at",
	"snooze_reason",
}

// JobColumns lists hopper_jobs columns in scan order. prefix qualifies them
// with a table alias.
func JobColumns(prefix string) string {
	out := make([]string, len(jobColumns))
	for i, c := range jobColumns {
		out[i] = prefix + c
	}
	return strings.Join(out, ", ")
}

// historyColumns lists hopper_job_history columns in JobColumns order.
// History has no snooze_reason, because a finalized job is not waiting.
func historyColumns(prefix string) string {
	return strings.Replace(JobColumns(prefix), prefix+"snooze_reason", "NULL::text AS snooze_reason", 1)
}

// scanJob scans a row of JobColumns, plus finalized_at and output when
// history is true, plus trailing columns into extra.
func scanJob(row Row, history bool, extra ...any) (*driver.JobRow, error) {
	var (
		j                                driver.JobRow
		state                            string
		priority, attempt, maxAttempts   int
		attemptedAt, expiresAt, cancelAt sql.NullTime
		finalizedAt                      sql.NullTime
		attemptedBy                      sql.NullInt64
		uniqueKey, orderingKey, partKey  sql.NullString
		snoozeReason                     sql.NullString
		args, metadata, output           jsonText
		errs                             attemptErrors
		batchID                          nullUUID
	)
	dest := []any{
		&j.ID, &j.Kind, &j.Queue, &state, &priority, &attempt, &maxAttempts,
		&j.ScheduledAt, &attemptedAt, &attemptedBy, &args, &metadata, &errs,
		&uniqueKey, &orderingKey, &partKey, &batchID, &expiresAt, &cancelAt, &j.Await, &j.CreatedAt,
		&snoozeReason,
	}
	if history {
		dest = append(dest, &finalizedAt, &output)
	}
	if err := row.Scan(append(dest, extra...)...); err != nil {
		return nil, err
	}
	j.State = driver.JobState(state)
	j.Priority, j.Attempt, j.MaxAttempts = priority, attempt, maxAttempts
	j.AttemptedAt, j.ExpiresAt, j.CancelRequestedAt, j.FinalizedAt = attemptedAt.Time, expiresAt.Time, cancelAt.Time, finalizedAt.Time
	j.AttemptedBy = attemptedBy.Int64
	j.Args, j.Metadata, j.Output = args.raw(), metadata.raw(), output.raw()
	j.Errors = []driver.AttemptError(errs)
	j.UniqueKey, j.OrderingKey, j.PartitionKey = uniqueKey.String, orderingKey.String, partKey.String
	// Only a snoozed job is waiting for a reason. A client of the previous
	// release neither clears it on claim nor on finalize, so during a rolling
	// deploy a running or retrying row can still hold one.
	if j.State == driver.JobStateScheduled {
		j.SnoozeReason = snoozeReason.String
	}
	if batchID.Valid {
		j.BatchID = batchID.ID
	}
	return &j, nil
}

var (
	liveJobs    = collect(func(r Row) (*driver.JobRow, error) { return scanJob(r, false) })
	historyJobs = collect(func(r Row) (*driver.JobRow, error) { return scanJob(r, true) })
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
INSERT INTO {{schema}}.hopper_jobs (kind, queue, state, priority, max_attempts, scheduled_at, args, metadata, unique_key, expires_at, await, ordering_key, partition_key, batch_id)
SELECT p.kind, p.queue,
       CASE WHEN p.scheduled_at > now() THEN 'scheduled' ELSE 'available' END::{{schema}}.hopper_job_state,
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
  state        = CASE WHEN hopper_jobs.state = 'running' THEN hopper_jobs.state        ELSE EXCLUDED.state        END,
  snooze_reason = NULL`
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
	if slices.ContainsFunc(params, countsHistory) {
		return e.jobInsertCheckingHistory(ctx, params, opts)
	}
	return e.jobInsert(ctx, params, opts)
}

func (e *Executor) jobInsert(ctx context.Context, params []driver.JobInsertParams, opts driver.JobInsertOpts) ([]driver.JobInsertResult, error) {
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

func countsHistory(p driver.JobInsertParams) bool {
	return p.UniqueKey != "" && len(p.UniqueFinished) > 0
}

// jobInsertCheckingHistory inserts a batch in which some jobs count finished
// duplicates in history. It inserts the batch as usual, which reports or
// replaces live duplicates, and then looks in history for the jobs it
// inserted, deleting each one whose duplicate it finds there, in the same
// transaction, before any worker can claim it. Looking after the insert
// rather than before also finds a duplicate finalized while the insert ran:
// its live row was gone, and its history row not yet in the insert's
// snapshot, but the insert's conflict check waited for that finalize to
// commit, and the look, a statement of its own at read committed, takes a
// snapshot that includes it. A duplicate already in history costs a row
// inserted and deleted, which is less than looking it up twice.
func (e *Executor) jobInsertCheckingHistory(ctx context.Context, params []driver.JobInsertParams, opts driver.JobInsertOpts) ([]driver.JobInsertResult, error) {
	var results []driver.JobInsertResult
	insert := func(conn Conn) error {
		var err error
		results, err = (&Executor{Conn: conn, Schema: e.Schema, InTx: true}).jobInsert(ctx, params, opts)
		if err != nil {
			return err
		}
		found, err := uniqueHistory(ctx, conn, params, results)
		if err != nil {
			return err
		}
		for i, job := range found {
			if job != nil {
				results[i] = driver.JobInsertResult{Job: job, Duplicate: true}
			}
		}
		return recordInBatches(ctx, conn, params, results)
	}
	var err error
	if e.InTx {
		err = insert(e.Conn)
	} else {
		err = e.withTx(ctx, func(tx Tx) error {
			// The look needs a snapshot of its own, which only read
			// committed takes per statement. The server's default
			// (default_transaction_isolation) may be stricter.
			if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"); err != nil {
				return fmt.Errorf("hopper: insert jobs: %w", err)
			}
			return insert(tx)
		})
	}
	if err != nil {
		return nil, err
	}
	return results, nil
}

// inBatch reports whether a job of a batch was found finished in history.
func inBatch(params []driver.JobInsertParams, results []driver.JobInsertResult) bool {
	for i, r := range results {
		if r.Job != nil && !params[i].BatchID.IsZero() && r.Duplicate && !r.Job.FinalizedAt.IsZero() {
			return true
		}
	}
	return false
}

// recordInBatches counts each job of a batch that was found finished in
// history as that job's outcome, done and, unless it completed, failed, in
// the batch's progress, as its finalize would have. The job never joins the
// batch, which would otherwise wait for it forever. BatchSettle completes a
// batch this leaves with nothing pending.
func recordInBatches(ctx context.Context, conn Conn, params []driver.JobInsertParams, results []driver.JobInsertResult) error {
	if !inBatch(params, results) {
		return nil
	}
	var (
		batches []driver.JobID
		failed  []bool
	)
	for i, r := range results {
		if r.Job != nil && !params[i].BatchID.IsZero() && r.Duplicate && !r.Job.FinalizedAt.IsZero() {
			batches = append(batches, params[i].BatchID)
			failed = append(failed, r.Job.State != driver.JobStateCompleted)
		}
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO {{schema}}.hopper_batch_progress (batch_id, done, failed)
		SELECT f.batch_id, count(*), count(*) FILTER (WHERE f.failed)
		FROM unnest($1::uuid[], $2::boolean[]) AS f(batch_id, failed)
		GROUP BY f.batch_id`, uuidArray(batches), boolArray(failed)); err != nil {
		return fmt.Errorf("hopper: insert jobs: record finished duplicates in their batch: %w", err)
	}
	return nil
}

// uniqueHistorySQL finds the latest duplicate in history of each job just
// inserted, and deletes the job. It reads the outcome table %[1]s, which
// holds every state the jobs count.
const uniqueHistorySQL = `
WITH p AS (
  SELECT p.ord, p.kind, p.unique_key, p.inserted, string_to_array(p.finished, ',')::{{schema}}.hopper_job_state[] AS states
  FROM unnest($1::int[], $2::text[], $3::text[], $4::text[], $5::uuid[]) AS p(ord, kind, unique_key, finished, inserted)
), hit AS (
  SELECT p.ord, p.inserted, h.*
  FROM p CROSS JOIN LATERAL (
    SELECT %[2]s, x.finalized_at, x.output FROM {{schema}}.%[1]s x
    WHERE x.kind = p.kind AND x.unique_key = p.unique_key AND x.state = ANY(p.states)
    ORDER BY x.finalized_at DESC
    LIMIT 1
  ) h
), del AS (
  DELETE FROM {{schema}}.hopper_jobs j USING hit WHERE j.id = hit.inserted
)
SELECT %[3]s, finalized_at, output, ord FROM hit`

// uniqueHistoryQueries reads only the completed or the failed outcome table
// when the jobs count states of one alone, so the other's partitions are
// neither planned nor probed.
var uniqueHistoryQueries = map[string]string{
	"hopper_job_history":           fmt.Sprintf(uniqueHistorySQL, "hopper_job_history", historyColumns("x."), JobColumns("")),
	"hopper_job_history_completed": fmt.Sprintf(uniqueHistorySQL, "hopper_job_history_completed", historyColumns("x."), JobColumns("")),
	"hopper_job_history_failed":    fmt.Sprintf(uniqueHistorySQL, "hopper_job_history_failed", historyColumns("x."), JobColumns("")),
}

// uniqueHistory looks in history for a duplicate of each job inserted (not
// a duplicate itself) that counts history, deletes each job it finds one
// for, and returns the duplicates by input, nil where there was none.
func uniqueHistory(ctx context.Context, conn Conn, params []driver.JobInsertParams, inserted []driver.JobInsertResult) ([]*driver.JobRow, error) {
	var (
		ords              []int
		kinds, keys, fin  []string
		ids               []driver.JobID
		completed, failed bool
	)
	for i, p := range params {
		if !countsHistory(p) || inserted[i].Duplicate {
			continue
		}
		states := make([]string, len(p.UniqueFinished))
		for k, s := range p.UniqueFinished {
			states[k] = string(s)
			if s == driver.JobStateCompleted {
				completed = true
			} else {
				failed = true
			}
		}
		ords, ids = append(ords, i), append(ids, inserted[i].Job.ID)
		kinds, keys, fin = append(kinds, p.Kind), append(keys, p.UniqueKey), append(fin, strings.Join(states, ","))
	}
	found := make([]*driver.JobRow, len(params))
	if len(ords) == 0 {
		return found, nil
	}
	table := "hopper_job_history"
	if !failed {
		table = "hopper_job_history_completed"
	} else if !completed {
		table = "hopper_job_history_failed"
	}
	type hit struct {
		ord int
		job *driver.JobRow
	}
	hits, err := collect(func(r Row) (hit, error) {
		var h hit
		var err error
		h.job, err = scanJob(r, true, &h.ord)
		return h, err
	})(conn.Query(ctx, uniqueHistoryQueries[table], intArray(ords), textArray(kinds), textArray(keys), textArray(fin), uuidArray(ids)))
	if err != nil {
		return nil, fmt.Errorf("hopper: insert jobs: find duplicates in history: %w", err)
	}
	for _, h := range hits {
		found[h.ord] = h.job
	}
	return found, nil
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
        NOT EXISTS (SELECT 1 FROM {{schema}}.hopper_jobs r
                    WHERE r.queue = cand.queue AND r.ordering_key = cand.ordering_key AND r.state = 'running')
        AND cand.seq = (SELECT min(o.seq) FROM {{schema}}.hopper_jobs o
                        WHERE o.queue = cand.queue AND o.ordering_key = cand.ordering_key
                          AND o.state IN ('available', 'scheduled', 'retryable'))))`

// jobClaimSQL is the hot path. The candidate CTE walks the claim index in
// claim order and locks its rows, skipping any locked by other clients. It
// is MATERIALIZED so that it runs exactly once however the planner joins
// it to the update: as a plain subquery the planner may put it on the
// inner side of a nested loop and re-execute it, locks and all, for every
// row of the table it thinks is empty. The outer SELECT restores claim
// order, which UPDATE ... RETURNING does not guarantee.
//
// The LIMIT is a constant, one statement per bucket (claimBuckets), and
// the count asked for is applied afterwards through row numbers. With the
// limit as a parameter the planner's generic plan assumes it selects a
// tenth of the table and joins the update to it with a sequential scan
// and a hash: every claim then scans the whole table, or, in the automatic
// plan cache mode, the planner rejects that plan and replans every
// execution instead, which costs a fifth of a small server's throughput.
// Rows locked beyond the count are released when the statement ends.
const jobClaimSQL = `
WITH c AS MATERIALIZED (
  SELECT cand.id, row_number() OVER (ORDER BY cand.priority, cand.scheduled_at, cand.seq) AS n FROM (
    SELECT cand.id, cand.priority, cand.scheduled_at, cand.seq FROM {{schema}}.hopper_jobs cand
    WHERE ` + claimEligibleSQL + `
    ORDER BY cand.priority, cand.scheduled_at, cand.seq
    LIMIT %d
    FOR UPDATE SKIP LOCKED
  ) cand
),
claimed AS (
  UPDATE {{schema}}.hopper_jobs j
  SET state = 'running', attempt = j.attempt + 1, attempted_at = now(), attempted_by = $2, snooze_reason = NULL
  FROM c
  WHERE j.id = c.id AND c.n <= $3
  RETURNING j.seq, ` + "%s" + `
)
SELECT ` + "%s" + ` FROM claimed ORDER BY priority, scheduled_at, seq`

// jobClaimPartitionedSQL ranks candidates within their partition key and
// admits only those whose rank, counting the key's running jobs, fits under
// the limit. The ranking scans the queue's waiting jobs, so only
// partition-limited queues pay for it.
const jobClaimPartitionedSQL = `
WITH ranked AS MATERIALIZED (
  SELECT cand.id, cand.partition_key,
    row_number() OVER (PARTITION BY cand.partition_key ORDER BY cand.priority, cand.scheduled_at, cand.seq)
      + coalesce((SELECT count(*) FROM {{schema}}.hopper_jobs pr
                  WHERE pr.queue = cand.queue AND pr.partition_key = cand.partition_key AND pr.state = 'running'), 0) AS slot
  FROM {{schema}}.hopper_jobs cand
  WHERE ` + claimEligibleSQL + `
),
c AS MATERIALIZED (
  SELECT cand.id, row_number() OVER (ORDER BY cand.priority, cand.scheduled_at, cand.seq) AS n FROM (
    SELECT cand.id, cand.priority, cand.scheduled_at, cand.seq FROM {{schema}}.hopper_jobs cand
    WHERE cand.id IN (SELECT id FROM ranked WHERE partition_key IS NULL OR slot <= $4)
    ORDER BY cand.priority, cand.scheduled_at, cand.seq
    LIMIT %d
    FOR UPDATE SKIP LOCKED
  ) cand
),
claimed AS (
  UPDATE {{schema}}.hopper_jobs j
  SET state = 'running', attempt = j.attempt + 1, attempted_at = now(), attempted_by = $2, snooze_reason = NULL
  FROM c
  WHERE j.id = c.id AND c.n <= $3
  RETURNING j.seq, ` + "%s" + `
)
SELECT ` + "%s" + ` FROM claimed ORDER BY priority, scheduled_at, seq`

// claimBuckets are the LIMIT constants the claim statements are built
// with; a claim uses the smallest bucket that covers its count, and locks
// at most that many rows. ClaimMax is the largest count a claim can ask
// for.
var claimBuckets = []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512}

// ClaimMax is the most jobs one claim statement returns.
const ClaimMax = 512

var (
	jobClaimQueries            = claimQueries(jobClaimSQL)
	jobClaimPartitionedQueries = claimQueries(jobClaimPartitionedSQL)
)

func claimQueries(format string) map[int]string {
	out := make(map[int]string, len(claimBuckets))
	for _, b := range claimBuckets {
		out[b] = fmt.Sprintf(format, b, JobColumns("j."), JobColumns(""))
	}
	return out
}

// claimQuery returns the statement for a count and the count clamped to
// ClaimMax.
func claimQuery(queries map[int]string, limit int) (string, int) {
	limit = min(max(limit, 1), ClaimMax)
	for _, b := range claimBuckets {
		if limit <= b {
			return queries[b], limit
		}
	}
	return queries[ClaimMax], ClaimMax
}

// JobClaim implements driver.Executor.
func (e *Executor) JobClaim(ctx context.Context, params driver.JobClaimParams) (driver.JobClaimResult, error) {
	if params.Limit <= 0 {
		return driver.JobClaimResult{}, nil
	}
	if params.Limited {
		return e.claimLimited(ctx, params)
	}
	query, limit := claimQuery(jobClaimQueries, params.Limit)
	jobs, err := claimRows(ctx, e.Conn, query, params.Queue, params.ClientID, limit)
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
// Retries and snoozes are updated in place, with a snooze's reason (or
// NULL), and run at once if JobRetryOrderingKey woke the key during the
// attempt; terminal outcomes go through finalizeTailSQL: history, batches,
// and workflow dependents. Error timestamps come from database time and
// awaited jobs announce themselves on the done channel.
var jobFinalizeHeadSQL = `
WITH RECURSIVE r AS (
  SELECT * FROM unnest(
    $1::uuid[], $2::bigint[], $3::text[], $4::float8[], $5::boolean[], $6::jsonb[], $7::jsonb[], $8::boolean[], $11::text[]
  ) AS r(id, attempted_by, state, delay, snooze, error, output, archive, snooze_reason)
),
retry AS (
  UPDATE {{schema}}.hopper_jobs j
  SET state = r.state::{{schema}}.hopper_job_state,
      scheduled_at = CASE WHEN coalesce(j.wake_requested_at >= j.attempted_at, false) THEN now()
                          ELSE now() + make_interval(secs => r.delay) END,
      attempt = CASE WHEN r.snooze THEN j.attempt - 1 ELSE j.attempt END,
      errors = CASE WHEN r.error IS NULL THEN j.errors
                    ELSE j.errors || jsonb_set(r.error, '{at}', to_jsonb(now())) END,
      attempted_by = NULL,
      snooze_reason = CASE WHEN r.snooze AND NOT coalesce(j.wake_requested_at >= j.attempted_at, false)
                           THEN r.snooze_reason END,
      wake_requested_at = NULL
  FROM r
  WHERE j.id = r.id AND j.state = 'running' AND j.attempted_by = r.attempted_by
    AND r.state IN ('retryable', 'scheduled')
  RETURNING j.id
),
finished AS (
  DELETE FROM {{schema}}.hopper_jobs j
  USING r
  WHERE j.id = r.id AND j.state = 'running' AND j.attempted_by = r.attempted_by
    AND r.state IN ('completed', 'canceled', 'discarded')
  RETURNING ` + finishedColumns(`CASE WHEN r.error IS NULL THEN j.errors
         ELSE j.errors || jsonb_set(r.error, '{at}', to_jsonb(now())) END`, "r.output", "r.archive", "r.state", "$9") + `
),
`

var jobFinalizeSQL = jobFinalizeHeadSQL + finalizeTailSQL("$9", "$10") + `
SELECT id FROM retry UNION ALL SELECT id FROM finished`

// jobFinalizePlainSQL is jobFinalizeSQL for results whose jobs belong to
// no batch or workflow (driver.JobFinalizeParams.NoBatches): without the
// insert channel, so the snooze reasons are $10.
var jobFinalizePlainSQL = strings.ReplaceAll(jobFinalizeHeadSQL, "$11", "$10") + plainFinalizeTailSQL + `
SELECT id FROM retry UNION ALL SELECT id FROM finished`

// skipLockedSQL makes a finalize statement skip the jobs whose rows another
// transaction holds (driver.JobFinalizeParams.SkipLocked).
func skipLockedSQL(query string) string {
	const unnested = "  ) AS r(id, attempted_by, state, delay, snooze, error, output, archive, snooze_reason)\n"
	if !strings.Contains(query, unnested) {
		panic("pgsql: finalize statement has no unnested results")
	}
	return strings.Replace(query, unnested, unnested+
		"  WHERE r.id IN (SELECT id FROM {{schema}}.hopper_jobs WHERE id = ANY($1::uuid[]) FOR UPDATE SKIP LOCKED)\n", 1)
}

var (
	jobFinalizeSkipLockedSQL      = skipLockedSQL(jobFinalizeSQL)
	jobFinalizePlainSkipLockedSQL = skipLockedSQL(jobFinalizePlainSQL)
)

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
		reasons     = make([]string, n)
		errs        = make([][]byte, n)
		outputs     = make([][]byte, n)
		archives    = make([]bool, n)
	)
	for i, f := range params.Jobs {
		switch f.State {
		case driver.JobStateCompleted, driver.JobStateCanceled, driver.JobStateDiscarded,
			driver.JobStateRetryable, driver.JobStateScheduled:
		default:
			return nil, fmt.Errorf("hopper: finalize jobs: %s is not a finalize state", f.State)
		}
		ids[i], attemptedBy[i], states[i] = f.ID, f.AttemptedBy, string(f.State)
		delays[i], snoozes[i], reasons[i] = max(f.Delay, 0).Seconds(), f.Snooze, storableText(f.SnoozeReason)
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
	args := []any{
		uuidArray(ids), int64Array(attemptedBy), textArray(states), floatArray(delays), boolArray(snoozes),
		jsonArray(errs), jsonArray(outputs), boolArray(archives), driver.ChannelDone, driver.ChannelInsert,
		nullableTextArray(reasons),
	}
	query := jobFinalizeSQL
	if params.SkipLocked {
		query = jobFinalizeSkipLockedSQL
	}
	if params.NoBatches {
		query, args = jobFinalizePlainSQL, append(args[:9], args[10])
		if params.SkipLocked {
			query = jobFinalizePlainSkipLockedSQL
		}
	}
	applied, err := scanIDs(e.Conn.Query(ctx, query, args...))
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
	}{e.Attempt, storableText(e.Error), storableText(e.Trace)})
}

var jobGetQuery = fmt.Sprintf(`
SELECT %s, NULL::timestamptz AS finalized_at, NULL::jsonb AS output FROM {{schema}}.hopper_jobs WHERE id = $1
UNION ALL
SELECT %s, finalized_at, output FROM {{schema}}.hopper_job_history WHERE id = $1
LIMIT 1`, JobColumns(""), historyColumns(""))

// JobGet implements driver.Executor.
func (e *Executor) JobGet(ctx context.Context, id driver.JobID) (*driver.JobRow, error) {
	job, err := scanJob(e.Conn.QueryRow(ctx, jobGetQuery, uuidParam(id)), true)
	if errors.Is(err, ErrNoRows) {
		return nil, driver.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: get job: %w", err)
	}
	return job, nil
}

var (
	jobListLiveSQL    = fmt.Sprintf(`SELECT %s, NULL::timestamptz, NULL::jsonb FROM {{schema}}.hopper_jobs`, JobColumns(""))
	jobListHistorySQL = fmt.Sprintf(`SELECT %s, finalized_at, output FROM {{schema}}.hopper_job_history`, historyColumns(""))
	jobListFilterSQL  = ` WHERE ($1 = '' OR queue = $1) AND (cardinality($2::text[]) = 0 OR kind = ANY($2::text[]))
  AND (cardinality($3::text[]) = 0 OR state::text = ANY($3::text[]))`
	// A key's live jobs are found through the claim's ordering-key indexes,
	// one for waiting jobs and one for the running job, and its pending jobs
	// through their dependency edges, which only live workflow jobs have.
	jobListKeyedSQL   = ` AND (state IN ('available', 'scheduled', 'retryable') OR state = 'running')`
	jobListPendingSQL = ` AND state = 'pending' AND id IN (SELECT job_id FROM {{schema}}.hopper_job_deps)`
)

// maxJobID starts a newest-first listing that has no cursor.
var maxJobID = driver.JobID{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

// errOrderingKeyNeedsQueue refuses a key listing across queues: a key is a
// lane only within its queue.
var errOrderingKeyNeedsQueue = errors.New("hopper: list jobs: an ordering key is listed within a queue; set the queue")

// JobList implements driver.Executor. The ordering key is matched only when
// set, as a plain equality, so the ordering-key indexes serve it.
func (e *Executor) JobList(ctx context.Context, params driver.JobListParams) ([]*driver.JobRow, error) {
	if params.OrderingKey != "" && params.Queue == "" {
		return nil, errOrderingKeyNeedsQueue
	}
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
	states := make([]string, len(params.States))
	for i, s := range params.States {
		states[i] = string(s)
	}
	after := params.After
	if params.NewestFirst && after.IsZero() {
		after = maxJobID
	}
	args := []any{params.Queue, textArray(params.Kinds), textArray(states), uuidParam(after), max(params.Limit, 1)}
	filter, order := jobListFilterSQL+" AND id > $4::uuid", " ORDER BY id LIMIT $5"
	if params.NewestFirst {
		filter, order = jobListFilterSQL+" AND id < $4::uuid", " ORDER BY id DESC LIMIT $5"
	}
	if params.OrderingKey != "" {
		filter += " AND ordering_key = $6"
		args = append(args, params.OrderingKey)
	}
	var arms []string
	if live {
		if params.OrderingKey == "" {
			arms = append(arms, jobListLiveSQL+filter)
		} else {
			arms = append(arms, jobListLiveSQL+filter+jobListKeyedSQL, jobListLiveSQL+filter+jobListPendingSQL)
		}
	}
	if history {
		arms = append(arms, jobListHistorySQL+filter)
	}
	query := arms[0]
	if len(arms) > 1 {
		query = "(" + strings.Join(arms, order+") UNION ALL (") + order + ")"
	}
	query += order
	jobs, err := historyJobs(e.Conn.Query(ctx, query, args...))
	if err != nil {
		return nil, fmt.Errorf("hopper: list jobs: %w", err)
	}
	return jobs, nil
}

// jobLockStateSQL locks a live job's row and reads its state, which the
// control operations branch on.
const jobLockStateSQL = "SELECT state FROM {{schema}}.hopper_jobs WHERE id = $1 FOR UPDATE"

var (
	jobCancelRunningSQL = fmt.Sprintf(`
UPDATE {{schema}}.hopper_jobs SET cancel_requested_at = coalesce(cancel_requested_at, now())
WHERE id = $1 AND state = 'running'
RETURNING %s`, JobColumns(""))
	// jobCancelWaitingSQL finalizes a waiting job as canceled, with
	// everything that entails for its batch and its dependents.
	jobCancelWaitingSQL = `
WITH RECURSIVE finished AS (
  DELETE FROM {{schema}}.hopper_jobs j WHERE j.id = $1 AND j.state <> 'running'
  RETURNING ` + finishedColumns(appendErrorSQL("hopper: canceled"), "NULL::jsonb", "true", "'canceled'::text", "$2") + `
),
` + finalizeTailSQL("$2", "$3") + `
SELECT ` + finishedSelectColumns + ` FROM finished`
	// jobDeleteWaitingSQL is jobCancelWaitingSQL without the job's history
	// row: its batch, its dependents and anyone awaiting it see it canceled,
	// and nothing of it is kept.
	jobDeleteWaitingSQL = `
WITH RECURSIVE finished AS (
  DELETE FROM {{schema}}.hopper_jobs j WHERE j.id = $1 AND j.state <> 'running'
  RETURNING ` + finishedColumns(appendErrorSQL("hopper: canceled"), "NULL::jsonb", "false", "'canceled'::text", "$2") + `
),
` + finalizeTailSQL("$2", "$3") + `
SELECT ` + finishedSelectColumns + ` FROM finished`
)

// JobCancel implements driver.Executor.
func (e *Executor) JobCancel(ctx context.Context, id driver.JobID) (*driver.JobRow, error) {
	var job *driver.JobRow
	err := e.withTx(ctx, func(tx Tx) error {
		var state string
		err := tx.QueryRow(ctx, jobLockStateSQL, uuidParam(id)).Scan(&state)
		if errors.Is(err, ErrNoRows) {
			// Not live: finalized already, or unknown.
			job, err = e.JobGet(ctx, id)
			return err
		}
		if err != nil {
			return err
		}
		if driver.JobState(state) == driver.JobStateRunning {
			job, err = scanJob(tx.QueryRow(ctx, jobCancelRunningSQL, uuidParam(id)), false)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, NotifySQL, driver.ChannelControl, textArray([]string{"cancel:" + id.String()}))
			return err
		}
		job, err = scanJob(tx.QueryRow(ctx, jobCancelWaitingSQL, uuidParam(id), driver.ChannelDone, driver.ChannelInsert), true)
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

// jobDeleteHistorySQL removes a finalized job from history.
var jobDeleteHistorySQL = fmt.Sprintf(`DELETE FROM {{schema}}.hopper_job_history WHERE id = $1
RETURNING %s, finalized_at, output`, historyColumns(""))

// JobDelete implements driver.Executor. A JobRetry that is moving the job
// from history back to the live table makes the history delete wait for it
// and then find nothing, so before the job is called unknown the live table,
// where the retry put it, is read once more.
func (e *Executor) JobDelete(ctx context.Context, id driver.JobID) (*driver.JobRow, error) {
	var job *driver.JobRow
	err := e.withTx(ctx, func(tx Tx) error {
		// A finalized job that a JobRetry moves back to the live table while
		// this waits on its history row is found live on the second look.
		for second := false; ; second = true {
			var state string
			err := tx.QueryRow(ctx, jobLockStateSQL, uuidParam(id)).Scan(&state)
			switch {
			case errors.Is(err, ErrNoRows):
				job, err = scanJob(tx.QueryRow(ctx, jobDeleteHistorySQL, uuidParam(id)), true)
				if errors.Is(err, ErrNoRows) {
					if second {
						return driver.ErrNotFound
					}
					continue
				}
				return err
			case err != nil:
				return err
			case driver.JobState(state) == driver.JobStateRunning:
				return driver.ErrJobRunning
			default:
				job, err = scanJob(tx.QueryRow(ctx, jobDeleteWaitingSQL, uuidParam(id), driver.ChannelDone, driver.ChannelInsert), true)
				return err
			}
		}
	})
	if err != nil {
		if errors.Is(err, driver.ErrNotFound) || errors.Is(err, driver.ErrJobRunning) {
			return nil, err
		}
		return nil, fmt.Errorf("hopper: delete job: %w", err)
	}
	return job, nil
}

// jobRunNowSQL is the SET list that makes a waiting job runnable now and
// drops the reason it was waiting. A job that was already due keeps its
// scheduled_at, so it keeps its place in claim order and its aging. Now is
// the statement's time: in a caller's transaction (JobRetryTx) the
// transaction's began earlier.
const jobRunNowSQL = `state = 'available', scheduled_at = least(scheduled_at, statement_timestamp()), snooze_reason = NULL, wake_requested_at = NULL`

var (
	jobRetryLiveSQL = fmt.Sprintf(`
UPDATE {{schema}}.hopper_jobs SET `+jobRunNowSQL+`, expires_at = NULL
WHERE id = $1 AND state <> 'running'
RETURNING %s`, JobColumns(""))
	// jobRetryHistorySQL moves a finalized job back to the live table with
	// its history (attempt count and errors) intact. It gets a fresh seq,
	// as a newly inserted job would, and one more attempt if it had run out.
	jobRetryHistorySQL = fmt.Sprintf(`
WITH h AS (DELETE FROM {{schema}}.hopper_job_history WHERE id = $1 RETURNING *)
INSERT INTO {{schema}}.hopper_jobs (id, kind, queue, state, priority, attempt, max_attempts, scheduled_at, args, metadata,
  errors, unique_key, ordering_key, partition_key, batch_id, await, created_at)
SELECT id, kind, queue, 'available', priority, attempt,
  CASE WHEN attempt >= max_attempts THEN attempt + 1 ELSE max_attempts END, statement_timestamp(), args, metadata,
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
		err := tx.QueryRow(ctx, jobLockStateSQL, uuidParam(id)).Scan(&state)
		switch {
		case errors.Is(err, ErrNoRows):
			job, err = scanJob(tx.QueryRow(ctx, jobRetryHistorySQL, uuidParam(id)), false)
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
		job, err = scanJob(tx.QueryRow(ctx, jobRetryLiveSQL, uuidParam(id)), false)
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

// jobMarkOrderingKeySQL marks the key's running job (through the unique
// index on running ordering keys) so that its finalize honors the wake. The
// mark is the statement's time, not the transaction's: in a caller's
// transaction that began before the attempt, now() would date the mark
// before attempted_at, and the finalize would take it for a stale one.
const jobMarkOrderingKeySQL = `
UPDATE {{schema}}.hopper_jobs SET wake_requested_at = statement_timestamp()
WHERE queue = $1 AND ordering_key = $2 AND state = 'running'`

// jobRetryOrderingKeySQL locks a batch of the key's waiting jobs in seq
// order through hopper_jobs_ordering and makes them runnable. Jobs past
// their TTL are left to the expiry sweep, so whether one runs does not
// depend on whether the sweep reached it first. The claim, the aging pass
// and the expiry sweep skip locked rows, so they cannot deadlock with this
// fixed lock order. A replacing insert of several jobs of one key still
// can, and Postgres then aborts one of the two statements.
const jobRetryOrderingKeySQL = `
WITH waiting AS (
  SELECT id FROM {{schema}}.hopper_jobs
  WHERE queue = $1 AND ordering_key = $2 AND state IN ('scheduled', 'retryable')
    AND (expires_at IS NULL OR expires_at > statement_timestamp())
  ORDER BY seq
  LIMIT $3
  FOR UPDATE
),
woken AS (
  UPDATE {{schema}}.hopper_jobs j SET ` + jobRunNowSQL + `
  FROM waiting w
  WHERE j.id = w.id
  RETURNING j.id
)
SELECT count(*) FROM woken`

// JobRetryOrderingKey implements driver.Executor. The mark and the wake are
// separate statements, in that order, so the wake reads a snapshot taken
// after the mark: a job that ends its attempt before the mark reaches it is
// waiting by then and is woken, and one that ends after sees the mark.
func (e *Executor) JobRetryOrderingKey(ctx context.Context, params driver.JobRetryOrderingKeyParams) (int, error) {
	if _, err := e.Conn.Exec(ctx, jobMarkOrderingKeySQL, params.Queue, params.OrderingKey); err != nil {
		return 0, fmt.Errorf("hopper: retry ordering key: %w", err)
	}
	var n int
	if err := e.Conn.QueryRow(ctx, jobRetryOrderingKeySQL, params.Queue, params.OrderingKey, max(params.Limit, 1)).Scan(&n); err != nil {
		return 0, fmt.Errorf("hopper: retry ordering key: %w", err)
	}
	return n, nil
}

var jobDiscardExpiredSQL = `
WITH RECURSIVE finished AS (
  DELETE FROM {{schema}}.hopper_jobs j
  WHERE j.id IN (
    SELECT id FROM {{schema}}.hopper_jobs
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
SELECT %s FROM {{schema}}.hopper_jobs j
WHERE j.state = 'running'
  AND (NOT EXISTS (SELECT 1 FROM {{schema}}.hopper_clients c WHERE c.id = j.attempted_by AND c.expires_at > now())
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
	// A row another statement holds is aged next period instead of waited
	// for, so aging never takes part in a deadlock.
	n, err := e.Conn.Exec(ctx, `
		UPDATE {{schema}}.hopper_jobs SET priority = priority - 1, scheduled_at = now()
		WHERE id IN (
		  SELECT id FROM {{schema}}.hopper_jobs
		  WHERE queue = $1 AND state IN ('available', 'scheduled', 'retryable') AND priority > 1
		    AND scheduled_at <= now() - make_interval(secs => $2::float8)
		  FOR UPDATE SKIP LOCKED)`, queue, after.Seconds())
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
