package pgsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/internal/jobtags"
)

var jobColumns = []string{
	"id", "kind", "queue", "state", "priority", "attempt", "max_attempts",
	"scheduled_at", "attempted_at", "attempted_by", "args", "metadata", "errors",
	"unique_key", "ordering_key", "partition_key", "batch_id", "expires_at", "cancel_requested_at", "await", "created_at",
	"snooze_reason", "progress", "tags",
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
		args, metadata, output, progress jsonText
		errs                             attemptErrors
		batchID                          nullUUID
		tags                             tagList
	)
	dest := []any{
		&j.ID, &j.Kind, &j.Queue, &state, &priority, &attempt, &maxAttempts,
		&j.ScheduledAt, &attemptedAt, &attemptedBy, &args, &metadata, &errs,
		&uniqueKey, &orderingKey, &partKey, &batchID, &expiresAt, &cancelAt, &j.Await, &j.CreatedAt,
		&snoozeReason, &progress, &tags,
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
	j.Tags = []string(tags)
	// Only a snoozed job is waiting for a reason. A client of the previous
	// release neither clears it on claim nor on finalize, so during a rolling
	// deploy a running or retrying row can still hold one.
	if j.State == driver.JobStateScheduled {
		j.SnoozeReason = snoozeReason.String
	}
	if batchID.Valid {
		j.BatchID = batchID.ID
	}
	if progress != "" {
		j.Progress = decodeProgress(string(progress))
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
    $9::float8[], $10::boolean[], $11::text[], $12::text[], $13::uuid[], $14::jsonb[]
  ) AS p(kind, queue, priority, max_attempts, scheduled_at, args, metadata, unique_key, ttl, await, ordering_key, partition_key, batch_id, tags)
)
INSERT INTO {{schema}}.hopper_jobs (kind, queue, state, priority, max_attempts, scheduled_at, args, metadata, unique_key, expires_at, await, ordering_key, partition_key, batch_id, tags)
SELECT p.kind, p.queue,
       CASE WHEN p.scheduled_at > now() THEN 'scheduled' ELSE 'available' END::{{schema}}.hopper_job_state,
       p.priority, p.max_attempts, coalesce(p.scheduled_at, now()), p.args, p.metadata, p.unique_key,
       CASE WHEN p.ttl > 0 THEN now() + make_interval(secs => p.ttl) END, p.await, p.ordering_key, p.partition_key, p.batch_id, p.tags
FROM p
ON CONFLICT (kind, unique_key) WHERE unique_key IS NOT NULL DO UPDATE SET ` + "%s" + `
RETURNING ` + "%s" + `, (xmax <> 0) AS duplicate`

const (
	conflictSkipSet    = `kind = EXCLUDED.kind`
	conflictReplaceSet = `
  args         = CASE WHEN hopper_jobs.state = 'running' THEN hopper_jobs.args         ELSE EXCLUDED.args         END,
  metadata     = CASE WHEN hopper_jobs.state = 'running' THEN hopper_jobs.metadata     ELSE EXCLUDED.metadata     END,
  tags         = CASE WHEN hopper_jobs.state = 'running' THEN hopper_jobs.tags         ELSE EXCLUDED.tags         END,
  priority     = CASE WHEN hopper_jobs.state = 'running' THEN hopper_jobs.priority     ELSE EXCLUDED.priority     END,
  max_attempts = CASE WHEN hopper_jobs.state = 'running' THEN hopper_jobs.max_attempts ELSE EXCLUDED.max_attempts END,
  scheduled_at = CASE WHEN hopper_jobs.state = 'running' THEN hopper_jobs.scheduled_at ELSE EXCLUDED.scheduled_at END,
  state        = CASE WHEN hopper_jobs.state = 'running' THEN hopper_jobs.state        ELSE EXCLUDED.state        END,
  progress     = CASE WHEN hopper_jobs.state = 'running' THEN hopper_jobs.progress     ELSE NULL                  END,
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
		args, metadata, tags                            = make([][]byte, n), make([][]byte, n), make([][]byte, n)
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
		tags[i] = TagsJSON(p.Tags)
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
		nullableTextArray(ordering), nullableTextArray(partitions), uuidArray(batches), jsonArray(tags),
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
	table := outcomeTable("hopper_job_history", completed, failed)
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
// awaited jobs announce themselves on the done channel. An attempt's log
// is appended to the job's, which keep the latest $13 attempts.
var jobFinalizeHeadSQL = `
WITH RECURSIVE r AS (
  SELECT * FROM unnest(
    $1::uuid[], $2::bigint[], $3::text[], $4::float8[], $5::boolean[], $6::jsonb[], $7::jsonb[], $8::boolean[], $11::text[],
    $12::jsonb[]
  ) AS r(id, attempted_by, state, delay, snooze, error, output, archive, snooze_reason, log)
),
retry AS (
  UPDATE {{schema}}.hopper_jobs j
  SET state = r.state::{{schema}}.hopper_job_state,
      scheduled_at = CASE WHEN coalesce(j.wake_requested_at >= j.attempted_at, false) THEN now()
                          ELSE now() + make_interval(secs => r.delay) END,
      attempt = CASE WHEN r.snooze THEN j.attempt - 1 ELSE j.attempt END,
      errors = CASE WHEN r.error IS NULL THEN j.errors
                    ELSE j.errors || jsonb_set(r.error, '{at}', to_jsonb(now())) END,
      logs = ` + appendLogSQL + `,
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
         ELSE j.errors || jsonb_set(r.error, '{at}', to_jsonb(now())) END`, appendLogSQL, "r.output", "r.archive", "r.state", "$9") + `
),
`

var jobFinalizeSQL = jobFinalizeHeadSQL + finalizeTailSQL("$9", "$10") + `
SELECT id FROM retry UNION ALL SELECT id FROM finished`

// appendLogSQL appends an attempt's log, if it has one, to its job's logs,
// keeping the latest $13. A snooze gives its attempt back, so a job that
// snoozes again and again would fill its logs with them: a snooze's log,
// or its lack of one, replaces the log of the previous snooze of the same
// attempt number. The SET reads j.attempt before the snooze gives it back.
// A job whose logs do not change keeps its stored value, which is not
// rewritten: one with no logs keeps NULL (jsonb_path_exists of NULL is
// NULL, which would otherwise take the ELSE branch), and one whose last
// entry is removed goes back to NULL.
const appendLogSQL = `CASE
         WHEN r.log IS NULL AND NOT (r.snooze AND coalesce(jsonb_path_exists(j.logs, '$[*] ? (exists(@.snoozed) && @.attempt == $a)',
           jsonb_build_object('a', j.attempt)), false)) THEN j.logs
         ELSE nullif(jsonb_path_query_array(
           CASE WHEN r.snooze
                THEN jsonb_path_query_array(coalesce(j.logs, '[]'), '$[*] ? (!exists(@.snoozed) || @.attempt != $a)',
                       jsonb_build_object('a', j.attempt))
                ELSE coalesce(j.logs, '[]') END || CASE WHEN r.log IS NULL THEN '[]' ELSE jsonb_build_array(r.log) END,
           '$[last - $keep + 1 to last]', jsonb_build_object('keep', $13::int)), '[]') END`

// jobFinalizePlainSQL is jobFinalizeSQL for results whose jobs belong to
// no batch or workflow (driver.JobFinalizeParams.NoBatches): without the
// insert channel, so the parameters after it move down one.
var jobFinalizePlainSQL = strings.NewReplacer("$11", "$10", "$12", "$11", "$13", "$12").Replace(jobFinalizeHeadSQL) + plainFinalizeTailSQL + `
SELECT id FROM retry UNION ALL SELECT id FROM finished`

// skipLockedSQL makes a finalize statement skip the jobs whose rows another
// transaction holds (driver.JobFinalizeParams.SkipLocked).
func skipLockedSQL(query string) string {
	const unnested = "  ) AS r(id, attempted_by, state, delay, snooze, error, output, archive, snooze_reason, log)\n"
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
		logs        = make([][]byte, n)
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
		archives[i] = f.Archived()
		if f.Log != nil && f.KeepsLog() {
			b, err := json.Marshal(driver.AttemptLog{Attempt: f.Log.Attempt, Snoozed: f.Snooze, Log: storableText(f.Log.Log)})
			if err != nil {
				return nil, fmt.Errorf("hopper: finalize jobs: %w", err)
			}
			logs[i] = b
		}
	}
	args := []any{
		uuidArray(ids), int64Array(attemptedBy), textArray(states), floatArray(delays), boolArray(snoozes),
		jsonArray(errs), jsonArray(outputs), boolArray(archives), driver.ChannelDone, driver.ChannelInsert,
		nullableTextArray(reasons), jsonArray(logs), keepLogs(params.KeepLogs),
	}
	query := jobFinalizeSQL
	if params.SkipLocked {
		query = jobFinalizeSkipLockedSQL
	}
	if params.NoBatches {
		query, args = jobFinalizePlainSQL, append(args[:9], args[10:]...)
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

// keepLogs is the number of attempt logs a finalize keeps for keep.
func keepLogs(keep int) int32 {
	if keep <= 0 || keep > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(keep)
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

// jobRecordProgressSQL stores a running job's progress, fenced to the
// attempt. A cursor of NULL leaves the key out, rather than storing a JSON
// null. The time is the statement's, which in a caller's transaction is not
// the transaction's start.
//
// With NoWait, the row is locked first with NOWAIT, and the update applies
// the fence again to the row as locked.
const jobRecordProgressSQL = `%s
UPDATE {{schema}}.hopper_jobs
SET progress = jsonb_build_object(
      'step', CASE WHEN $5 THEN coalesce(progress->>'step', '') ELSE $4::text END,
      'at', statement_timestamp())
    || CASE WHEN $6::jsonb IS NULL THEN '{}'::jsonb ELSE jsonb_build_object('cursor', $6::jsonb) END
WHERE id = $1 AND state = 'running' AND attempted_by = $2 AND attempt = $3%s
RETURNING progress::text`

var (
	jobRecordProgressQuery       = fmt.Sprintf(jobRecordProgressSQL, "", "")
	jobRecordProgressNoWaitQuery = fmt.Sprintf(jobRecordProgressSQL, `
WITH locked AS (
  SELECT id FROM {{schema}}.hopper_jobs
  WHERE id = $1 AND state = 'running' AND attempted_by = $2 AND attempt = $3
  FOR UPDATE NOWAIT
)`, " AND id IN (SELECT id FROM locked)")
)

// JobRecordProgress implements driver.Executor.
func (e *Executor) JobRecordProgress(ctx context.Context, params driver.JobRecordProgressParams) (*driver.Progress, error) {
	var cursor *string
	if len(params.Cursor) > 0 {
		c := string(params.Cursor)
		cursor = &c
	}
	query := jobRecordProgressQuery
	if params.NoWait {
		query = jobRecordProgressNoWaitQuery
	}
	var stored string
	err := e.Conn.QueryRow(ctx, query, uuidParam(params.ID), params.AttemptedBy, params.Attempt,
		params.Step, params.KeepStep, cursor).Scan(&stored)
	if errors.Is(err, ErrNoRows) {
		return nil, nil
	}
	if lockNotAvailable(err) {
		return nil, driver.ErrJobLocked
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: record job progress: %w", err)
	}
	return decodeProgress(stored), nil
}

// decodeProgress reads a progress value, keeping what it can of one that
// is not in the form JobRecordProgress writes, such as one written by hand:
// a row must still scan, or a claim that took it would fail with the claim
// already applied. A value that is not an object reads as none.
func decodeProgress(stored string) *driver.Progress {
	var v struct {
		Step   any             `json:"step"`
		Cursor json.RawMessage `json:"cursor"`
		At     any             `json:"at"`
	}
	if !strings.HasPrefix(strings.TrimSpace(stored), "{") || json.Unmarshal([]byte(stored), &v) != nil {
		return nil
	}
	p := &driver.Progress{Cursor: v.Cursor}
	if s, ok := v.Step.(string); ok {
		p.Step = s
	}
	if s, ok := v.At.(string); ok {
		p.RecordedAt, _ = time.Parse(time.RFC3339Nano, s)
	}
	if string(p.Cursor) == "null" {
		p.Cursor = nil
	}
	return p
}

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

const jobLogsQuery = `
SELECT logs FROM {{schema}}.hopper_jobs WHERE id = $1
UNION ALL
SELECT logs FROM {{schema}}.hopper_job_history WHERE id = $1
LIMIT 1`

// JobLogs implements driver.Executor.
func (e *Executor) JobLogs(ctx context.Context, id driver.JobID) ([]driver.AttemptLog, error) {
	var logs jsonText
	err := e.Conn.QueryRow(ctx, jobLogsQuery, uuidParam(id)).Scan(&logs)
	if errors.Is(err, ErrNoRows) {
		return nil, driver.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: get job logs: %w", err)
	}
	return decodeLogs(string(logs)), nil
}

// decodeLogs reads a job's logs, skipping entries that are not in the form
// the finalize writes, such as ones written by hand.
func decodeLogs(stored string) []driver.AttemptLog {
	// Anything but an array (or null) fails to decode into entries.
	var entries []json.RawMessage
	if json.Unmarshal([]byte(stored), &entries) != nil {
		return nil
	}
	var logs []driver.AttemptLog
	for _, raw := range entries {
		var l driver.AttemptLog
		if json.Unmarshal(raw, &l) == nil && l.Log != "" {
			logs = append(logs, l)
		}
	}
	return logs
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

// rarestTagSQL picks, in the listing's own statement, the tag of the n tags
// in the array parameter $param with the fewest jobs, counting at most
// rarestTagCap of each in the tags tables the listing walks (live, the part
// of history's it reads, or both), for a TagsAll listing to walk: every job
// it lists has that tag too. The counts are summed over the tables and the
// sum is capped at rarestTagCap too: a tag at the cap in one table is common,
// however few rows it has in another, and an uncapped sum would rank it below
// a tag just past the cap in both. Tags whose sums both reach the cap are told
// apart by the statistics of the partitions of the tables it walks: a tag's
// most common frequency in each, times its rows, estimates the tag's rows
// there, so that a listing of history is not decided by the live table's. The
// live table is analyzed by the leader, and history's partitions by
// autovacuum; the schema's name is $schemaParam. It is a WITH query, so the
// listing's arms share one count.
//
// The tables' leaves are the live table itself, which pg_partition_tree
// lists no rows for, and history's partitions. They and the estimates are
// materialized, so that each is read once a statement: inlined into the
// ORDER BY, pg_stats' join ran pg_partition_tree again for each of its rows
// and each tag, a cost that grows with the square of history's partitions
// (half a second a tag for 380 of them).
func rarestTagSQL(param, schemaParam, n int, tables []string) (with, tag string) {
	if n == 1 {
		return "", fmt.Sprintf("($%d::text[])[1]", param)
	}
	limit := strconv.Itoa(rarestTagCap)
	counts := make([]string, len(tables))
	names := make([]string, len(tables))
	for i, table := range tables {
		counts[i] = fmt.Sprintf("(SELECT count(*) FROM (SELECT FROM {{schema}}.%s t WHERE t.tag = u.tag ORDER BY t.id LIMIT %s) c%d)", table, limit, i)
		names[i] = "'" + table + "'"
	}
	return fmt.Sprintf(`WITH rarest_leaves AS MATERIALIZED (
  SELECT coalesce(p.relid, w.rel) AS relid
  FROM (SELECT to_regclass(quote_ident($%[2]d) || '.' || name) FROM unnest(ARRAY[%[1]s]) name) w(rel)
  LEFT JOIN LATERAL pg_partition_tree(w.rel) p ON true
  WHERE w.rel IS NOT NULL AND coalesce(p.isleaf, true)
), rarest_estimates AS MATERIALIZED (
  SELECT m.v AS tag, sum(m.f * greatest(c.reltuples, 0)) AS n
  FROM rarest_leaves l JOIN pg_class c ON c.oid = l.relid
  JOIN pg_stats s ON s.schemaname = $%[2]d AND s.tablename = c.relname AND s.attname = 'tag' AND NOT s.inherited
  CROSS JOIN unnest(s.most_common_vals::text::text[], s.most_common_freqs) m(v, f)
  WHERE m.v = ANY($%[3]d::text[])
  GROUP BY m.v
), rarest AS (
  SELECT u.tag FROM unnest($%[3]d::text[]) WITH ORDINALITY u(tag, o)
  ORDER BY least(%[4]s, %[5]s), coalesce((SELECT e.n FROM rarest_estimates e WHERE e.tag = u.tag), 0), u.o LIMIT 1
) `, strings.Join(names, ", "), schemaParam, param, strings.Join(counts, " + "), limit), "(SELECT tag FROM rarest)"
}

// rarestTagCap bounds rarestTagSQL's count of each tag, so that choosing costs
// a few index pages however common the tags are. The counts are ordered by
// ID, as the index is, so that the planner reads the index rather than the
// table even when it takes the tag for a common one.
const rarestTagCap = 1000

// tagArms returns a tag listing's arms over the live (or the history) table:
// for each of the n tags in the array parameter $param, the tag's rows of the
// tags table past the cursor $4 in listing order, each job looked up by its
// key with jobs, the arm's job query and filter, until the page is full.
// A page then reads about as many rows as it holds, however common the tags
// are, and the page's jobs are among each arm's. A job with several of the
// tags comes from each of their arms, so a listing with more than one arm
// per table merges them with DISTINCT ON (id): each arm still reads at most
// a page, however much the tags overlap, where leaving an earlier tag's jobs
// out of each later arm walked the whole range of a tag whose jobs mostly
// have an earlier one too. A single tag may be given as an SQL expression,
// tag, instead.
//
// The walk is ordered and limited in the same query as the lookup, so that
// the planner merges history's partitions in index order even when it takes
// the tag for a rare one, as a generic plan does; ordering a subquery's walk
// sorted every row of the tag past the cursor. The OFFSET 0 keeps it from
// joining the walk to a scan of the whole job table instead, and from
// narrowing the walk by the job query's filter: narrow adds to the walk's
// own WHERE what the tags table holds beside the tag (tagWalk).
//
// The lookup also checks the walked tag on the job's own row, which it has
// read already, so that a tags row the column no longer holds lists nothing:
// a TRUNCATE of one of history's partitions fires none of history's
// triggers, which keep its tags rows, and a write with the triggers
// disabled fires none at all.
func tagArms(param, n int, newestFirst bool, tag, jobs, order string, walk tagWalk) []string {
	cmp := ">"
	if newestFirst {
		cmp = "<"
	}
	match := " AND id = t.id AND tags ? t.tag"
	if walk.history {
		match = " AND state = t.state AND finalized_at = t.finalized_at AND id = t.id AND tags ? t.tag"
	}
	walkOrder := strings.Replace(order, "ORDER BY id", "ORDER BY t.id", 1)
	arms := make([]string, n)
	for k := range n {
		expr := fmt.Sprintf("($%d::text[])[%d]", param, k+1)
		if tag != "" {
			expr = tag
		}
		arms[k] = fmt.Sprintf("SELECT j.* FROM {{schema}}.%s t CROSS JOIN LATERAL (%s%s OFFSET 0) j WHERE t.tag = %s AND t.id %s $4::uuid%s",
			walk.table, jobs, match, expr, cmp, walk.narrow) + walkOrder
	}
	return arms
}

// tagWalk is the tags table a listing's arms over the live table or history
// walk, and the conditions on its rows that narrow the walk to the jobs the
// listing can return: the IDs it asks for, and in history the states, by
// which history's tags table is partitioned as history is. Without them, a
// page by IDs or by a finished state walked every row of the tag past the
// cursor, looking each job up only to drop it.
type tagWalk struct {
	table, narrow string
	history       bool
}

// historyTagsTable is the part of history's tags table that holds the
// finished states listed: an outcome's partition when they are all of one
// outcome, as history's are, or the whole table.
func historyTagsTable(states []driver.JobState) string {
	var completed, failed bool
	for _, s := range states {
		switch s {
		case driver.JobStateCompleted:
			completed = true
		case driver.JobStateCanceled, driver.JobStateDiscarded:
			failed = true
		}
	}
	return outcomeTable("hopper_job_history_tags", completed, failed)
}

// outcomeTable is the part of a table partitioned by outcome, as history
// and its tags rows are, that holds the outcomes named: root's completed
// or failed partition when only one is, or root itself.
func outcomeTable(root string, completed, failed bool) string {
	switch {
	case completed && !failed:
		return root + "_completed"
	case failed && !completed:
		return root + "_failed"
	}
	return root
}

// maxJobID starts a newest-first listing that has no cursor.
var maxJobID = driver.JobID{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

// errOrderingKeyNeedsQueue refuses a key listing across queues: a key is a
// lane only within its queue. It is a filter JobList cannot list by.
var errOrderingKeyNeedsQueue = fmt.Errorf("%w: an ordering key is listed within a queue; set the queue", driver.ErrInvalidFilter)

// JobList implements driver.Executor. The ordering key, IDs, priorities
// and metadata are matched only when set, each as a plain condition, so that
// the indexes on them serve it: the ordering-key indexes and the primary
// keys. Tags are found through the tags tables (tagArms).
func (e *Executor) JobList(ctx context.Context, params driver.JobListParams) ([]*driver.JobRow, error) {
	if params.OrderingKey != "" && params.Queue == "" {
		return nil, errOrderingKeyNeedsQueue
	}
	for _, p := range params.Priorities {
		if _, err := smallint(p, "Priorities"); err != nil {
			return nil, fmt.Errorf("%w: %w", driver.ErrInvalidFilter, err)
		}
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
	// The limit is a constant, as the claim's is (jobClaimSQL): as a
	// parameter, a cached generic plan would cost the listing as if it
	// returned a tenth of the table, and choose parallel scans and JIT
	// compilation that take longer than the page itself.
	args := []any{params.Queue, textArray(params.Kinds), textArray(states), uuidParam(after)}
	limit := strconv.Itoa(max(params.Limit, 1))
	filter, order := jobListFilterSQL+" AND id > $4::uuid", " ORDER BY id LIMIT "+limit
	if params.NewestFirst {
		filter, order = jobListFilterSQL+" AND id < $4::uuid", " ORDER BY id DESC LIMIT "+limit
	}
	where := func(cond string, arg any) {
		args = append(args, arg)
		filter += fmt.Sprintf(" AND "+cond, len(args))
	}
	if params.OrderingKey != "" {
		where("ordering_key = $%d", params.OrderingKey)
	}
	liveWalk := tagWalk{table: "hopper_job_tags"}
	historyWalk := tagWalk{table: historyTagsTable(params.States), history: true}
	if len(params.IDs) > 0 {
		where("id = ANY($%d::uuid[])", uuidArray(params.IDs))
		liveWalk.narrow = fmt.Sprintf(" AND t.id = ANY($%d::uuid[])", len(args))
		historyWalk.narrow = liveWalk.narrow
	}
	if len(params.States) > 0 {
		historyWalk.narrow += " AND t.state::text = ANY($3::text[])"
	}
	if len(params.Priorities) > 0 {
		where("priority = ANY($%d::smallint[])", intArray(params.Priorities))
	}
	if len(params.Metadata) > 0 {
		where("metadata @> $%d::jsonb", string(params.Metadata))
	}
	// A tag listing walks the tags tables in ID order from the cursor, one
	// tag at a time: TagsAll's rarest tag, the rest checked on the job's row,
	// or each of TagsAny's.
	var (
		walk func(jobs string, w tagWalk) []string
		with string
		// merge is set when a job can come from more than one arm of a
		// table: TagsAny's arms, one per tag.
		merge bool
	)
	// Repeats are dropped: each of TagsAll's would be counted again to choose
	// the rarest, and each of TagsAny's would walk its range again, so a
	// filter that repeats one tag would cost as much as one with that many.
	tagsAll, tagsAny := jobtags.Distinct(params.TagsAll), jobtags.Distinct(params.TagsAny)
	switch {
	case len(tagsAll) > 0:
		args = append(args, textArray(tagsAll))
		all, schema := len(args), 0
		if len(tagsAll) > 1 {
			filter += fmt.Sprintf(" AND tags ?& $%d::text[]", all)
			args = append(args, e.Schema.Name)
			schema = len(args)
		}
		if len(tagsAny) > 0 {
			where("tags ?| $%d::text[]", textArray(tagsAny))
		}
		var tables []string
		if live {
			tables = append(tables, liveWalk.table)
		}
		if history {
			tables = append(tables, historyWalk.table)
		}
		var tag string
		with, tag = rarestTagSQL(all, schema, len(tagsAll), tables)
		walk = func(jobs string, w tagWalk) []string {
			return tagArms(all, 1, params.NewestFirst, tag, jobs, order, w)
		}
	case len(tagsAny) > 0:
		args = append(args, textArray(tagsAny))
		anyTags := len(args)
		merge = len(tagsAny) > 1
		walk = func(jobs string, w tagWalk) []string {
			return tagArms(anyTags, len(tagsAny), params.NewestFirst, "", jobs, order, w)
		}
	}
	var arms []string
	switch {
	case live && walk != nil:
		arms = append(arms, walk(jobListLiveSQL+filter, liveWalk)...)
	case live && params.OrderingKey == "":
		arms = append(arms, jobListLiveSQL+filter+order)
	case live:
		arms = append(arms, jobListLiveSQL+filter+jobListKeyedSQL+order, jobListLiveSQL+filter+jobListPendingSQL+order)
	}
	if history && walk != nil {
		arms = append(arms, walk(jobListHistorySQL+filter, historyWalk)...)
	} else if history {
		arms = append(arms, jobListHistorySQL+filter+order)
	}
	query := arms[0]
	if len(arms) > 1 {
		query = "(" + strings.Join(arms, ") UNION ALL (") + ")"
		if merge {
			// A job is in the live table or in history, never both, so
			// its copies are equal rows; any one of them will do.
			query = "SELECT DISTINCT ON (id) * FROM (" + query + ") u"
		}
		query += order
	}
	// The query's text varies with the filters, so it is expanded without
	// the schema's cache, which keeps every text it is given.
	query = e.Schema.expand(with + query)
	// In the caller's transaction, a filter the server refuses would abort
	// it, though the error says only that the filter was invalid; a
	// savepoint (withTx, which rolls back to it even when ctx is done) keeps
	// the transaction usable. It costs two round trips, so it is taken only
	// for filters the server could refuse.
	var (
		jobs []*driver.JobRow
		err  error
	)
	if e.InTx && refusable(params, states, tagsAll, tagsAny) {
		err = e.withTx(ctx, func(tx Tx) error {
			var qerr error
			jobs, qerr = historyJobs(tx.Query(ctx, query, args...))
			return qerr
		})
	} else {
		jobs, err = historyJobs(e.Conn.Query(ctx, query, args...))
	}
	if err != nil {
		if code := sqlState(err); strings.HasPrefix(code, "22") || code == "54001" {
			if ferr := e.checkFilters(ctx, params, states, tagsAll, tagsAny); ferr != nil {
				return nil, ferr
			}
		}
		return nil, fmt.Errorf("hopper: list jobs: %w", err)
	}
	return jobs, nil
}

// refusable reports whether the server could refuse a listing's filters
// (see checkFilters). Text of ASCII characters other than NUL is valid in
// every server encoding, and IDs and priorities are checked before the
// listing, so only Metadata and other text can be refused.
func refusable(params driver.JobListParams, states, tagsAll, tagsAny []string) bool {
	if len(params.Metadata) > 0 {
		return true
	}
	odd := func(s string) bool {
		return strings.ContainsFunc(s, func(r rune) bool { return r == 0 || r >= utf8.RuneSelf })
	}
	for _, list := range [][]string{{params.Queue, params.OrderingKey}, params.Kinds, states, tagsAll, tagsAny} {
		if slices.ContainsFunc(list, odd) {
			return true
		}
	}
	return false
}

// checkFilters tells a listing that failed with a data exception (class 22)
// or too deep a nesting (54001) whether its filters were the cause. Their text
// (the queue, kinds, states, ordering key and tags; a state is any string a
// caller passes, such as the web UI's query parameter) and Metadata are the
// listing's only inputs whose values the server can
// refuse: text with a NUL or with characters the database's encoding lacks,
// and what jsonb's input refuses (bytes that are not UTF-8, a \u0000 escape,
// an unpaired surrogate, a number beyond numeric's range, nesting deeper than
// its stack allows). The listing's own error cannot tell them from a stored
// row's, so the filters are given to the server again on their own. It
// returns ErrInvalidFilter when the server refuses them, and nil otherwise.
func (e *Executor) checkFilters(ctx context.Context, params driver.JobListParams, states, tagsAll, tagsAny []string) error {
	var (
		casts []string
		args  []any
	)
	cast := func(typ string, arg any) {
		args = append(args, arg)
		casts = append(casts, fmt.Sprintf("$%d::%s", len(args), typ))
	}
	cast("text", params.Queue)
	cast("text[]", textArray(params.Kinds))
	cast("text[]", textArray(states))
	cast("text", params.OrderingKey)
	cast("text[]", textArray(tagsAll))
	cast("text[]", textArray(tagsAny))
	if len(params.Metadata) > 0 {
		cast("jsonb", string(params.Metadata))
	}
	check := func(c Conn) error {
		_, err := c.Exec(ctx, "SELECT "+strings.Join(casts, ", "), args...)
		return err
	}
	var err error
	if e.InTx {
		err = e.withTx(ctx, func(tx Tx) error { return check(tx) })
	} else {
		err = check(e.Conn)
	}
	if code := sqlState(err); strings.HasPrefix(code, "22") || code == "54001" {
		return fmt.Errorf("%w: %w", driver.ErrInvalidFilter, err)
	}
	return nil
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
  RETURNING ` + finishedColumns(appendErrorSQL("hopper: canceled"), "j.logs", "NULL::jsonb", "true", "'canceled'::text", "$2") + `
),
` + finalizeTailSQL("$2", "$3") + `
SELECT ` + finishedSelectColumns + ` FROM finished`
	// jobDeleteWaitingSQL is jobCancelWaitingSQL without the job's history
	// row: its batch, its dependents and anyone awaiting it see it canceled,
	// and nothing of it is kept.
	jobDeleteWaitingSQL = `
WITH RECURSIVE finished AS (
  DELETE FROM {{schema}}.hopper_jobs j WHERE j.id = $1 AND j.state <> 'running'
  RETURNING ` + finishedColumns(appendErrorSQL("hopper: canceled"), "j.logs", "NULL::jsonb", "false", "'canceled'::text", "$2") + `
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
UPDATE {{schema}}.hopper_jobs SET `+jobRunNowSQL+`, expires_at = NULL, progress = CASE WHEN $2 THEN NULL ELSE progress END
WHERE id = $1 AND state <> 'running'
RETURNING %s`, JobColumns(""))
	// jobRetryHistorySQL moves a finalized job back to the live table with
	// its history (attempt count and errors) intact. It gets a fresh seq,
	// as a newly inserted job would, and one more attempt if it had run out.
	jobRetryHistorySQL = fmt.Sprintf(`
WITH h AS (DELETE FROM {{schema}}.hopper_job_history WHERE id = $1 RETURNING *)
INSERT INTO {{schema}}.hopper_jobs (id, kind, queue, state, priority, attempt, max_attempts, scheduled_at, args, metadata,
  errors, unique_key, ordering_key, partition_key, batch_id, await, created_at, progress, logs, tags)
SELECT id, kind, queue, 'available', priority, attempt,
  CASE WHEN attempt >= max_attempts THEN attempt + 1 ELSE max_attempts END, statement_timestamp(), args, metadata,
  errors, unique_key, ordering_key, partition_key, batch_id, await, created_at, CASE WHEN $2 THEN NULL ELSE progress END, logs,
  %s
FROM h
RETURNING %s`, storedTagsSQL("tags"), JobColumns(""))
)

// storedTagsSQL returns the tags a stored tags value, expr, holds, as a
// value hopper_jobs accepts: its distinct non-empty strings in order, NULL
// for none. hopper writes only such values, but a direct SQL write can
// store anything, and tagList and hopper_tags_of read it the same way.
func storedTagsSQL(expr string) string { return keptTagsSQL(expr, "") }

// callbackTagsSQL is storedTagsSQL for a batch callback's tags, which also
// drops a tag longer than an insert accepts (MaxTagBytes). BatchInsert
// stores only checked tags, so only a direct SQL write to the batch row
// stores a longer one, and one past the tags table's largest index row would
// fail the finalize that completes the batch, with every other job in it.
// Its bytes are counted in UTF-8, as hopper_tags and the Go client count
// them, whatever the database's encoding, so that a callback is never given
// a tag a listing refuses as too long.
func callbackTagsSQL(expr string) string {
	return keptTagsSQL(expr, " AND octet_length(convert_to(e #>> '{}', 'UTF8')) <= "+strconv.Itoa(jobtags.MaxBytes))
}

func keptTagsSQL(expr, cond string) string {
	return `(SELECT jsonb_agg(e ORDER BY n) FROM (SELECT DISTINCT ON (e) e, n
     FROM jsonb_array_elements(CASE WHEN jsonb_typeof(` + expr + `) = 'array' THEN ` + expr + ` END) WITH ORDINALITY AS t(e, n)
     WHERE jsonb_typeof(e) = 'string' AND e <> '""'` + cond + ` ORDER BY e, n) d)`
}

// IsUniqueViolation reports a unique_violation error from the transport.
// Transports wrap their error type so that this can tell.
type uniqueViolation interface{ UniqueViolation() bool }

func isUniqueViolation(err error) bool {
	var uv uniqueViolation
	return errors.As(err, &uv) && uv.UniqueViolation()
}

// JobRetry implements driver.Executor.
func (e *Executor) JobRetry(ctx context.Context, params driver.JobRetryParams) (*driver.JobRow, error) {
	id := params.ID
	var job *driver.JobRow
	err := e.withTx(ctx, func(tx Tx) error {
		var state string
		err := tx.QueryRow(ctx, jobLockStateSQL, uuidParam(id)).Scan(&state)
		switch {
		case errors.Is(err, ErrNoRows):
			job, err = scanJob(tx.QueryRow(ctx, jobRetryHistorySQL, uuidParam(id), params.FromStart), false)
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
		job, err = scanJob(tx.QueryRow(ctx, jobRetryLiveSQL, uuidParam(id), params.FromStart), false)
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
  RETURNING ` + finishedColumns(appendErrorSQL("hopper: expired"), "j.logs", "NULL::jsonb", "true", "'discarded'::text", "$2") + `
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
