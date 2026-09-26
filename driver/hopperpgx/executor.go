package hopperpgx

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/parallelworks/hopper/driver"
)

// jobColumns lists hopper_jobs columns in scan order. prefix qualifies them
// with a table alias.
func jobColumns(prefix string) string {
	cols := []string{
		"id", "kind", "queue", "state", "priority", "attempt", "max_attempts",
		"scheduled_at", "attempted_at", "attempted_by", "args", "metadata", "errors",
		"unique_key", "ordering_key", "partition_key", "batch_id", "expires_at", "cancel_requested_at", "await", "created_at",
	}
	out := ""
	for i, c := range cols {
		if i > 0 {
			out += ", "
		}
		out += prefix + c
	}
	return out
}

// scanJob scans a row of jobColumns (plus finalized_at and output when
// history is true) into a JobRow.
func scanJob(row pgx.Row, history bool) (*driver.JobRow, error) {
	var (
		j                                       driver.JobRow
		state                                   string
		priority, attempt, maxAttempts          int16
		attemptedAt, expiresAt, cancelRequested pgtype.Timestamptz
		finalizedAt                             pgtype.Timestamptz
		attemptedBy                             pgtype.Int8
		uniqueKey, orderingKey, partitionKey    pgtype.Text
		batchID                                 pgtype.UUID
	)
	dest := []any{
		&j.ID, &j.Kind, &j.Queue, &state, &priority, &attempt, &maxAttempts,
		&j.ScheduledAt, &attemptedAt, &attemptedBy, &j.Args, &j.Metadata, &j.Errors,
		&uniqueKey, &orderingKey, &partitionKey, &batchID, &expiresAt, &cancelRequested, &j.Await, &j.CreatedAt,
	}
	if history {
		dest = append(dest, &finalizedAt, &j.Output)
	}
	if err := row.Scan(dest...); err != nil {
		return nil, err
	}
	j.State = driver.JobState(state)
	j.Priority = int(priority)
	j.Attempt = int(attempt)
	j.MaxAttempts = int(maxAttempts)
	j.AttemptedAt = attemptedAt.Time
	j.AttemptedBy = attemptedBy.Int64
	j.UniqueKey = uniqueKey.String
	j.OrderingKey = orderingKey.String
	j.PartitionKey = partitionKey.String
	if batchID.Valid {
		j.BatchID = driver.JobID(batchID.Bytes)
	}
	j.ExpiresAt = expiresAt.Time
	j.CancelRequestedAt = cancelRequested.Time
	j.FinalizedAt = finalizedAt.Time
	return &j, nil
}

func collectJobs(rows pgx.Rows, history bool) ([]*driver.JobRow, error) {
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (*driver.JobRow, error) {
		return scanJob(row, history)
	})
}

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
	jobInsertSkipQuery    = fmt.Sprintf(jobInsertSQL, conflictSkipSet, jobColumns(""))
	jobInsertReplaceQuery = fmt.Sprintf(jobInsertSQL, conflictReplaceSet, jobColumns(""))
)

// notifySQL sends one notification per payload. Postgres delivers them on
// commit and collapses identical ones within a transaction.
const notifySQL = `SELECT pg_notify($1, p) FROM unnest($2::text[]) AS p`

func (e *executor) Notify(ctx context.Context, channel string, payloads []string) error {
	if len(payloads) == 0 {
		return nil
	}
	if _, err := e.db.Exec(ctx, notifySQL, channel, payloads); err != nil {
		return fmt.Errorf("hopperpgx: notify: %w", err)
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

func (e *executor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams, opts driver.JobInsertOpts) ([]driver.JobInsertResult, error) {
	if len(params) == 0 {
		return nil, nil
	}
	n := len(params)
	var (
		kinds       = make([]string, n)
		queues      = make([]string, n)
		priorities  = make([]int16, n)
		maxAttempts = make([]int16, n)
		scheduledAt = make([]pgtype.Timestamptz, n)
		args        = make([][]byte, n)
		metadata    = make([][]byte, n)
		uniqueKeys  = make([]pgtype.Text, n)
		ttls        = make([]float64, n)
		awaits      = make([]bool, n)
		ordering    = make([]pgtype.Text, n)
		partitions  = make([]pgtype.Text, n)
		batches     = make([]pgtype.UUID, n)
	)
	for i, p := range params {
		var err error
		ttls[i] = max(p.TTL, 0).Seconds()
		awaits[i] = p.Await
		ordering[i] = pgtype.Text{String: p.OrderingKey, Valid: p.OrderingKey != ""}
		partitions[i] = pgtype.Text{String: p.PartitionKey, Valid: p.PartitionKey != ""}
		batches[i] = pgtype.UUID{Bytes: p.BatchID, Valid: !p.BatchID.IsZero()}
		if priorities[i], err = smallint(p.Priority, "priority"); err != nil {
			return nil, err
		}
		if maxAttempts[i], err = smallint(p.MaxAttempts, "max_attempts"); err != nil {
			return nil, err
		}
		kinds[i] = p.Kind
		queues[i] = p.Queue
		scheduledAt[i] = pgtype.Timestamptz{Time: p.ScheduledAt, Valid: !p.ScheduledAt.IsZero()}
		args[i] = jsonOrEmptyObject(p.Args)
		metadata[i] = jsonOrEmptyObject(p.Metadata)
		uniqueKeys[i] = pgtype.Text{String: p.UniqueKey, Valid: p.UniqueKey != ""}
	}
	query := jobInsertSkipQuery
	if opts.OnConflict == driver.ConflictReplace {
		query = jobInsertReplaceQuery
	}
	queryArgs := []any{kinds, queues, priorities, maxAttempts, scheduledAt, args, metadata, uniqueKeys, ttls, awaits, ordering, partitions, batches}
	collect := func(rows pgx.Rows, err error) ([]driver.JobInsertResult, error) {
		if err != nil {
			return nil, err
		}
		return pgx.CollectRows(rows, func(row pgx.CollectableRow) (driver.JobInsertResult, error) {
			return scanInsertResult(row)
		})
	}

	var results []driver.JobInsertResult
	if !opts.Notify {
		var err error
		if results, err = collect(e.db.Query(ctx, query, queryArgs...)); err != nil {
			return nil, fmt.Errorf("hopperpgx: insert jobs: %w", err)
		}
	} else {
		// The notify rides in the same pipelined batch, so the insert and
		// its notification cost one round trip and commit together.
		b := &pgx.Batch{}
		b.Queue(query, queryArgs...)
		b.Queue(notifySQL, driver.ChannelInsert, distinctQueues(params))
		br := e.db.SendBatch(ctx, b)
		results, err := collect(br.Query())
		if err == nil {
			_, err = br.Exec()
		}
		if cerr := br.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return nil, fmt.Errorf("hopperpgx: insert jobs: %w", err)
		}
		if len(results) != n {
			return nil, fmt.Errorf("hopperpgx: insert jobs: inserted %d rows for %d inputs", len(results), n)
		}
		return results, nil
	}
	if len(results) != n {
		return nil, fmt.Errorf("hopperpgx: insert jobs: inserted %d rows for %d inputs", len(results), n)
	}
	return results, nil
}

// scanInsertResult scans jobColumns followed by the duplicate flag.
func scanInsertResult(row pgx.Row) (driver.JobInsertResult, error) {
	var (
		j                                       driver.JobRow
		state                                   string
		priority, attempt, maxAttempts          int16
		attemptedAt, expiresAt, cancelRequested pgtype.Timestamptz
		attemptedBy                             pgtype.Int8
		uniqueKey, orderingKey, partitionKey    pgtype.Text
		batchID                                 pgtype.UUID
		duplicate                               bool
	)
	err := row.Scan(
		&j.ID, &j.Kind, &j.Queue, &state, &priority, &attempt, &maxAttempts,
		&j.ScheduledAt, &attemptedAt, &attemptedBy, &j.Args, &j.Metadata, &j.Errors,
		&uniqueKey, &orderingKey, &partitionKey, &batchID, &expiresAt, &cancelRequested, &j.Await, &j.CreatedAt,
		&duplicate,
	)
	if err != nil {
		return driver.JobInsertResult{}, err
	}
	j.State = driver.JobState(state)
	j.Priority = int(priority)
	j.Attempt = int(attempt)
	j.MaxAttempts = int(maxAttempts)
	j.AttemptedAt = attemptedAt.Time
	j.AttemptedBy = attemptedBy.Int64
	j.UniqueKey = uniqueKey.String
	j.OrderingKey = orderingKey.String
	j.PartitionKey = partitionKey.String
	if batchID.Valid {
		j.BatchID = driver.JobID(batchID.Bytes)
	}
	j.ExpiresAt = expiresAt.Time
	j.CancelRequestedAt = cancelRequested.Time
	return driver.JobInsertResult{Job: &j, Duplicate: duplicate}, nil
}

// smallint converts a value bound for a smallint column.
func smallint(v int, column string) (int16, error) {
	if v < math.MinInt16 || v > math.MaxInt16 {
		return 0, fmt.Errorf("hopperpgx: %s %d is out of range", column, v)
	}
	return int16(v), nil
}

func jsonOrEmptyObject(b []byte) []byte {
	if len(b) == 0 {
		return []byte("{}")
	}
	return b
}

// JobInsertCopy loads a batch with COPY. IDs and the insert time come from
// the database first, so that results are complete and IDs are generated
// the same way as on every other path.
func (e *executor) JobInsertCopy(ctx context.Context, params []driver.JobInsertParams, opts driver.JobInsertOpts) ([]driver.JobInsertResult, error) {
	if len(params) == 0 {
		return nil, nil
	}
	n := len(params)

	// COPY encodes in binary, so the connection's type map must know the
	// enum. Hold one connection for the ID query and the COPY.
	conn, copier, release, err := e.copyConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: copy jobs: %w", err)
	}
	defer release()
	if err := registerJobState(ctx, conn); err != nil {
		return nil, fmt.Errorf("hopperpgx: copy jobs: %w", err)
	}

	rows, err := conn.Query(ctx, "SELECT hopper_uuidv7(), now() FROM generate_series(1, $1)", n)
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: copy jobs: generate ids: %w", err)
	}
	var now time.Time
	ids, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (driver.JobID, error) {
		var id driver.JobID
		err := row.Scan(&id, &now)
		return id, err
	})
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: copy jobs: generate ids: %w", err)
	}

	results := make([]driver.JobInsertResult, n)
	values := make([][]any, n)
	for i, p := range params {
		priority, err := smallint(p.Priority, "priority")
		if err != nil {
			return nil, err
		}
		maxAttempts, err := smallint(p.MaxAttempts, "max_attempts")
		if err != nil {
			return nil, err
		}
		scheduledAt := p.ScheduledAt
		if scheduledAt.IsZero() {
			scheduledAt = now
		}
		state := driver.JobStateAvailable
		if scheduledAt.After(now) {
			state = driver.JobStateScheduled
		}
		j := &driver.JobRow{
			ID:          ids[i],
			Kind:        p.Kind,
			Queue:       p.Queue,
			State:       state,
			Priority:    p.Priority,
			MaxAttempts: p.MaxAttempts,
			ScheduledAt: scheduledAt,
			Args:        jsonOrEmptyObject(p.Args),
			Metadata:    jsonOrEmptyObject(p.Metadata),
			Errors:      []driver.AttemptError{},
			Await:       p.Await,
			CreatedAt:   now,
		}
		var expiresAt pgtype.Timestamptz
		if p.TTL > 0 {
			j.ExpiresAt = now.Add(p.TTL)
			expiresAt = pgtype.Timestamptz{Time: j.ExpiresAt, Valid: true}
		}
		results[i] = driver.JobInsertResult{Job: j}
		j.OrderingKey, j.PartitionKey, j.BatchID = p.OrderingKey, p.PartitionKey, p.BatchID
		values[i] = []any{
			[16]byte(j.ID), j.Kind, j.Queue, string(j.State), priority, maxAttempts,
			j.ScheduledAt, []byte(j.Args), []byte(j.Metadata), expiresAt, j.Await,
			pgtype.Text{String: p.OrderingKey, Valid: p.OrderingKey != ""},
			pgtype.Text{String: p.PartitionKey, Valid: p.PartitionKey != ""},
			pgtype.UUID{Bytes: p.BatchID, Valid: !p.BatchID.IsZero()},
			j.CreatedAt,
		}
	}
	columns := []string{"id", "kind", "queue", "state", "priority", "max_attempts", "scheduled_at", "args", "metadata", "expires_at", "await", "ordering_key", "partition_key", "batch_id", "created_at"}
	copied, err := copier.CopyFrom(ctx, pgx.Identifier{"hopper_jobs"}, columns, pgx.CopyFromRows(values))
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: copy jobs: %w", err)
	}
	if copied != int64(n) {
		return nil, fmt.Errorf("hopperpgx: copy jobs: copied %d rows for %d inputs", copied, n)
	}
	if opts.Notify {
		if _, err := copier.Exec(ctx, notifySQL, driver.ChannelInsert, distinctQueues(params)); err != nil {
			return nil, fmt.Errorf("hopperpgx: copy jobs: notify: %w", err)
		}
	}
	return results, nil
}

type copyFromer interface {
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// copyConn returns a single connection to run a COPY on, and the object to
// run it through (the transaction, when inside one).
func (e *executor) copyConn(ctx context.Context) (*pgx.Conn, copyFromer, func(), error) {
	if e.tx != nil {
		return e.tx.Conn(), e.tx, func() {}, nil
	}
	c, err := e.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	return c.Conn(), c, c.Release, nil
}

// registerJobState teaches a connection the hopper_job_state enum, once per
// connection, so that COPY can encode it in binary.
func registerJobState(ctx context.Context, conn *pgx.Conn) error {
	if _, ok := conn.TypeMap().TypeForName("hopper_job_state"); ok {
		return nil
	}
	t, err := conn.LoadType(ctx, "hopper_job_state")
	if err != nil {
		return fmt.Errorf("load type hopper_job_state: %w", err)
	}
	conn.TypeMap().RegisterType(t)
	return nil
}

// jobClaimSQL is the hot path. The inner SELECT walks the claim index in
// claim order and locks its rows, skipping any locked by other clients. The
// outer SELECT restores claim order, which UPDATE ... RETURNING does not
// guarantee.
//
// A job with an ordering key is claimable only if it is the oldest waiting
// job of its key and no job of that key is running; jobs without a key pay
// nothing for the check. The unique index on running ordering keys is the
// backstop for two clients passing the check at once, in which case one
// claim fails and is retried.
//
// With a partition limit, candidates are ranked within their partition key
// and only those whose rank, counting the key's running jobs, fits under
// the limit are eligible. That ranking scans the queue's waiting jobs, so
// only partition-limited queues pay for it.
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
	jobClaimQuery            = fmt.Sprintf(jobClaimSQL, jobColumns("j."), jobColumns(""))
	jobClaimPartitionedQuery = fmt.Sprintf(jobClaimPartitionedSQL, jobColumns("j."), jobColumns(""))
)

func (e *executor) JobClaim(ctx context.Context, params driver.JobClaimParams) (driver.JobClaimResult, error) {
	if params.Limit <= 0 {
		return driver.JobClaimResult{}, nil
	}
	if params.Limited {
		return e.claimLimited(ctx, params)
	}
	jobs, err := claimRows(ctx, e.db, jobClaimQuery, params.Queue, params.ClientID, params.Limit)
	if err != nil {
		return driver.JobClaimResult{}, err
	}
	return driver.JobClaimResult{Jobs: jobs}, nil
}

func claimRows(ctx context.Context, db dbtx, query string, args ...any) ([]*driver.JobRow, error) {
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: claim jobs: %w", err)
	}
	jobs, err := collectJobs(rows, false)
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: claim jobs: %w", err)
	}
	return jobs, nil
}

// jobFinalizeSQL applies a batch of results in one statement. Every
// transition checks the prior state (running) and the owning client, so a
// stale result from a fenced client never overwrites a newer attempt.
// Retries and snoozes are updated in place; terminal outcomes are moved to
// history. Error timestamps come from database time.
var jobFinalizeSQL = `
WITH r AS (
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
done AS (
  DELETE FROM hopper_jobs j
  USING r
  WHERE j.id = r.id AND j.state = 'running' AND j.attempted_by = r.attempted_by
    AND r.state IN ('completed', 'cancelled', 'discarded')
  RETURNING j.id, j.seq, j.kind, j.queue, r.state, j.priority, j.attempt, j.max_attempts,
    j.scheduled_at, j.attempted_at, j.attempted_by, j.args, j.metadata,
    CASE WHEN r.error IS NULL THEN j.errors
         ELSE j.errors || jsonb_set(r.error, '{at}', to_jsonb(now())) END AS errors,
    j.unique_key, j.ordering_key, j.partition_key, j.batch_id, j.expires_at,
    j.cancel_requested_at, j.await, j.created_at, r.output, r.archive, r.state AS final_state,
    CASE WHEN j.await THEN pg_notify($9, j.id::text) END AS notified
),
archived AS (
  INSERT INTO hopper_job_history (
    id, seq, kind, queue, state, priority, attempt, max_attempts, scheduled_at, attempted_at,
    attempted_by, args, metadata, errors, unique_key, ordering_key, partition_key, batch_id,
    expires_at, cancel_requested_at, await, created_at, finalized_at, output)
  SELECT id, seq, kind, queue, state::hopper_job_state, priority, attempt, max_attempts, scheduled_at, attempted_at,
    attempted_by, args, metadata, errors, unique_key, ordering_key, partition_key, batch_id,
    expires_at, cancel_requested_at, await, created_at, now(), output
  FROM done WHERE archive
  RETURNING id
),
` + batchAccountingSQL + `
SELECT id FROM retry UNION ALL SELECT id FROM done`

func (e *executor) JobFinalizeMany(ctx context.Context, params driver.JobFinalizeParams) ([]driver.JobID, error) {
	if len(params.Jobs) == 0 {
		return nil, nil
	}
	n := len(params.Jobs)
	var (
		ids         = make([][16]byte, n)
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
			return nil, fmt.Errorf("hopperpgx: finalize jobs: %s is not a finalize state", f.State)
		}
		ids[i] = f.ID
		attemptedBy[i] = f.AttemptedBy
		states[i] = string(f.State)
		delays[i] = max(f.Delay, 0).Seconds()
		snoozes[i] = f.Snooze
		if f.Error != nil {
			b, err := marshalAttemptError(f.Error)
			if err != nil {
				return nil, fmt.Errorf("hopperpgx: finalize jobs: %w", err)
			}
			errs[i] = b
		}
		if len(f.Output) > 0 {
			outputs[i] = f.Output
		}
		// Only completed jobs may skip history; the failed partitions are the
		// dead-letter queue.
		archives[i] = f.Archive || f.State != driver.JobStateCompleted
	}
	rows, err := e.db.Query(ctx, jobFinalizeSQL, ids, attemptedBy, states, delays, snoozes, errs, outputs, archives, driver.ChannelDone, driver.ChannelInsert)
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: finalize jobs: %w", err)
	}
	applied, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (driver.JobID, error) {
		var id driver.JobID
		err := row.Scan(&id)
		return id, err
	})
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: finalize jobs: %w", err)
	}
	return applied, nil
}

var jobGetQuery = fmt.Sprintf(`
SELECT %s, NULL::timestamptz AS finalized_at, NULL::jsonb AS output FROM hopper_jobs WHERE id = $1
UNION ALL
SELECT %s, finalized_at, output FROM hopper_job_history WHERE id = $1
LIMIT 1`, jobColumns(""), jobColumns(""))

func (e *executor) JobGet(ctx context.Context, id driver.JobID) (*driver.JobRow, error) {
	job, err := scanJob(e.db.QueryRow(ctx, jobGetQuery, [16]byte(id)), true)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, driver.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: get job: %w", err)
	}
	return job, nil
}

// jobRescueSQL finds running jobs whose client has no live lease, plus jobs
// running longer than a stuck threshold. It walks the running index, which is
// bounded by total concurrency.
var jobRescueQuery = fmt.Sprintf(`
SELECT %s FROM hopper_jobs j
WHERE j.state = 'running'
  AND (NOT EXISTS (SELECT 1 FROM hopper_clients c WHERE c.id = j.attempted_by AND c.expires_at > now())
       OR ($1::float8 > 0 AND j.attempted_at < now() - make_interval(secs => $1)))
ORDER BY j.attempted_at
LIMIT $2`, jobColumns("j."))

func (e *executor) JobRescueCandidates(ctx context.Context, params driver.JobRescueParams) ([]*driver.JobRow, error) {
	rows, err := e.db.Query(ctx, jobRescueQuery, max(params.StuckAfter, 0).Seconds(), params.Limit)
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: rescue candidates: %w", err)
	}
	jobs, err := collectJobs(rows, false)
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: rescue candidates: %w", err)
	}
	return jobs, nil
}

func (e *executor) ClientPruneExpired(ctx context.Context) (int64, error) {
	tag, err := e.db.Exec(ctx, `DELETE FROM hopper_clients WHERE expires_at < now()`)
	if err != nil {
		return 0, fmt.Errorf("hopperpgx: prune clients: %w", err)
	}
	return tag.RowsAffected(), nil
}

// leaderSQL takes the lease if it is free or expired, or renews it for the
// holder. No row is returned when another client holds it.
const leaderSQL = `
INSERT INTO hopper_leader (name, client_id, elected_at, expires_at)
VALUES ('default', $1, now(), now() + make_interval(secs => $2))
ON CONFLICT (name) DO UPDATE SET
  client_id  = EXCLUDED.client_id,
  elected_at = CASE WHEN hopper_leader.client_id = EXCLUDED.client_id THEN hopper_leader.elected_at ELSE now() END,
  expires_at = EXCLUDED.expires_at
WHERE hopper_leader.expires_at < now() OR hopper_leader.client_id = EXCLUDED.client_id
RETURNING client_id`

func (e *executor) LeaderAttempt(ctx context.Context, params driver.LeaderParams) (bool, error) {
	var id int64
	err := e.db.QueryRow(ctx, leaderSQL, params.ClientID, params.TTL.Seconds()).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("hopperpgx: leader attempt: %w", err)
	}
	return id == params.ClientID, nil
}

// leaderResignSQL deletes the lease if this client holds it and, only then,
// signals other clients to elect a replacement.
const leaderResignSQL = `
WITH resigned AS (
  DELETE FROM hopper_leader WHERE name = 'default' AND client_id = $1 RETURNING client_id
)
SELECT pg_notify($2, 'resigned') FROM resigned`

func (e *executor) LeaderResign(ctx context.Context, clientID int64) error {
	if _, err := e.db.Exec(ctx, leaderResignSQL, clientID, driver.ChannelLeader); err != nil {
		return fmt.Errorf("hopperpgx: leader resign: %w", err)
	}
	return nil
}

func (e *executor) ClientRegister(ctx context.Context, params driver.ClientRegisterParams) (int64, error) {
	var id int64
	err := e.db.QueryRow(ctx,
		`INSERT INTO hopper_clients (hostname, expires_at, info)
		 VALUES ($1, now() + make_interval(secs => $2), $3) RETURNING id`,
		params.Hostname, params.TTL.Seconds(), jsonOrEmptyObject(params.Info),
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("hopperpgx: register client: %w", err)
	}
	return id, nil
}

// clientRenewSQL renews the lease and, in the same round trip, reads the
// control state the client must apply: cancel requests for its running jobs
// (through the running index) and paused queues.
const clientRenewSQL = `
WITH renewed AS (
  UPDATE hopper_clients SET expires_at = now() + make_interval(secs => $2)
  WHERE id = $1 AND expires_at > now()
  RETURNING id
)
SELECT EXISTS (SELECT 1 FROM renewed),
       (SELECT coalesce(array_agg(id), '{}') FROM hopper_jobs
         WHERE state = 'running' AND attempted_by = $1 AND cancel_requested_at IS NOT NULL),
       (SELECT coalesce(array_agg(name), '{}') FROM hopper_queues WHERE paused_at IS NOT NULL),
       (SELECT coalesce(array_agg(name), '{}') FROM hopper_queues
         WHERE global_limit IS NOT NULL OR rate_per_sec IS NOT NULL OR partition_limit IS NOT NULL)`

func (e *executor) ClientRenew(ctx context.Context, params driver.ClientRenewParams) (driver.ClientRenewResult, error) {
	var (
		res     driver.ClientRenewResult
		cancels [][16]byte
	)
	err := e.db.QueryRow(ctx, clientRenewSQL, params.ClientID, params.TTL.Seconds()).Scan(&res.Renewed, &cancels, &res.PausedQueues, &res.LimitedQueues)
	if err != nil {
		return res, fmt.Errorf("hopperpgx: renew client lease: %w", err)
	}
	for _, id := range cancels {
		res.CancelRequested = append(res.CancelRequested, driver.JobID(id))
	}
	return res, nil
}

func (e *executor) ClientDelete(ctx context.Context, clientID int64) error {
	if _, err := e.db.Exec(ctx, `DELETE FROM hopper_clients WHERE id = $1`, clientID); err != nil {
		return fmt.Errorf("hopperpgx: delete client: %w", err)
	}
	return nil
}
