package pgsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// Every statement that finalizes jobs (the batched finalize, canceling a
// waiting job, expiring) produces a CTE named finished, in the shape
// finishedColumns builds, and ends with finalizeTailSQL, which does
// everything a finalized job entails: canceling the dependents of a failed
// job, archiving, recording batch progress and completing batches, promoting
// dependents whose dependencies are all finished, and dropping satisfied
// edges.
//
// A batch's progress is appended, one hopper_batch_progress row per batch
// per statement, rather than counted down on the batch row, so finalizes do
// not queue on that row, and a caller's transaction that finalized a job
// and is still open (CompleteTx) holds up none of the job's siblings. The
// statement that finds nothing remaining (pending less the rows' done and
// its own) completes the batch instead of appending, guarded by
// completed_at so the callbacks run once, and zeroes pending, which a
// client of the previous release reads as is. Whether the batch is ready is
// decided from the statement's snapshot alone, and the update re-checks only
// completed_at, so a settle committed meanwhile cannot make the
// statement count rows twice. Two last jobs finalized in concurrent
// statements each miss the other's uncommitted row, so neither completes
// the batch; BatchSettle, which each client runs on its batches after the
// statement commits and the leader on any left, does.

// finishedColumns lists the columns a finishing CTE returns from a
// hopper_jobs row aliased j: the row, with the given expressions for its
// errors, logs, output, whether to archive it and its final state (text), and
// the done-channel notification for an awaited job. The notification sits
// in RETURNING because a data-modifying CTE always runs, while a plain
// SELECT CTE that nothing references does not.
func finishedColumns(errorsExpr, logsExpr, outputExpr, archiveExpr, stateExpr, doneChannelParam string) string {
	return fmt.Sprintf(`j.id, j.seq, j.kind, j.queue, j.priority, j.attempt, j.max_attempts, j.scheduled_at, j.attempted_at,
    j.attempted_by, j.args, j.metadata, %s AS errors, j.unique_key, j.ordering_key, j.partition_key, j.batch_id,
    j.expires_at, j.cancel_requested_at, j.await, j.created_at, j.progress, j.tags, j.fence, %s AS logs, %s AS output, %s AS archive,
    %s AS final_state, CASE WHEN j.await THEN pg_notify(%s, j.id::text) END AS notified`,
		errorsExpr, logsExpr, outputExpr, archiveExpr, stateExpr, doneChannelParam)
}

// appendErrorSQL is an errors expression that records a failure hopper
// itself decided, at database time. A snoozed job's reason is kept in it,
// since the row that held the reason is leaving the live table.
func appendErrorSQL(message string) string {
	return fmt.Sprintf(`j.errors || jsonb_build_object('at', now(), 'attempt', j.attempt, 'error',
      CASE WHEN j.state = 'scheduled' AND j.snooze_reason IS NOT NULL THEN '%[1]s while snoozed: ' || j.snooze_reason
           ELSE '%[1]s' END)`, message)
}

// finishedSelectColumns reads finished rows back in JobColumns order plus
// finalized_at and output, as history rows scan.
const finishedSelectColumns = `id, kind, queue, final_state, priority, attempt, max_attempts, scheduled_at, attempted_at,
  attempted_by, args, metadata, errors, unique_key, ordering_key, partition_key, batch_id, expires_at,
  cancel_requested_at, await, created_at, NULL::text AS snooze_reason, progress, tags, fence, now() AS finalized_at, output`

// batchProgressSQL sums a batch's progress rows, joined LATERAL to a
// relation that has the batch's id as id.
const batchProgressSQL = `LATERAL (SELECT coalesce(sum(p.done), 0) AS done, coalesce(sum(p.failed), 0) AS failed
    FROM {{schema}}.hopper_batch_progress p WHERE p.batch_id = b.id) p`

// batchCallbacksSQL returns the CTE that inserts the callbacks of the
// batches a CTE named batches has just completed, and notifies their
// queues. BatchInsert rejects a priority or max_attempts out of range, and
// those of rows written before it did are clamped, and its tags read as
// callbackTagsSQL reads them, so that one batch's callback cannot fail the
// statement that completes or settles others.
func batchCallbacksSQL(insertChannelParam string) string {
	return `callbacks AS (
  INSERT INTO {{schema}}.hopper_jobs (kind, queue, priority, max_attempts, args, metadata, tags)
  SELECT cb->>'kind', coalesce(cb->>'queue', 'default'),
    least(greatest(coalesce((cb->>'priority')::numeric, 2), 1), 4)::smallint,
    least(greatest(coalesce((cb->>'max_attempts')::numeric, 25), 1), 32767)::smallint, coalesce(cb->'args', '{}'),
    coalesce(cb->'metadata', '{}') || jsonb_build_object('batch_id', b.id::text, 'batch_failed', b.failed), ` + callbackTagsSQL("cb->'tags'") + `
  FROM batches b, LATERAL (VALUES (CASE WHEN b.failed = 0 THEN b.on_success ELSE b.on_failure END), (b.on_complete)) AS v(cb)
  WHERE cb IS NOT NULL
  RETURNING pg_notify(` + insertChannelParam + `, queue)
)`
}

// archivedSQL is the CTE that writes the rows of a CTE named done that are
// to be archived to history.
const archivedSQL = `archived AS (
  INSERT INTO {{schema}}.hopper_job_history (
    id, seq, kind, queue, state, priority, attempt, max_attempts, scheduled_at, attempted_at,
    attempted_by, args, metadata, errors, unique_key, ordering_key, partition_key, batch_id,
    expires_at, cancel_requested_at, await, created_at, progress, tags, fence, logs, finalized_at, output)
  SELECT id, seq, kind, queue, final_state::{{schema}}.hopper_job_state, priority, attempt, max_attempts, scheduled_at, attempted_at,
    attempted_by, args, metadata, errors, unique_key, ordering_key, partition_key, batch_id,
    expires_at, cancel_requested_at, await, created_at, progress, tags, fence, logs, now(), output
  FROM done WHERE archive
  RETURNING id
)`

// plainFinalizeTailSQL is finalizeTailSQL for statements whose jobs belong
// to no batch or workflow, which therefore have no batch to record and no
// dependents: it only archives.
const plainFinalizeTailSQL = `done AS (SELECT * FROM finished),
` + archivedSQL

// finalizeTailSQL returns the CTEs that follow a finished CTE; the statement
// must start with WITH RECURSIVE. Canceled dependents that were awaited
// are announced on the done channel, and promoted jobs and callbacks on
// the insert channel. Jobs without a batch or dependents cost index probes
// on small tables and nothing else.
//
// Every lookup keyed by the statement's own rows (the cascade, done, the
// batches counted and those completed) is a = ANY (ARRAY(...)), which the
// planner takes for ten keys and so probes an index for. Joined to the CTEs
// instead, it guessed hundreds of rows for them, and with no statistics on
// hopper_job_deps a probe looked like a few hundred rows more, so it scanned
// hopper_job_deps, hopper_jobs and hopper_batches whole on every finalize,
// at a cost that grew with the workflows in flight. The cascade itself
// still joins, its first step to finished and its recursive step to its
// own rows, which cannot take that form, and relies on the leader keeping
// hopper_job_deps analyzed (JobsMaintain).
func finalizeTailSQL(doneChannelParam, insertChannelParam string) string {
	return `cascade AS (
  SELECT d.job_id FROM {{schema}}.hopper_job_deps d JOIN finished f ON d.depends_on = f.id
  WHERE f.final_state IN ('canceled', 'discarded') AND d.on_failure = 'cancel'
  UNION
  SELECT d.job_id FROM {{schema}}.hopper_job_deps d JOIN cascade c ON d.depends_on = c.job_id
  WHERE d.on_failure = 'cancel'
),
cascaded AS (
  DELETE FROM {{schema}}.hopper_jobs j
  WHERE j.id = ANY (ARRAY(SELECT job_id FROM cascade)) AND j.state = 'pending'
  RETURNING ` + finishedColumns(appendErrorSQL("hopper: dependency failed"), "j.logs", "NULL::jsonb", "true", "'canceled'::text", doneChannelParam) + `
),
done AS (SELECT * FROM finished UNION ALL SELECT * FROM cascaded),
` + archivedSQL + `,
batch_counts AS (
  SELECT batch_id, count(*) AS n, count(*) FILTER (WHERE final_state IN ('canceled', 'discarded')) AS f
  FROM done WHERE batch_id IS NOT NULL GROUP BY batch_id
),
ready AS (
  SELECT b.id, b.failed + p.failed + c.f AS failed
  FROM batch_counts c JOIN {{schema}}.hopper_batches b ON b.id = c.batch_id, ` + batchProgressSQL + `
  WHERE b.id = ANY (ARRAY(SELECT batch_id FROM batch_counts)) AND b.completed_at IS NULL AND b.pending - p.done - c.n <= 0
),
batches AS (
  UPDATE {{schema}}.hopper_batches b SET completed_at = now(), pending = 0, failed = r.failed
  FROM ready r WHERE b.id = r.id AND b.completed_at IS NULL
  RETURNING b.id, b.failed, b.on_success, b.on_failure, b.on_complete
),
progress AS (
  INSERT INTO {{schema}}.hopper_batch_progress (batch_id, done, failed)
  SELECT c.batch_id, c.n, c.f FROM batch_counts c WHERE c.batch_id NOT IN (SELECT id FROM batches)
  RETURNING batch_id
),
progress_cleared AS (
  DELETE FROM {{schema}}.hopper_batch_progress p WHERE p.batch_id = ANY (ARRAY(SELECT id FROM batches)) RETURNING p.batch_id
),
` + batchCallbacksSQL(insertChannelParam) + `,
promoted AS (
  UPDATE {{schema}}.hopper_jobs j
  SET state = CASE WHEN j.scheduled_at > now() THEN 'scheduled' ELSE 'available' END::{{schema}}.hopper_job_state
  WHERE j.state = 'pending'
    AND j.id = ANY (ARRAY(SELECT d.job_id FROM {{schema}}.hopper_job_deps d WHERE d.depends_on = ANY (ARRAY(SELECT id FROM done))))
    AND j.id NOT IN (SELECT id FROM cascaded)
    AND NOT EXISTS (
      SELECT 1 FROM {{schema}}.hopper_job_deps d JOIN {{schema}}.hopper_jobs x ON x.id = d.depends_on
      WHERE d.job_id = j.id AND x.id NOT IN (SELECT id FROM done))
  RETURNING pg_notify(` + insertChannelParam + `, j.queue)
),
edges AS (
  DELETE FROM {{schema}}.hopper_job_deps d WHERE d.job_id = ANY (ARRAY(SELECT id FROM done)) RETURNING d.job_id
)`
}

// workflowPromoteSQL promotes the pending jobs among those a CTE named
// candidates selects none of whose dependencies is still live, skipping rows
// a finalize holds: that finalize decides them. It returns how many it
// promoted, how many candidates there were, and the greatest candidate ID.
// The candidates are looked up as finalizeTailSQL looks up its own rows:
// joined instead, a sweep's LIMIT $2, which a generic plan takes for a
// tenth of the edges, had it scan hopper_jobs whole for every chunk.
func workflowPromoteSQL(candidates string) string {
	return `
WITH candidates AS (` + candidates + `),
ready AS (
  SELECT j.id FROM {{schema}}.hopper_jobs j
  WHERE j.id = ANY (ARRAY(SELECT id FROM candidates))
    AND j.state = 'pending'
    AND NOT EXISTS (
      SELECT 1 FROM {{schema}}.hopper_job_deps d JOIN {{schema}}.hopper_jobs x ON x.id = d.depends_on
      WHERE d.job_id = j.id)
  FOR UPDATE OF j SKIP LOCKED
),
promoted AS (
  UPDATE {{schema}}.hopper_jobs j
  SET state = CASE WHEN j.scheduled_at > now() THEN 'scheduled' ELSE 'available' END::{{schema}}.hopper_job_state
  FROM ready WHERE j.id = ready.id AND j.state = 'pending'
  RETURNING pg_notify($1, j.queue)
)
SELECT (SELECT count(*) FROM promoted), (SELECT count(*) FROM candidates),
  (SELECT id FROM candidates ORDER BY id DESC LIMIT 1)`
}

// workflowPromoteSweepSQL considers, in ID order, up to $2 jobs with
// dependencies and an ID after $3. It walks the edges, the working set of
// unfinished workflows, so it costs nothing without them, and a chunk at a
// time however many pending steps they have.
var workflowPromoteSweepSQL = workflowPromoteSQL(`
  SELECT DISTINCT d.job_id AS id FROM {{schema}}.hopper_job_deps d
  WHERE d.job_id > $3 ORDER BY d.job_id LIMIT $2
`)

// workflowPromoteDependentsSQL considers the dependents of the jobs in $2.
var workflowPromoteDependentsSQL = workflowPromoteSQL(`
  SELECT DISTINCT d.job_id AS id FROM {{schema}}.hopper_job_deps d WHERE d.depends_on = ANY($2::uuid[])
`)

// WorkflowPromoteReady implements driver.Executor.
func (e *Executor) WorkflowPromoteReady(ctx context.Context, params driver.WorkflowPromoteParams) (driver.WorkflowPromoteResult, error) {
	var (
		res  driver.WorkflowPromoteResult
		last nullUUID
		err  error
	)
	switch {
	case params.DependsOn == nil:
		err = e.Conn.QueryRow(ctx, workflowPromoteSweepSQL, driver.ChannelInsert, max(params.Limit, 1), uuidParam(params.After)).
			Scan(&res.Promoted, &res.Scanned, &last)
	case len(params.DependsOn) == 0:
		return res, nil
	default:
		err = e.Conn.QueryRow(ctx, workflowPromoteDependentsSQL, driver.ChannelInsert, uuidArray(params.DependsOn)).
			Scan(&res.Promoted, &res.Scanned, &last)
	}
	if err != nil {
		return driver.WorkflowPromoteResult{}, fmt.Errorf("hopper: promote ready workflow jobs: %w", err)
	}
	res.Last = last.ID
	return res, nil
}

// workflowJobInsertSQL inserts a workflow's jobs with the IDs generated up
// front, pending when they have dependencies. RETURNING follows input order.
var workflowJobInsertSQL = `
WITH p AS (
  SELECT * FROM unnest(
    $1::uuid[], $2::text[], $3::text[], $4::smallint[], $5::smallint[], $6::timestamptz[], $7::jsonb[], $8::jsonb[],
    $9::float8[], $10::boolean[], $11::text[], $12::text[], $13::boolean[], $15::jsonb[]
  ) AS p(id, kind, queue, priority, max_attempts, scheduled_at, args, metadata, ttl, await, ordering_key, partition_key, pending, tags)
)
INSERT INTO {{schema}}.hopper_jobs (id, kind, queue, state, priority, max_attempts, scheduled_at, args, metadata, expires_at, await, ordering_key, partition_key, batch_id, tags)
SELECT p.id, p.kind, p.queue,
       CASE WHEN p.pending THEN 'pending' WHEN p.scheduled_at > now() THEN 'scheduled' ELSE 'available' END::{{schema}}.hopper_job_state,
       p.priority, p.max_attempts, coalesce(p.scheduled_at, now()), p.args, p.metadata,
       CASE WHEN p.ttl > 0 THEN now() + make_interval(secs => p.ttl) END, p.await, p.ordering_key, p.partition_key, $14::uuid, p.tags
FROM p
RETURNING ` + JobColumns("") + `, false AS duplicate`

// WorkflowInsert implements driver.Executor.
func (e *Executor) WorkflowInsert(ctx context.Context, params driver.WorkflowInsertParams) (*driver.WorkflowInsertResult, error) {
	n := len(params.Jobs)
	if n == 0 {
		return nil, errors.New("hopper: insert workflow: no jobs")
	}
	pending := make([]bool, n)
	for _, d := range params.Deps {
		if d.Job < 0 || d.Job >= n || d.DependsOn < 0 || d.DependsOn >= n || d.Job == d.DependsOn {
			return nil, fmt.Errorf("hopper: insert workflow: dependency %d -> %d is out of range", d.Job, d.DependsOn)
		}
		pending[d.Job] = true
	}
	var (
		kinds, queues, ordering, partitions = make([]string, n), make([]string, n), make([]string, n), make([]string, n)
		priorities, maxAttempts             = make([]int, n), make([]int, n)
		scheduledAt                         = make([]time.Time, n)
		args, metadata, tags                = make([][]byte, n), make([][]byte, n), make([][]byte, n)
		ttls                                = make([]float64, n)
		awaits                              = make([]bool, n)
		queueSet                            = map[string]struct{}{}
		notifyQueues                        []string
	)
	for i, p := range params.Jobs {
		if p.UniqueKey != "" {
			return nil, fmt.Errorf("hopper: insert workflow: job %d has a unique key", i)
		}
		var err error
		if priorities[i], err = smallint(p.Priority, "priority"); err != nil {
			return nil, err
		}
		if maxAttempts[i], err = smallint(p.MaxAttempts, "max_attempts"); err != nil {
			return nil, err
		}
		kinds[i], queues[i], scheduledAt[i] = p.Kind, p.Queue, p.ScheduledAt
		args[i], metadata[i], tags[i] = []byte(jsonOrEmptyObject(p.Args)), []byte(jsonOrEmptyObject(p.Metadata)), TagsJSON(p.Tags)
		ttls[i], awaits[i], ordering[i], partitions[i] = p.TTL.Seconds(), p.Await, p.OrderingKey, p.PartitionKey
		if _, seen := queueSet[p.Queue]; !pending[i] && !seen {
			queueSet[p.Queue] = struct{}{}
			notifyQueues = append(notifyQueues, p.Queue)
		}
	}
	onSuccess, onFailure, onComplete, err := callbacksJSON(params.OnSuccess, params.OnFailure, params.OnComplete)
	if err != nil {
		return nil, err
	}

	res := &driver.WorkflowInsertResult{}
	err = e.withTx(ctx, func(tx Tx) error {
		ids, err := scanIDs(tx.Query(ctx, "SELECT {{schema}}.hopper_uuidv7() FROM generate_series(1, $1)", n))
		if err != nil {
			return fmt.Errorf("generate ids: %w", err)
		}
		// The graph, for inspection, and the working set of edges.
		edges := make([][2]string, len(params.Deps))
		jobIDs, depIDs, policies := make([]driver.JobID, len(params.Deps)), make([]driver.JobID, len(params.Deps)), make([]string, len(params.Deps))
		for i, d := range params.Deps {
			jobIDs[i], depIDs[i] = ids[d.Job], ids[d.DependsOn]
			edges[i] = [2]string{ids[d.Job].String(), ids[d.DependsOn].String()}
			policies[i] = string(d.OnFailure)
			if d.OnFailure == "" {
				policies[i] = string(driver.DependencyCancel)
			}
		}
		edgesJSON, err := json.Marshal(edges)
		if err != nil {
			return err
		}
		err = tx.QueryRow(ctx, `
			INSERT INTO {{schema}}.hopper_batches (name, pending, total, on_success, on_failure, on_complete, metadata, edges)
			VALUES ($1, $2, $2, $3::jsonb, $4::jsonb, $5::jsonb, $6::jsonb, $7::jsonb) RETURNING id`,
			nullable(params.Name), n, onSuccess, onFailure, onComplete, jsonOrEmptyObject(params.Metadata), string(edgesJSON)).Scan(&res.ID)
		if err != nil {
			return fmt.Errorf("insert batch: %w", err)
		}
		res.Jobs, err = scanInsertResults(tx.Query(ctx, workflowJobInsertSQL,
			uuidArray(ids), textArray(kinds), textArray(queues), intArray(priorities), intArray(maxAttempts), timeArray(scheduledAt),
			jsonArray(args), jsonArray(metadata), floatArray(ttls), boolArray(awaits),
			nullableTextArray(ordering), nullableTextArray(partitions), boolArray(pending), uuidParam(res.ID), jsonArray(tags)))
		if err != nil {
			return fmt.Errorf("insert jobs: %w", err)
		}
		if len(params.Deps) > 0 {
			if _, err := tx.Exec(ctx, `INSERT INTO {{schema}}.hopper_job_deps (job_id, depends_on, on_failure)
				SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::text[]) ON CONFLICT DO NOTHING`,
				uuidArray(jobIDs), uuidArray(depIDs), textArray(policies)); err != nil {
				return fmt.Errorf("insert dependencies: %w", err)
			}
		}
		if params.Notify && len(notifyQueues) > 0 {
			if _, err := tx.Exec(ctx, NotifySQL, driver.ChannelInsert, textArray(notifyQueues)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("hopper: insert workflow: %w", err)
	}
	return res, nil
}

func callbacksJSON(cbs ...*driver.JobInsertParams) (onSuccess, onFailure, onComplete *string, err error) {
	out := make([]*string, len(cbs))
	for i, cb := range cbs {
		if out[i], err = callbackJSON(cb); err != nil {
			return nil, nil, nil, err
		}
	}
	return out[0], out[1], out[2], nil
}

var workflowJobsSQL = fmt.Sprintf(`
SELECT %s, finalized_at, output FROM (
  SELECT %s, seq, NULL::timestamptz AS finalized_at, NULL::jsonb AS output FROM {{schema}}.hopper_jobs WHERE batch_id = $1
  UNION ALL
  SELECT %s, seq, finalized_at, output FROM {{schema}}.hopper_job_history WHERE batch_id = $1
) u ORDER BY seq`, JobColumns(""), JobColumns(""), historyColumns(""))

// WorkflowGet implements driver.Executor.
func (e *Executor) WorkflowGet(ctx context.Context, id driver.JobID) (*driver.WorkflowRow, error) {
	var (
		w         driver.WorkflowRow
		name      sql.NullString
		completed sql.NullTime
		metadata  jsonText
		edges     jsonText
	)
	err := e.Conn.QueryRow(ctx, `SELECT id, name, `+batchProgressSelect+`, total, created_at, completed_at, metadata, edges
		FROM {{schema}}.hopper_batches b, `+batchProgressSQL+` WHERE id = $1`, uuidParam(id)).
		Scan(&w.Batch.ID, &name, &w.Batch.Pending, &w.Batch.Failed, &w.Batch.Total, &w.Batch.CreatedAt, &completed, &metadata, &edges)
	if errors.Is(err, ErrNoRows) {
		return nil, driver.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: get workflow: %w", err)
	}
	w.Batch.Name, w.Batch.CompletedAt, w.Batch.Metadata = name.String, completed.Time, metadata.raw()
	if edges != "" {
		var pairs [][2]driver.JobID
		if err := json.Unmarshal([]byte(edges), &pairs); err != nil {
			return nil, fmt.Errorf("hopper: get workflow: edges: %w", err)
		}
		w.Edges = make([]driver.JobEdge, len(pairs))
		for i, p := range pairs {
			w.Edges[i] = driver.JobEdge{Job: p[0], DependsOn: p[1]}
		}
	}
	if w.Jobs, err = historyJobs(e.Conn.Query(ctx, workflowJobsSQL, uuidParam(id))); err != nil {
		return nil, fmt.Errorf("hopper: get workflow: jobs: %w", err)
	}
	return &w, nil
}
