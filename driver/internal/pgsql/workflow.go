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

// Every statement that finalizes jobs (the batched finalize, cancelling a
// waiting job, expiring) produces a CTE named finished, in the shape
// finishedColumns builds, and ends with finalizeTailSQL, which does
// everything a finalized job entails: cancelling the dependents of a failed
// job, archiving, counting batches down, promoting dependents whose
// dependencies are all finished, and dropping satisfied edges.

// finishedColumns lists the columns a finishing CTE returns from a
// hopper_jobs row aliased j: the row, with the given expressions for its
// errors, output, whether to archive it and its final state (text), and
// the done-channel notification for an awaited job. The notification sits
// in RETURNING because a data-modifying CTE always runs, while a plain
// SELECT CTE that nothing references does not.
func finishedColumns(errorsExpr, outputExpr, archiveExpr, stateExpr, doneChannelParam string) string {
	return fmt.Sprintf(`j.id, j.seq, j.kind, j.queue, j.priority, j.attempt, j.max_attempts, j.scheduled_at, j.attempted_at,
    j.attempted_by, j.args, j.metadata, %s AS errors, j.unique_key, j.ordering_key, j.partition_key, j.batch_id,
    j.expires_at, j.cancel_requested_at, j.await, j.created_at, %s AS output, %s AS archive, %s AS final_state,
    CASE WHEN j.await THEN pg_notify(%s, j.id::text) END AS notified`,
		errorsExpr, outputExpr, archiveExpr, stateExpr, doneChannelParam)
}

// appendErrorSQL is an errors expression that records a failure hopper
// itself decided, at database time.
func appendErrorSQL(message string) string {
	return fmt.Sprintf(`j.errors || jsonb_build_object('at', now(), 'attempt', j.attempt, 'error', '%s')`, message)
}

// finishedSelectColumns reads finished rows back in JobColumns order plus
// finalized_at and output, as history rows scan.
const finishedSelectColumns = `id, kind, queue, final_state, priority, attempt, max_attempts, scheduled_at, attempted_at,
  attempted_by, args, metadata, errors, unique_key, ordering_key, partition_key, batch_id, expires_at,
  cancel_requested_at, await, created_at, now() AS finalized_at, output`

// finalizeTailSQL returns the CTEs that follow a finished CTE; the statement
// must start with WITH RECURSIVE. Cancelled dependents that were awaited
// are announced on the done channel, and promoted jobs and callbacks on
// the insert channel. Jobs without a batch or dependents cost index probes
// on small tables and nothing else.
func finalizeTailSQL(doneChannelParam, insertChannelParam string) string {
	return `cascade AS (
  SELECT d.job_id FROM hopper_job_deps d JOIN finished f ON d.depends_on = f.id
  WHERE f.final_state IN ('cancelled', 'discarded') AND d.on_failure = 'cancel'
  UNION
  SELECT d.job_id FROM hopper_job_deps d JOIN cascade c ON d.depends_on = c.job_id
  WHERE d.on_failure = 'cancel'
),
cascaded AS (
  DELETE FROM hopper_jobs j USING (SELECT DISTINCT job_id FROM cascade) c
  WHERE j.id = c.job_id AND j.state = 'pending'
  RETURNING ` + finishedColumns(appendErrorSQL("hopper: dependency failed"), "NULL::jsonb", "true", "'cancelled'::text", doneChannelParam) + `
),
done AS (SELECT * FROM finished UNION ALL SELECT * FROM cascaded),
archived AS (
  INSERT INTO hopper_job_history (
    id, seq, kind, queue, state, priority, attempt, max_attempts, scheduled_at, attempted_at,
    attempted_by, args, metadata, errors, unique_key, ordering_key, partition_key, batch_id,
    expires_at, cancel_requested_at, await, created_at, finalized_at, output)
  SELECT id, seq, kind, queue, final_state::hopper_job_state, priority, attempt, max_attempts, scheduled_at, attempted_at,
    attempted_by, args, metadata, errors, unique_key, ordering_key, partition_key, batch_id,
    expires_at, cancel_requested_at, await, created_at, now(), output
  FROM done WHERE archive
  RETURNING id
),
batches AS (
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
  RETURNING pg_notify(` + insertChannelParam + `, queue)
),
promoted AS (
  UPDATE hopper_jobs j
  SET state = CASE WHEN j.scheduled_at > now() THEN 'scheduled' ELSE 'available' END::hopper_job_state
  WHERE j.state = 'pending'
    AND j.id IN (SELECT d.job_id FROM hopper_job_deps d JOIN done ON d.depends_on = done.id)
    AND j.id NOT IN (SELECT id FROM cascaded)
    AND NOT EXISTS (
      SELECT 1 FROM hopper_job_deps d JOIN hopper_jobs x ON x.id = d.depends_on
      WHERE d.job_id = j.id AND x.id NOT IN (SELECT id FROM done))
  RETURNING pg_notify(` + insertChannelParam + `, j.queue)
),
edges AS (
  DELETE FROM hopper_job_deps d WHERE d.job_id IN (SELECT id FROM done) RETURNING d.job_id
)`
}

// workflowJobInsertSQL inserts a workflow's jobs with the IDs generated up
// front, pending when they have dependencies. RETURNING follows input order.
var workflowJobInsertSQL = `
WITH p AS (
  SELECT * FROM unnest(
    $1::uuid[], $2::text[], $3::text[], $4::smallint[], $5::smallint[], $6::timestamptz[], $7::jsonb[], $8::jsonb[],
    $9::float8[], $10::boolean[], $11::text[], $12::text[], $13::boolean[]
  ) AS p(id, kind, queue, priority, max_attempts, scheduled_at, args, metadata, ttl, await, ordering_key, partition_key, pending)
)
INSERT INTO hopper_jobs (id, kind, queue, state, priority, max_attempts, scheduled_at, args, metadata, expires_at, await, ordering_key, partition_key, batch_id)
SELECT p.id, p.kind, p.queue,
       CASE WHEN p.pending THEN 'pending' WHEN p.scheduled_at > now() THEN 'scheduled' ELSE 'available' END::hopper_job_state,
       p.priority, p.max_attempts, coalesce(p.scheduled_at, now()), p.args, p.metadata,
       CASE WHEN p.ttl > 0 THEN now() + make_interval(secs => p.ttl) END, p.await, p.ordering_key, p.partition_key, $14::uuid
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
		args, metadata                      = make([][]byte, n), make([][]byte, n)
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
		args[i], metadata[i] = []byte(jsonOrEmptyObject(p.Args)), []byte(jsonOrEmptyObject(p.Metadata))
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
		ids, err := scanIDs(tx.Query(ctx, "SELECT hopper_uuidv7() FROM generate_series(1, $1)", n))
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
			INSERT INTO hopper_batches (name, pending, total, on_success, on_failure, on_complete, metadata, edges)
			VALUES ($1, $2, $2, $3::jsonb, $4::jsonb, $5::jsonb, $6::jsonb, $7::jsonb) RETURNING id`,
			nullable(params.Name), n, onSuccess, onFailure, onComplete, jsonOrEmptyObject(params.Metadata), string(edgesJSON)).Scan(&res.ID)
		if err != nil {
			return fmt.Errorf("insert batch: %w", err)
		}
		res.Jobs, err = scanInsertResults(tx.Query(ctx, workflowJobInsertSQL,
			uuidArray(ids), textArray(kinds), textArray(queues), intArray(priorities), intArray(maxAttempts), timeArray(scheduledAt),
			jsonArray(args), jsonArray(metadata), floatArray(ttls), boolArray(awaits),
			nullableTextArray(ordering), nullableTextArray(partitions), boolArray(pending), uuidParam(res.ID)))
		if err != nil {
			return fmt.Errorf("insert jobs: %w", err)
		}
		if len(params.Deps) > 0 {
			if _, err := tx.Exec(ctx, `INSERT INTO hopper_job_deps (job_id, depends_on, on_failure)
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
  SELECT %s, seq, NULL::timestamptz AS finalized_at, NULL::jsonb AS output FROM hopper_jobs WHERE batch_id = $1
  UNION ALL
  SELECT %s, seq, finalized_at, output FROM hopper_job_history WHERE batch_id = $1
) u ORDER BY seq`, JobColumns(""), JobColumns(""), JobColumns(""))

// WorkflowGet implements driver.Executor.
func (e *Executor) WorkflowGet(ctx context.Context, id driver.JobID) (*driver.WorkflowRow, error) {
	var (
		w         driver.WorkflowRow
		name      sql.NullString
		completed sql.NullTime
		metadata  jsonText
		edges     jsonText
	)
	err := e.Conn.QueryRow(ctx, `SELECT id, name, pending, failed, total, created_at, completed_at, metadata, edges FROM hopper_batches WHERE id = $1`, uuidParam(id)).
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
