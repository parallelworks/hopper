package pgsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/internal/jobcodec"
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
// The lookups keyed by the statement's own rows (the cascade's jobs, done,
// the batches completed) are = ANY (ARRAY(...)), which the planner takes
// for ten keys and so probes an index for. Joined to the CTEs instead, it
// guessed hundreds of rows for them, the cascade's alone guessed at 180,
// and with no statistics on hopper_job_deps a probe looked like a few
// hundred rows more, so it scanned hopper_job_deps, hopper_jobs and
// hopper_batches whole on every finalize, at a cost that grew with the
// workflows in flight. ready looks each counted batch up by primary key
// in a LATERAL subquery that OFFSET 0 keeps from being flattened into a
// join: joined, a hopper_batches without statistics was scanned whole, and
// with a = ANY the planner rescanned batch_counts for every batch row.
// The cascade still joins too, its first step to finished and its
// recursive step to its own rows, which cannot take that form, and relies
// on the leader keeping hopper_job_deps analyzed (JobsMaintain).
//
// The cascade cancels a dependent of a job that failed in the statement
// through an any_cancel group when one of its any_cancel groups is
// exhausted (exhausted), and promoted skips one a group holds (held). Its
// recursive step, which cannot read its own rows in a subquery, follows
// cancel edges only: a group exhausted by a job it canceled holds that
// group's dependent, which the follow-up of the jobs the cascade canceled
// cancels (the finalize statements return them, JobFinalized.Cascaded).
func finalizeTailSQL(doneChannelParam, insertChannelParam string) string {
	return anyOfGroupsSQL("exhausted",
		"ARRAY(SELECT d.job_id FROM {{schema}}.hopper_job_deps d WHERE d.on_failure = 'any_cancel'"+
			" AND d.depends_on = ANY (ARRAY(SELECT id FROM finished WHERE final_state IN ('canceled', 'discarded'))))",
		"ARRAY(SELECT batch_id FROM finished)",
		"ARRAY(SELECT id FROM finished WHERE final_state IN ('canceled', 'discarded'))", "'{}'::uuid[]") + `,
cascade AS (
  SELECT d.job_id FROM {{schema}}.hopper_job_deps d JOIN finished f ON d.depends_on = f.id
  WHERE f.final_state IN ('canceled', 'discarded')
    AND (d.on_failure = 'cancel' OR (d.on_failure = 'any_cancel'
      AND d.job_id = ANY (ARRAY(SELECT job_id FROM exhausted WHERE ` + anyOfExhaustedCancel + `))))
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
  FROM batch_counts c,
    LATERAL (SELECT b.id, b.failed, b.pending FROM {{schema}}.hopper_batches b
             WHERE b.id = c.batch_id AND b.completed_at IS NULL OFFSET 0) b, ` + batchProgressSQL + `
  WHERE b.pending - p.done - c.n <= 0
),
batches AS (
  UPDATE {{schema}}.hopper_batches b SET completed_at = now(), pending = 0, failed = r.failed
  FROM ready r WHERE b.id = r.id AND b.completed_at IS NULL
  RETURNING b.id, b.failed, b.on_success, b.on_failure, b.on_complete, b.reads
),
progress AS (
  INSERT INTO {{schema}}.hopper_batch_progress (batch_id, done, failed)
  SELECT c.batch_id, c.n, c.f FROM batch_counts c WHERE c.batch_id NOT IN (SELECT id FROM batches)
  RETURNING batch_id
),
progress_cleared AS (
  DELETE FROM {{schema}}.hopper_batch_progress p WHERE p.batch_id = ANY (ARRAY(SELECT id FROM batches)) RETURNING p.batch_id
),
` + resultsSQL + `,
` + batchCallbacksSQL(insertChannelParam) + `,
` + anyOfGroupsSQL("held",
		"ARRAY(SELECT d.job_id FROM {{schema}}.hopper_job_deps d WHERE d.depends_on = ANY (ARRAY(SELECT id FROM done)))",
		"ARRAY(SELECT batch_id FROM done)",
		"ARRAY(SELECT id FROM done)", "ARRAY(SELECT id FROM done WHERE final_state = 'completed')") + `,
promoted AS (
  UPDATE {{schema}}.hopper_jobs j
  SET state = CASE WHEN j.scheduled_at > now() THEN 'scheduled' ELSE 'available' END::{{schema}}.hopper_job_state
  WHERE j.state = 'pending'
    AND j.id = ANY (ARRAY(SELECT d.job_id FROM {{schema}}.hopper_job_deps d WHERE d.depends_on = ANY (ARRAY(SELECT id FROM done))))
    AND j.id NOT IN (SELECT id FROM cascaded)
    AND NOT EXISTS (
      SELECT 1 FROM {{schema}}.hopper_job_deps d JOIN {{schema}}.hopper_jobs x ON x.id = d.depends_on
      WHERE d.job_id = j.id AND d.any_of IS NULL AND x.id NOT IN (SELECT id FROM done))
    AND j.id <> ALL (ARRAY(SELECT job_id FROM held WHERE ` + anyOfHolds + `))
  RETURNING pg_notify(` + insertChannelParam + `, j.queue)
),
edges AS (
  DELETE FROM {{schema}}.hopper_job_deps d WHERE d.job_id = ANY (ARRAY(SELECT id FROM done)) RETURNING d.job_id
)`
}

// The edges of a dependent's any-of group (hopper_job_deps.any_of, the
// group's number) hold it until one of their jobs has completed, which
// satisfies the group. A group none of whose jobs completed holds the
// dependent while one of them is live, and once none is, its policy
// applies: any_ignore lets the dependent run, and any_cancel cancels it,
// as a 'cancel' edge does when its job fails. A job of a group leaves the
// live table with its final state recorded in hopper_job_results, in the
// statement that finalizes it (resultsSQL), so every snapshot sees each
// job of a group either live or with its outcome. One gone without a
// result was finalized by a client of the previous release, which records
// none; it counts as completed, as that release, which runs the
// dependent once the group's jobs have all finished, counts it.
//
// A statement sees the rows it finalizes in the live table still, so
// anyOfGroupsSQL takes the IDs of those that finished or completed in it,
// as arrays: a job of a group in the live table is live unless it is
// among them. A group's edges are found through hopper_job_deps_any_of_job,
// the partial index on the edges in a group (any_of IS NOT NULL) by
// dependent, which is empty for workflows without groups, so they cost a
// probe of it per statement. With coalesce(any_of, 0) <> 0 as its
// predicate, which the planner estimates true of all but one edge in two
// hundred, it chose to walk the edges whole for a statement's dependents
// now and then. The partial index on the edges that read or are in a
// group, by dependency, has coalesce(reads, any_of IS NOT NULL) for its
// predicate, which any_of IS NOT NULL does not imply: with reads OR
// any_of IS NOT NULL, a lookup of a dependent's groups could walk that
// one whole, filtering it by the dependent, as v22 found of the edges
// that read.

// anyOfGroupsSQL returns two CTEs: one, named name+"_edges", of the
// edges in a group of the jobs in the array dependents, and one, named
// name, of each of those groups, with its dependent (job_id), whether its
// policy is any_cancel (cancels), whether one of its jobs completed
// (satisfied) and whether one is live (live). A group is its dependent's
// edges of one number, whatever policy each stores, as SQLite reads it:
// grouped by policy too, a group whose edges disagree would be split in
// two, each decided without the other's jobs. batches is an array that
// holds the dependents' batches, done the IDs of the jobs that finished in
// the statement and completed those of them that completed. A job of a
// group in the live table completed if it is among completed and is live
// unless it is among done; one gone from it completed unless its result
// says it failed.
//
// Each job of the groups is looked up once, in one probe of the live
// table and one of the results for all of them, which the CTE of
// edges, materialized, feeds, so the groups cost what their edges do:
// a check of a group's jobs per edge or per dependent, a subquery the
// planner multiplies by the edges it guesses, guessed at a statement's
// dependents times four hundred edges each without statistics, and
// planned at millions, past jit_above_cost, where JIT compiled the
// leader's statements for seconds.
func anyOfGroupsSQL(name, dependents, batches, done, completed string) string {
	return name + `_edges AS MATERIALIZED (
  SELECT g.job_id, g.any_of, g.on_failure, g.depends_on FROM {{schema}}.hopper_job_deps g
  WHERE g.job_id = ANY (` + dependents + `) AND g.any_of IS NOT NULL
),
` + name + ` AS (
  SELECT e.job_id, bool_or(e.on_failure = 'any_cancel') AS cancels,
    bool_or(CASE WHEN e.depends_on = ANY (m.present) THEN e.depends_on = ANY (` + completed + `)
                 ELSE e.depends_on <> ALL (m.failed) END) AS satisfied,
    bool_or(e.depends_on = ANY (m.present) AND e.depends_on <> ALL (` + done + `)) AS live
  FROM ` + name + `_edges e, (
    SELECT ARRAY(SELECT x.id FROM {{schema}}.hopper_jobs x
                 WHERE x.id = ANY (ARRAY(SELECT depends_on FROM ` + name + `_edges))) AS present,
      ARRAY(SELECT r.job_id FROM {{schema}}.hopper_job_results r
            WHERE r.batch_id = ANY (` + batches + `) AND r.job_id = ANY (ARRAY(SELECT depends_on FROM ` + name + `_edges))
              AND r.state <> 'completed') AS failed
    OFFSET 0) m
  GROUP BY e.job_id, e.any_of
)`
}

// candidateGroupsSQL is anyOfGroupsSQL of the jobs a CTE named candidates
// selects, outside a finalize. The dependents' batches are read only for
// those with a group, so that candidates without one cost no read of
// their rows.
func candidateGroupsSQL(name string) string {
	return anyOfGroupsSQL(name, "ARRAY(SELECT id FROM candidates)",
		"ARRAY(SELECT x.batch_id FROM {{schema}}.hopper_jobs x WHERE x.id = ANY (ARRAY(SELECT job_id FROM "+name+"_edges)))",
		"'{}'::uuid[]", "'{}'::uuid[]")
}

// anyOfHolds is true of a row of anyOfGroupsSQL's groups that holds its
// dependent: none of its jobs completed, and one is live or its policy
// is any_cancel, which holds the dependent of an exhausted group until it
// is canceled.
const anyOfHolds = `NOT satisfied AND (live OR cancels)`

// anyOfExhaustedCancel is true of a row of anyOfGroupsSQL's groups that
// cancels its dependent: none of its jobs is live or completed, and its
// policy is any_cancel.
const anyOfExhaustedCancel = `NOT satisfied AND NOT live AND cancels`

// resultsSQL records the final state and output of each job of done with
// a dependent that reads it (StepOpts.Dependencies), for that dependent's
// claims, and the final state of each with a dependent that has it in an
// any-of group, for that dependent's promotion; and deletes the results of
// the workflows the statement completes that have any such dependent,
// whose jobs have all finished. A job of a workflow completing in the
// statement has no dependent left, so it records none. Whether a job has
// such a dependent is a probe per job of the partial index on the edges
// that read or are in a group, a scalar subquery, which finds nothing in a
// workflow with neither: a filter on an array of the statement's jobs
// compared every pair. Whether one reads it, and its output is kept, is
// a second probe, for the jobs the first finds. A job retried from
// history and finished again replaces its result.
const resultsSQL = `results AS (
  INSERT INTO {{schema}}.hopper_job_results (job_id, batch_id, state, output)
  SELECT d.id, d.batch_id, d.final_state::{{schema}}.hopper_job_state,
    CASE WHEN (SELECT 1 FROM {{schema}}.hopper_job_deps e WHERE e.depends_on = d.id AND e.reads LIMIT 1) IS NOT NULL
         THEN d.output END
  FROM done d
  WHERE d.batch_id IS NOT NULL
    AND (SELECT 1 FROM {{schema}}.hopper_job_deps e WHERE e.depends_on = d.id AND coalesce(e.reads, e.any_of IS NOT NULL) LIMIT 1) IS NOT NULL
    AND d.batch_id <> ALL (ARRAY(SELECT id FROM batches))
  ON CONFLICT (batch_id, job_id) DO UPDATE SET state = EXCLUDED.state, output = EXCLUDED.output,
    finalized_at = EXCLUDED.finalized_at
  RETURNING job_id
),
` + resultsClearedSQL

// resultsClearedSQL deletes the dependency results of the batches a CTE
// named batches has just completed that have a step that reads them
// (hopper_batches.reads), for the statements that complete batches: the
// finalize tail and BatchSettle. It skips a row another transaction holds:
// a JobRetryTx held open that deletes its step's result, which the
// statement would otherwise wait for, with its whole flush. A retry that
// commits has deleted the row, and one that rolls back leaves it to the
// leader's sweep of the results of completed workflows.
const resultsClearedSQL = `results_cleared AS (
  DELETE FROM {{schema}}.hopper_job_results r WHERE (r.batch_id, r.job_id) IN (
    SELECT l.batch_id, l.job_id FROM {{schema}}.hopper_job_results l
    WHERE l.batch_id = ANY (ARRAY(SELECT id FROM batches WHERE reads))
    FOR UPDATE SKIP LOCKED)
  RETURNING r.job_id
)`

// workflowPromoteSQL promotes the pending jobs among those a CTE named
// candidates selects none of whose dependencies is still live and none of
// whose any-of groups holds them, skipping rows a finalize holds: that
// finalize decides them. It returns how many it promoted, how many
// candidates there were, the greatest candidate ID, and how many of them
// an exhausted any-of group whose policy is any_cancel holds, for
// workflowCancelExhaustedSQL to cancel.
// The candidates are looked up as finalizeTailSQL looks up its own rows:
// joined instead, a sweep's LIMIT, which a generic plan takes for a
// tenth of the edges, had it scan hopper_jobs whole for every chunk.
func workflowPromoteSQL(candidates string) string {
	return `
WITH candidates AS (` + candidates + `),
` + candidateGroupsSQL("held") + `,
ready AS (
  SELECT j.id FROM {{schema}}.hopper_jobs j
  WHERE j.id = ANY (ARRAY(SELECT id FROM candidates))
    AND j.state = 'pending'
    AND NOT EXISTS (
      SELECT 1 FROM {{schema}}.hopper_job_deps d JOIN {{schema}}.hopper_jobs x ON x.id = d.depends_on
      WHERE d.job_id = j.id AND d.any_of IS NULL)
    AND j.id <> ALL (ARRAY(SELECT job_id FROM held WHERE ` + anyOfHolds + `))
  FOR UPDATE OF j SKIP LOCKED
),
promoted AS (
  UPDATE {{schema}}.hopper_jobs j
  SET state = CASE WHEN j.scheduled_at > now() THEN 'scheduled' ELSE 'available' END::{{schema}}.hopper_job_state
  FROM ready WHERE j.id = ready.id AND j.state = 'pending'
  RETURNING pg_notify($1, j.queue)
)
SELECT (SELECT count(*) FROM promoted), (SELECT count(*) FROM candidates),
  (SELECT id FROM candidates ORDER BY id DESC LIMIT 1),
  (SELECT count(*) FROM held WHERE ` + anyOfExhaustedCancel + `)`
}

// workflowCancelExhaustedSQL cancels the pending jobs among those a CTE
// named candidates selects that an exhausted any-of group whose policy is
// any_cancel holds, as the finalize that failed the group's last job
// cancels its dependent when no other job of the group finished in a
// concurrent statement, and returns the IDs of every job it finished, its
// dependents canceled in turn among them. It skips a row another
// statement holds, which decides it.
func workflowCancelExhaustedSQL(candidates string) string {
	return `
WITH RECURSIVE candidates AS (` + candidates + `),
` + candidateGroupsSQL("candidate") + `,
finished AS (
  DELETE FROM {{schema}}.hopper_jobs j
  WHERE j.id IN (
    SELECT j.id FROM {{schema}}.hopper_jobs j
    WHERE j.id = ANY (ARRAY(SELECT id FROM candidates)) AND j.state = 'pending'
      AND j.id = ANY (ARRAY(SELECT job_id FROM candidate WHERE ` + anyOfExhaustedCancel + `))
    FOR UPDATE OF j SKIP LOCKED
  )
  RETURNING ` + finishedColumns(appendErrorSQL("hopper: dependency failed"), "j.logs", "NULL::jsonb", "true", "'canceled'::text", "$1") + `
),
` + finalizeTailSQL("$1", "$2") + `
SELECT id FROM done`
}

// workflowSweepCandidates selects, in ID order, up to %[1]s jobs with
// dependencies and an ID after %[2]s. It walks the edges, the working set
// of unfinished workflows, so it costs nothing without them, and a chunk
// at a time however many pending steps they have.
const workflowSweepCandidates = `
  SELECT DISTINCT d.job_id AS id FROM {{schema}}.hopper_job_deps d
  WHERE d.job_id > %[2]s ORDER BY d.job_id LIMIT %[1]s
`

// workflowDependentsCandidates selects the dependents of the jobs in %s.
const workflowDependentsCandidates = `
  SELECT DISTINCT d.job_id AS id FROM {{schema}}.hopper_job_deps d WHERE d.depends_on = ANY(%s::uuid[])
`

var (
	workflowPromoteSweepSQL      = workflowPromoteSQL(fmt.Sprintf(workflowSweepCandidates, "$2", "$3"))
	workflowPromoteDependentsSQL = workflowPromoteSQL(fmt.Sprintf(workflowDependentsCandidates, "$2"))
	workflowCancelSweepSQL       = workflowCancelExhaustedSQL(fmt.Sprintf(workflowSweepCandidates, "$3", "$4"))
	workflowCancelDependentsSQL  = workflowCancelExhaustedSQL(fmt.Sprintf(workflowDependentsCandidates, "$3"))
)

// WorkflowPromoteReady implements driver.Executor. When a candidate is
// held by an exhausted any-of group that cancels it, it cancels the
// candidates so held, waiting only briefly for rows another transaction
// holds, and then considers the dependents of what it canceled the same
// way, until it cancels none. A wait that runs out leaves the rest to the
// leader's next pass.
func (e *Executor) WorkflowPromoteReady(ctx context.Context, params driver.WorkflowPromoteParams) (driver.WorkflowPromoteResult, error) {
	var (
		res       driver.WorkflowPromoteResult
		last      nullUUID
		exhausted int64
		err       error
	)
	cancelSQL, cancelArgs := workflowCancelDependentsSQL, []any{uuidArray(params.DependsOn)}
	switch {
	case params.DependsOn == nil:
		err = e.Conn.QueryRow(ctx, workflowPromoteSweepSQL, driver.ChannelInsert, max(params.Limit, 1), uuidParam(params.After)).
			Scan(&res.Promoted, &res.Scanned, &last, &exhausted)
		cancelSQL, cancelArgs = workflowCancelSweepSQL, []any{max(params.Limit, 1), uuidParam(params.After)}
	case len(params.DependsOn) == 0:
		return res, nil
	default:
		err = e.Conn.QueryRow(ctx, workflowPromoteDependentsSQL, driver.ChannelInsert, uuidArray(params.DependsOn)).
			Scan(&res.Promoted, &res.Scanned, &last, &exhausted)
	}
	if err != nil {
		return driver.WorkflowPromoteResult{}, fmt.Errorf("hopper: promote ready workflow jobs: %w", err)
	}
	res.Last = last.ID
	for exhausted > 0 {
		args := append([]any{driver.ChannelDone, driver.ChannelInsert}, cancelArgs...)
		finished, err := briefly(ctx, e, func(c Conn) ([]driver.JobID, error) {
			return scanIDs(c.Query(ctx, cancelSQL, args...))
		})
		if errors.Is(err, driver.ErrRowsHeld) {
			break
		}
		if err != nil {
			return res, fmt.Errorf("hopper: cancel workflow jobs of exhausted any-of groups: %w", err)
		}
		res.Canceled += int64(len(finished))
		if len(finished) == 0 {
			break
		}
		var promoted int64
		if err := e.Conn.QueryRow(ctx, workflowPromoteDependentsSQL, driver.ChannelInsert, uuidArray(finished)).
			Scan(&promoted, new(int64), new(nullUUID), &exhausted); err != nil {
			return res, fmt.Errorf("hopper: promote ready workflow jobs: %w", err)
		}
		res.Promoted += promoted
		cancelSQL, cancelArgs = workflowCancelDependentsSQL, []any{uuidArray(finished)}
	}
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
	pending, err := jobcodec.CheckDependencies(params.Deps, n)
	if err != nil {
		return nil, fmt.Errorf("hopper: insert workflow: %w", err)
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
		edgesJSON, err := jobcodec.EdgesJSON(params.Deps, ids)
		if err != nil {
			return err
		}
		steps, err := jobcodec.StepsJSON(params.Steps, ids)
		if err != nil {
			return err
		}
		jobIDs, depIDs, policies := make([]driver.JobID, len(params.Deps)), make([]driver.JobID, len(params.Deps)), make([]string, len(params.Deps))
		depSteps, groups := make([]string, len(params.Deps)), make([]int, len(params.Deps))
		// An edge that does not read, and a workflow with none that do, store
		// NULL, as the partial indexes on the edges expect. Only a claim
		// reads an edge's step name, and only of an edge that reads. A
		// workflow with an any-of group records results too, for its
		// members, and is marked to have them cleared as one that reads is.
		reads, yes := make([]*bool, len(params.Deps)), true
		var anyResults *bool
		for i, d := range params.Deps {
			jobIDs[i], depIDs[i] = ids[d.Job], ids[d.DependsOn]
			if d.Reads {
				reads[i], anyResults = &yes, &yes
				if len(params.Steps) > 0 {
					depSteps[i] = params.Steps[d.DependsOn]
				}
			}
			policies[i] = jobcodec.EdgePolicy(d)
			if d.AnyOf != 0 {
				groups[i], anyResults = d.AnyOf, &yes
			}
		}
		err = tx.QueryRow(ctx, `
			INSERT INTO {{schema}}.hopper_batches (name, pending, total, on_success, on_failure, on_complete, metadata, edges, steps, reads)
			VALUES ($1, $2, $2, $3::jsonb, $4::jsonb, $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb, $9) RETURNING id`,
			nullable(params.Name), n, onSuccess, onFailure, onComplete, jsonOrEmptyObject(params.Metadata), string(edgesJSON),
			nullable(string(steps)), anyResults).Scan(&res.ID)
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
			if _, err := tx.Exec(ctx, `INSERT INTO {{schema}}.hopper_job_deps (job_id, depends_on, on_failure, step, reads, any_of)
				SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::text[], $4::text[], $5::boolean[], $6::smallint[]) ON CONFLICT DO NOTHING`,
				uuidArray(jobIDs), uuidArray(depIDs), textArray(policies), nullableTextArray(depSteps), reads, nullableIntArray(groups)); err != nil {
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
		steps     jsonText
	)
	err := e.Conn.QueryRow(ctx, `SELECT id, name, `+batchProgressSelect+`, total, created_at, completed_at, metadata, edges, steps
		FROM {{schema}}.hopper_batches b, `+batchProgressSQL+` WHERE id = $1`, uuidParam(id)).
		Scan(&w.Batch.ID, &name, &w.Batch.Pending, &w.Batch.Failed, &w.Batch.Total, &w.Batch.CreatedAt, &completed, &metadata, &edges, &steps)
	if errors.Is(err, ErrNoRows) {
		return nil, driver.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: get workflow: %w", err)
	}
	w.Batch.Name, w.Batch.CompletedAt, w.Batch.Metadata = name.String, completed.Time, metadata.raw()
	w.Steps = jobcodec.DecodeSteps(string(steps))
	if edges != "" {
		if w.Edges, err = jobcodec.DecodeEdges(string(edges)); err != nil {
			return nil, fmt.Errorf("hopper: get workflow: edges: %w", err)
		}
	}
	if w.Jobs, err = historyJobs(e.Conn.Query(ctx, workflowJobsSQL, uuidParam(id))); err != nil {
		return nil, fmt.Errorf("hopper: get workflow: jobs: %w", err)
	}
	return &w, nil
}
