package hoppersqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/internal/jobcodec"
)

// WorkflowInsert implements driver.Executor.
func (e *executor) WorkflowInsert(ctx context.Context, params driver.WorkflowInsertParams) (*driver.WorkflowInsertResult, error) {
	n := len(params.Jobs)
	if n == 0 {
		return nil, errors.New("hopper: insert workflow: no jobs")
	}
	pending, err := jobcodec.CheckDependencies(params.Deps, n)
	if err != nil {
		return nil, fmt.Errorf("hopper: insert workflow: %w", err)
	}
	jobs := make([]jobInsert, n)
	var notifyQueues []string
	for i, p := range params.Jobs {
		if p.UniqueKey != "" {
			return nil, fmt.Errorf("hopper: insert workflow: job %d has a unique key", i)
		}
		var err error
		if jobs[i], err = encodeInsert(p); err != nil {
			return nil, err
		}
		jobs[i].pending = pending[i]
		if !pending[i] && !slices.Contains(notifyQueues, p.Queue) {
			notifyQueues = append(notifyQueues, p.Queue)
		}
	}
	batch, err := encodeBatch(params.OnSuccess, params.OnFailure, params.OnComplete, params.Metadata)
	if err != nil {
		return nil, err
	}
	res := &driver.WorkflowInsertResult{}
	err = e.write(ctx, func(x *w) error {
		for i := range jobs {
			jobs[i].id = newID(x.now)
		}
		ids := make([]driver.JobID, n)
		for i := range jobs {
			ids[i] = jobs[i].id
		}
		steps, err := jobcodec.StepsJSON(params.Steps, ids)
		if err != nil {
			return err
		}
		// The graph, for inspection, and the working set of edges.
		edgesJSON, err := jobcodec.EdgesJSON(params.Deps, ids)
		if err != nil {
			return err
		}
		res.ID = newID(x.now)
		for i := range jobs {
			jobs[i].batchID = res.ID
		}
		// An edge that does not read, and a workflow with none that do, store
		// NULL, as the partial indexes on the edges expect. A workflow with
		// an any-of group records results too, for its members, and is
		// marked to have them deleted as one that reads is.
		var reads any
		for _, d := range params.Deps {
			if d.Reads || d.AnyOf != 0 {
				reads = 1
			}
		}
		if err := x.insertBatch(ctx, res.ID, params.Name, n, batch, string(edgesJSON), nullable(string(steps)), reads); err != nil {
			return fmt.Errorf("insert batch: %w", err)
		}
		res.Jobs = make([]driver.JobInsertResult, n)
		for i, j := range jobs {
			job, err := x.insertJob(ctx, j)
			if err != nil {
				return fmt.Errorf("insert jobs: %w", err)
			}
			res.Jobs[i] = driver.JobInsertResult{Job: job}
		}
		for _, d := range params.Deps {
			// Only a claim reads an edge's step name, and only of an edge
			// that reads.
			var step, read, group any
			if d.Reads {
				read = 1
				if len(params.Steps) > 0 {
					step = nullable(params.Steps[d.DependsOn])
				}
			}
			if d.AnyOf != 0 {
				group = d.AnyOf
			}
			if _, err := x.ExecContext(ctx, `INSERT OR IGNORE INTO hopper_job_deps (job_id, depends_on, on_failure, step, reads, any_of) VALUES (?, ?, ?, ?, ?, ?)`,
				idParam(jobs[d.Job].id), idParam(jobs[d.DependsOn].id), jobcodec.EdgePolicy(d), step, read, group); err != nil {
				return fmt.Errorf("insert dependencies: %w", err)
			}
		}
		if params.Notify {
			return x.notify(ctx, driver.ChannelInsert, notifyQueues...)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("hopper: insert workflow: %w", err)
	}
	return res, nil
}

// WorkflowGet implements driver.Executor.
func (e *executor) WorkflowGet(ctx context.Context, id driver.JobID) (*driver.WorkflowRow, error) {
	var w driver.WorkflowRow
	err := e.readTx(ctx, func(q querier) error {
		var (
			name  sql.NullString
			edges sql.NullString
			steps sql.NullString
		)
		b, err := scanBatch(q.QueryRowContext(ctx, `SELECT `+batchColumns+`, name, edges, steps FROM hopper_batches WHERE id = ?`, idParam(id)), &name, &edges, &steps)
		if errors.Is(err, sql.ErrNoRows) {
			return driver.ErrNotFound
		}
		if err != nil {
			return err
		}
		w.Batch = *b
		w.Batch.Name = name.String
		w.Steps = jobcodec.DecodeSteps(steps.String)
		if edges.Valid {
			if w.Edges, err = jobcodec.DecodeEdges(edges.String); err != nil {
				return fmt.Errorf("edges: %w", err)
			}
		}
		// Jobs in insertion order, live and finished alike.
		w.Jobs, err = queryAll(ctx, q, func(r scanner) (*driver.JobRow, error) {
			var seq int64
			return scanJob(r, true, &seq)
		}, `SELECT * FROM (
			  SELECT `+liveAsHistory+`, seq FROM hopper_jobs WHERE batch_id = ?1
			  UNION ALL
			  SELECT `+historyColumns+`, seq FROM hopper_job_history WHERE batch_id = ?1
			) ORDER BY seq`, idParam(id))
		return err
	})
	if err != nil {
		if errors.Is(err, driver.ErrNotFound) {
			return nil, err
		}
		return nil, fmt.Errorf("hopper: get workflow: %w", err)
	}
	return &w, nil
}

// readySQL selects, of the IDs it is given, the pending jobs none of whose
// dependencies is still live and none of whose any-of groups holds them.
var readySQL = `SELECT j.id FROM hopper_jobs j
WHERE j.id IN (%s) AND j.state = 'pending'
  AND NOT EXISTS (SELECT 1 FROM hopper_job_deps d JOIN hopper_jobs x ON x.id = d.depends_on WHERE d.job_id = j.id AND d.any_of IS NULL)
  AND ` + anyOfReadySQL

// exhaustedSQL selects the pending jobs among those in %s that an exhausted
// any-of group whose policy is any_cancel holds.
var exhaustedSQL = `SELECT j.id FROM hopper_jobs j WHERE j.id IN (%s) AND j.state = 'pending' AND ` + anyOfCancelSQL

// WorkflowPromoteReady implements driver.Executor. The transaction that
// finalizes a job promotes or cancels its dependents itself, so a pass
// finds only what a direct write left pending: it looks without the write
// lock, and takes the lock only to promote or cancel what it found.
func (e *executor) WorkflowPromoteReady(ctx context.Context, params driver.WorkflowPromoteParams) (driver.WorkflowPromoteResult, error) {
	if params.DependsOn != nil && len(params.DependsOn) == 0 {
		return driver.WorkflowPromoteResult{}, nil
	}
	var (
		res        driver.WorkflowPromoteResult
		candidates []driver.JobID
		err        error
	)
	if params.DependsOn == nil {
		candidates, err = queryIDs(ctx, e.read(), `SELECT DISTINCT job_id FROM hopper_job_deps WHERE job_id > ? ORDER BY job_id LIMIT ?`,
			idParam(params.After), max(params.Limit, 1))
	} else {
		ids := make([]any, len(params.DependsOn))
		for i, id := range params.DependsOn {
			ids[i] = idParam(id)
		}
		candidates, err = queryIDs(ctx, e.read(), `SELECT DISTINCT job_id FROM hopper_job_deps WHERE depends_on IN (`+placeholders(len(ids))+`)`, ids...)
	}
	if err != nil {
		return res, fmt.Errorf("hopper: promote ready workflow jobs: %w", err)
	}
	sortIDs(candidates)
	res.Scanned = int64(len(candidates))
	if len(candidates) == 0 {
		return res, nil
	}
	res.Last = candidates[len(candidates)-1]
	ids := make([]any, len(candidates))
	for i, id := range candidates {
		ids[i] = idParam(id)
	}
	ready, err := queryIDs(ctx, e.read(), fmt.Sprintf(readySQL, placeholders(len(ids))), ids...)
	if err != nil {
		return res, err
	}
	exhausted, err := queryIDs(ctx, e.read(), fmt.Sprintf(exhaustedSQL, placeholders(len(ids))), ids...)
	if err != nil || len(ready)+len(exhausted) == 0 {
		return res, err
	}
	err = e.write(ctx, func(x *w) error {
		res.Promoted, res.Canceled = 0, 0
		for _, id := range ready {
			promoted, err := x.promoteIfReady(ctx, id)
			if err != nil {
				return err
			}
			if promoted {
				res.Promoted++
			}
		}
		for _, id := range exhausted {
			canceled, err := x.cancelExhausted(ctx, id)
			if err != nil {
				return err
			}
			res.Canceled += int64(canceled)
		}
		return nil
	})
	if err != nil {
		return driver.WorkflowPromoteResult{}, fmt.Errorf("hopper: promote ready workflow jobs: %w", err)
	}
	return res, nil
}

// cancelExhausted cancels a pending job that an exhausted any-of group whose
// policy is any_cancel holds, as the finalize of the group's last job does,
// and returns how many jobs it finished, its dependents canceled in turn
// among them: none if the job is no longer so held.
func (x *w) cancelExhausted(ctx context.Context, id driver.JobID) (int, error) {
	held, err := x.ids(ctx, fmt.Sprintf(exhaustedSQL, "?"), idParam(id))
	if err != nil || len(held) == 0 {
		return 0, err
	}
	row, err := x.liveRow(ctx, id)
	if err != nil || row == nil {
		return 0, err
	}
	errs, err := hopperError(row, "hopper: dependency failed", x.now)
	if err != nil {
		return 0, err
	}
	f := finished{row: row, state: driver.JobStateCanceled, errors: errs, logs: row.logs, archive: true}
	if err := x.finish(ctx, f); err != nil {
		return 0, err
	}
	done, err := x.tail(ctx, []finished{f})
	return len(done), err
}
