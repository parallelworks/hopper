package hopper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// DependencyFailure says what happens to a step when a step it depends on
// is cancelled or discarded.
type DependencyFailure = driver.DependencyFailure

const (
	// DependencyCancel cancels the dependent step, and its dependents in
	// turn. It is the default.
	DependencyCancel = driver.DependencyCancel
	// DependencyIgnore runs the dependent step once the steps it depends on
	// have all finished, whatever their outcome.
	DependencyIgnore = driver.DependencyIgnore
)

// WorkflowOpts sets a workflow's completion callbacks and metadata. A
// workflow is a batch, so the callbacks are those of BatchOpts: OnSuccess
// when every step completed, OnFailure when any was cancelled or
// discarded, OnComplete either way.
type WorkflowOpts struct {
	OnSuccess  JobArgs
	OnFailure  JobArgs
	OnComplete JobArgs
	// CallbackOpts applies to every callback job.
	CallbackOpts *InsertOpts
	// Metadata is stored with the workflow.
	Metadata json.RawMessage
}

// StepOpts sets a step's dependencies and insert options.
type StepOpts struct {
	// After lists the steps this one waits for. It is inserted pending and
	// becomes available once they have all finished.
	After []Step
	// OnDependencyFailure defaults to DependencyCancel.
	OnDependencyFailure DependencyFailure
	// Insert holds the job's own options.
	Insert *InsertOpts
}

// After returns options for a step that waits for the given steps.
func After(steps ...Step) *StepOpts {
	return &StepOpts{After: steps}
}

// Workflow collects steps with dependencies between them, to insert
// together. Steps are jobs; a step with dependencies waits, pending, until
// they have finished. Because a step can only depend on steps added before
// it, a workflow is always a DAG.
type Workflow struct {
	name  string
	opts  WorkflowOpts
	steps []workflowStep
}

type workflowStep struct {
	params InsertParams
	opts   StepOpts
}

// Step identifies a step of a workflow, for other steps to depend on.
type Step struct {
	wf *Workflow
	i  int
}

// WorkflowResult reports an inserted workflow.
type WorkflowResult struct {
	ID JobID
	// Jobs are in step order.
	Jobs []*InsertResult
}

// WorkflowRow is a workflow's progress, jobs and graph.
type WorkflowRow = driver.WorkflowRow

// NewWorkflow starts a workflow. Add steps to it, then insert it with
// Client.InsertWorkflow or InsertWorkflowTx.
func NewWorkflow(name string, opts *WorkflowOpts) *Workflow {
	w := &Workflow{name: name}
	if opts != nil {
		w.opts = *opts
	}
	return w
}

// Add adds a step. opts may be nil for a step with no dependencies;
// After(steps...) is the usual way to declare them.
func (w *Workflow) Add(args JobArgs, opts *StepOpts) Step {
	s := workflowStep{params: InsertParams{Args: args}}
	if opts != nil {
		s.opts = *opts
		s.params.Opts = opts.Insert
	}
	w.steps = append(w.steps, s)
	return Step{wf: w, i: len(w.steps) - 1}
}

// InsertWorkflow inserts a workflow's steps and dependencies atomically.
func (c *Client[TTx]) InsertWorkflow(ctx context.Context, wf *Workflow) (*WorkflowResult, error) {
	return c.insertWorkflow(ctx, c.exec, wf, false)
}

// InsertWorkflowTx is InsertWorkflow in the caller's transaction.
func (c *Client[TTx]) InsertWorkflowTx(ctx context.Context, tx TTx, wf *Workflow) (*WorkflowResult, error) {
	return c.insertWorkflow(ctx, c.driver.UnwrapTx(tx), wf, true)
}

func (c *Client[TTx]) insertWorkflow(ctx context.Context, exec driver.Executor, wf *Workflow, inTx bool) (*WorkflowResult, error) {
	if wf == nil || len(wf.steps) == 0 {
		return nil, errors.New("hopper: workflow has no steps")
	}
	now := time.Now()
	wp := driver.WorkflowInsertParams{Name: wf.name, Metadata: wf.opts.Metadata, Notify: inTx}
	for _, cb := range []struct {
		args JobArgs
		dst  **driver.JobInsertParams
	}{{wf.opts.OnSuccess, &wp.OnSuccess}, {wf.opts.OnFailure, &wp.OnFailure}, {wf.opts.OnComplete, &wp.OnComplete}} {
		if cb.args == nil {
			continue
		}
		p, _, err := c.buildInsertParams(InsertParams{Args: cb.args, Opts: wf.opts.CallbackOpts}, now)
		if err != nil {
			return nil, fmt.Errorf("hopper: workflow callback: %w", err)
		}
		*cb.dst = &p
	}
	params := make([]InsertParams, len(wf.steps))
	for i, s := range wf.steps {
		params[i] = s.params
		for _, dep := range s.opts.After {
			if dep.wf != wf || dep.i >= i {
				return nil, fmt.Errorf("hopper: step %d depends on a step of another workflow", i)
			}
			wp.Deps = append(wp.Deps, driver.JobDependency{Job: i, DependsOn: dep.i, OnFailure: s.opts.OnDependencyFailure})
		}
	}

	// The steps go through the insert middleware like any other insert,
	// with the workflow insert as the innermost step.
	res := &WorkflowResult{}
	next := func(ctx context.Context, params []InsertParams) ([]*InsertResult, error) {
		if len(params) != len(wf.steps) {
			return nil, errors.New("hopper: insert middleware changed the number of workflow steps")
		}
		wp.Jobs = make([]driver.JobInsertParams, len(params))
		for i, p := range params {
			dp, _, err := c.buildInsertParams(p, now)
			if err != nil {
				return nil, fmt.Errorf("step %d: %w", i, err)
			}
			if dp.UniqueKey != "" {
				return nil, fmt.Errorf("hopper: step %d: workflow steps cannot be unique", i)
			}
			wp.Jobs[i] = dp
		}
		out, err := exec.WorkflowInsert(ctx, wp)
		if err != nil {
			return nil, err
		}
		res.ID = out.ID
		results := make([]*InsertResult, len(out.Jobs))
		for i, r := range out.Jobs {
			results[i] = &InsertResult{Job: r.Job}
		}
		return results, nil
	}
	for i := len(c.cfg.Middleware) - 1; i >= 0; i-- {
		m, inner := c.cfg.Middleware[i], next
		next = func(ctx context.Context, params []InsertParams) ([]*InsertResult, error) {
			return m.Insert(ctx, params, inner)
		}
	}
	results, err := next(ctx, params)
	if err != nil {
		return nil, err
	}
	res.Jobs = results

	if !inTx {
		queues := map[string]struct{}{}
		for _, r := range results {
			if r.Job.State != driver.JobStatePending {
				queues[r.Job.Queue] = struct{}{}
			}
		}
		for q := range queues {
			c.wakeQueue(q)
			c.notifier.mark(q)
		}
	}
	return res, nil
}

// WorkflowGet returns a workflow's progress, its jobs (live and finished)
// and its edges.
func (c *Client[TTx]) WorkflowGet(ctx context.Context, id JobID) (*WorkflowRow, error) {
	return c.exec.WorkflowGet(ctx, id)
}
