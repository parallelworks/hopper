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
// is canceled or discarded.
type DependencyFailure = driver.DependencyFailure

const (
	// DependencyCancel cancels the dependent step, and its dependents in
	// turn. It is the default.
	DependencyCancel = driver.DependencyCancel
	// DependencyIgnore runs the dependent step once the steps it depends on
	// have all finished, whatever their outcome.
	DependencyIgnore = driver.DependencyIgnore
)

// maxAnyOfGroups is the most any-of groups a step can have; the engines
// number them in a smallint.
const maxAnyOfGroups = 32767

// WorkflowOpts sets a workflow's completion callbacks and metadata. A
// workflow is a batch, so the callbacks are those of BatchOpts: OnSuccess
// when every step completed, OnFailure when any was canceled or
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
	// AfterAny lists groups of steps this one waits for one of: it becomes
	// available once the steps in After have finished and, in each group,
	// at least one step has completed. The other steps of a group run on,
	// and how they finish does not change this one. A step can wait for
	// several groups and for steps in After at once; no step may be in two
	// groups, or in a group and in After.
	AfterAny [][]Step
	// OnDependencyFailure says what happens when a step in After is
	// canceled or discarded, and when every step of a group in AfterAny
	// has finished without one completing: DependencyCancel, the default,
	// cancels this step, and DependencyIgnore runs it all the same.
	OnDependencyFailure DependencyFailure
	// Name names the step for its dependents, which find it among their
	// JobRow.Dependencies by it, and for WorkflowGet. Names are unique
	// within a workflow; a step may have none.
	Name string
	// Dependencies has the step claimed with the steps in After and in
	// AfterAny that have finished by its claim, in JobRow.Dependencies:
	// each one's name, final state and output. A step of a group still
	// running when this one is claimed is not among them. The engine records a step's state and output only for a
	// step with a dependent that sets it, so a workflow that does not costs
	// what one without dependencies' results did.
	Dependencies bool
	// Insert holds the job's own options.
	Insert *InsertOpts
}

// Dependency is a finished dependency of a workflow step, as the claim
// that started the step's attempt read it: its step name, its final state
// (completed, canceled or discarded) and its output. A step's
// dependencies are kept until its workflow completes, whatever the
// history's retention and DeleteCompleted, so a step whose attempt is
// retried or snoozed gets them again. A step retried from history
// (JobRetry) after it finished has no edges left, and is claimed without
// them.
type Dependency = driver.Dependency

// After returns options for a step that waits for the given steps.
func After(steps ...Step) *StepOpts {
	return &StepOpts{After: steps}
}

// AfterAny returns options for a step that waits for any one of the given
// steps to complete: a group of AfterAny, such as a step that goes on with
// whichever of several equivalent attempts at a task completes first.
func AfterAny(steps ...Step) *StepOpts {
	return &StepOpts{AfterAny: [][]Step{steps}}
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
	names := map[string]int{}
	for i, s := range wf.steps {
		params[i] = s.params
		if s.opts.Name != "" {
			if !storableName(s.opts.Name) {
				return nil, fmt.Errorf("hopper: step %d: name %q holds a NUL or invalid UTF-8, which PostgreSQL cannot store", i, s.opts.Name)
			}
			if j, dup := names[s.opts.Name]; dup {
				return nil, fmt.Errorf("hopper: steps %d and %d are both named %q", j, i, s.opts.Name)
			}
			names[s.opts.Name] = i
			if wp.Steps == nil {
				wp.Steps = make([]string, len(wf.steps))
			}
			wp.Steps[i] = s.opts.Name
		}
		for _, dep := range s.opts.After {
			if dep.wf != wf || dep.i >= i {
				return nil, fmt.Errorf("hopper: step %d depends on a step of another workflow", i)
			}
			wp.Deps = append(wp.Deps, driver.JobDependency{Job: i, DependsOn: dep.i, OnFailure: s.opts.OnDependencyFailure, Reads: s.opts.Dependencies})
		}
		if len(s.opts.AfterAny) > 0 {
			deps, err := anyOfDeps(wf, i, s.opts)
			if err != nil {
				return nil, err
			}
			wp.Deps = append(wp.Deps, deps...)
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

// anyOfDeps returns the edges of step i's AfterAny groups, numbered from
// one. A step in two groups, or in a group and in After, would be one edge
// with two meanings, and an empty group could never be satisfied.
func anyOfDeps(wf *Workflow, i int, opts StepOpts) ([]driver.JobDependency, error) {
	if len(opts.AfterAny) > maxAnyOfGroups {
		return nil, fmt.Errorf("hopper: step %d has %d any-of groups, more than %d", i, len(opts.AfterAny), maxAnyOfGroups)
	}
	where := make(map[int]int, len(opts.After)) // step -> group, 0 for After
	for _, dep := range opts.After {
		where[dep.i] = 0
	}
	var deps []driver.JobDependency
	for g, group := range opts.AfterAny {
		if len(group) == 0 {
			return nil, fmt.Errorf("hopper: step %d has an empty any-of group", i)
		}
		for _, dep := range group {
			if dep.wf != wf || dep.i >= i {
				return nil, fmt.Errorf("hopper: step %d depends on a step of another workflow", i)
			}
			if prev, seen := where[dep.i]; seen {
				if prev == g+1 {
					continue
				}
				return nil, fmt.Errorf("hopper: step %d waits for step %d in two places among After and AfterAny", i, dep.i)
			}
			where[dep.i] = g + 1
			deps = append(deps, driver.JobDependency{
				Job: i, DependsOn: dep.i, OnFailure: opts.OnDependencyFailure,
				Reads: opts.Dependencies, AnyOf: g + 1,
			})
		}
	}
	return deps, nil
}

// WorkflowGet returns a workflow's progress, its jobs (live and finished)
// and its edges.
func (c *Client[TTx]) WorkflowGet(ctx context.Context, id JobID) (*WorkflowRow, error) {
	return c.exec.WorkflowGet(ctx, id)
}
