package hopper

import (
	"context"
	"errors"
	"fmt"

	"github.com/parallelworks/hopper/driver"
)

// ErrJobFenced is returned by CompleteTx when the attempt no longer owns its
// job: the job was rescued after this client lost its lease, or it was
// already finalized.
var ErrJobFenced = errors.New("hopper: job is no longer running in this attempt")

// CompleteTx completes the running job inside tx, so that the worker's own
// writes and the job's completion commit together or not at all. ctx must be
// the context the worker was given. The job moves to history (unless its
// queue has DeleteCompleted) with the output SetOutput recorded before the
// call; awaited jobs, batches and workflow dependents are handled as on any
// completion, once tx commits.
//
// The completion is fenced like every finalize: if the attempt no longer
// owns the job, nothing is written and ErrJobFenced is returned, and the
// worker should roll tx back. When the worker then returns nil, the client
// does not finalize the job again. A worker whose transaction does not
// commit must return an error, so the job is retried; returning nil would
// leave the job running until this client's lease ends.
//
//	func (w *Worker) Work(ctx context.Context, job *hopper.Job[Args]) error {
//		tx, err := w.pool.Begin(ctx)
//		if err != nil {
//			return err
//		}
//		defer tx.Rollback(ctx)
//		if err := apply(ctx, tx, job.Args); err != nil {
//			return err
//		}
//		if err := hopper.CompleteTx(ctx, tx, job); err != nil {
//			return err
//		}
//		return tx.Commit(ctx)
//	}
func CompleteTx[TTx any, T JobArgs](ctx context.Context, tx TTx, job *Job[T]) error {
	a, ok := ctx.Value(attemptKey{}).(*attempt)
	if !ok {
		return ErrNotInJob
	}
	if job == nil || job.JobRow == nil || job.ID != a.row.ID {
		return fmt.Errorf("hopper: complete job: ctx belongs to job %s, not this one", a.row.ID)
	}
	c, ok := ClientFromContext[TTx](ctx)
	if !ok {
		return fmt.Errorf("hopper: complete job %s: the transaction is not of the type its client uses", a.row.ID)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	applied, err := c.driver.UnwrapTx(tx).JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{{
		ID:          a.row.ID,
		AttemptedBy: a.row.AttemptedBy,
		State:       driver.JobStateCompleted,
		Output:      a.output,
		Archive:     a.archive,
	}}})
	if err != nil {
		return err
	}
	if len(applied) == 0 {
		return ErrJobFenced
	}
	a.completed = true
	return nil
}
