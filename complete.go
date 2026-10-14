package hopper

import (
	"context"
	"errors"
	"fmt"

	"github.com/parallelworks/hopper/driver"
)

// ErrJobFenced is returned by CompleteTx, RecordStep and RecordCursor when
// the attempt no longer owns its job: the job was rescued after this client
// lost its lease, or it was already finalized.
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
// worker should roll tx back. Once the worker returns, the client reads the
// job to learn whether tx committed. If it did, the job is finished,
// whatever the worker returned, and the job.completed event fires. If it did
// not, the attempt fails with the worker's error, or with a failure if the
// worker returned nil, and the job is retried. A worker that returns before
// tx ends holds up only its own job's result, until tx ends.
//
// Call it last, just before the commit. From CompleteTx until tx ends, tx
// holds the job's row and those of workflow dependents it promotes or
// cancels, and holds off the creation and drop of history partitions. The
// job's siblings in its batch finalize without waiting for it.
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
	id := a.completion.ID
	if job == nil || job.JobRow == nil || job.ID != id {
		return fmt.Errorf("hopper: complete job: ctx belongs to job %s, not this one", id)
	}
	c, ok := ClientFromContext[TTx](ctx)
	if !ok {
		return fmt.Errorf("hopper: complete job %s: the transaction is not of the type its client uses", id)
	}
	// The statement can wait on row locks for as long as another
	// transaction holds them, so a.mu is not held across it: SetOutput
	// from another goroutine must not block on the database.
	result := a.completion
	result.Output = a.get()
	applied, err := c.driver.UnwrapTx(tx).JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{result}, NoBatches: job.BatchID.IsZero()})
	if err != nil {
		return err
	}
	if len(applied) == 0 {
		return ErrJobFenced
	}
	a.mu.Lock()
	a.completed = &result
	a.mu.Unlock()
	return nil
}
