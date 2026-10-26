package hopper

import (
	"context"
	"errors"
	"fmt"

	"github.com/parallelworks/hopper/driver"
)

// ErrJobFenced is returned by CompleteTx, RecordStep and RecordCursor when
// the attempt no longer owns its job: the job was rescued, after this client
// lost its lease or for RescueStuckAfter, and perhaps claimed again, or it
// was already finalized. It is also the cause of the job's context
// cancellation (check it with context.Cause) when the job, rescued while the
// attempt still ran, is claimed again on the same client.
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
// tx ends holds up only its own job's result, until tx ends, on an engine
// that locks rows.
//
// Call it last, just before the commit. From CompleteTx until tx ends, tx
// holds the job's row and those of workflow dependents it promotes or
// cancels, and holds off the creation and drop of history partitions. The
// job's siblings in its batch finalize without waiting for it.
//
// On an engine that runs one writer at a time
// (driver.Capabilities.SingleWriter, SQLite), tx holds the whole database
// for writes from its first statement until it ends: every other write
// waits for it, the results of other jobs and lease renewals included, and
// a write of the worker's own on the pool fails rather than wait for tx,
// provided tx was begun with the worker's ctx (or one derived from it),
// which names the job's attempt.
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
	a, _ := ctx.Value(attemptKey{}).(*attempt)
	if a == nil {
		return ErrNotInJob
	}
	id := a.completion.ID
	if job == nil || job.JobRow == nil || job.ID != id {
		return fmt.Errorf("hopper: complete job: ctx belongs to job %s, not this one", id)
	}
	exec, err := txExecutor(ctx, tx, id)
	if err != nil {
		return err
	}
	// The statement can wait on row locks for as long as another
	// transaction holds them, so a.mu is not held across it: SetOutput
	// from another goroutine must not block on the database.
	result := a.completion
	result.Output = a.get()
	if result.KeepsLog() {
		result.Log = a.keptLog()
	}
	applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{
		Jobs: []driver.JobFinalize{result}, NoBatches: job.BatchID.IsZero(), KeepLogs: a.logs.MaxAttempts,
	})
	if err != nil {
		return err
	}
	if len(applied) == 0 {
		return ErrJobFenced
	}
	a.mu.Lock()
	a.completed = &result
	a.heldByTx = true
	a.mu.Unlock()
	return nil
}
