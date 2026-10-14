package hopper

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// execute runs one claimed job and turns its outcome into a finalize result.
// It never panics and never blocks past the job's timeout, other than for
// one bounded read of a job the worker completed with CompleteTx.
//
// A job the worker completed with CompleteTx is finished once the worker's
// transaction commits, which the client cannot see, so it reads the job. If
// the job finished in this attempt, its completion is reported and nothing
// is written. Otherwise its result is the attempt's other outcome, the
// worker's error or errNotCommitted, which the finalizer writes without
// waiting on a transaction that has not ended.
//
// The job's context descends from the lease generation (canceled by a hard
// stop or fencing) and can also be canceled by JobCancel, with cause
// ErrJobCanceled, and by the timeout, with cause ErrJobTimeout.
func (c *Client[TTx]) execute(ctx context.Context, row *driver.JobRow, qcfg QueueConfig) pending {
	jobCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	a := &attempt{codec: c.cfg.Codec, completion: completedResult(row, qcfg), exec: c.exec, attempt: row.Attempt}
	jobCtx = context.WithValue(jobCtx, attemptKey{}, a)
	jobCtx = context.WithValue(jobCtx, clientKey{}, c)
	c.trackRunning(row.ID, cancel)
	defer c.untrackRunning(row.ID)

	unit, err := c.runWorker(jobCtx, row)
	completed := a.completedTx()
	state := txUnknown
	if completed != nil {
		if state = readTx(ctx, c.exec, row); state == txCommitted {
			return pending{job: row, result: *completed, completedTx: completed, committed: true}
		}
		if err == nil {
			err = errNotCommitted
		}
	}
	// A job completed with CompleteTx whose read failed may have committed:
	// its failure is neither logged nor shown to Config.OnError.
	known := completed == nil || state != txUnknown
	f := c.outcome(ctx, jobCtx, row, qcfg, unit, err, known)
	if f.State.Terminal() {
		f.Output = a.get()
	}
	if known {
		c.logOutcome(ctx, row, f, err)
	}
	return pending{job: row, result: f, completedTx: completed}
}

// errNotCommitted fails an attempt whose worker completed it with
// CompleteTx and returned nil, but whose transaction did not commit.
var errNotCommitted = errors.New("hopper: the worker returned nil, but the transaction it completed the job in did not commit")

// txState is what the database shows of a job its worker completed with
// CompleteTx.
type txState int

const (
	// txUnknown: the read failed.
	txUnknown txState = iota
	// txCommitted: the job finished as this attempt completed it, or is
	// gone, as a completed job on a DeleteCompleted queue is.
	txCommitted
	// txRunning: the job is still running in this attempt, so the
	// transaction rolled back or has not ended.
	txRunning
	// txLost: the job has moved on without this attempt, as when a rescue
	// applied once the transaction rolled back.
	txLost
)

// readTxTimeout bounds readTx.
const readTxTimeout = 5 * time.Second

// readTx reads the job a worker completed with CompleteTx. A fenced result
// cannot tell a commit from a rescue that applied once the transaction
// rolled back, since both fence it; the job's row can.
func readTx(ctx context.Context, exec interface {
	JobGet(context.Context, driver.JobID) (*driver.JobRow, error)
}, row *driver.JobRow,
) txState {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), readTxTimeout)
	defer cancel()
	job, err := exec.JobGet(rctx, row.ID)
	switch {
	case errors.Is(err, driver.ErrNotFound):
		return txCommitted
	case err != nil:
		return txUnknown
	case job.AttemptedBy != row.AttemptedBy || job.Attempt != row.Attempt:
		return txLost
	case job.State == driver.JobStateCompleted:
		return txCommitted
	case job.State == driver.JobStateRunning:
		return txRunning
	default:
		return txLost
	}
}

// completedResult is the result of an attempt that succeeded.
func completedResult(row *driver.JobRow, qcfg QueueConfig) driver.JobFinalize {
	return driver.JobFinalize{ID: row.ID, AttemptedBy: row.AttemptedBy, State: driver.JobStateCompleted, Archive: !qcfg.DeleteCompleted}
}

func (c *Client[TTx]) runWorker(ctx context.Context, row *driver.JobRow) (workUnit, error) {
	info, ok := c.workers.lookup(row.Kind)
	if !ok {
		return nil, &UnknownKindError{Kind: row.Kind}
	}
	unit, err := info.newUnit(row, c.cfg.Codec)
	if err != nil {
		return nil, fmt.Errorf("hopper: decode args for %q: %w", row.Kind, err)
	}

	timeout := unit.Timeout()
	if timeout == 0 {
		timeout = c.cfg.JobTimeout
	}
	jobCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		jobCtx, cancel = context.WithTimeoutCause(ctx, timeout, ErrJobTimeout)
		defer cancel()
	}

	// Middleware wraps the worker, outermost first; the panic guard wraps
	// everything.
	work := unit.Work
	for i := len(c.cfg.Middleware) - 1; i >= 0; i-- {
		m, inner := c.cfg.Middleware[i], work
		work = func(ctx context.Context) error { return m.Work(ctx, row, inner) }
	}
	err = safeWork(jobCtx, work)
	if err != nil && jobTimeoutCause(jobCtx) {
		err = fmt.Errorf("%w after %s: %w", ErrJobTimeout, timeout, err)
	}
	return unit, err
}

// safeWork runs the worker, converting a panic into a PanicError.
func safeWork(ctx context.Context, work func(ctx context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Value: r, Stack: string(debug.Stack())}
		}
	}()
	return work(ctx)
}

// outcome maps a worker's return value to a state transition. unit is nil
// when the job never reached a worker. ctx is the lease generation and
// jobCtx the job's own context. known is false when the attempt may have
// succeeded after all, and then Config.OnError is not called.
func (c *Client[TTx]) outcome(ctx, jobCtx context.Context, row *driver.JobRow, qcfg QueueConfig, unit workUnit, err error, known bool) driver.JobFinalize {
	if err == nil {
		return completedResult(row, qcfg)
	}

	var snooze *SnoozeError
	if errors.As(err, &snooze) {
		return snoozed(row, snooze.Duration, snooze.Reason)
	}
	f := driver.JobFinalize{ID: row.ID, AttemptedBy: row.AttemptedBy}

	attemptErr := &driver.AttemptError{Attempt: row.Attempt, Error: err.Error()}
	var panicked *PanicError
	if errors.As(err, &panicked) {
		attemptErr.Trace = panicked.Stack
	}
	f.Error = attemptErr
	f.Archive = true

	var cancel *CancelError
	byWorker := errors.As(err, &cancel)
	canceled := errors.Is(context.Cause(jobCtx), ErrJobCanceled)
	switch {
	case !byWorker && canceled:
		f.State = driver.JobStateCanceled
		return f
	case !byWorker && ctx.Err() != nil && row.Attempt < row.MaxAttempts:
		// The client is stopping (or was fenced) and canceled the job. That
		// is not the job's fault: retry immediately elsewhere. On its last
		// attempt it is dead-lettered instead, as a rescued job would be.
		f.State = driver.JobStateRetryable
		return f
	}
	// A job canceled with JobCancel stays finished even if its worker
	// returned Cancel: the hook must not reschedule it.
	if known && !canceled && ctx.Err() == nil {
		o := c.onError(ctx, row, err)
		// So does a JobCancel that arrived while the hook ran.
		if errors.Is(context.Cause(jobCtx), ErrJobCanceled) {
			f.State = driver.JobStateCanceled
			if byWorker {
				f.State = driver.JobStateDiscarded
			}
			return f
		}
		switch {
		case o.Discard:
			f.State = driver.JobStateDiscarded
			return f
		case o.Snooze > 0:
			return snoozed(row, o.Snooze, err.Error())
		case !o.RetryAt.IsZero() && row.Attempt < row.MaxAttempts:
			f.State = driver.JobStateRetryable
			f.Delay = max(time.Until(o.RetryAt), 0)
			return f
		}
	}
	if byWorker || row.Attempt >= row.MaxAttempts {
		f.State = driver.JobStateDiscarded
	} else {
		f.State = driver.JobStateRetryable
		f.Delay = c.retryDelay(row, qcfg, unit)
	}
	return f
}

// snoozed is the result of an attempt snoozed for d, which gives the attempt
// back.
func snoozed(row *driver.JobRow, d time.Duration, reason string) driver.JobFinalize {
	return driver.JobFinalize{
		ID: row.ID, AttemptedBy: row.AttemptedBy, State: driver.JobStateScheduled,
		Delay: d, Snooze: true, SnoozeReason: reason,
	}
}

// logOutcome logs a failed attempt's outcome. A retry because the client
// is stopping is not the job's fault and is not logged.
func (c *Client[TTx]) logOutcome(ctx context.Context, row *driver.JobRow, f driver.JobFinalize, err error) {
	var snooze *SnoozeError
	if f.Snooze && !errors.As(err, &snooze) {
		c.logger.InfoContext(ctx, "hopper: job failed; snoozed by Config.OnError", "job_id", row.ID, "kind", row.Kind, "attempt", row.Attempt, "snooze", f.Delay, "error", err)
		return
	}
	if f.Error == nil {
		return
	}
	var cancel *CancelError
	switch {
	case errors.As(err, &cancel) && f.State == driver.JobStateDiscarded:
		c.logger.InfoContext(ctx, "hopper: job canceled by worker", "job_id", row.ID, "kind", row.Kind, "error", err)
	case f.State == driver.JobStateCanceled:
		c.logger.InfoContext(ctx, "hopper: job canceled", "job_id", row.ID, "kind", row.Kind)
	case f.State == driver.JobStateDiscarded:
		c.logger.ErrorContext(ctx, "hopper: job discarded", "job_id", row.ID, "kind", row.Kind, "attempt", row.Attempt, "error", err)
	case ctx.Err() == nil:
		c.logger.WarnContext(ctx, "hopper: job failed", "job_id", row.ID, "kind", row.Kind, "attempt", row.Attempt, "retry_in", f.Delay, "error", err)
	}
}
