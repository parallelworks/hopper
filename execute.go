package hopper

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"runtime/debug"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// execute runs one claimed job and turns its outcome into a finalize result.
// It never panics and never blocks past the job's timeout.
//
// The job's context descends from the lease generation (cancelled by a hard
// stop or fencing) and can also be cancelled by JobCancel, with cause
// ErrJobCancelled, and by the timeout, with cause ErrJobTimeout.
func (c *Client[TTx]) execute(ctx context.Context, row *driver.JobRow, qcfg QueueConfig) driver.JobFinalize {
	jobCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	output := &outputHolder{codec: c.cfg.Codec}
	jobCtx = context.WithValue(jobCtx, outputKey{}, output)
	c.trackRunning(row.ID, cancel)
	defer c.untrackRunning(row.ID)

	unit, err := c.runWorker(jobCtx, row)
	f := c.outcome(ctx, jobCtx, row, qcfg, unit, err)
	if f.State.Terminal() {
		f.Output = output.get()
	}
	return f
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
// jobCtx the job's own context.
func (c *Client[TTx]) outcome(ctx, jobCtx context.Context, row *driver.JobRow, qcfg QueueConfig, unit workUnit, err error) driver.JobFinalize {
	f := driver.JobFinalize{ID: row.ID, AttemptedBy: row.AttemptedBy}
	if err == nil {
		f.State = driver.JobStateCompleted
		f.Archive = !qcfg.DeleteCompleted
		return f
	}

	var snooze *SnoozeError
	if errors.As(err, &snooze) {
		f.State = driver.JobStateScheduled
		f.Delay = snooze.Duration
		f.Snooze = true
		return f
	}

	attemptErr := &driver.AttemptError{Attempt: row.Attempt, Error: err.Error()}
	var panicked *PanicError
	if errors.As(err, &panicked) {
		attemptErr.Trace = panicked.Stack
	}
	f.Error = attemptErr
	f.Archive = true

	var cancel *CancelError
	switch {
	case errors.As(err, &cancel):
		f.State = driver.JobStateDiscarded
		c.logger.InfoContext(ctx, "hopper: job cancelled by worker", "job_id", row.ID, "kind", row.Kind, "error", err)
	case errors.Is(context.Cause(jobCtx), ErrJobCancelled):
		f.State = driver.JobStateCancelled
		c.logger.InfoContext(ctx, "hopper: job cancelled", "job_id", row.ID, "kind", row.Kind)
	case ctx.Err() != nil && row.Attempt < row.MaxAttempts:
		// The client is stopping (or was fenced) and cancelled the job. That
		// is not the job's fault: retry immediately elsewhere. On its last
		// attempt it is dead-lettered instead, as a rescued job would be.
		f.State = driver.JobStateRetryable
	case row.Attempt >= row.MaxAttempts:
		f.State = driver.JobStateDiscarded
		c.logger.ErrorContext(ctx, "hopper: job discarded", "job_id", row.ID, "kind", row.Kind, "attempt", row.Attempt, "error", err)
	default:
		f.State = driver.JobStateRetryable
		f.Delay = c.retryDelay(row, unit)
		c.logger.WarnContext(ctx, "hopper: job failed", "job_id", row.ID, "kind", row.Kind, "attempt", row.Attempt, "retry_in", f.Delay, "error", err)
	}
	return f
}

func (c *Client[TTx]) retryDelay(row *driver.JobRow, unit workUnit) time.Duration {
	if unit != nil {
		if at := unit.NextRetry(); !at.IsZero() {
			return max(time.Until(at), 0)
		}
	}
	return defaultBackoff(row.Attempt)
}

// maxBackoff caps the default retry delay.
const maxBackoff = 24 * time.Hour

// defaultBackoff returns attempt^4 + 5s with ±10% jitter, capped at 24h.
// attempt is the attempt that just failed, starting at 1.
func defaultBackoff(attempt int) time.Duration {
	secs := math.Pow(float64(attempt), 4) + 5
	secs *= 0.9 + rand.Float64()*0.2
	if secs > maxBackoff.Seconds() {
		return maxBackoff
	}
	return time.Duration(secs * float64(time.Second))
}
