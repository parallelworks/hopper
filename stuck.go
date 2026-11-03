package hopper

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// stuckWatch watches one attempt's worker for not returning once its
// context is canceled: StuckGrace after the cancellation, its timer reports
// the worker, unless it has returned by then.
type stuckWatch struct {
	ctx context.Context
	// job is the attempt's copy of the row as claimed (attempt.claimed),
	// which the worker cannot change, as it does its own row's Progress
	// with RecordStep.
	job *JobRow
	// done is set once the worker has returned or was reported.
	done      atomic.Bool
	timer     atomic.Pointer[time.Timer]
	stopAfter func() bool
}

// watchStuck starts watching the worker about to run under ctx, whose row
// as claimed is claimed; call stuckReturned once it has returned.
func (c *Client[TTx]) watchStuck(ctx context.Context, claimed *driver.JobRow) *stuckWatch {
	w := &stuckWatch{ctx: ctx, job: claimed}
	w.stopAfter = context.AfterFunc(ctx, func() {
		t := time.AfterFunc(c.cfg.StuckGrace, func() { c.reportStuck(w) })
		w.timer.Store(t)
		// The worker may have returned as the timer was set: either this
		// sees it, or stuckReturned sees the timer.
		if w.done.Load() {
			t.Stop()
		}
	})
	return w
}

// stuckReturned ends a watch whose worker has returned.
func (c *Client[TTx]) stuckReturned(w *stuckWatch) {
	w.done.Store(true)
	if w.stopAfter() {
		return
	}
	if t := w.timer.Load(); t != nil {
		t.Stop()
	}
}

// reportStuck logs a worker that has not returned since its context was
// canceled, emits EventJobStuck and starts Config.OnStuck on a goroutine of
// its own, unless the worker has returned or was reported already. The
// event and the hook each get a copy of the row, which they may change
// without racing each other or the attempt's log.
func (c *Client[TTx]) reportStuck(w *stuckWatch) {
	if !w.done.CompareAndSwap(false, true) {
		return
	}
	cause := stuckCause(w.ctx)
	c.logger.WarnContext(w.ctx, "hopper: job has not returned since its context was canceled",
		"job_id", w.job.ID, "kind", w.job.Kind, "attempt", w.job.Attempt, "cause", cause)
	evJob := *w.job
	c.emit(EventJobStuck, &evJob, cause.Error())
	if c.cfg.OnStuck == nil {
		return
	}
	// The hook's goroutine holds only its copy of the row and the job's ID
	// and kind, not the watch, whose context carries the attempt.
	job, id, kind := *w.job, w.job.ID, w.job.Kind
	go func() {
		defer func() {
			if r := recover(); r != nil {
				c.logger.Error("hopper: Config.OnStuck panicked", "job_id", id, "kind", kind,
					"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			}
		}()
		c.cfg.OnStuck(context.WithValue(context.Background(), clientKey{}, c), &job, cause)
	}()
}

// stuckCause is why a worker's context was canceled. A fence cancels the
// lease generation with no cause of its own, and is reported as a later
// claim's replacement of the attempt is, as ErrJobFenced.
func stuckCause(ctx context.Context) error {
	cause := context.Cause(ctx)
	if errors.Is(cause, context.Canceled) {
		return ErrJobFenced
	}
	return cause
}
