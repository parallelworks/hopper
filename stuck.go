package hopper

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime/debug"
	"slices"
	"sync/atomic"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// stuckWatch watches one attempt's worker for not returning once its
// context is canceled. It is reported once at most: by its timer, StuckGrace
// after the cancellation, or by a stop that gives up waiting for the worker.
// The client registers it in Client.stuck only once the context is
// canceled, which few attempts are.
type stuckWatch struct {
	ctx context.Context
	// job is the row as claimed, copied before the worker can change it,
	// as RecordStep does its Progress.
	job       JobRow
	state     atomic.Int32
	stopAfter func() bool
	// timer is set, under Client.stuckMu, once the context is canceled.
	timer *time.Timer
}

const (
	watchRunning int32 = iota
	watchReturned
	watchReported
)

// watchStuck starts watching the worker about to run row under ctx; call
// stuckReturned once it has returned.
func (c *Client[TTx]) watchStuck(ctx context.Context, row *driver.JobRow) *stuckWatch {
	w := &stuckWatch{ctx: ctx, job: *row}
	w.stopAfter = context.AfterFunc(ctx, func() { c.stuckCanceled(w) })
	return w
}

// stuckCanceled registers a watch whose context was canceled and starts its
// timer, unless its worker has returned.
func (c *Client[TTx]) stuckCanceled(w *stuckWatch) {
	c.stuckMu.Lock()
	defer c.stuckMu.Unlock()
	if w.state.Load() != watchRunning {
		return
	}
	c.stuck[w] = struct{}{}
	w.timer = time.AfterFunc(c.cfg.StuckGrace, func() { c.reportStuck(w) })
}

// stuckReturned ends a watch whose worker has returned.
func (c *Client[TTx]) stuckReturned(w *stuckWatch) {
	w.state.CompareAndSwap(watchRunning, watchReturned)
	if w.stopAfter() {
		return
	}
	c.stuckMu.Lock()
	defer c.stuckMu.Unlock()
	if w.timer != nil {
		w.timer.Stop()
	}
	delete(c.stuck, w)
}

// reportStuckNow reports every canceled worker that has not returned, as a
// stop does once its grace ends.
func (c *Client[TTx]) reportStuckNow() {
	c.stuckMu.Lock()
	watches := slices.Collect(maps.Keys(c.stuck))
	c.stuckMu.Unlock()
	for _, w := range watches {
		c.reportStuck(w)
	}
}

// reportStuck logs a worker that has not returned since its context was
// canceled, emits EventJobStuck and starts Config.OnStuck on a goroutine of
// its own, unless the worker has returned or was reported already.
func (c *Client[TTx]) reportStuck(w *stuckWatch) {
	if !w.state.CompareAndSwap(watchRunning, watchReported) {
		return
	}
	cause := stuckCause(w.ctx)
	c.logger.Warn("hopper: job has not returned since its context was canceled",
		"job_id", w.job.ID, "kind", w.job.Kind, "attempt", w.job.Attempt, "cause", cause)
	c.emit(EventJobStuck, &w.job, cause.Error())
	if c.cfg.OnStuck == nil {
		return
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				c.logger.Error("hopper: Config.OnStuck panicked", "job_id", w.job.ID, "kind", w.job.Kind,
					"panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			}
		}()
		c.cfg.OnStuck(context.WithValue(context.Background(), clientKey{}, c), &w.job, cause)
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
