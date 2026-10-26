package hopper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/parallelworks/hopper/driver"
)

type attemptKey struct{}

// attempt is the state a running job's context carries: its output from
// SetOutput, and the completion CompleteTx wrote, if it did.
type attempt struct {
	codec Codec
	// exec is the client's pool, and attempt the job's attempt number, for
	// recording progress, fenced with completion's client and fence, and for
	// the attempt's logger. progressMu serializes the attempt's progress
	// writes, so that job.Progress ends as the last one stored.
	exec       driver.Executor
	attempt    int
	progressMu sync.Mutex
	// completion is the attempt's result if it succeeds, without output.
	completion driver.JobFinalize
	// row, baseLogger and logs make the attempt's logger, which Logger
	// builds on first use.
	row        *driver.JobRow
	baseLogger *slog.Logger
	logs       JobLogsConfig
	loggerOnce sync.Once
	logger     *slog.Logger

	mu        sync.Mutex
	output    json.RawMessage
	completed *driver.JobFinalize
	// heldByTx reports that a transaction of the worker's wrote the job's
	// row (CompleteTx, RecordStepTx, RecordCursorTx), which it may still
	// hold.
	heldByTx bool
	// kept is the attempt's log, or stored its writer in the client's
	// LogStore, once Logger has built its logger. logEnded reports that
	// closeLog ran, after which no log starts.
	kept     *logBuffer
	stored   *storeWriter
	logEnded bool
}

// String names the attempt, as the owner of its worker's writes
// (driver.WithOwner).
func (a *attempt) String() string {
	return fmt.Sprintf("job %s (%s), attempt %d", a.row.ID, a.row.Kind, a.attempt)
}

func (a *attempt) markHeldByTx() {
	a.mu.Lock()
	a.heldByTx = true
	a.mu.Unlock()
}

func (a *attempt) txHeld() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.heldByTx
}

func (a *attempt) get() json.RawMessage {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.output
}

func (a *attempt) completedTx() *driver.JobFinalize {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.completed
}

// ErrNotInJob is returned by SetOutput, CompleteTx, RecordStep and
// RecordCursor outside a worker.
var ErrNotInJob = errors.New("hopper: context does not belong to a running job")

// SetOutput records the job's result, which is stored with the finalized
// job, returned by JobGet and delivered to Await. The last call wins. Output
// is kept only for archived jobs, so it is lost on queues with
// DeleteCompleted. A job finalized with CompleteTx stores the output set
// before that call.
func SetOutput(ctx context.Context, v any) error {
	a, _ := ctx.Value(attemptKey{}).(*attempt)
	if a == nil {
		return ErrNotInJob
	}
	data, err := a.codec.Marshal(v)
	if err != nil {
		return fmt.Errorf("hopper: encode output: %w", err)
	}
	a.mu.Lock()
	a.output = data
	a.mu.Unlock()
	return nil
}

// JobFailedError is returned by Await when the job ended other than
// completed.
type JobFailedError struct {
	Job *JobRow
}

func (e *JobFailedError) Error() string {
	msg := fmt.Sprintf("hopper: job %s %s", e.Job.ID, e.Job.State)
	if n := len(e.Job.Errors); n > 0 {
		msg += ": " + e.Job.Errors[n-1].Error
	}
	return msg
}

// Await blocks until the job is finalized and returns its output decoded
// into T, or a *JobFailedError if it was canceled or discarded. Insert the
// job with InsertOpts.Await so that the finalize is announced and Await
// returns as soon as it commits; otherwise, and on a client that is not
// started, Await polls every Config.PollInterval. A completed job without
// output yields the zero T. A job that does not exist, or no longer does
// (JobDelete), yields ErrNotFound.
func Await[T any, TTx any](ctx context.Context, c *Client[TTx], id JobID) (T, error) {
	var zero T
	ch := c.awaitDone(id)
	defer c.awaitCancel(id, ch)
	for {
		job, err := c.exec.JobGet(ctx, id)
		if err != nil {
			return zero, err
		}
		if job.State.Terminal() {
			if job.State != JobStateCompleted {
				return zero, &JobFailedError{Job: job}
			}
			if len(job.Output) == 0 {
				return zero, nil
			}
			var out T
			if err := c.cfg.Codec.Unmarshal(job.Output, &out); err != nil {
				return zero, fmt.Errorf("hopper: decode output of job %s: %w", id, err)
			}
			return out, nil
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-ch:
		case <-time.After(c.cfg.PollInterval):
		}
	}
}

// awaitDone registers interest in a job's finalize.
func (c *Client[TTx]) awaitDone(id JobID) chan struct{} {
	ch := make(chan struct{}, 1)
	c.doneMu.Lock()
	c.doneWaiters[id] = append(c.doneWaiters[id], ch)
	c.doneMu.Unlock()
	return ch
}

func (c *Client[TTx]) awaitCancel(id JobID, ch chan struct{}) {
	c.doneMu.Lock()
	defer c.doneMu.Unlock()
	waiters := c.doneWaiters[id]
	for i, w := range waiters {
		if w == ch {
			waiters = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
	if len(waiters) == 0 {
		delete(c.doneWaiters, id)
	} else {
		c.doneWaiters[id] = waiters
	}
}

// signalDone wakes waiters on a finalized job.
func (c *Client[TTx]) signalDone(id JobID) {
	c.doneMu.Lock()
	waiters := c.doneWaiters[id]
	c.doneMu.Unlock()
	for _, ch := range waiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
