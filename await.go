package hopper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type outputKey struct{}

// outputHolder collects a job's output from SetOutput.
type outputHolder struct {
	codec Codec
	mu    sync.Mutex
	data  json.RawMessage
}

func (h *outputHolder) get() json.RawMessage {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.data
}

// ErrNotInJob is returned by SetOutput outside a worker.
var ErrNotInJob = errors.New("hopper: context does not belong to a running job")

// SetOutput records the job's result, which is stored with the finalized
// job, returned by JobGet and delivered to Await. The last call wins. Output
// is kept only for archived jobs, so it is lost on queues with
// DeleteCompleted.
func SetOutput(ctx context.Context, v any) error {
	h, ok := ctx.Value(outputKey{}).(*outputHolder)
	if !ok {
		return ErrNotInJob
	}
	data, err := h.codec.Marshal(v)
	if err != nil {
		return fmt.Errorf("hopper: encode output: %w", err)
	}
	h.mu.Lock()
	h.data = data
	h.mu.Unlock()
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
// into T, or a *JobFailedError if it was cancelled or discarded. Insert the
// job with InsertOpts.Await so that the finalize is announced and Await
// returns as soon as it commits; otherwise, and on a client that is not
// started, Await polls every Config.PollInterval. A completed job without
// output yields the zero T.
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
