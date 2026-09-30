package hopper

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// finalizer buffers results and writes them in one statement per flush. It
// flushes every finalizeInterval or every finalizeBatch results, whichever
// comes first. Results wait in a buffer of finalizeBuffer, many batches
// deep, so a commit that is slow for a while is absorbed while the client
// keeps claiming; only when the buffer is full do job goroutines block on
// submit, which stops the producers. A flush that fails is retried with
// backoff while its results stay buffered, so a database that cannot take
// results fills the buffer and stops the claims. A flush that times out is
// retried in halves, so one statement the database cannot finish in time
// degrades into smaller ones instead of being sent again as it was.
type finalizer struct {
	exec     finalizeExecutor
	logger   *slog.Logger
	interval time.Duration
	maxBatch int
	// wake is called with the queue of a result that made a job claimable
	// again right away, so the local producer need not wait for its poll.
	wake func(queue string)
	// applied is called for each result that was applied, for events and
	// local waiters.
	applied func(job *driver.JobRow, result driver.JobFinalize)

	in   chan pending
	done chan struct{} // closed when run returns
	// crashed makes run exit without its final flush, to simulate a crash
	// in tests.
	crashed atomic.Bool
}

// finalizeExecutor is the part of driver.Executor the finalizer uses.
type finalizeExecutor interface {
	JobFinalizeMany(ctx context.Context, params driver.JobFinalizeParams) ([]driver.JobID, error)
}

type pending struct {
	job    *driver.JobRow
	result driver.JobFinalize
}

func newFinalizer(exec finalizeExecutor, logger *slog.Logger, t tuning, wake func(string), applied func(*driver.JobRow, driver.JobFinalize)) *finalizer {
	return &finalizer{
		exec:     exec,
		logger:   logger,
		interval: t.finalizeInterval,
		maxBatch: t.finalizeBatch,
		wake:     wake,
		applied:  applied,
		in:       make(chan pending, max(t.finalizeBuffer, 2*t.finalizeBatch)),
		done:     make(chan struct{}),
	}
}

// submit queues a result. It blocks while the buffer is full. A result
// submitted after the finalizer has stopped is dropped: the job stays
// running in the database and is rescued once this client's lease expires.
func (f *finalizer) submit(job *driver.JobRow, result driver.JobFinalize) {
	select {
	case f.in <- pending{job: job, result: result}:
	case <-f.done:
		f.logger.Warn("hopper: result dropped after finalizer stopped; job will be rescued", "job_id", result.ID)
	}
}

// run flushes until ctx is done, then flushes what is buffered with a bounded
// context and closes done.
func (f *finalizer) run(ctx context.Context) {
	defer close(f.done)
	var (
		batch []pending
		timer <-chan time.Time
	)
	for {
		select {
		case p := <-f.in:
			batch = append(batch, p)
			if len(batch) >= f.maxBatch {
				f.flush(ctx, batch)
				batch, timer = nil, nil
			} else if timer == nil {
				timer = time.After(f.interval)
			}
		case <-timer:
			f.flush(ctx, batch)
			batch, timer = nil, nil
		case <-ctx.Done():
			if f.crashed.Load() {
				return
			}
			// Drain what job goroutines have already submitted, then write
			// it with one attempt per batch.
			for {
				select {
				case p := <-f.in:
					batch = append(batch, p)
					continue
				default:
				}
				break
			}
			for len(batch) > 0 {
				n := min(len(batch), f.maxBatch)
				f.flush(ctx, batch[:n])
				batch = batch[n:]
			}
			return
		}
	}
}

// flushTimeout bounds one finalize statement.
const flushTimeout = 30 * time.Second

// flush writes one batch. While ctx is live it retries with backoff until
// the write succeeds; a statement that timed out is retried as two halves.
// Once ctx is done (the client is stopping) it makes one more attempt and
// then gives the results up; their jobs stay running in the database and
// are rescued when this client's lease expires.
//
// The statement itself runs on a context that ctx does not cancel: a result
// that reached the finalizer belongs to a job that has finished, and the
// stop signal must not cut its write short.
func (f *finalizer) flush(ctx context.Context, batch []pending) {
	if len(batch) == 0 {
		return
	}
	jobs := make([]driver.JobFinalize, len(batch))
	for i, p := range batch {
		jobs[i] = p.result
	}
	backoff := 50 * time.Millisecond
	for {
		opCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), flushTimeout)
		applied, err := f.exec.JobFinalizeMany(opCtx, driver.JobFinalizeParams{Jobs: jobs})
		cancel()
		if err == nil {
			f.afterFlush(ctx, batch, applied)
			return
		}
		if ctx.Err() != nil {
			f.logger.ErrorContext(ctx, "hopper: results dropped; jobs will be rescued", "count", len(jobs), "error", err)
			return
		}
		f.logger.ErrorContext(ctx, "hopper: finalize jobs", "count", len(jobs), "retry_in", backoff, "error", err)
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
		if errors.Is(err, context.DeadlineExceeded) && len(batch) > 1 {
			half := len(batch) / 2
			f.flush(ctx, batch[:half])
			f.flush(ctx, batch[half:])
			return
		}
	}
}

// afterFlush logs fenced results and wakes producers for immediate retries.
func (f *finalizer) afterFlush(ctx context.Context, batch []pending, applied []driver.JobID) {
	var ok map[driver.JobID]struct{}
	if len(applied) != len(batch) {
		ok = make(map[driver.JobID]struct{}, len(applied))
		for _, id := range applied {
			ok[id] = struct{}{}
		}
	}
	for _, p := range batch {
		if ok != nil {
			if _, was := ok[p.result.ID]; !was {
				f.logger.WarnContext(ctx, "hopper: result dropped: job no longer owned by this attempt", "job_id", p.result.ID)
				continue
			}
		}
		if p.result.Delay == 0 && !p.result.State.Terminal() {
			f.wake(p.job.Queue)
		}
		f.applied(p.job, p.result)
	}
}
