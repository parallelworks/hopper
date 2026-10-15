package hopper

import (
	"context"
	"errors"
	"log/slog"
	"sync"
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
//
// Results also carry their attempts' logs, which count against two bounds
// of their own: a flush starts once its results hold finalizeLogBytes of
// logs, and job goroutines block on submit while the buffered results hold
// bufferedLogBytes.
type finalizer struct {
	exec     finalizeExecutor
	logger   *slog.Logger
	interval time.Duration
	maxBatch int
	// keepLogs is how many attempt logs a job keeps.
	keepLogs int
	// flushLogBytes is finalizeLogBytes, and logs counts the bytes of logs
	// that submitted results hold until they are flushed, up to
	// bufferedLogBytes.
	flushLogBytes int
	logs          logBudget
	// draining is closed when run starts its final drain, which lets
	// submits waiting on the log budget through. The drain takes results
	// for at most drainTimeout and ends once no submit is in flight; a
	// submit after it is dropped. submits counts the submits that started,
	// times two, plus one once the drain has ended, and submitted those
	// that are done.
	draining     chan struct{}
	drainTimeout time.Duration
	submits      atomic.Int64
	submitted    atomic.Int64
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
	JobGet(ctx context.Context, id driver.JobID) (*driver.JobRow, error)
	BatchSettle(ctx context.Context, params driver.BatchSettleParams) (driver.BatchSettleResult, error)
	WorkflowPromoteReady(ctx context.Context, params driver.WorkflowPromoteParams) (driver.WorkflowPromoteResult, error)
}

type pending struct {
	job    *driver.JobRow
	result driver.JobFinalize
	// completedTx is the completion the worker wrote with CompleteTx, if
	// it did. committed reports that the client saw it commit: the job is
	// finished, and there is nothing to write. Otherwise result is the
	// attempt's other outcome, written without waiting on the worker's
	// transaction, and the job is read again if the write skips it.
	completedTx *driver.JobFinalize
	committed   bool
	// heldByTx reports that a transaction of the worker's wrote the job's
	// row (RecordStepTx, RecordCursorTx), which may still hold it.
	heldByTx bool
	// waits counts the flushes that found the worker's transaction still
	// open.
	waits int
}

func newFinalizer(exec finalizeExecutor, logger *slog.Logger, t tuning, keepLogs int, wake func(string), applied func(*driver.JobRow, driver.JobFinalize)) *finalizer {
	return &finalizer{
		exec:          exec,
		logger:        logger,
		interval:      t.finalizeInterval,
		maxBatch:      t.finalizeBatch,
		keepLogs:      keepLogs,
		flushLogBytes: finalizeLogBytes,
		logs:          logBudget{max: bufferedLogBytes, freed: make(chan struct{})},
		draining:      make(chan struct{}),
		drainTimeout:  drainTimeout,
		wake:          wake,
		applied:       applied,
		in:            make(chan pending, max(t.finalizeBuffer, 2*t.finalizeBatch)),
		done:          make(chan struct{}),
	}
}

// submit queues a result. It blocks while the buffer is full, or while
// the buffered results' logs fill their budget. A result
// submitted after the finalizer has stopped is dropped: the job stays
// running in the database and is rescued once this client's lease expires.
func (f *finalizer) submit(p pending) {
	for {
		n := f.submits.Load()
		if n&1 == 1 {
			f.drop(p)
			return
		}
		if f.submits.CompareAndSwap(n, n+2) {
			break
		}
	}
	defer f.submitted.Add(1)
	if n := p.logBytes(); n > 0 && !f.logs.acquire(n, f.draining, f.done) {
		f.drop(p)
		return
	}
	select {
	case f.in <- p:
	case <-f.done:
		f.drop(p)
	}
}

func (f *finalizer) drop(p pending) {
	f.logger.Warn("hopper: result dropped after finalizer stopped; job will be rescued", "job_id", p.result.ID)
}

// stopIfIdle reports whether no submit is in flight or buffered and, if
// so, stops the finalizer taking more. Once force is set it stops taking
// more either way, and reports whether the submits in flight are done.
func (f *finalizer) stopIfIdle(force bool) bool {
	for {
		n := f.submits.Load()
		idle := f.submitted.Load() == n>>1 && len(f.in) == 0
		if n&1 == 1 || (!idle && !force) {
			return idle
		}
		if f.submits.CompareAndSwap(n, n|1) {
			return idle
		}
	}
}

// run flushes until ctx is done, then flushes what is buffered with a bounded
// context and closes done.
func (f *finalizer) run(ctx context.Context) {
	defer close(f.done)
	var (
		batch    []pending
		logBytes int
		timer    <-chan time.Time
	)
	for {
		select {
		case p := <-f.in:
			batch = append(batch, p)
			logBytes += p.logBytes()
			if len(batch) >= f.maxBatch || logBytes >= f.flushLogBytes {
				f.flush(ctx, batch)
				batch, logBytes, timer = nil, 0, nil
			} else if timer == nil {
				timer = time.After(f.interval)
			}
		case <-timer:
			f.flush(ctx, batch)
			batch, logBytes, timer = nil, 0, nil
		case <-ctx.Done():
			if f.crashed.Load() {
				return
			}
			// Drain what job goroutines have submitted, and are submitting,
			// and write it with one attempt per batch. Submits waiting on
			// the log budget are let through, as those waiting on a full
			// buffer are taken by the drain. Past drainTimeout, results from
			// jobs still finishing are dropped.
			close(f.draining)
			deadline := time.Now().Add(f.drainTimeout)
			for {
				select {
				case p := <-f.in:
					batch = append(batch, p)
					continue
				default:
				}
				late := time.Now().After(deadline)
				for len(batch) > 0 {
					if late {
						f.drop(batch[0])
						batch = batch[1:]
						continue
					}
					n, size := 0, 0
					for n < len(batch) && n < f.maxBatch && size < f.flushLogBytes {
						size += batch[n].logBytes()
						n++
					}
					f.flush(ctx, batch[:n])
					batch = batch[n:]
				}
				if f.stopIfIdle(late) {
					return
				}
				select {
				case p := <-f.in:
					batch = append(batch, p)
				case <-time.After(time.Millisecond):
				}
			}
		}
	}
}

// drainTimeout bounds how long a stop's final drain takes results from jobs
// that are still finishing. What it has taken by then is written.
const drainTimeout = 5 * time.Second

// finalizeLogBytes is the size of the logs from which a batch of results is
// flushed without waiting for more, which keeps a finalize statement's
// parameters small. bufferedLogBytes bounds the logs that submitted results
// hold in memory.
const (
	finalizeLogBytes = 8 << 20
	bufferedLogBytes = 64 << 20
)

// logBytes is the size of the result's log.
func (p pending) logBytes() int {
	if p.result.Log == nil {
		return 0
	}
	return len(p.result.Log.Log)
}

// logBudget is a count of bytes that waits for room. A single acquire larger
// than max is let through when nothing else holds the budget.
type logBudget struct {
	mu    sync.Mutex
	used  int
	max   int
	freed chan struct{} // closed and replaced on each release
}

// acquire waits until n bytes fit and reports whether it took them. Once
// over is closed it takes them without waiting; once done is closed it
// gives up.
func (b *logBudget) acquire(n int, over, done <-chan struct{}) bool {
	for {
		b.mu.Lock()
		fits := b.used == 0 || b.used+n <= b.max
		if !fits {
			select {
			case <-over:
				fits = true
			default:
			}
		}
		if fits {
			b.used += n
			b.mu.Unlock()
			return true
		}
		freed := b.freed
		b.mu.Unlock()
		select {
		case <-freed:
		case <-over:
		case <-done:
			return false
		}
	}
}

func (b *logBudget) release(n int) {
	if n == 0 {
		return
	}
	b.mu.Lock()
	b.used -= n
	close(b.freed)
	b.freed = make(chan struct{})
	b.mu.Unlock()
}

// flushTimeout bounds one finalize statement.
const flushTimeout = 30 * time.Second

// heldRetry is how long a result whose worker's CompleteTx transaction is
// still open waits before the finalizer tries it again.
const heldRetry = 250 * time.Millisecond

// heldWait bounds a finalize statement that waits for held rows before
// the batch is written again skipping them (write).
const heldWait = 2 * time.Second

// flush writes one batch: the results of jobs whose rows no worker's
// transaction wrote in one statement, and those whose rows one may still
// hold (CompleteTx, RecordStepTx, RecordCursorTx, or a row found held
// before) in another that skips held rows. Completions the client saw
// commit are reported without a write.
func (f *finalizer) flush(ctx context.Context, batch []pending) {
	defer func() {
		n := 0
		for _, p := range batch {
			n += p.logBytes()
		}
		f.logs.release(n)
	}()
	var plain, tx, committed []pending
	for _, p := range batch {
		switch {
		case p.committed:
			committed = append(committed, p)
		case p.completedTx != nil || p.heldByTx || p.waits > 0:
			tx = append(tx, p)
		default:
			plain = append(plain, p)
		}
	}
	f.write(ctx, plain, false)
	f.write(ctx, tx, true)
	for _, p := range committed {
		f.applied(p.job, *p.completedTx)
	}
	f.followUp(ctx, committed)
}

// write writes results in one statement. While ctx is live it retries with
// backoff until the write succeeds; a statement that timed out is retried
// as two halves. Once ctx is done (the client is stopping) it makes one more
// attempt and then gives the results up; their jobs stay running in the
// database and are rescued when this client's lease expires.
//
// The statement itself runs on a context that ctx does not cancel: a result
// that reached the finalizer belongs to a job that has finished, and the
// stop signal must not cut its write short.
//
// A statement that does not skip held rows gets heldWait: a caller's
// transaction that ran a control call such as JobCancelTx on a job and has
// not committed holds its row, and the statement would wait for it with
// every other result of the batch. Past heldWait the batch is written again
// skipping held rows, and a held job's result is tried again later
// (notWritten), so that the transaction holds up only its own job.
func (f *finalizer) write(ctx context.Context, batch []pending, skipLocked bool) {
	if len(batch) == 0 {
		return
	}
	jobs := make([]driver.JobFinalize, len(batch))
	noBatches := true
	for i, p := range batch {
		jobs[i] = p.result
		noBatches = noBatches && p.job.BatchID.IsZero()
	}
	backoff := 50 * time.Millisecond
	for {
		timeout := flushTimeout
		if !skipLocked {
			timeout = heldWait
		}
		opCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		applied, err := f.exec.JobFinalizeMany(opCtx, driver.JobFinalizeParams{Jobs: jobs, NoBatches: noBatches, SkipLocked: skipLocked, KeepLogs: f.keepLogs})
		cancel()
		if !skipLocked && errors.Is(err, context.DeadlineExceeded) {
			skipLocked = true
			continue
		}
		if err == nil {
			finished := f.afterFlush(ctx, batch, applied)
			if !noBatches {
				f.followUp(ctx, finished)
			}
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
			f.write(ctx, batch[:half], skipLocked)
			f.write(ctx, batch[half:], skipLocked)
			return
		}
	}
}

// followUp settles the batches of jobs that have finished and promotes
// their dependents, once the statements that finished them have committed:
// of two clients that finalized a batch's or a dependent's last jobs at the
// same time, each missing the other's, the one that follows up second
// completes the batch and promotes the dependent.
func (f *finalizer) followUp(ctx context.Context, finished []pending) {
	seen := map[driver.JobID]struct{}{}
	var batches, jobs []driver.JobID
	for _, p := range finished {
		if p.job.BatchID.IsZero() {
			continue
		}
		jobs = append(jobs, p.job.ID)
		if _, dup := seen[p.job.BatchID]; !dup {
			seen[p.job.BatchID] = struct{}{}
			batches = append(batches, p.job.BatchID)
		}
	}
	if len(jobs) == 0 {
		return
	}
	opCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), flushTimeout)
	defer cancel()
	if _, err := f.exec.BatchSettle(opCtx, driver.BatchSettleParams{IDs: batches}); err != nil {
		f.logger.WarnContext(ctx, "hopper: settle batches; the leader will", "count", len(batches), "error", err)
	}
	if _, err := f.exec.WorkflowPromoteReady(opCtx, driver.WorkflowPromoteParams{DependsOn: jobs}); err != nil {
		f.logger.WarnContext(ctx, "hopper: promote dependents; the leader will", "count", len(jobs), "error", err)
	}
}

// afterFlush reports applied results, wakes producers for immediate
// retries, and returns the jobs that finished. A result the statement did
// not apply was fenced, or, for a job completed with CompleteTx, skipped
// because the worker's transaction still held its row; the job's row tells
// which.
func (f *finalizer) afterFlush(ctx context.Context, batch []pending, applied []driver.JobID) []pending {
	var ok map[driver.JobID]struct{}
	if len(applied) != len(batch) {
		ok = make(map[driver.JobID]struct{}, len(applied))
		for _, id := range applied {
			ok[id] = struct{}{}
		}
	}
	var finished []pending
	for _, p := range batch {
		if ok != nil {
			if _, was := ok[p.result.ID]; !was {
				if p, done := f.notWritten(ctx, p); done {
					finished = append(finished, p)
				}
				continue
			}
		}
		if p.result.Delay == 0 && !p.result.State.Terminal() {
			f.wake(p.job.Queue)
		}
		f.applied(p.job, p.result)
		if p.result.State.Terminal() {
			finished = append(finished, p)
		}
	}
	return finished
}

// notWritten handles a result the statement did not apply. If the job is
// still running in this attempt, a transaction holds its row: the result is
// tried again later. A job completed with CompleteTx that finished in this
// attempt had its transaction commit after all, and the completion is
// reported. Otherwise the job has moved on, and the result is dropped.
func (f *finalizer) notWritten(ctx context.Context, p pending) (pending, bool) {
	switch state := readTx(ctx, f.exec, p.job); {
	case state == txCommitted && p.completedTx != nil:
		f.applied(p.job, *p.completedTx)
		return p, true
	case state == txCommitted, state == txLost:
		f.logger.WarnContext(ctx, "hopper: result dropped: job no longer owned by this attempt", "job_id", p.result.ID)
		return p, false
	}
	if p.waits++; p.waits == 1 {
		switch {
		case p.completedTx != nil:
			f.logger.WarnContext(ctx, "hopper: the transaction a worker completed its job in is still open after the worker returned; the job's result waits for it", "job_id", p.result.ID)
		case p.heldByTx:
			f.logger.WarnContext(ctx, "hopper: a transaction a worker recorded its job's progress in is still open after the worker returned; the job's result waits for it", "job_id", p.result.ID)
		default:
			f.logger.WarnContext(ctx, "hopper: another transaction holds a finished job's row, such as one that called JobCancelTx and has not committed; the job's result waits for it", "job_id", p.result.ID)
		}
	}
	time.AfterFunc(heldRetry, func() { f.submit(p) })
	return p, false
}
