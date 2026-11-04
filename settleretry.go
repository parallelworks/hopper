package hopper

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// maxHeld bounds the batches the finalizer keeps to settle again; past it,
// those whose settle gave up are left to the leader's sweep.
const maxHeld = 10000

// stopSettleTimeout bounds the last attempt a stopping client makes at the
// batches it holds to settle again.
const stopSettleTimeout = 2 * time.Second

// settleRetrier settles again the batches whose follow-up settle gave up
// on a batch row another transaction held (driver.ErrRowsHeld). It runs
// beside the finalizer's loop, so that waiting for a held row never holds
// up a flush. Each batch has a delay of its own, first the flush interval
// and then doubling while its row stays held, so a batch held after
// another whose row stays held is not kept waiting on that one's delay.
// Once a batch's delay would pass maxDelay, it is left to the leader's
// sweep.
type settleRetrier struct {
	exec     finalizeExecutor
	logger   *slog.Logger
	first    time.Duration
	maxDelay time.Duration

	mu   sync.Mutex
	held map[driver.JobID]heldBatch
	// wake is signaled when a batch is added.
	wake chan struct{}
}

// heldBatch is a batch whose settle gave up: delay is the wait before its
// next attempt, which is due at due. again reports that a settle gave up on
// it again since its attempt last began: that attempt's statement may have
// missed the progress the second settle was for, and settling it does not
// let the batch go.
type heldBatch struct {
	delay time.Duration
	due   time.Time
	again bool
}

func newSettleRetrier(exec finalizeExecutor, logger *slog.Logger, first, maxDelay time.Duration) *settleRetrier {
	// A delay of zero would never double past maxDelay.
	return &settleRetrier{exec: exec, logger: logger, first: max(first, time.Millisecond), maxDelay: maxDelay, wake: make(chan struct{}, 1)}
}

// add keeps batches to settle again, each first after the flush interval.
// A batch already kept keeps its delay.
func (r *settleRetrier) add(batches []driver.JobID) {
	if len(batches) == 0 {
		return
	}
	due := time.Now().Add(r.first)
	full := 0
	r.mu.Lock()
	for _, id := range batches {
		if h, ok := r.held[id]; ok {
			h.again = true
			r.held[id] = h
			continue
		}
		if len(r.held) == maxHeld {
			full++
			continue
		}
		if r.held == nil {
			r.held = make(map[driver.JobID]heldBatch, len(batches))
		}
		r.held[id] = heldBatch{delay: r.first, due: due}
	}
	r.mu.Unlock()
	if full > 0 {
		r.logger.Debug("hopper: too many batches to settle again; the leader will settle them", "count", full)
	}
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// run settles the batches as they come due until ctx is done, and then,
// unless abandon reports true, makes one last attempt at every batch it
// still holds, within stopSettleTimeout. ctx cuts short an attempt in
// flight, whose batches the last attempt tries, so that a stop waits for
// that attempt alone.
func (r *settleRetrier) run(ctx context.Context, abandon func() bool) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		timer.Stop()
		if next, ok := r.nextDue(); ok {
			timer.Reset(time.Until(next))
		}
		select {
		case <-r.wake:
			// A batch added is not due yet: only the timer changes.
			continue
		case <-timer.C:
		case <-ctx.Done():
			if !abandon() {
				r.attempt(ctx, true)
			}
			return
		}
		r.attempt(ctx, false)
	}
}

// nextDue returns when the earliest attempt is due, if any batch is held.
func (r *settleRetrier) nextDue() (time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var next time.Time
	for _, h := range r.held {
		if next.IsZero() || h.due.Before(next) {
			next = h.due
		}
	}
	return next, !next.IsZero()
}

// attempt settles the batches whose attempt is due, or, when final, every
// one. A batch whose row is still held is tried again later, its delay
// doubled; once that would pass maxDelay, or when final, it is left to the
// leader's sweep. A batch a settle gave up on again during the attempt is
// tried again after the flush interval, even if the attempt settled it. A
// non-final attempt runs on ctx, and leaves the batches it did not settle
// before ctx was done as they are, for the final one.
func (r *settleRetrier) attempt(ctx context.Context, final bool) {
	now := time.Now()
	r.mu.Lock()
	var ids []driver.JobID
	for id, h := range r.held {
		if final || !h.due.After(now) {
			ids = append(ids, id)
			if h.again {
				h.again = false
				r.held[id] = h
			}
		}
	}
	r.mu.Unlock()
	if len(ids) == 0 {
		return
	}
	slices.SortFunc(ids, func(a, b driver.JobID) int { return bytes.Compare(a[:], b[:]) })
	parent, cancel := ctx, context.CancelFunc(func() {})
	if final {
		parent, cancel = context.WithTimeout(context.WithoutCancel(ctx), stopSettleTimeout)
	}
	still := map[driver.JobID]bool{}
	for _, id := range r.settle(ctx, parent, ids) {
		still[id] = true
	}
	stopped := !final && ctx.Err() != nil
	cancel()
	now = time.Now()
	left := 0
	r.mu.Lock()
	for _, id := range ids {
		h := r.held[id]
		switch {
		case final:
		case still[id] && stopped:
			continue
		case !still[id] && h.again:
			r.held[id] = heldBatch{delay: r.first, due: now.Add(r.first)}
			continue
		case still[id] && h.delay*2 <= r.maxDelay:
			h.delay *= 2
			h.due = now.Add(h.delay)
			r.held[id] = h
			continue
		}
		if still[id] {
			left++
		}
		delete(r.held, id)
	}
	r.mu.Unlock()
	if left > 0 {
		r.logger.DebugContext(ctx, "hopper: batch rows still held; the leader will settle them", "count", left)
	}
}

// settle settles each of ids in a statement of its own, so that waiting
// for a held row holds no other batch's row, and returns those whose rows
// are still held. Once parent is done, the rest, and one whose settle it
// cut short, are returned as they are.
func (r *settleRetrier) settle(ctx, parent context.Context, ids []driver.JobID) (still []driver.JobID) {
	for i, id := range ids {
		if parent.Err() != nil {
			return append(still, ids[i:]...)
		}
		opCtx, cancel := context.WithTimeout(parent, flushTimeout)
		_, err := r.exec.BatchSettle(opCtx, driver.BatchSettleParams{IDs: []driver.JobID{id}})
		cancel()
		switch {
		case errors.Is(err, driver.ErrRowsHeld), err != nil && parent.Err() != nil:
			still = append(still, id)
		case err != nil:
			r.logger.WarnContext(ctx, "hopper: settle batch; the leader will", "batch_id", id, "error", err)
		}
	}
	return still
}
