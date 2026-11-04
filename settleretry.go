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
// next attempt, which is due at due.
type heldBatch struct {
	delay time.Duration
	due   time.Time
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
	r.mu.Lock()
	for _, id := range batches {
		if _, ok := r.held[id]; ok {
			continue
		}
		if len(r.held) == maxHeld {
			break
		}
		if r.held == nil {
			r.held = make(map[driver.JobID]heldBatch, len(batches))
		}
		r.held[id] = heldBatch{delay: r.first, due: due}
	}
	r.mu.Unlock()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// run settles the batches as they come due until stop is closed, and then,
// unless abandon reports true, makes one last attempt at every batch it
// still holds, within stopSettleTimeout.
func (r *settleRetrier) run(ctx context.Context, stop <-chan struct{}, abandon func() bool) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		timer.Stop()
		if next, ok := r.nextDue(); ok {
			timer.Reset(time.Until(next))
		}
		select {
		case <-r.wake:
		case <-timer.C:
		case <-stop:
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
// leader's sweep.
func (r *settleRetrier) attempt(ctx context.Context, final bool) {
	now := time.Now()
	r.mu.Lock()
	var ids []driver.JobID
	for id, h := range r.held {
		if final || !h.due.After(now) {
			ids = append(ids, id)
		}
	}
	r.mu.Unlock()
	if len(ids) == 0 {
		return
	}
	slices.SortFunc(ids, func(a, b driver.JobID) int { return bytes.Compare(a[:], b[:]) })
	parent, cancel := context.WithoutCancel(ctx), context.CancelFunc(func() {})
	if final {
		parent, cancel = context.WithTimeout(parent, stopSettleTimeout)
	}
	still := map[driver.JobID]bool{}
	for _, id := range r.settle(ctx, parent, ids) {
		still[id] = true
	}
	cancel()
	now = time.Now()
	left := 0
	r.mu.Lock()
	for _, id := range ids {
		h := r.held[id]
		h.delay *= 2
		if !still[id] || final || h.delay > r.maxDelay {
			if still[id] {
				left++
			}
			delete(r.held, id)
			continue
		}
		h.due = now.Add(h.delay)
		r.held[id] = h
	}
	r.mu.Unlock()
	if left > 0 {
		r.logger.DebugContext(ctx, "hopper: batch rows still held; the leader will settle them", "count", left)
	}
}

// settle settles ids and returns those whose rows are still held. A set
// that gives up is split in halves, as a flush that times out is, so that
// only the batches whose rows are held are left; once parent is done, the
// rest are left as they are.
func (r *settleRetrier) settle(ctx, parent context.Context, ids []driver.JobID) []driver.JobID {
	opCtx, cancel := context.WithTimeout(parent, flushTimeout)
	_, err := r.exec.BatchSettle(opCtx, driver.BatchSettleParams{IDs: ids})
	cancel()
	switch {
	case errors.Is(err, driver.ErrRowsHeld):
		if len(ids) == 1 || parent.Err() != nil {
			return ids
		}
		half := len(ids) / 2
		return append(r.settle(ctx, parent, ids[:half]), r.settle(ctx, parent, ids[half:])...)
	case err != nil:
		r.logger.WarnContext(ctx, "hopper: settle batches; the leader will", "count", len(ids), "error", err)
	}
	return nil
}
