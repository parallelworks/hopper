package hopper

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// BatchOpts sets a batch's callbacks. Each is inserted as a job when the
// batch completes: OnSuccess if no job of the batch was cancelled or
// discarded, OnFailure otherwise, and OnComplete either way. Callback
// metadata gains "batch_id" and "batch_failed" (the number of failed jobs).
type BatchOpts struct {
	OnSuccess  JobArgs
	OnFailure  JobArgs
	OnComplete JobArgs
	// CallbackOpts applies to every callback job.
	CallbackOpts *InsertOpts
	// Metadata is stored with the batch.
	Metadata []byte
}

// Batch collects jobs to insert together with completion callbacks.
type Batch[TTx any] struct {
	c      *Client[TTx]
	opts   BatchOpts
	params []InsertParams
}

// BatchResult reports an inserted batch.
type BatchResult struct {
	ID   JobID
	Jobs []*InsertResult
}

// BatchRow is a batch's progress.
type BatchRow = driver.BatchRow

// NewBatch starts a batch. Add jobs to it, then Insert or InsertTx.
func (c *Client[TTx]) NewBatch(opts BatchOpts) *Batch[TTx] {
	return &Batch[TTx]{c: c, opts: opts}
}

// Add queues a job for the batch.
func (b *Batch[TTx]) Add(args JobArgs, opts *InsertOpts) {
	b.params = append(b.params, InsertParams{Args: args, Opts: opts})
}

// Insert inserts the batch and its jobs.
func (b *Batch[TTx]) Insert(ctx context.Context) (*BatchResult, error) {
	return b.insert(ctx, b.c.exec, false)
}

// InsertTx inserts the batch and its jobs in the caller's transaction.
func (b *Batch[TTx]) InsertTx(ctx context.Context, tx TTx) (*BatchResult, error) {
	return b.insert(ctx, b.c.driver.UnwrapTx(tx), true)
}

func (b *Batch[TTx]) insert(ctx context.Context, exec driver.Executor, inTx bool) (*BatchResult, error) {
	if len(b.params) == 0 {
		return nil, errors.New("hopper: batch has no jobs")
	}
	if b.opts.OnSuccess == nil && b.opts.OnFailure == nil && b.opts.OnComplete == nil {
		return nil, errors.New("hopper: batch has no callbacks; use InsertMany for plain jobs")
	}
	var (
		bp  driver.BatchInsertParams
		err error
	)
	bp.Total = len(b.params)
	bp.Metadata = b.opts.Metadata
	for _, cb := range []struct {
		args JobArgs
		dst  **driver.JobInsertParams
	}{{b.opts.OnSuccess, &bp.OnSuccess}, {b.opts.OnFailure, &bp.OnFailure}, {b.opts.OnComplete, &bp.OnComplete}} {
		if cb.args == nil {
			continue
		}
		p, _, err := b.c.buildInsertParams(InsertParams{Args: cb.args, Opts: b.opts.CallbackOpts}, time.Now())
		if err != nil {
			return nil, fmt.Errorf("hopper: batch callback: %w", err)
		}
		*cb.dst = &p
	}
	// A batch is complete once none of its jobs is live, so the jobs go in
	// first: a batch without a transaction around it must not be seen
	// before them.
	bp.ID = newBatchID()
	params := make([]InsertParams, len(b.params))
	for i, p := range b.params {
		p.batchID = bp.ID
		params[i] = p
	}
	results, err := b.c.insertMany(ctx, exec, params, inTx)
	if err != nil {
		return nil, err
	}
	if _, err := exec.BatchInsert(ctx, bp); err != nil {
		return nil, err
	}
	return &BatchResult{ID: bp.ID, Jobs: results}, nil
}

// newBatchID returns a version 7 UUID, as the database generates: the Unix
// time in milliseconds, then random bits.
func newBatchID() JobID {
	var id JobID
	binary.BigEndian.PutUint64(id[0:8], uint64(time.Now().UnixMilli())<<16|uint64(rand.Uint32()&0xffff)) //nolint:gosec // a positive timestamp
	binary.BigEndian.PutUint64(id[8:16], rand.Uint64())
	id[6] = 0x70 | id[6]&0x0f
	id[8] = 0x80 | id[8]&0x3f
	return id
}

// BatchGet returns a batch's progress.
func (c *Client[TTx]) BatchGet(ctx context.Context, id JobID) (*BatchRow, error) {
	return c.exec.BatchGet(ctx, id)
}
