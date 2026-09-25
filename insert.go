package hopper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/parallelworks/hopper/driver"
)

// Insert inserts one job. It can be called before Start and on an
// insert-only client. When this client works the job's queue, its producer
// is woken directly, with no database round trip.
func (c *Client[TTx]) Insert(ctx context.Context, args JobArgs, opts *InsertOpts) (*InsertResult, error) {
	results, err := c.insertMany(ctx, c.exec, []InsertParams{{Args: args, Opts: opts}}, true)
	if err != nil {
		return nil, err
	}
	return results[0], nil
}

// InsertTx inserts one job in the caller's transaction. The job exists only
// if tx commits, and becomes visible to workers when it does.
func (c *Client[TTx]) InsertTx(ctx context.Context, tx TTx, args JobArgs, opts *InsertOpts) (*InsertResult, error) {
	results, err := c.insertMany(ctx, c.driver.UnwrapTx(tx), []InsertParams{{Args: args, Opts: opts}}, false)
	if err != nil {
		return nil, err
	}
	return results[0], nil
}

// InsertMany inserts a batch of jobs in one statement, or with COPY for
// large batches without unique keys. Results are in input order.
func (c *Client[TTx]) InsertMany(ctx context.Context, params []InsertParams) ([]*InsertResult, error) {
	return c.insertMany(ctx, c.exec, params, true)
}

// InsertManyTx is InsertMany in the caller's transaction.
func (c *Client[TTx]) InsertManyTx(ctx context.Context, tx TTx, params []InsertParams) ([]*InsertResult, error) {
	return c.insertMany(ctx, c.driver.UnwrapTx(tx), params, false)
}

func (c *Client[TTx]) insertMany(ctx context.Context, exec driver.Executor, params []InsertParams, wake bool) ([]*InsertResult, error) {
	if len(params) == 0 {
		return []*InsertResult{}, nil
	}
	dparams := make([]driver.JobInsertParams, len(params))
	unique := false
	for i, p := range params {
		dp, err := c.buildInsertParams(p)
		if err != nil {
			if len(params) > 1 {
				err = fmt.Errorf("job %d: %w", i, err)
			}
			return nil, err
		}
		dparams[i] = dp
		unique = unique || dp.UniqueKey != ""
	}

	var (
		rows []driver.JobInsertResult
		err  error
	)
	if !unique && c.caps.Copy && len(dparams) >= c.tuning.copyThreshold {
		rows, err = exec.JobInsertCopy(ctx, dparams)
	} else {
		rows, err = exec.JobInsertMany(ctx, dparams)
	}
	if err != nil {
		return nil, err
	}

	results := make([]*InsertResult, len(rows))
	for i, r := range rows {
		results[i] = &InsertResult{Job: r.Job, Duplicate: r.Duplicate}
	}
	if wake {
		for _, r := range rows {
			if !r.Duplicate {
				c.wakeQueue(r.Job.Queue)
			}
		}
	}
	return results, nil
}

func (c *Client[TTx]) buildInsertParams(p InsertParams) (driver.JobInsertParams, error) {
	if p.Args == nil {
		return driver.JobInsertParams{}, errors.New("hopper: Args must not be nil")
	}
	kind := p.Args.Kind()
	if kind == "" {
		return driver.JobInsertParams{}, fmt.Errorf("hopper: %T has an empty kind", p.Args)
	}
	if c.cfg.StrictKinds {
		if _, ok := c.workers.lookup(kind); !ok {
			return driver.JobInsertParams{}, &UnknownKindError{Kind: kind}
		}
	}

	var opts InsertOpts
	if a, ok := p.Args.(JobArgsWithInsertOpts); ok {
		opts = a.InsertOpts()
	}
	if p.Opts != nil {
		opts = opts.merge(*p.Opts)
	}
	if opts.Queue == "" {
		opts.Queue = QueueDefault
	}
	if opts.Priority == 0 {
		opts.Priority = PriorityNormal
	}
	if opts.Priority < PriorityHigh || opts.Priority > PriorityLowest {
		return driver.JobInsertParams{}, fmt.Errorf("hopper: priority %d is out of range 1..4", opts.Priority)
	}
	if opts.MaxAttempts == 0 {
		opts.MaxAttempts = c.cfg.MaxAttempts
	}
	if opts.MaxAttempts < 1 {
		return driver.JobInsertParams{}, fmt.Errorf("hopper: MaxAttempts %d must be positive", opts.MaxAttempts)
	}
	if len(opts.Metadata) > 0 && !json.Valid(opts.Metadata) {
		return driver.JobInsertParams{}, errors.New("hopper: Metadata is not valid JSON")
	}

	args, err := c.cfg.Codec.Marshal(p.Args)
	if err != nil {
		return driver.JobInsertParams{}, fmt.Errorf("hopper: encode args for %q: %w", kind, err)
	}
	return driver.JobInsertParams{
		Kind:        kind,
		Queue:       opts.Queue,
		Priority:    int(opts.Priority),
		MaxAttempts: opts.MaxAttempts,
		ScheduledAt: opts.ScheduledAt,
		Args:        args,
		Metadata:    opts.Metadata,
	}, nil
}

// JobGet returns a job by ID, live or from history. It returns ErrNotFound
// if the job does not exist or its history has been dropped.
func (c *Client[TTx]) JobGet(ctx context.Context, id JobID) (*JobRow, error) {
	return c.exec.JobGet(ctx, id)
}
