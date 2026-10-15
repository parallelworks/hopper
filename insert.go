package hopper

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/parallelworks/hopper/driver"
)

// Insert inserts one job. It can be called before Start and on an
// insert-only client. When this client works the job's queue, its producer
// is woken directly, with no database round trip.
func (c *Client[TTx]) Insert(ctx context.Context, args JobArgs, opts *InsertOpts) (*InsertResult, error) {
	results, err := c.insertMany(ctx, c.exec, []InsertParams{{Args: args, Opts: opts}}, false)
	if err != nil {
		return nil, err
	}
	return results[0], nil
}

// InsertTx inserts one job in the caller's transaction. The job exists only
// if tx commits, and becomes visible to workers when it does.
func (c *Client[TTx]) InsertTx(ctx context.Context, tx TTx, args JobArgs, opts *InsertOpts) (*InsertResult, error) {
	results, err := c.insertMany(ctx, c.driver.UnwrapTx(tx), []InsertParams{{Args: args, Opts: opts}}, true)
	if err != nil {
		return nil, err
	}
	return results[0], nil
}

// InsertMany inserts a batch of jobs in one statement, or with COPY for
// large batches without unique keys. Results are in input order.
func (c *Client[TTx]) InsertMany(ctx context.Context, params []InsertParams) ([]*InsertResult, error) {
	return c.insertMany(ctx, c.exec, params, false)
}

// InsertManyTx is InsertMany in the caller's transaction.
func (c *Client[TTx]) InsertManyTx(ctx context.Context, tx TTx, params []InsertParams) ([]*InsertResult, error) {
	return c.insertMany(ctx, c.driver.UnwrapTx(tx), params, true)
}

// uniqueRef identifies a unique job within a batch.
type uniqueRef struct{ kind, key string }

// insertMany builds driver params, folds duplicates within the batch, runs
// one statement per conflict action (and per round of repeated keys that
// count different finished states), and wakes workers.
//
// inTx says the executor is the caller's transaction. Those inserts notify
// from inside the transaction, so the notification is delivered on commit.
// Pool inserts are already committed when they return, so they notify
// through the coalescing notifier and wake local producers directly.
func (c *Client[TTx]) insertMany(ctx context.Context, exec driver.Executor, params []InsertParams, inTx bool) ([]*InsertResult, error) {
	if len(params) == 0 {
		return []*InsertResult{}, nil
	}
	next := func(ctx context.Context, params []InsertParams) ([]*InsertResult, error) {
		return c.insertDirect(ctx, exec, params, inTx)
	}
	for i := len(c.cfg.Middleware) - 1; i >= 0; i-- {
		m, inner := c.cfg.Middleware[i], next
		next = func(ctx context.Context, params []InsertParams) ([]*InsertResult, error) {
			return m.Insert(ctx, params, inner)
		}
	}
	return next(ctx, params)
}

func (c *Client[TTx]) insertDirect(ctx context.Context, exec driver.Executor, params []InsertParams, inTx bool) ([]*InsertResult, error) {
	if len(params) == 0 {
		return []*InsertResult{}, nil
	}
	now := time.Now()
	dparams := make([]driver.JobInsertParams, len(params))
	actions := make([]driver.ConflictAction, len(params))
	unique := false
	for i, p := range params {
		dp, action, err := c.buildInsertParams(p, now)
		if err != nil {
			if len(params) > 1 {
				err = fmt.Errorf("job %d: %w", i, err)
			}
			return nil, err
		}
		dparams[i] = dp
		actions[i] = action
		unique = unique || dp.UniqueKey != ""
	}

	// Within a batch, later inputs with the same unique key resolve to the
	// one before, as they would if inserted one at a time. One that counts
	// other finished states than the one before may not resolve to the same
	// job, so it is inserted in a later round instead, which sees what the
	// earlier rounds did.
	results := make([]*InsertResult, len(params))
	var rounds []map[driver.ConflictAction][]int
	dupOf := make([]int, len(params))
	type keyState struct{ last, round int }
	seen := map[uniqueRef]keyState{}
	for i, dp := range dparams {
		dupOf[i] = -1
		round := 0
		if dp.UniqueKey != "" {
			ref := uniqueRef{dp.Kind, dp.UniqueKey}
			if s, ok := seen[ref]; ok {
				if sameStates(dparams[s.last].UniqueFinished, dp.UniqueFinished) {
					dupOf[i] = s.last
					seen[ref] = keyState{i, s.round}
					continue
				}
				round = s.round + 1
			}
			seen[ref] = keyState{i, round}
		}
		if round == len(rounds) {
			rounds = append(rounds, map[driver.ConflictAction][]int{})
		}
		rounds[round][actions[i]] = append(rounds[round][actions[i]], i)
	}

	for _, byAction := range rounds {
		for action, idx := range byAction {
			sub := make([]driver.JobInsertParams, len(idx))
			for k, i := range idx {
				sub[k] = dparams[i]
			}
			opts := driver.JobInsertOpts{Notify: inTx, OnConflict: action}
			var (
				rows []driver.JobInsertResult
				err  error
			)
			if !unique && c.caps.Copy && len(sub) >= c.tuning.copyThreshold {
				rows, err = exec.JobInsertCopy(ctx, sub, opts)
			} else {
				rows, err = exec.JobInsertMany(ctx, sub, opts)
			}
			if err != nil {
				return nil, err
			}
			for k, r := range rows {
				results[idx[k]] = &InsertResult{Job: r.Job, Duplicate: r.Duplicate}
			}
		}
	}
	for i, j := range dupOf {
		if j >= 0 {
			results[i] = &InsertResult{Job: results[j].Job, Duplicate: true}
		}
	}

	if !inTx {
		queues := map[string]struct{}{}
		for _, r := range results {
			// A finished duplicate is never replaced, so nothing changed.
			if !r.Duplicate || (actions[0] == driver.ConflictReplace && r.Job.FinalizedAt.IsZero()) {
				queues[r.Job.Queue] = struct{}{}
			}
		}
		for q := range queues {
			c.wakeQueue(q)
			c.notifier.mark(q)
		}
	}
	return results, nil
}

// sameStates reports whether a and b, each without repeats, hold the same
// states.
func sameStates(a, b []driver.JobState) bool {
	return len(a) == len(b) && !slices.ContainsFunc(a, func(s driver.JobState) bool { return !slices.Contains(b, s) })
}

func (c *Client[TTx]) buildInsertParams(p InsertParams, now time.Time) (driver.JobInsertParams, driver.ConflictAction, error) {
	if p.Args == nil {
		return driver.JobInsertParams{}, 0, errors.New("hopper: Args must not be nil")
	}
	kind := p.Args.Kind()
	if kind == "" {
		return driver.JobInsertParams{}, 0, fmt.Errorf("hopper: %T has an empty kind", p.Args)
	}
	if c.cfg.StrictKinds {
		if _, ok := c.workers.lookup(kind); !ok {
			return driver.JobInsertParams{}, 0, &UnknownKindError{Kind: kind}
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
	if strings.ContainsFunc(opts.Queue, unicode.IsControl) {
		return driver.JobInsertParams{}, 0, fmt.Errorf("hopper: queue name %q contains control characters", opts.Queue)
	}
	if opts.Priority == 0 {
		opts.Priority = PriorityNormal
	}
	if opts.Priority < PriorityHigh || opts.Priority > PriorityLowest {
		return driver.JobInsertParams{}, 0, fmt.Errorf("hopper: priority %d is out of range 1..4", opts.Priority)
	}
	if opts.MaxAttempts == 0 {
		opts.MaxAttempts = c.cfg.MaxAttempts
	}
	if opts.MaxAttempts < 1 {
		return driver.JobInsertParams{}, 0, fmt.Errorf("hopper: MaxAttempts %d must be positive", opts.MaxAttempts)
	}
	if len(opts.Metadata) > 0 && !json.Valid(opts.Metadata) {
		return driver.JobInsertParams{}, 0, errors.New("hopper: Metadata is not valid JSON")
	}
	if opts.TTL < 0 {
		return driver.JobInsertParams{}, 0, fmt.Errorf("hopper: TTL %s must not be negative", opts.TTL)
	}

	args, err := c.cfg.Codec.Marshal(p.Args)
	if err != nil {
		return driver.JobInsertParams{}, 0, fmt.Errorf("hopper: encode args for %q: %w", kind, err)
	}
	dp := driver.JobInsertParams{
		Kind:         kind,
		Queue:        opts.Queue,
		Priority:     int(opts.Priority),
		MaxAttempts:  opts.MaxAttempts,
		ScheduledAt:  opts.ScheduledAt,
		Args:         args,
		Metadata:     opts.Metadata,
		TTL:          opts.TTL,
		Await:        opts.Await,
		OrderingKey:  opts.OrderingKey,
		PartitionKey: opts.PartitionKey,
		BatchID:      p.batchID,
	}
	action := driver.ConflictSkip
	if opts.Unique != nil {
		if dp.UniqueKey, err = opts.Unique.key(p.Args, args, opts.Queue, now); err != nil {
			return driver.JobInsertParams{}, 0, err
		}
		if dp.UniqueFinished, err = opts.Unique.finished(); err != nil {
			return driver.JobInsertParams{}, 0, err
		}
		if opts.Unique.OnConflict == UniqueReplace {
			action = driver.ConflictReplace
		}
	}
	return dp, action, nil
}

// JobGet returns a job by ID, live or from history. It returns ErrNotFound
// if the job does not exist or its history has been dropped.
func (c *Client[TTx]) JobGet(ctx context.Context, id JobID) (*JobRow, error) {
	return c.exec.JobGet(ctx, id)
}

// JobLogs returns the logs a job's worker kept with Logger, of its latest
// attempts, oldest first, live or from history. It returns ErrNotFound if
// the job does not exist or its history has been dropped. With
// Config.JobLogs.Store, the store's logs follow those the job's row holds,
// from before the store or from clients without it, in attempt order, and
// a log the store has for an attempt number again after it reads as a
// snooze. If the store fails, the row's logs are returned with the error.
func (c *Client[TTx]) JobLogs(ctx context.Context, id JobID) ([]AttemptLog, error) {
	logs, err := c.exec.JobLogs(ctx, id)
	s := c.cfg.JobLogs.Store
	if s == nil || (err != nil && !errors.Is(err, ErrNotFound)) {
		return logs, err
	}
	stored, serr := s.Logs(ctx, id)
	if serr != nil {
		return logs, fmt.Errorf("hopper: read the logs of job %s from the store: %w", id, serr)
	}
	if err != nil && len(stored) == 0 {
		return nil, err
	}
	for i := range stored {
		stored[i].Snoozed = stored[i].Snoozed || (i+1 < len(stored) && stored[i+1].Attempt == stored[i].Attempt)
	}
	logs = append(logs, stored...)
	slices.SortStableFunc(logs, func(a, b AttemptLog) int { return cmp.Compare(a.Attempt, b.Attempt) })
	return logs, nil
}
