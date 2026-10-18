package hopper

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"sync"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// Sentinel errors from control operations.
var (
	// ErrJobRunning is returned by JobRetry and JobDelete for a running job.
	ErrJobRunning = driver.ErrJobRunning
	// ErrUniqueConflict is returned by JobRetry when a live job holds the
	// same unique key.
	ErrUniqueConflict = driver.ErrUniqueConflict
	// ErrJobCanceled is the cause of a job's context cancellation after
	// JobCancel. Check it with context.Cause.
	ErrJobCanceled = errors.New("hopper: job canceled")
)

// JobCancel cancels a job. A waiting job moves to history as canceled. A
// running job, on any client, has its context canceled (with cause
// ErrJobCanceled) and finalizes as canceled unless it completes anyway. A
// finalized job is returned unchanged. The job as it is afterwards is
// returned.
func (c *Client[TTx]) JobCancel(ctx context.Context, id JobID) (*JobRow, error) {
	job, err := c.exec.JobCancel(ctx, id)
	if err != nil {
		return nil, err
	}
	if job.State == JobStateRunning {
		c.cancelLocal(id)
	}
	return job, nil
}

// JobCancelTx is JobCancel inside the caller's transaction, so the cancel
// commits or rolls back with the caller's own changes. A running job's
// client is told when the transaction commits.
//
// Until tx ends it holds the job's row, so a running job that finishes
// meanwhile has its result wait for tx; its client's other results do not.
// Call it late in tx, as CompleteTx.
func (c *Client[TTx]) JobCancelTx(ctx context.Context, tx TTx, id JobID) (*JobRow, error) {
	return c.driver.UnwrapTx(tx).JobCancel(ctx, id)
}

// JobRetry makes a job run again as soon as possible: a canceled or
// discarded job is re-driven from history with one more attempt allowed if
// it had run out, and a waiting job has its schedule brought forward. An
// explicit retry overrides the wait, so the job's TTL no longer applies:
// a job past its deadline still runs. The job resumes from its progress
// (RecordStep), unless FromStart is given. It returns ErrJobRunning for a
// running job.
func (c *Client[TTx]) JobRetry(ctx context.Context, id JobID, opts ...RetryOption) (*JobRow, error) {
	job, err := c.exec.JobRetry(ctx, retryParams(id, opts))
	if err != nil {
		return nil, err
	}
	c.wakeQueue(job.Queue)
	c.notifier.mark(job.Queue)
	return job, nil
}

// JobRetryTx is JobRetry inside the caller's transaction. The job's queue
// is woken when the transaction commits.
func (c *Client[TTx]) JobRetryTx(ctx context.Context, tx TTx, id JobID, opts ...RetryOption) (*JobRow, error) {
	exec := c.driver.UnwrapTx(tx)
	job, err := exec.JobRetry(ctx, retryParams(id, opts))
	if err != nil {
		return nil, err
	}
	if err := exec.Notify(ctx, driver.ChannelInsert, []string{job.Queue}); err != nil {
		return nil, err
	}
	return job, nil
}

// RetryOption changes what JobRetry and JobRetryTx do.
type RetryOption func(*driver.JobRetryParams)

// FromStart clears the job's progress, so that it runs all its steps again
// instead of resuming after the last one it finished: to run a finished job
// over, or one whose recorded step its worker no longer has.
func FromStart() RetryOption {
	return func(p *driver.JobRetryParams) { p.FromStart = true }
}

func retryParams(id JobID, opts []RetryOption) driver.JobRetryParams {
	p := driver.JobRetryParams{ID: id}
	for _, o := range opts {
		o(&p)
	}
	return p
}

// JobDelete removes a job that is not running, from the live table or from
// history, and returns it as it was last stored. A waiting job is canceled
// as JobCancel does, but keeps no history row: its batch counts it as
// failed, its dependents react as they do to a cancel, and Await on it
// returns ErrNotFound rather than waiting forever. It returns ErrJobRunning for a
// running job (cancel it, then delete it once it has finished) and
// ErrNotFound for an unknown one.
func (c *Client[TTx]) JobDelete(ctx context.Context, id JobID) (*JobRow, error) {
	return c.exec.JobDelete(ctx, id)
}

// JobDeleteTx is JobDelete inside the caller's transaction. Until tx ends
// it holds the job's row and, for a waiting job, those of the workflow
// dependents it cancels or promotes.
func (c *Client[TTx]) JobDeleteTx(ctx context.Context, tx TTx, id JobID) (*JobRow, error) {
	return c.driver.UnwrapTx(tx).JobDelete(ctx, id)
}

// retryOrderingKeyBatch bounds how many jobs one statement of
// JobRetryOrderingKey locks and rewrites.
const retryOrderingKeyBatch = 1000

// JobRetryOrderingKey wakes an ordering key: its jobs waiting out a snooze,
// a retry delay or their schedule become runnable now, and still run one at
// a time, oldest first. Unlike JobRetry it keeps each job's TTL, and leaves
// a job already past it to expire. A job of the key that is running is not
// interrupted; if its attempt ends in a snooze or a retry, it runs again at
// once instead of after its delay. It returns how many waiting jobs it woke.
func (c *Client[TTx]) JobRetryOrderingKey(ctx context.Context, queue, orderingKey string) (int, error) {
	queue = cmp.Or(queue, QueueDefault)
	total, err := retryOrderingKey(ctx, c.exec, queue, orderingKey)
	if total > 0 {
		c.wakeQueue(queue)
		c.notifier.mark(queue)
	}
	return total, err
}

// JobRetryOrderingKeyTx is JobRetryOrderingKey inside the caller's
// transaction. The queue is woken when the transaction commits.
//
// Until tx ends it holds the rows of the jobs it woke and of the key's
// running job, whose result waits for tx; its client's other results do
// not. Call it late in tx, as CompleteTx.
func (c *Client[TTx]) JobRetryOrderingKeyTx(ctx context.Context, tx TTx, queue, orderingKey string) (int, error) {
	queue = cmp.Or(queue, QueueDefault)
	exec := c.driver.UnwrapTx(tx)
	total, err := retryOrderingKey(ctx, exec, queue, orderingKey)
	if err != nil || total == 0 {
		return total, err
	}
	return total, exec.Notify(ctx, driver.ChannelInsert, []string{queue})
}

func retryOrderingKey(ctx context.Context, exec driver.Executor, queue, orderingKey string) (int, error) {
	if orderingKey == "" {
		return 0, errors.New("hopper: ordering key is required")
	}
	total := 0
	for {
		n, err := exec.JobRetryOrderingKey(ctx, driver.JobRetryOrderingKeyParams{Queue: queue, OrderingKey: orderingKey, Limit: retryOrderingKeyBatch})
		if err != nil {
			return total, err
		}
		total += n
		if n < retryOrderingKeyBatch {
			return total, nil
		}
	}
}

// JobFilter selects jobs for Jobs. Empty fields match everything.
type JobFilter struct {
	Queue  string
	Kinds  []string
	States []JobState
	// OrderingKey lists one ordering key's jobs, its lane. Keys are scoped
	// to a queue, so it needs Queue; the listing fails without one.
	OrderingKey string
	// NewestFirst lists in descending ID order: the latest inserted jobs
	// first, though IDs from the same millisecond can sort either way.
	NewestFirst bool
	// After starts the listing after the given job, for paging: pass the
	// last ID of one page to get the next.
	After JobID
	// IDs lists only the jobs with one of the given IDs.
	IDs []JobID
	// Priorities lists only the jobs with one of the given priorities.
	Priorities []Priority
	// TagsAny lists jobs with at least one of the tags, and TagsAll jobs
	// with all of them. Both can be set, each with at most MaxTags distinct
	// tags.
	TagsAny []string
	TagsAll []string
	// Metadata, a JSON object, lists jobs whose metadata contains it: every
	// key it has, with an equal value, where an object value contains the
	// same way and an array value contains each of its elements, as
	// Postgres's jsonb containment (@>) does. {"tenant": "acme"} lists the
	// jobs with that tenant, whatever else their metadata holds. Unlike
	// tags, it has no index: it narrows what the other filters find, and
	// alone reads every job.
	Metadata json.RawMessage
}

// jobsPageSize is how many jobs Jobs fetches at a time.
const jobsPageSize = 100

// Jobs lists jobs matching the filter, live and from history, in ID order
// (which is insertion order), or newest first with NewestFirst. It pages
// transparently; stop iterating to stop fetching.
func (c *Client[TTx]) Jobs(ctx context.Context, filter JobFilter) iter.Seq2[*JobRow, error] {
	return c.jobs(ctx, c.exec, filter)
}

// JobsTx is Jobs inside the caller's transaction, so that jobs inserted with
// InsertTx can be listed before the transaction commits. A filter the
// database refuses (ErrInvalidFilter) leaves the transaction usable.
func (c *Client[TTx]) JobsTx(ctx context.Context, tx TTx, filter JobFilter) iter.Seq2[*JobRow, error] {
	return c.jobs(ctx, c.driver.UnwrapTx(tx), filter)
}

func (c *Client[TTx]) jobs(ctx context.Context, exec driver.Executor, filter JobFilter) iter.Seq2[*JobRow, error] {
	return func(yield func(*JobRow, error) bool) {
		if err := checkJobFilter(filter); err != nil {
			yield(nil, err)
			return
		}
		priorities := make([]int, len(filter.Priorities))
		for i, p := range filter.Priorities {
			priorities[i] = int(p)
		}
		after := filter.After
		for {
			page, err := exec.JobList(ctx, driver.JobListParams{
				Queue: filter.Queue, Kinds: filter.Kinds, States: filter.States, OrderingKey: filter.OrderingKey,
				NewestFirst: filter.NewestFirst, After: after, Limit: jobsPageSize,
				IDs: filter.IDs, Priorities: priorities, TagsAny: filter.TagsAny, TagsAll: filter.TagsAll, Metadata: filter.Metadata,
			})
			if err != nil {
				yield(nil, err)
				return
			}
			for _, job := range page {
				if !yield(job, nil) {
					return
				}
				after = job.ID
			}
			if len(page) < jobsPageSize {
				return
			}
		}
	}
}

// ClientRow is a client process's lease.
type ClientRow = driver.ClientRow

// Clients lists every client lease, live or expired (expired rows are
// pruned by the leader).
func (c *Client[TTx]) Clients(ctx context.Context) ([]*ClientRow, error) {
	return c.exec.ClientList(ctx)
}

// Stats is a snapshot of queue depths and cluster state.
type Stats = driver.Stats

// QueueStats is one queue's depth by state.
type QueueStats = driver.QueueStats

// Stats returns queue depths by state, the age of the oldest claimable job,
// recent throughput, the running count by client and the current leader.
func (c *Client[TTx]) Stats(ctx context.Context) (*Stats, error) {
	return c.exec.Stats(ctx)
}

// QueueRow is a queue's cluster-wide state.
type QueueRow = driver.QueueRow

// Queues returns the queue controller.
func (c *Client[TTx]) Queues() *QueueControl[TTx] {
	return &QueueControl[TTx]{c: c}
}

// QueueControl pauses, resumes, lists and adds queues.
type QueueControl[TTx any] struct {
	c *Client[TTx]
}

// Pause stops every client from claiming from the queue. Running jobs
// finish. Clients learn of it through a notification and on every lease
// renewal.
func (q *QueueControl[TTx]) Pause(ctx context.Context, name string) error {
	if err := q.c.exec.QueuePause(ctx, name); err != nil {
		return err
	}
	q.c.setPaused(name, true)
	return nil
}

// Resume undoes Pause.
func (q *QueueControl[TTx]) Resume(ctx context.Context, name string) error {
	if err := q.c.exec.QueueResume(ctx, name); err != nil {
		return err
	}
	q.c.setPaused(name, false)
	return nil
}

// List returns every queue known to the cluster.
func (q *QueueControl[TTx]) List(ctx context.Context) ([]*QueueRow, error) {
	return q.c.exec.QueueList(ctx)
}

// QueueLimits are a queue's cluster-wide limits; see QueueConfig.
type QueueLimits = driver.QueueLimits

// SetLimits replaces a queue's cluster-wide limits. Zero values remove a
// limit. Clients learn of the change through a notification and on their
// next lease renewal.
func (q *QueueControl[TTx]) SetLimits(ctx context.Context, name string, limits QueueLimits) error {
	if name == "" {
		return errors.New("hopper: queue name is required")
	}
	if limits.GlobalLimit < 0 || limits.RatePerSec < 0 || limits.RateBurst < 0 || limits.PartitionLimit < 0 || limits.Aging < 0 {
		return errors.New("hopper: limits must not be negative")
	}
	limits.Name = name
	if err := q.c.exec.QueueSetLimits(ctx, limits); err != nil {
		return err
	}
	limited, err := q.c.limitedQueues(ctx)
	if err != nil {
		return err
	}
	q.c.applyLimited(limited)
	return nil
}

// Add starts working a queue on this client at runtime, for example a
// tenant's queue. The client must be started.
func (q *QueueControl[TTx]) Add(ctx context.Context, name string, cfg QueueConfig) error {
	c := q.c
	if name == "" || cfg.MaxWorkers <= 0 {
		return fmt.Errorf("hopper: queue %q: name and a positive MaxWorkers are required", name)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != clientStarted || c.producers == nil {
		return errors.New("hopper: Queues().Add needs a started client with queues")
	}
	if _, exists := c.producers[name]; exists {
		return fmt.Errorf("hopper: queue %q is already worked by this client", name)
	}
	if err := c.exec.QueueEnsure(ctx, []string{name}); err != nil {
		return err
	}
	state, err := c.exec.QueueList(ctx)
	if err != nil {
		return err
	}
	paused, limited := queueState(state, name)
	c.startProducer(name, cfg, paused, limited)
	return nil
}

// Remove stops working a queue on this client, waiting for its running
// jobs until ctx is done.
func (q *QueueControl[TTx]) Remove(ctx context.Context, name string) error {
	c := q.c
	c.mu.Lock()
	p := c.producers[name]
	delete(c.producers, name)
	c.mu.Unlock()
	if p == nil {
		return fmt.Errorf("hopper: queue %q is not worked by this client", name)
	}
	p.cancel()
	<-p.done
	finished := make(chan struct{})
	go func() {
		p.jobs.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// EventKind names an event stream.
type EventKind string

// Event kinds.
const (
	EventJobCompleted  EventKind = "job.completed"
	EventJobFailed     EventKind = "job.failed" // an attempt failed; the job will retry
	EventJobSnoozed    EventKind = "job.snoozed"
	EventJobCanceled   EventKind = "job.canceled"
	EventJobDiscarded  EventKind = "job.discarded"
	EventJobRescued    EventKind = "job.rescued"
	EventLeaderElected EventKind = "leader.elected"
	EventLeaderLost    EventKind = "leader.lost"
	EventLeaseLost     EventKind = "lease.lost"
)

// Event is one occurrence on a client.
type Event struct {
	Kind EventKind
	At   time.Time
	// Job is set for job events: the row as claimed, before the outcome.
	Job *JobRow
	// Error is the attempt's error text for failed, discarded and
	// canceled jobs, and the snooze reason, if any, for snoozed jobs.
	Error string
	// ClientID is the client the event happened on.
	ClientID int64
}

// eventBus fans events out to subscribers without blocking the producer: a
// subscriber that falls behind misses events.
type eventBus struct {
	mu   sync.Mutex
	subs map[*eventSub]struct{}
}

type eventSub struct {
	kinds map[EventKind]struct{}
	ch    chan Event
}

func newEventBus() *eventBus { return &eventBus{subs: map[*eventSub]struct{}{}} }

func (b *eventBus) emit(ev Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for s := range b.subs {
		if len(s.kinds) > 0 {
			if _, ok := s.kinds[ev.Kind]; !ok {
				continue
			}
		}
		select {
		case s.ch <- ev:
		default:
		}
	}
}

// Events streams events of the given kinds (all kinds if none) until ctx is
// done. Events are delivered best-effort: a consumer that falls behind by
// more than a buffer's worth misses some.
func (c *Client[TTx]) Events(ctx context.Context, kinds ...EventKind) iter.Seq[Event] {
	return func(yield func(Event) bool) {
		s := &eventSub{kinds: map[EventKind]struct{}{}, ch: make(chan Event, 256)}
		for _, k := range kinds {
			s.kinds[k] = struct{}{}
		}
		c.events.mu.Lock()
		c.events.subs[s] = struct{}{}
		c.events.mu.Unlock()
		defer func() {
			c.events.mu.Lock()
			delete(c.events.subs, s)
			c.events.mu.Unlock()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-s.ch:
				if !yield(ev) {
					return
				}
			}
		}
	}
}

func (c *Client[TTx]) emit(kind EventKind, job *JobRow, errText string) {
	c.events.emit(Event{Kind: kind, At: time.Now(), Job: job, Error: errText, ClientID: c.clientID.Load()})
}

// runningJob is the claim of a job running on this client and its cancel
// function.
type runningJob struct {
	fence  int64
	cancel context.CancelCauseFunc
}

// trackRunning registers a running job's claim and cancel function. A job
// rescued while its attempt still runs here can be claimed here again: the
// later claim replaces the earlier.
func (c *Client[TTx]) trackRunning(id JobID, fence int64, cancel context.CancelCauseFunc) {
	c.runningMu.Lock()
	c.running[id] = runningJob{fence: fence, cancel: cancel}
	c.runningMu.Unlock()
}

// untrackRunning removes a job's claim, unless a later claim replaced it.
func (c *Client[TTx]) untrackRunning(id JobID, fence int64) {
	c.runningMu.Lock()
	if c.running[id].fence == fence {
		delete(c.running, id)
	}
	c.runningMu.Unlock()
}

// cancelLocal cancels a job running on this client, if it is.
func (c *Client[TTx]) cancelLocal(id JobID) {
	c.runningMu.Lock()
	cancel := c.running[id].cancel
	c.runningMu.Unlock()
	if cancel != nil {
		cancel(ErrJobCanceled)
	}
}

// setPaused applies a queue's pause state to the local producer, if any.
func (c *Client[TTx]) setPaused(queue string, paused bool) {
	c.mu.Lock()
	p := c.producers[queue]
	c.mu.Unlock()
	if p != nil {
		p.setPaused(paused)
	}
}

// applyPaused sets every local producer's pause state from the list of
// paused queues.
func (c *Client[TTx]) applyPaused(paused []string) {
	set := make(map[string]struct{}, len(paused))
	for _, q := range paused {
		set[q] = struct{}{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, p := range c.producers {
		_, isPaused := set[name]
		p.setPaused(isPaused)
	}
}
