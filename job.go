package hopper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// JobArgs is the payload of a job. Kind names the job type and maps to
// exactly one worker. Args must be a value type (not a pointer), encodable by
// the client's Codec, and Kind must not depend on the value's fields.
type JobArgs interface {
	Kind() string
}

// JobArgsWithInsertOpts is implemented by args that carry per-kind insert
// defaults. Options passed to Insert override them field by field.
type JobArgsWithInsertOpts interface {
	JobArgs
	InsertOpts() InsertOpts
}

// Job is a claimed job with its decoded args.
type Job[T JobArgs] struct {
	*JobRow
	Args T
}

// Priority orders jobs within a queue. Lower runs first.
type Priority int

// The four priority levels.
const (
	PriorityHigh   Priority = 1
	PriorityNormal Priority = 2 // the default
	PriorityLow    Priority = 3
	PriorityLowest Priority = 4
)

// InsertOpts customizes one insert. A nil *InsertOpts means the defaults,
// which come from the args' InsertOpts method if it has one, then from the
// client's Config.
type InsertOpts struct {
	// Queue defaults to QueueDefault.
	Queue string
	// Priority defaults to PriorityNormal.
	Priority Priority
	// MaxAttempts defaults to Config.MaxAttempts.
	MaxAttempts int
	// ScheduledAt delays the job until the given time. Zero runs it as soon
	// as possible.
	ScheduledAt time.Time
	// Metadata is a JSON object stored with the job, for trace context and
	// similar. It is not passed to the worker as args.
	Metadata json.RawMessage
	// Unique makes the job unique among live jobs of its kind. See
	// UniqueOpts.
	Unique *UniqueOpts
	// TTL discards the job if it has not started within this long of its
	// insert. Zero means no limit.
	TTL time.Duration
	// Await announces the job's finalize, so that Await returns as soon as
	// it commits instead of on its next poll.
	Await bool
	// OrderingKey serializes the job with others of the same key in its
	// queue: at most one runs at a time, oldest first, and a failing job
	// blocks its key while it retries. See PublishOpts.OrderingKey.
	OrderingKey string
	// PartitionKey groups the job for the queue's PartitionLimit, for
	// example a customer ID.
	PartitionKey string
}

// merge returns opts with the non-zero fields of over applied on top.
func (opts InsertOpts) merge(over InsertOpts) InsertOpts {
	if over.Queue != "" {
		opts.Queue = over.Queue
	}
	if over.Priority != 0 {
		opts.Priority = over.Priority
	}
	if over.MaxAttempts != 0 {
		opts.MaxAttempts = over.MaxAttempts
	}
	if !over.ScheduledAt.IsZero() {
		opts.ScheduledAt = over.ScheduledAt
	}
	if len(over.Metadata) > 0 {
		opts.Metadata = over.Metadata
	}
	if over.Unique != nil {
		opts.Unique = over.Unique
	}
	if over.TTL != 0 {
		opts.TTL = over.TTL
	}
	if over.Await {
		opts.Await = true
	}
	if over.OrderingKey != "" {
		opts.OrderingKey = over.OrderingKey
	}
	if over.PartitionKey != "" {
		opts.PartitionKey = over.PartitionKey
	}
	return opts
}

// InsertParams is one job in an InsertMany call.
type InsertParams struct {
	Args JobArgs
	Opts *InsertOpts

	batchID JobID
}

// InsertResult is the outcome of inserting one job.
type InsertResult struct {
	Job *JobRow
	// Duplicate reports that a live job with the same unique key already
	// existed. Job is then that existing job.
	Duplicate bool
}

// SnoozeError is returned by a worker to run the job again later without
// using up an attempt. Use Snooze to create one.
type SnoozeError struct {
	Duration time.Duration
}

func (e *SnoozeError) Error() string {
	return fmt.Sprintf("hopper: job snoozed for %s", e.Duration)
}

// Snooze returns an error a worker can return to reschedule the job after d
// without using up an attempt.
func Snooze(d time.Duration) error {
	return &SnoozeError{Duration: max(d, 0)}
}

// CancelError is returned by a worker to discard the job immediately, with no
// retry. Use Cancel to create one.
type CancelError struct {
	Err error
}

func (e *CancelError) Error() string {
	if e.Err == nil {
		return "hopper: job cancelled by worker"
	}
	return "hopper: job cancelled by worker: " + e.Err.Error()
}

func (e *CancelError) Unwrap() error { return e.Err }

// Cancel returns an error a worker can return to discard the job now, with no
// retry. The job is archived as discarded with err recorded.
func Cancel(err error) error {
	return &CancelError{Err: err}
}

// UnknownKindError reports a job kind with no registered worker.
type UnknownKindError struct {
	Kind string
}

func (e *UnknownKindError) Error() string {
	return fmt.Sprintf("hopper: no worker registered for kind %q", e.Kind)
}

// PanicError records a worker panic. The job is treated as failed.
type PanicError struct {
	Value any
	Stack string
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("hopper: worker panicked: %v", e.Value)
}

// ErrJobTimeout is the cause of a job's context cancellation when the job ran
// past its timeout. Check it with context.Cause.
var ErrJobTimeout = errors.New("hopper: job timed out")

// Sentinel errors from Client lifecycle methods.
var (
	ErrClientStarted = errors.New("hopper: client already started")
	ErrClientStopped = errors.New("hopper: client stopped")
)

// jobTimeoutCause is stored via context.WithTimeoutCause so workers can
// tell a timeout from a shutdown.
func jobTimeoutCause(ctx context.Context) bool {
	return errors.Is(context.Cause(ctx), ErrJobTimeout)
}
