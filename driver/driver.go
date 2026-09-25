// Package driver defines the contract between the hopper core and a storage
// engine.
//
// The methods are queue operations, not SQL: claim N jobs, finalize N results,
// insert N rows. Each engine implements them in whatever way performs best on
// that engine. hopperpgx (Postgres via pgx) is the reference implementation.
//
// Applications do not use this package directly. They pass a Driver to
// hopper.NewClient and work with the hopper package's API.
package driver

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// ErrNotSupported is returned by an engine for an optional operation it does
// not implement, such as push notifications.
var ErrNotSupported = errors.New("hopper: not supported by this driver")

// ErrNotFound is returned when a job does not exist, live or in history.
var ErrNotFound = errors.New("hopper: not found")

// Driver binds the core to a storage engine. TTx is the engine's transaction
// type (pgx.Tx for hopperpgx), so that transactional inserts only accept a
// transaction from the right engine.
type Driver[TTx any] interface {
	// Executor returns the pool-scoped executor.
	Executor() Executor
	// UnwrapTx returns an executor that runs inside the caller's transaction.
	UnwrapTx(tx TTx) Executor
	// Capabilities reports which optional strategies the engine supports.
	Capabilities() Capabilities
	// Migrator returns the engine's schema migrator.
	Migrator() Migrator
}

// Capabilities describes optional strategies an engine supports. The core
// picks its strategy from these; an engine without a capability still works
// through the baseline path.
type Capabilities struct {
	// Copy reports whether JobInsertCopy is implemented (COPY on Postgres).
	Copy bool
}

// Executor runs queue operations, either against the pool or inside a
// caller's transaction.
type Executor interface {
	// JobInsertMany inserts jobs in one statement and returns one result per
	// input, in input order. An input whose unique key matches a live job is
	// not inserted; its result carries the existing job and Duplicate = true.
	// Inputs are already deduplicated by unique key within the batch.
	JobInsertMany(ctx context.Context, params []JobInsertParams) ([]JobInsertResult, error)
	// JobInsertCopy bulk-loads jobs without conflict handling. Callers use it
	// only for batches with no unique keys, and only if Capabilities().Copy.
	JobInsertCopy(ctx context.Context, params []JobInsertParams) ([]JobInsertResult, error)
	// JobClaim atomically moves up to Limit claimable jobs in a queue to
	// running, owned by ClientID, and returns them in claim order.
	JobClaim(ctx context.Context, params JobClaimParams) ([]*JobRow, error)
	// JobFinalizeMany applies a batch of results in one statement. A result is
	// applied only if its job is still running and owned by the client that
	// produced it; the IDs that were applied are returned, and the caller
	// treats the rest as fenced.
	JobFinalizeMany(ctx context.Context, params JobFinalizeParams) ([]JobID, error)
	// JobGet returns a job by ID, live or from history.
	JobGet(ctx context.Context, id JobID) (*JobRow, error)

	// ClientRegister creates a lease row for a client process and returns its ID.
	ClientRegister(ctx context.Context, params ClientRegisterParams) (int64, error)
	// ClientRenew extends an unexpired lease. It returns false if the lease
	// had already expired, in which case the client has been fenced.
	ClientRenew(ctx context.Context, params ClientRenewParams) (bool, error)
	// ClientDelete removes a client's lease row.
	ClientDelete(ctx context.Context, clientID int64) error
}

// Migrator applies schema migrations. Migrations are held by the
// hoppermigrate package; the engine provides the lock and the executor.
type Migrator interface {
	// Lock takes the engine's cross-process migration lock on a dedicated
	// connection (never a pooled one, so that processes waiting for the lock
	// cannot starve the migration of connections) and returns the executor to
	// migrate with. The caller must Close it to release the lock.
	Lock(ctx context.Context) (MigrationExecutor, error)
}

// MigrationExecutor runs migrations under the lock returned by Migrator.Lock.
type MigrationExecutor interface {
	// Versions returns the applied schema versions in ascending order, or
	// nil if the schema has never been installed.
	Versions(ctx context.Context) ([]int, error)
	// Apply runs a migration script and records (up) or removes (down) its
	// version, atomically.
	Apply(ctx context.Context, version int, script string, up bool) error
	// Close releases the lock and the connection.
	Close(ctx context.Context) error
}

// JobState is a job's position in the state machine.
type JobState string

// Job states. Available, scheduled and retryable all mean "claim once
// scheduled_at has passed"; they are distinct only so operators can see why a
// job is waiting.
const (
	JobStatePending   JobState = "pending"   // waiting on workflow dependencies
	JobStateAvailable JobState = "available" // ready once scheduled_at <= now()
	JobStateScheduled JobState = "scheduled" // inserted or snoozed with a future scheduled_at
	JobStateRunning   JobState = "running"
	JobStateRetryable JobState = "retryable" // failed; runs again at scheduled_at
	JobStateCompleted JobState = "completed"
	JobStateCancelled JobState = "cancelled"
	JobStateDiscarded JobState = "discarded" // out of attempts or cancelled by the worker: dead-lettered
)

// Terminal reports whether the state is final, meaning the job has left the
// live table.
func (s JobState) Terminal() bool {
	switch s {
	case JobStateCompleted, JobStateCancelled, JobStateDiscarded:
		return true
	default:
		return false
	}
}

// JobRow is a job as stored by the engine. It is shared by the live table and
// history; FinalizedAt and Output are set only for finalized jobs.
type JobRow struct {
	ID          JobID
	Kind        string
	Queue       string
	State       JobState
	Priority    int
	Attempt     int
	MaxAttempts int
	ScheduledAt time.Time
	// AttemptedAt is the time of the latest claim, or zero if never claimed.
	AttemptedAt time.Time
	// AttemptedBy is the client ID holding the claim, or zero.
	AttemptedBy int64
	Args        json.RawMessage
	Metadata    json.RawMessage
	Errors      []AttemptError
	UniqueKey   string
	OrderingKey string
	// ExpiresAt is the TTL deadline, or zero.
	ExpiresAt time.Time
	// CancelRequestedAt is set when JobCancel is called on a running job.
	CancelRequestedAt time.Time
	Await             bool
	CreatedAt         time.Time
	// FinalizedAt is set for jobs in history.
	FinalizedAt time.Time
	// Output is the value recorded by the worker, for jobs in history.
	Output json.RawMessage
}

// AttemptError records one failed attempt.
type AttemptError struct {
	// At is the database time at which the failure was recorded.
	At      time.Time `json:"at"`
	Attempt int       `json:"attempt"`
	Error   string    `json:"error"`
	// Trace is the stack trace when the attempt panicked.
	Trace string `json:"trace,omitempty"`
}

// JobInsertParams describes one job to insert.
type JobInsertParams struct {
	Kind        string
	Queue       string
	Priority    int
	MaxAttempts int
	// ScheduledAt is zero to run as soon as possible.
	ScheduledAt time.Time
	Args        json.RawMessage
	Metadata    json.RawMessage
	// UniqueKey is empty for non-unique jobs.
	UniqueKey string
}

// JobInsertResult is the outcome of inserting one job.
type JobInsertResult struct {
	Job *JobRow
	// Duplicate is true when a live job with the same unique key already
	// existed. Job is then that existing job.
	Duplicate bool
}

// JobClaimParams selects jobs to claim.
type JobClaimParams struct {
	Queue    string
	ClientID int64
	Limit    int
}

// JobFinalizeParams is a batch of results to apply.
type JobFinalizeParams struct {
	Jobs []JobFinalize
}

// JobFinalize is the result of one attempt.
type JobFinalize struct {
	ID JobID
	// AttemptedBy is the client that claimed the attempt. The engine applies
	// the result only if the job is still running and owned by this client.
	AttemptedBy int64
	// State is the state to move to: completed, cancelled or discarded (the
	// job moves to history) or retryable or scheduled (it stays live).
	State JobState
	// Delay is how long from database now() the job waits before it can be
	// claimed again. Only for retryable and scheduled.
	Delay time.Duration
	// Snooze gives the attempt back, so the retry does not count against
	// MaxAttempts.
	Snooze bool
	// Error is appended to the job's errors. Its At field is set by the
	// engine from database time.
	Error *AttemptError
	// Output is stored with the finalized job.
	Output json.RawMessage
	// Archive controls whether a terminal job is written to history. Cancelled
	// and discarded jobs are always archived; completed jobs may skip it.
	Archive bool
}

// ClientRegisterParams describes a client process taking out a lease.
type ClientRegisterParams struct {
	Hostname string
	TTL      time.Duration
	Info     json.RawMessage
}

// ClientRenewParams extends a lease.
type ClientRenewParams struct {
	ClientID int64
	TTL      time.Duration
}
