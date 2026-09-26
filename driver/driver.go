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

// ErrJobRunning is returned by operations that need a job not to be running.
var ErrJobRunning = errors.New("hopper: job is running")

// ErrUniqueConflict is returned when re-driving a job would violate the
// uniqueness of a live job with the same key.
var ErrUniqueConflict = errors.New("hopper: a live job with the same unique key exists")

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
	// Listener opens a connection for push notifications. Engines without
	// them return ErrNotSupported, and the core polls instead.
	Listener(ctx context.Context) (Listener, error)
}

// Capabilities describes optional strategies an engine supports. The core
// picks its strategy from these; an engine without a capability still works
// through the baseline path.
type Capabilities struct {
	// Copy reports whether JobInsertCopy is implemented (COPY on Postgres).
	Copy bool
	// Listen reports whether Listener is implemented (LISTEN/NOTIFY on
	// Postgres).
	Listen bool
}

// Notification channels. Payloads are documented on each.
const (
	// ChannelInsert carries the queue name of a committed insert.
	ChannelInsert = "hopper_insert"
	// ChannelLeader is signalled when a leader resigns, so a replacement is
	// elected without waiting for the lease to expire.
	ChannelLeader = "hopper_leader"
	// ChannelControl carries operator actions: "cancel:<job id>",
	// "pause:<queue>" and "resume:<queue>".
	ChannelControl = "hopper_control"
	// ChannelDone carries the ID of a finalized job that was inserted with
	// Await, so waiters return as soon as the finalize commits.
	ChannelDone = "hopper_done"
)

// Listener receives notifications. One listener multiplexes every channel.
type Listener interface {
	// Listen subscribes to channels.
	Listen(ctx context.Context, channels ...string) error
	// Next blocks until a notification arrives or ctx is done.
	Next(ctx context.Context) (Notification, error)
	// Close releases the connection.
	Close(ctx context.Context) error
}

// Notification is one message from a channel.
type Notification struct {
	Channel string
	Payload string
}

// Executor runs queue operations, either against the pool or inside a
// caller's transaction.
type Executor interface {
	// Now returns the database's clock.
	Now(ctx context.Context) (time.Time, error)

	// JobInsertMany inserts jobs in one statement and returns one result per
	// input, in input order. An input whose unique key matches a live job is
	// not inserted; its result carries the existing job and Duplicate = true.
	// With ConflictReplace, the existing job's args, metadata, priority,
	// max attempts and schedule are replaced unless it is running. Inputs are
	// already deduplicated by unique key within the batch.
	JobInsertMany(ctx context.Context, params []JobInsertParams, opts JobInsertOpts) ([]JobInsertResult, error)
	// JobInsertCopy bulk-loads jobs without conflict handling. Callers use it
	// only for batches with no unique keys, and only if Capabilities().Copy.
	JobInsertCopy(ctx context.Context, params []JobInsertParams, opts JobInsertOpts) ([]JobInsertResult, error)
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
	// JobList returns jobs matching the filter in ID order, after the cursor.
	JobList(ctx context.Context, params JobListParams) ([]*JobRow, error)
	// JobCancel cancels a job. A waiting job moves to history as cancelled.
	// A running job gets a cancel request that its client picks up through
	// ChannelControl and on its next lease renewal. A finalized job is
	// returned unchanged. The job as it is afterwards is returned.
	JobCancel(ctx context.Context, id JobID) (*JobRow, error)
	// JobRetry makes a job available now: a cancelled or discarded job is
	// moved back from history (with one more attempt allowed if it had run
	// out), and a waiting job has its schedule brought forward. It returns
	// ErrJobRunning for a running job and ErrUniqueConflict if a live job
	// holds the same unique key.
	JobRetry(ctx context.Context, id JobID) (*JobRow, error)
	// JobDiscardExpired moves waiting jobs whose TTL has passed to history
	// as discarded, and returns them.
	JobDiscardExpired(ctx context.Context, limit int) ([]*JobRow, error)
	// JobRescueCandidates returns running jobs whose owning client has no
	// unexpired lease, plus, if StuckAfter is positive, running jobs claimed
	// longer ago than that. The caller finalizes them.
	JobRescueCandidates(ctx context.Context, params JobRescueParams) ([]*JobRow, error)

	// ClientRegister creates a lease row for a client process and returns its ID.
	ClientRegister(ctx context.Context, params ClientRegisterParams) (int64, error)
	// ClientRenew extends an unexpired lease. Renewed is false if the lease
	// had already expired, in which case the client has been fenced. The
	// result also carries control state the client re-reads on every
	// renewal, in case a notification was missed.
	ClientRenew(ctx context.Context, params ClientRenewParams) (ClientRenewResult, error)
	// ClientDelete removes a client's lease row.
	ClientDelete(ctx context.Context, clientID int64) error
	// ClientPruneExpired removes lease rows that have expired and returns
	// how many.
	ClientPruneExpired(ctx context.Context) (int64, error)
	// ClientList returns every client lease row, live or expired.
	ClientList(ctx context.Context) ([]*ClientRow, error)

	// Notify sends a notification per payload on a channel, delivered to
	// listeners when the surrounding transaction commits. Engines without
	// notifications return nil.
	Notify(ctx context.Context, channel string, payloads []string) error

	// LeaderAttempt takes or renews the leader lease for a client. It
	// returns true if the client is the leader afterwards.
	LeaderAttempt(ctx context.Context, params LeaderParams) (bool, error)
	// LeaderResign gives up leadership if the client holds it, and signals
	// other clients to elect a replacement.
	LeaderResign(ctx context.Context, clientID int64) error

	// QueueEnsure records queues a client works, so they are listed even
	// before any job is inserted.
	QueueEnsure(ctx context.Context, names []string) error
	// QueuePause stops every client from claiming from a queue, and
	// signals them through ChannelControl.
	QueuePause(ctx context.Context, name string) error
	// QueueResume undoes QueuePause.
	QueueResume(ctx context.Context, name string) error
	// QueueList returns every known queue.
	QueueList(ctx context.Context) ([]*QueueRow, error)

	// PeriodicInsert inserts the job for a periodic slot if that slot has
	// not been inserted yet, atomically, and reports whether it did. A slot
	// therefore produces at most one job across leaders.
	PeriodicInsert(ctx context.Context, params PeriodicInsertParams) (*JobRow, bool, error)
	// PeriodicLastSlots returns the last inserted slot per periodic job.
	PeriodicLastSlots(ctx context.Context) (map[string]time.Time, error)

	// Stats returns queue depths and cluster state.
	Stats(ctx context.Context) (*Stats, error)

	// SubscriptionUpsert records subscriptions, updating pattern, queue and
	// settings of existing ones by name.
	SubscriptionUpsert(ctx context.Context, subs []SubscriptionRow) error
	// SubscriptionList returns every subscription.
	SubscriptionList(ctx context.Context) ([]*SubscriptionRow, error)
	// MessagePublish inserts one delivery per subscription whose pattern
	// matches the topic, in one statement, and returns them. A delivery
	// whose dedup key matches a live one is reported as a duplicate.
	MessagePublish(ctx context.Context, params MessagePublishParams) ([]JobInsertResult, error)

	// HistoryMaintain enforces retention on finalized jobs and prepares
	// storage for the near future, in whatever way suits the engine (dropping
	// time partitions on Postgres). It is idempotent and may run on two
	// leaders at once.
	HistoryMaintain(ctx context.Context, params HistoryMaintainParams) (HistoryMaintainResult, error)
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
	// OrderingKey, if set, serializes the job with others of the same key
	// in its queue: at most one runs at a time, oldest first.
	OrderingKey string
	// TTL, if positive, discards the job if it has not started within this
	// long of database now().
	TTL time.Duration
	// Await marks the job so that its finalize is announced on ChannelDone.
	Await bool
}

// ConflictAction says what to do when a unique key matches a live job.
type ConflictAction int

// Conflict actions.
const (
	// ConflictSkip keeps the existing job and reports it as a duplicate.
	ConflictSkip ConflictAction = iota
	// ConflictReplace updates the existing job's args, metadata, priority,
	// max attempts and schedule, unless it is running. It is still reported
	// as a duplicate.
	ConflictReplace
)

// JobInsertOpts applies to a whole insert batch.
type JobInsertOpts struct {
	// Notify wakes workers on the inserted queues once the insert commits.
	Notify bool
	// OnConflict applies to inputs with a unique key.
	OnConflict ConflictAction
}

// JobInsertResult is the outcome of inserting one job.
type JobInsertResult struct {
	Job *JobRow
	// Duplicate is true when a live job with the same unique key already
	// existed. Job is then that existing job.
	Duplicate bool
}

// JobListParams filters a listing. Empty fields match everything.
type JobListParams struct {
	Queue  string
	Kinds  []string
	States []JobState
	// After is the cursor: only jobs with a greater ID are returned.
	After JobID
	Limit int
}

// QueueRow is a queue's cluster-wide state.
type QueueRow struct {
	Name string
	// PausedAt is zero when the queue is not paused.
	PausedAt  time.Time
	UpdatedAt time.Time
}

// PeriodicInsertParams identifies a periodic slot and the job to insert
// for it.
type PeriodicInsertParams struct {
	Name string
	Slot time.Time
	Job  JobInsertParams
}

// Stats is a snapshot of queue depths and cluster state.
type Stats struct {
	Queues map[string]*QueueStats
	// LiveClients counts clients with an unexpired lease.
	LiveClients int
	// Leader is the leader's client ID, or 0 if the lease is free or
	// expired.
	Leader int64
	// RunningByClient counts running jobs per client ID.
	RunningByClient map[int64]int
}

// QueueStats is one queue's depth by state.
type QueueStats struct {
	Available int
	Scheduled int
	Retryable int
	Running   int
	// OldestAvailable is the age of the oldest job that could be claimed
	// now, or zero.
	OldestAvailable time.Duration
	// CompletedLastMinute counts jobs completed in the last minute.
	CompletedLastMinute int
	Paused              bool
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

// JobRescueParams selects jobs to rescue.
type JobRescueParams struct {
	// StuckAfter, if positive, also selects running jobs claimed longer ago
	// than this, whatever the state of their client's lease.
	StuckAfter time.Duration
	Limit      int
}

// LeaderParams describes a leadership attempt.
type LeaderParams struct {
	ClientID int64
	TTL      time.Duration
}

// HistoryMaintainParams sets retention per outcome.
type HistoryMaintainParams struct {
	// CompletedRetention is how long completed jobs are kept.
	CompletedRetention time.Duration
	// FailedRetention is how long cancelled and discarded jobs are kept.
	FailedRetention time.Duration
}

// HistoryMaintainResult reports what maintenance did.
type HistoryMaintainResult struct {
	// Created and Dropped name storage units (partitions on Postgres).
	Created []string
	Dropped []string
	// Pruned counts rows deleted individually, outside partition drops.
	Pruned int64
}

// SubscriptionRow is a topic subscription: messages published to a topic
// matching Pattern are delivered as jobs of Kind on Queue.
type SubscriptionRow struct {
	Name    string
	Pattern string
	Kind    string
	Queue   string
	// MaxAttempts is the retry budget of deliveries, or 0 for the default
	// (25, or 10 for deliveries with an ordering key).
	MaxAttempts int
	// Metadata is merged into every delivery's metadata.
	Metadata  json.RawMessage
	CreatedAt time.Time
}

// MessagePublishParams describes a message to fan out.
type MessagePublishParams struct {
	Topic   string
	Payload json.RawMessage
	// Headers are stored in each delivery's metadata.
	Headers map[string]string
	// OrderingKey serializes deliveries with the same key per queue.
	OrderingKey string
	// DedupKey makes the publish idempotent while an earlier delivery is
	// live: it becomes each delivery's unique key.
	DedupKey string
	// Delay schedules the deliveries for later.
	Delay time.Duration
	// TTL discards deliveries not started within this long.
	TTL      time.Duration
	Priority int
	Await    bool
	// Notify wakes workers on the delivery queues once the publish commits.
	Notify bool
}

// ClientRegisterParams describes a client process taking out a lease.
type ClientRegisterParams struct {
	Hostname string
	TTL      time.Duration
	Info     json.RawMessage
}

// ClientRow is a client process's lease.
type ClientRow struct {
	ID        int64
	Hostname  string
	StartedAt time.Time
	ExpiresAt time.Time
	Info      json.RawMessage
}

// ClientRenewParams extends a lease.
type ClientRenewParams struct {
	ClientID int64
	TTL      time.Duration
}

// ClientRenewResult is the outcome of a renewal plus the control state the
// client should apply.
type ClientRenewResult struct {
	Renewed bool
	// CancelRequested lists the client's running jobs that have a pending
	// cancel request.
	CancelRequested []JobID
	// PausedQueues lists the queues currently paused.
	PausedQueues []string
}
