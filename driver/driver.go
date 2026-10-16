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
	"fmt"
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

// ErrJobLocked is returned by JobRecordProgress with NoWait when another
// transaction holds the job's row.
var ErrJobLocked = errors.New("hopper: another transaction holds the job's row")

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
	// ChannelLeader is signaled when a leader resigns, so a replacement is
	// elected without waiting for the lease to expire.
	ChannelLeader = "hopper_leader"
	// ChannelControl carries operator actions: "cancel:<job id>",
	// "pause:<queue>", "resume:<queue>" and "limit:<queue>" (limits
	// changed; the payload does not say how).
	ChannelControl = "hopper_control"
	// ChannelDone carries the ID of a finalized job that was inserted with
	// Await, so waiters return as soon as the finalize commits.
	ChannelDone = "hopper_done"
	// ChannelStream is signaled when events are appended to the stream
	// log, with the topic as payload, so the leader pumps consumers at once.
	ChannelStream = "hopper_stream"
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
	// SchemaVersion returns the newest applied schema version, or 0 if the
	// schema has not been installed. It takes no lock, so it does not wait
	// for a migration in progress.
	SchemaVersion(ctx context.Context) (int, error)

	// JobInsertMany inserts jobs in one statement and returns one result per
	// input, in input order. An input whose unique key matches a live job, or
	// with UniqueFinished a job in history in one of those states, is not
	// inserted; its result carries the existing job and Duplicate = true. A
	// batch with UniqueFinished inputs takes more than one statement, and
	// counts each input of a batch found in history toward the batch's
	// progress.
	// With ConflictReplace, the existing job's args, metadata, priority,
	// max attempts and schedule are replaced unless it is running. Inputs are
	// already deduplicated by unique key within the batch.
	JobInsertMany(ctx context.Context, params []JobInsertParams, opts JobInsertOpts) ([]JobInsertResult, error)
	// JobInsertCopy bulk-loads jobs without conflict handling. Callers use it
	// only for batches with no unique keys, and only if Capabilities().Copy.
	JobInsertCopy(ctx context.Context, params []JobInsertParams, opts JobInsertOpts) ([]JobInsertResult, error)
	// JobClaim atomically moves up to Limit claimable jobs in a queue to
	// running, owned by ClientID, and returns them in claim order. With
	// Limited, the claim runs inside a transaction that locks the queue row
	// and applies its global, rate and partition limits; the result then
	// says how long to wait when the rate limit allowed nothing.
	JobClaim(ctx context.Context, params JobClaimParams) (JobClaimResult, error)
	// JobAge raises the priority of jobs in a queue that have been claimable
	// for longer than after, one level per call, and returns how many.
	JobAge(ctx context.Context, queue string, after time.Duration) (int64, error)
	// JobFinalizeMany applies a batch of results in one statement. A result is
	// applied only if its job is still running and owned by the client that
	// produced it; the IDs that were applied are returned, and the caller
	// treats the rest as fenced.
	JobFinalizeMany(ctx context.Context, params JobFinalizeParams) ([]JobID, error)
	// JobRecordProgress stores a running job's progress, fenced like a
	// finalize: only while the job is running in the given attempt of the
	// given client. It returns the progress as stored, or nil if the attempt
	// no longer owns the job.
	JobRecordProgress(ctx context.Context, params JobRecordProgressParams) (*Progress, error)
	// JobGet returns a job by ID, live or from history.
	JobGet(ctx context.Context, id JobID) (*JobRow, error)
	// JobLogs returns the logs of a job's latest attempts, oldest first,
	// live or from history.
	JobLogs(ctx context.Context, id JobID) ([]AttemptLog, error)
	// JobList returns jobs matching the filter in ID order (descending with
	// NewestFirst), after the cursor.
	JobList(ctx context.Context, params JobListParams) ([]*JobRow, error)
	// JobCancel cancels a job. A waiting job moves to history as canceled.
	// A running job gets a cancel request that its client picks up through
	// ChannelControl and on its next lease renewal. A finalized job is
	// returned unchanged. The job as it is afterwards is returned.
	JobCancel(ctx context.Context, id JobID) (*JobRow, error)
	// JobRetry makes a job available now: a canceled or discarded job is
	// moved back from history (with one more attempt allowed if it had run
	// out), and a waiting job has its schedule brought forward. Either way
	// the job's TTL is cleared, as an operator overriding the wait. It returns
	// ErrJobRunning for a running job and ErrUniqueConflict if a live job
	// holds the same unique key. The job keeps its progress unless
	// params.FromStart.
	JobRetry(ctx context.Context, params JobRetryParams) (*JobRow, error)
	// JobDelete removes a job that is not running and returns it as it was
	// last stored. A waiting job is canceled as JobCancel does, so its
	// batch, its dependents and anyone awaiting it see it finish, but no
	// history row is kept. It returns ErrJobRunning for a running
	// job and ErrNotFound for an unknown one.
	JobDelete(ctx context.Context, id JobID) (*JobRow, error)
	// JobRetryOrderingKey wakes an ordering key in a queue. Up to Limit of
	// its jobs waiting on their schedule (scheduled or retryable, oldest
	// first, TTL not passed) become runnable now, keeping their TTL and
	// losing their snooze reason; it returns how many. A running job of the
	// key is marked, so that if its attempt ends in a snooze or a retry the
	// job is runnable at once instead of after its delay. Pending and
	// finalized jobs are left alone.
	JobRetryOrderingKey(ctx context.Context, params JobRetryOrderingKeyParams) (int, error)
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
	// QueueSetLimits sets a queue's cluster-wide limits, creating the queue
	// if needed. Zero values remove the corresponding limit.
	QueueSetLimits(ctx context.Context, limits QueueLimits) error

	// BatchInsert creates a batch expecting Total jobs and returns its ID.
	// The jobs are inserted separately with their BatchID set.
	BatchInsert(ctx context.Context, params BatchInsertParams) (JobID, error)
	// BatchSettle brings batches' recorded progress up to date and
	// completes, inserting their callbacks, those with no job left.
	// Statements that finalize jobs record their batches' progress without
	// waiting on each other, so two that finalize a batch's last jobs at
	// the same time can each miss the other's progress and leave the batch
	// open. A client settles the batches it has just finalized jobs of, and
	// the leader sweeps up the rest.
	BatchSettle(ctx context.Context, params BatchSettleParams) (BatchSettleResult, error)
	// BatchGet returns a batch's progress.
	BatchGet(ctx context.Context, id JobID) (*BatchRow, error)
	// WorkflowInsert inserts a batch, its jobs and the dependencies between
	// them atomically. Jobs with dependencies start pending. Results are in
	// input order.
	WorkflowInsert(ctx context.Context, params WorkflowInsertParams) (*WorkflowInsertResult, error)
	// WorkflowGet returns a workflow's progress, its jobs (live and
	// finished) and its edges.
	WorkflowGet(ctx context.Context, id JobID) (*WorkflowRow, error)
	// WorkflowPromoteReady makes pending jobs whose dependencies have all
	// finalized claimable. The statement that finalizes a job promotes its
	// dependents itself, but two dependencies finalized by concurrent
	// transactions each see the other still live, and neither promotes. A
	// client therefore promotes the dependents of the jobs it has just
	// finalized, and the leader sweeps up the rest.
	WorkflowPromoteReady(ctx context.Context, params WorkflowPromoteParams) (WorkflowPromoteResult, error)

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

	// StreamAppend appends one event to the stream log and returns it with
	// its position.
	StreamAppend(ctx context.Context, params StreamAppendParams) (*StreamEvent, error)
	// StreamRead pages through the committed events after a position whose
	// topic matches a pattern, in position order. It is for browsing; a
	// consumer is what reads the log without missing late commits.
	StreamRead(ctx context.Context, params StreamReadParams) ([]*StreamEvent, error)
	// StreamConsumerUpsert records consumers, updating pattern, queue and
	// settings of existing ones by name; a new consumer starts at Start.
	StreamConsumerUpsert(ctx context.Context, consumers []StreamConsumerRow) error
	// StreamConsumerList returns every consumer with its position.
	StreamConsumerList(ctx context.Context) ([]*StreamConsumerRow, error)
	// StreamConsumerSeek moves a consumer's position: events after it are
	// delivered (again).
	StreamConsumerSeek(ctx context.Context, name string, params StreamSeekParams) error
	// StreamPump turns up to limit events after a consumer's position into
	// delivery jobs and advances the position, atomically. Two leaders
	// pumping the same consumer serialize on its row.
	StreamPump(ctx context.Context, name string, limit int) (StreamPumpResult, error)

	// HistoryMaintain enforces retention on finalized jobs and prepares
	// storage for the near future, in whatever way suits the engine (dropping
	// time partitions on Postgres). It is idempotent and may run on two
	// leaders at once.
	HistoryMaintain(ctx context.Context, params HistoryMaintainParams) (HistoryMaintainResult, error)
	// HistoryIndex builds the indexes a schema upgrade declared on history
	// for the partitions that existed before it, without blocking the
	// finalizes that write there. Building takes as long as the partitions
	// are large and can wait for long transactions anywhere in the database,
	// so it runs apart from the leader's other duties. It is idempotent, and
	// finds nothing to do once every partition has its indexes.
	HistoryIndex(ctx context.Context, params HistoryIndexParams) (HistoryIndexResult, error)
	// JobsMaintain keeps the live table fit between the engine's own
	// maintenance passes: it reclaims the space and index entries of jobs
	// that have run and left once enough have accumulated (a hundred
	// thousand on Postgres), and refreshes planner statistics when the
	// planner's idea of the table's size is off by an order of magnitude. It
	// never waits for a lock, so it costs nothing while the engine's own
	// pass is running. The live table churns at the job rate, and on
	// Postgres every claim walks the claim index past the entries of jobs
	// that have left, so pickup latency climbs between vacuums; autovacuum
	// looks only every autovacuum_naptime (a minute by default). Engines
	// without such maintenance return zero values.
	JobsMaintain(ctx context.Context) (JobsMaintainResult, error)
}

// JobsMaintainResult reports what JobsMaintain did.
type JobsMaintainResult struct {
	Vacuumed bool
	Analyzed bool
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
	JobStateCanceled  JobState = "canceled"
	JobStateDiscarded JobState = "discarded" // out of attempts or canceled by the worker: dead-lettered
)

// Terminal reports whether the state is final, meaning the job has left the
// live table.
func (s JobState) Terminal() bool {
	switch s {
	case JobStateCompleted, JobStateCanceled, JobStateDiscarded:
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
	AttemptedBy  int64
	Args         json.RawMessage
	Metadata     json.RawMessage
	Errors       []AttemptError
	UniqueKey    string
	OrderingKey  string
	PartitionKey string
	// BatchID is the batch the job belongs to, or zero.
	BatchID JobID
	// ExpiresAt is the TTL deadline, or zero. A job still waiting to run
	// at this time, for its first attempt or a later one, is discarded.
	ExpiresAt time.Time
	// CancelRequestedAt is set when JobCancel is called on a running job.
	CancelRequestedAt time.Time
	Await             bool
	CreatedAt         time.Time
	// SnoozeReason is the reason the worker gave when it last snoozed the
	// job, while the job waits out that snooze. It is empty in any state but
	// scheduled: the claim, any other finalize, a retry and a replacing
	// insert clear it, and a row in another state never reports one.
	SnoozeReason string
	// Progress is the last step the job's worker recorded, or nil. It
	// survives retries, snoozes and a JobRetry from history.
	Progress *Progress
	// FinalizedAt is set for jobs in history.
	FinalizedAt time.Time
	// Output is the value recorded by the worker, for jobs in history.
	Output json.RawMessage
}

// Progress is how far a job's worker got: the last step it finished, and a
// cursor within the work that follows.
type Progress struct {
	// Step is the last step finished, or empty before the first.
	Step string `json:"step"`
	// Cursor is a JSON value marking a position within the work after Step,
	// or nil.
	Cursor json.RawMessage `json:"cursor,omitempty"`
	// RecordedAt is the database time at which it was recorded.
	RecordedAt time.Time `json:"at"`
}

// AttemptLog is the log output of one attempt of a job.
type AttemptLog struct {
	Attempt int `json:"attempt"`
	// Snoozed reports that the attempt snoozed, which gives the attempt
	// back: the next attempt has the same number. A job keeps the log of
	// only the latest snooze of each attempt number.
	Snoozed bool `json:"snoozed,omitempty"`
	// Log is the attempt's lines, each ending in a newline.
	Log string `json:"log"`
}

// JobRetryParams selects the job to retry.
type JobRetryParams struct {
	ID JobID
	// FromStart clears the job's progress, so that it runs all its steps
	// again.
	FromStart bool
}

// JobRecordProgressParams is a running job's new progress.
type JobRecordProgressParams struct {
	ID JobID
	// AttemptedBy and Attempt identify the attempt, which must still own
	// the job.
	AttemptedBy int64
	Attempt     int
	// Step is the last step finished. With KeepStep the stored step is
	// kept and Step is ignored.
	Step     string
	KeepStep bool
	// Cursor is a JSON value, or nil for none.
	Cursor json.RawMessage
	// NoWait returns ErrJobLocked at once, instead of waiting, if another
	// transaction holds the job's row.
	NoWait bool
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
	// UniqueFinished lists finished states in which a job in history with
	// the same kind and UniqueKey is a duplicate too. The insert must find
	// one that finalizes while it runs: always on the pool, and in a
	// caller's transaction at read committed isolation.
	UniqueFinished []JobState
	// OrderingKey, if set, serializes the job with others of the same key
	// in its queue: at most one runs at a time, oldest first.
	OrderingKey string
	// PartitionKey groups the job for partitioned limits.
	PartitionKey string
	// BatchID, if set, counts the job toward a batch.
	BatchID JobID
	// TTL, if positive, sets ExpiresAt to this long after database now().
	// A job past ExpiresAt is not claimed while it waits (available,
	// scheduled or retryable) and JobDiscardExpired discards it.
	TTL time.Duration
	// Await marks the job so that its finalize is announced on ChannelDone.
	Await bool
}

// ConflictAction says what to do when a unique key matches a live job. A
// job in history is never replaced.
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
	// Duplicate is true when a job with the same unique key already
	// existed: a live one, or one in history in a UniqueFinished state. Job
	// is then that existing job.
	Duplicate bool
}

// JobRetryOrderingKeyParams selects the key to wake.
type JobRetryOrderingKeyParams struct {
	Queue       string
	OrderingKey string
	Limit       int
}

// JobListParams filters a listing. Empty fields match everything.
type JobListParams struct {
	Queue  string
	Kinds  []string
	States []JobState
	// OrderingKey needs Queue: a key is a lane only within its queue.
	OrderingKey string
	// NewestFirst lists in descending ID order.
	NewestFirst bool
	// After is the cursor: only jobs after it in the listing's order (a
	// greater ID, or a smaller one when NewestFirst) are returned.
	After JobID
	Limit int
}

// QueueRow is a queue's cluster-wide state.
type QueueRow struct {
	Name string
	// PausedAt is zero when the queue is not paused.
	PausedAt  time.Time
	UpdatedAt time.Time
	Limits    QueueLimits
}

// QueueLimits are a queue's cluster-wide limits. Zero means unlimited.
type QueueLimits struct {
	Name string
	// GlobalLimit caps running jobs across all clients.
	GlobalLimit int
	// RatePerSec caps claims per second across all clients, with a burst
	// of RateBurst (defaulting to one second's worth).
	RatePerSec float64
	RateBurst  int
	// PartitionLimit caps running jobs per partition key.
	PartitionLimit int
	// Aging raises a waiting job's priority by one level each time it has
	// waited this long, so low priorities cannot starve. Zero disables it.
	Aging time.Duration
}

// Limited reports whether any limit that affects claiming is set.
func (l QueueLimits) Limited() bool {
	return l.GlobalLimit > 0 || l.RatePerSec > 0 || l.PartitionLimit > 0
}

// BatchInsertParams describes a batch of jobs with completion callbacks.
type BatchInsertParams struct {
	Total int
	// Callbacks are inserted when the batch completes: OnSuccess if no job
	// was canceled or discarded, OnFailure otherwise, and OnComplete
	// either way. Each callback's Metadata gains a "batch_id".
	OnSuccess, OnFailure, OnComplete *JobInsertParams
	Metadata                         json.RawMessage
}

// BatchSettleParams selects the batches BatchSettle settles.
type BatchSettleParams struct {
	// IDs names the batches to settle, waiting briefly for a batch row
	// another statement holds; one it cannot get is left for the sweep. An
	// empty non-nil IDs settles nothing.
	IDs []JobID
	// When IDs is nil, up to Limit batches with progress and an ID after
	// After are settled, in ID order, skipping rows other statements hold.
	After JobID
	Limit int
}

// BatchSettleResult reports what BatchSettle did.
type BatchSettleResult struct {
	// Folded counts the batches whose progress was brought up to date, and
	// Completed those of them that completed.
	Folded, Completed int64
	// Last is the greatest ID among the batches folded, the After of the
	// next chunk of a sweep.
	Last JobID
}

// BatchRow is a batch's progress.
type BatchRow struct {
	ID JobID
	// Name is set for workflows.
	Name string
	// Pending counts the batch's jobs not yet finalized.
	Pending int
	// Failed counts its jobs that were canceled or discarded.
	Failed int
	Total  int
	// CompletedAt is set once every job has been finalized.
	CompletedAt time.Time
	CreatedAt   time.Time
	Metadata    json.RawMessage
}

// DependencyFailure says what happens to a dependent job when the job it
// depends on is canceled or discarded.
type DependencyFailure string

const (
	// DependencyCancel cancels the dependent, and so on down the graph.
	DependencyCancel DependencyFailure = "cancel"
	// DependencyIgnore lets the dependent run once its dependencies have
	// all finished, whatever their outcome.
	DependencyIgnore DependencyFailure = "ignore"
)

// JobDependency is an edge of a workflow: Job waits for DependsOn. Both
// are indexes into WorkflowInsertParams.Jobs.
type JobDependency struct {
	Job, DependsOn int
	OnFailure      DependencyFailure
}

// WorkflowInsertParams describes a workflow: a named batch whose jobs
// depend on each other.
type WorkflowInsertParams struct {
	Name string
	// Jobs must have no unique keys; a skipped job would leave its
	// dependents waiting forever.
	Jobs []JobInsertParams
	Deps []JobDependency
	// Callbacks and Metadata are the batch's; see BatchInsertParams.
	OnSuccess, OnFailure, OnComplete *JobInsertParams
	Metadata                         json.RawMessage
	// Notify sends insert notifications for the jobs that start
	// available from inside the statement.
	Notify bool
}

// WorkflowInsertResult reports an inserted workflow.
type WorkflowInsertResult struct {
	ID   JobID
	Jobs []JobInsertResult
}

// JobEdge is an edge of an inserted workflow.
type JobEdge struct {
	Job, DependsOn JobID
}

// WorkflowRow is a workflow's progress, jobs and graph.
type WorkflowRow struct {
	Batch BatchRow
	// Jobs are in insertion order, live and finished alike.
	Jobs  []*JobRow
	Edges []JobEdge
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
	// Limited selects the path that locks the queue row and applies its
	// limits. Unlimited queues never touch the row.
	Limited bool
}

// JobClaimResult is the outcome of a claim.
type JobClaimResult struct {
	Jobs []*JobRow
	// Wait, when positive, is how long the rate limit will allow nothing.
	Wait time.Duration
}

// JobFinalizeParams is a batch of results to apply.
type JobFinalizeParams struct {
	Jobs []JobFinalize
	// NoBatches reports that none of the jobs belongs to a batch or a
	// workflow, so the engine may skip the bookkeeping those need. Workflow
	// jobs belong to a batch, so the jobs then have no dependents either.
	NoBatches bool
	// SkipLocked skips the jobs whose rows another transaction holds, such
	// as a worker's CompleteTx that has not ended, instead of waiting for
	// it. A skipped job is not among the applied.
	SkipLocked bool
	// KeepLogs is the most attempt logs a job keeps, the latest; a job given
	// another drops its oldest. Zero keeps them all.
	KeepLogs int
}

// WorkflowPromoteParams selects the jobs WorkflowPromoteReady considers.
type WorkflowPromoteParams struct {
	// DependsOn names finalized jobs whose pending dependents to promote.
	// An empty non-nil DependsOn promotes nothing.
	DependsOn []JobID
	// When DependsOn is nil, up to Limit jobs with dependencies and an ID
	// after After are considered, in ID order.
	After JobID
	Limit int
}

// WorkflowPromoteResult reports what WorkflowPromoteReady did.
type WorkflowPromoteResult struct {
	// Promoted counts the jobs made claimable.
	Promoted int64
	// Scanned counts the jobs considered by a sweep, and Last is the
	// greatest of their IDs, the After of the next chunk.
	Scanned int64
	Last    JobID
}

// JobFinalize is the result of one attempt.
type JobFinalize struct {
	ID JobID
	// AttemptedBy is the client that claimed the attempt. The engine applies
	// the result only if the job is still running and owned by this client.
	AttemptedBy int64
	// State is the state to move to: completed, canceled or discarded (the
	// job moves to history) or retryable or scheduled (it stays live).
	State JobState
	// Delay is how long from database now() the job waits before it can be
	// claimed again. Only for retryable and scheduled.
	Delay time.Duration
	// Snooze gives the attempt back, so the retry does not count against
	// MaxAttempts.
	Snooze bool
	// SnoozeReason is stored as the job's SnoozeReason when Snooze is set.
	SnoozeReason string
	// Error is appended to the job's errors. Its At field is set by the
	// engine from database time.
	Error *AttemptError
	// Output is stored with the finalized job.
	Output json.RawMessage
	// Log is appended to the job's logs, if not nil and KeepsLog, with
	// Snoozed set from Snooze. A snooze, with a Log or without one, first
	// removes the log of the job's previous snooze of the same attempt
	// number; a job whose logs that leaves unchanged keeps them as stored.
	Log *AttemptLog
	// Archive controls whether a terminal job is written to history. Canceled
	// and discarded jobs are always archived; completed jobs may skip it.
	Archive bool
}

// Archived reports whether a terminal f writes its job to history: only a
// completed job may skip it.
func (f JobFinalize) Archived() bool {
	return f.Archive || f.State != JobStateCompleted
}

// KeepsLog reports whether the job keeps the log of the attempt that ends
// with f: one that finishes without history is deleted with its logs.
func (f JobFinalize) KeepsLog() bool {
	return !f.State.Terminal() || f.Archived()
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
	// FailedRetention is how long canceled and discarded jobs are kept.
	FailedRetention time.Duration
	// StreamRetention is how long stream events are kept.
	StreamRetention time.Duration
	// LockTimeout bounds how long creating or dropping a storage unit
	// waits for its lock, during which the writers of that table queue
	// behind it; what cannot get its lock in time is deferred. Zero means
	// the engine's default.
	LockTimeout time.Duration
}

// HistoryMaintainResult reports what maintenance did.
type HistoryMaintainResult struct {
	// Created and Dropped name storage units (partitions on Postgres).
	Created []string
	Dropped []string
	// Deferred names storage units whose creation or drop had to wait for a
	// transaction that has not ended, such as one that wrote history; a
	// later pass retries them.
	Deferred []string
	// Kept names storage units past retention that wait for HistoryIndex to
	// build their index: one whose build is under way or about to start, or the last one its
	// outcome table's index lacks, which dropping first would leave that
	// index unable to complete. A later pass drops them.
	Kept []string
	// Pruned counts rows deleted individually, outside partition drops.
	Pruned int64
}

// HistoryIndexParams leaves storage units out of an index pass.
type HistoryIndexParams struct {
	// Skip names storage units (partitions on Postgres) to leave for a later
	// pass, and indexes not to try to repair (HistoryIndexResult.Invalid).
	Skip []string
	// LockTimeout bounds how long repairing an index waits for its lock,
	// during which the queries of the storage unit it locks queue behind
	// it, as HistoryMaintainParams.LockTimeout does. Zero means the engine's
	// default.
	LockTimeout time.Duration
}

// HistoryIndexResult reports what an index pass could not finish.
type HistoryIndexResult struct {
	// TimedOut names storage units whose build ran past the session's
	// statement timeout. What the build left is removed, and the pass goes
	// on to the rest.
	TimedOut []string
	// Invalid names indexes that stay invalid although every storage unit
	// under them is indexed, which the server cannot repair: on Postgres, an
	// outcome table's index left invalid by an earlier release, before
	// Postgres's May 2026 minor releases. Queries that need it read every
	// partition instead.
	Invalid []string
	// Deferred names indexes whose repair could not get its lock within
	// LockTimeout, held off by a transaction that read or wrote the storage
	// unit it locks; a later pass retries them.
	Deferred []string
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
	// TTL sets each delivery's ExpiresAt, as JobInsertParams.TTL does.
	TTL      time.Duration
	Priority int
	Await    bool
	// Notify wakes workers on the delivery queues once the publish commits.
	Notify bool
}

// StreamPosition orders events in the stream log: by the transaction that
// wrote them, then by sequence within it. The zero position is before every
// event.
type StreamPosition struct {
	Xid uint64
	Seq int64
}

// Less reports whether p comes before q.
func (p StreamPosition) Less(q StreamPosition) bool {
	return p.Xid < q.Xid || (p.Xid == q.Xid && p.Seq < q.Seq)
}

// IsZero reports the position before every event.
func (p StreamPosition) IsZero() bool { return p == StreamPosition{} }

// String formats the position as "xid:seq".
func (p StreamPosition) String() string { return fmt.Sprintf("%d:%d", p.Xid, p.Seq) }

// ParseStreamPosition parses the String form.
func ParseStreamPosition(s string) (StreamPosition, error) {
	var p StreamPosition
	if _, err := fmt.Sscanf(s, "%d:%d", &p.Xid, &p.Seq); err != nil {
		return StreamPosition{}, fmt.Errorf("hopper: stream position %q is not xid:seq", s)
	}
	return p, nil
}

// StreamEvent is one event of the stream log.
type StreamEvent struct {
	Position StreamPosition
	Topic    string
	// Key, if set, becomes the ordering key of the event's deliveries.
	Key       string
	Payload   json.RawMessage
	Headers   map[string]string
	MessageID string
	CreatedAt time.Time
}

// StreamAppendParams describes an event to append.
type StreamAppendParams struct {
	Topic   string
	Key     string
	Payload json.RawMessage
	Headers map[string]string
	// Notify signals ChannelStream from inside the statement.
	Notify bool
}

// StreamReadParams selects events.
type StreamReadParams struct {
	// Pattern is an AMQP topic pattern; empty matches every topic.
	Pattern string
	After   StreamPosition
	Limit   int
}

// StreamStart says where a new consumer starts.
type StreamStart string

const (
	// StreamStartLatest delivers events appended from now on.
	StreamStartLatest StreamStart = "latest"
	// StreamStartEarliest delivers every retained event first.
	StreamStartEarliest StreamStart = "earliest"
)

// StreamConsumerRow is a consumer: a named position on the log whose
// events, matched by pattern, are delivered as jobs of Kind on Queue.
type StreamConsumerRow struct {
	Name    string
	Pattern string
	Kind    string
	Queue   string
	// MaxAttempts is the retry budget of deliveries, or 0 for the default
	// (25, or 10 for events with a key).
	MaxAttempts int
	// Metadata is merged into every delivery's metadata.
	Metadata json.RawMessage
	// Start applies when the consumer is created; it defaults to latest.
	Start StreamStart
	// Snapshot is the engine's record of what the consumer has read to:
	// on Postgres, the pg_snapshot every delivered transaction is visible
	// in.
	Snapshot string
	// Position is the last event delivered of the delta being read, or
	// zero between deltas.
	Position    StreamPosition
	DeliveredAt time.Time
	CreatedAt   time.Time
}

// StreamSeekParams says where to move a consumer. Exactly one of Position,
// Time, Earliest and Latest applies, in that order of precedence.
type StreamSeekParams struct {
	// Position, if set, becomes the consumer's position: the next event
	// delivered is the one after it.
	Position StreamPosition
	// Time moves the consumer to just before the first event appended at
	// or after it.
	Time time.Time
	// Earliest moves the consumer before every retained event.
	Earliest bool
	// Latest moves the consumer past every event appended so far.
	Latest bool
}

// StreamPumpResult reports one pump.
type StreamPumpResult struct {
	Delivered int
	// Position is the consumer's position afterwards.
	Position StreamPosition
	// Queues received deliveries.
	Queues []string
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
	// LimitedQueues lists the queues with a global, rate or partition
	// limit, which must be claimed through the limited path.
	LimitedQueues []string
}
