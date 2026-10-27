package hopper

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// ErrJobNotClaimable is returned by Leases.Claim for a job that cannot be
// claimed now: pending, finalized, past its TTL, held back by its ordering
// key, or in a paused queue.
var ErrJobNotClaimable = driver.ErrJobNotClaimable

// DefaultJobLeaseTTL is a job lease's time to live when LeaseOpts.TTL is zero.
const DefaultJobLeaseTTL = time.Minute

// RescueReason says why hopper, not the attempt, ended an attempt. It is
// set on the attempt's entry in JobRow.Errors.
type RescueReason = driver.RescueReason

// The reasons hopper ends attempts.
const (
	// RescueClientLost is the reason when the client running the attempt
	// lost its lease.
	RescueClientLost = driver.RescueClientLost
	// RescueStuck is the reason when the attempt ran past
	// Config.RescueStuckAfter.
	RescueStuck = driver.RescueStuck
	// RescueLeaseExpired is the reason when the attempt's job lease was not
	// renewed in time.
	RescueLeaseExpired = driver.RescueLeaseExpired
)

// Leases returns the controller of job leases: the attempts of jobs claimed
// by ID, which run outside any client, such as on a remote worker that
// reports through whichever client it reaches. The calls work on a client
// that is not started.
func (c *Client[TTx]) Leases() *Leases[TTx] {
	return &Leases[TTx]{c: c}
}

// Leases claims jobs by ID under job leases, and renews, records the
// progress of and finishes their attempts. See Client.Leases.
//
// A job claimed by ID runs with no owning client. Its lease owns it: a
// token that names the job, the claim's fence and a random value the
// database generates. Every call on the attempt names the token and is
// fenced to it, from any client of the cluster: once the lease has expired,
// or the attempt has ended and the job been claimed again, the call returns
// ErrJobFenced and changes nothing. A lease that is not renewed within its
// TTL expires, though a lease last renewed before a client's start is
// given at least its TTL, or Config.JobLeaseGrace, from that start, so
// that leases outlast a restart of the whole cluster. The leader then
// makes the job retryable at once, with an entry in its errors whose
// Rescue is RescueLeaseExpired and whose Fence is the lease's, or, on its
// last attempt, discards it, or cancels it if JobCancel was called.
// hopper never runs a leased attempt's work itself. Only a client with
// queues, and without Config.NeverLead, can lead, so a cluster whose
// clients only insert and take reports never expires a lease.
//
// The queue's pause applies to a claim by ID; its limits (GlobalLimit,
// PartitionLimit, RateLimit) do not, and leased jobs do not count toward
// them. The job's TTL and ordering key apply.
//
// Finish applies the calling client's configuration as the job's worker
// would: Config.OnError, the queue's configuration from Config.Queues or
// Queues().Add (its RetryPolicy and DeleteCompleted) and, if the client
// has the kind's worker, its NextRetry. A worker may report through any
// client, so give every client that takes reports the same configuration
// for those queues and kinds.
type Leases[TTx any] struct {
	c *Client[TTx]
}

// LeaseToken is the capability for one leased attempt: the job's ID, the
// claim's fence and a random value the database generated. It is opaque to
// the worker that carries it: String and ParseLeaseToken, and the text and
// JSON forms, move it between processes. hopper does not authenticate the
// holder; a token is as good as the worker's channel to the coordinator,
// which should authenticate the worker.
type LeaseToken struct {
	id    JobID
	fence int64
	nonce driver.JobID
}

// JobID returns the ID of the token's job.
func (t LeaseToken) JobID() JobID { return t.id }

// Fence returns the fence of the claim the token belongs to. The attempt's
// snooze keeps it, so the next claim by ID of the snoozed job names it.
func (t LeaseToken) Fence() int64 { return t.fence }

// IsZero reports whether t is the zero token, which names no lease.
func (t LeaseToken) IsZero() bool { return t == LeaseToken{} }

// String returns the token's text form.
func (t LeaseToken) String() string {
	return t.id.String() + "." + strconv.FormatInt(t.fence, 10) + "." + hex.EncodeToString(t.nonce[:])
}

// ParseLeaseToken parses a token's text form.
func ParseLeaseToken(s string) (LeaseToken, error) {
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return LeaseToken{}, errors.New("hopper: malformed lease token")
	}
	var t LeaseToken
	var err error
	if t.id, err = driver.ParseJobID(parts[0]); err != nil {
		return LeaseToken{}, errors.New("hopper: malformed lease token")
	}
	if t.fence, err = strconv.ParseInt(parts[1], 10, 64); err != nil || t.fence <= 0 {
		return LeaseToken{}, errors.New("hopper: malformed lease token")
	}
	// hex.Decode panics on a destination too short for the text, which
	// comes from a remote worker.
	if len(parts[2]) != hex.EncodedLen(len(t.nonce)) {
		return LeaseToken{}, errors.New("hopper: malformed lease token")
	}
	if _, err := hex.Decode(t.nonce[:], []byte(parts[2])); err != nil {
		return LeaseToken{}, errors.New("hopper: malformed lease token")
	}
	return t, nil
}

// MarshalText implements encoding.TextMarshaler.
func (t LeaseToken) MarshalText() ([]byte, error) {
	if t.IsZero() {
		return []byte{}, nil
	}
	return []byte(t.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (t *LeaseToken) UnmarshalText(b []byte) error {
	if len(b) == 0 {
		*t = LeaseToken{}
		return nil
	}
	parsed, err := ParseLeaseToken(string(b))
	if err != nil {
		return err
	}
	*t = parsed
	return nil
}

func (t LeaseToken) fenceOf() *driver.LeaseFence {
	return &driver.LeaseFence{Token: t.nonce, Fence: t.fence}
}

// LeaseOpts are the options of a claim by ID.
type LeaseOpts struct {
	// TTL is how long the lease lasts from the claim and from each renewal,
	// by database time: DefaultJobLeaseTTL if zero. Renew about every TTL/3.
	TTL time.Duration
}

// JobLease is a job claimed by ID and its lease.
type JobLease struct {
	// Job is the job as claimed: running with no owning client, its attempt
	// and fence incremented.
	Job   *JobRow
	Token LeaseToken
	// ExpiresAt is when the lease expires unless renewed, by database time.
	ExpiresAt time.Time
}

// LeaseStatus is a job lease as renewed.
type LeaseStatus struct {
	// ExpiresAt is when the lease now expires, by database time.
	ExpiresAt time.Time
	// CancelRequested reports that JobCancel was called on the job. The
	// holder should stop, and Finish with an error, which cancels the job.
	CancelRequested bool
}

// LeaseResult ends a leased attempt as if the job's worker had returned
// Err: nil completes the job, with Output; a *SnoozeError (Snooze) snoozes
// it, which gives the attempt back and keeps the fence; a *CancelError
// (Cancel) discards it; any other error retries it, with Config.OnError,
// the kind's worker's NextRetry (when this client has the worker) and the
// retry policies applied as for a worker's error, or discards it on its
// last attempt. After JobCancel, an error cancels the job.
type LeaseResult struct {
	Err error
	// Output is the completed job's output, encoded with Config.Codec and
	// kept as SetOutput's is. It is ignored unless the attempt ends the
	// job.
	Output any
}

// Claim claims a waiting job (available, scheduled or retryable) by ID
// under a new job lease, whatever its schedule, so a job snoozed while it
// waits for its worker can be claimed early. The job runs with no owning
// client until the lease's attempt ends or expires.
//
// fence is the job's fence the caller expects, such as the Fence of the
// handler's attempt that placed the job on a worker, recorded with the
// placement, or 0 for a job never claimed. The claim succeeds only at that
// fence, so a superseded placement, one made before another attempt ran,
// cannot claim the job.
//
// It returns the first of these that applies to the job:
//
//	neither live nor in history (never inserted, or deleted
//	  by DeleteCompleted, JobDelete or retention)            ErrNotFound
//	in history, at any fence                                 ErrJobNotClaimable
//	live at another fence than fence, running or not         ErrJobFenced
//	running at fence, by a client or another lease           ErrJobRunning
//	pending, past its TTL, in a paused queue, or held back
//	  by its ordering key                                    ErrJobNotClaimable
//	available, scheduled or retryable                        claimed
//
// ErrJobFenced means that the job is not at the fence the caller expects,
// as when another attempt ran, or runs, since the one that fence names:
// the placement is superseded. ErrJobRunning is transient, such as for the
// placing attempt whose snooze is still being written: try again shortly.
// A claim of a waiting job waits for its row if another transaction holds
// it.
func (l *Leases[TTx]) Claim(ctx context.Context, id JobID, fence int64, opts *LeaseOpts) (*JobLease, error) {
	return claimByID(ctx, l.c.exec, id, fence, opts)
}

// ClaimTx is Claim inside the caller's transaction, so that the claim
// commits or rolls back with the caller's own writes, such as a placement
// record locked in the same transaction. Until tx ends it holds the job's
// row. On PostgreSQL a claim that loses a race with another claim of a job
// with the same ordering key returns ErrJobNotClaimable once the database
// has aborted tx, which must then be rolled back.
func (l *Leases[TTx]) ClaimTx(ctx context.Context, tx TTx, id JobID, fence int64, opts *LeaseOpts) (*JobLease, error) {
	return claimByID(ctx, l.c.driver.UnwrapTx(tx), id, fence, opts)
}

func claimByID(ctx context.Context, exec driver.Executor, id JobID, fence int64, opts *LeaseOpts) (*JobLease, error) {
	var o LeaseOpts
	if opts != nil {
		o = *opts
	}
	if o.TTL < 0 || fence < 0 {
		return nil, errors.New("hopper: claim job by ID: TTL and fence must not be negative")
	}
	if o.TTL == 0 {
		o.TTL = DefaultJobLeaseTTL
	}
	row, err := exec.JobClaimByID(ctx, driver.JobClaimByIDParams{ID: id, Fence: fence, TTL: o.TTL})
	if err != nil {
		return nil, err
	}
	return &JobLease{
		Job:       row.Job,
		Token:     LeaseToken{id: row.Job.ID, fence: row.Lease.Fence, nonce: row.Lease.Token},
		ExpiresAt: row.ExpiresAt,
	}, nil
}

// Renew extends the lease by its TTL from database now(), and reports a
// cancel request. It returns ErrJobFenced once the lease is no longer the
// job's: it expired, or its attempt ended. A renewal succeeds until the
// leader applies the expiry, not only until ExpiresAt, so a holder learns
// that it lost the lease from ErrJobFenced, never from its own clock.
func (l *Leases[TTx]) Renew(ctx context.Context, token LeaseToken) (*LeaseStatus, error) {
	if token.IsZero() {
		return nil, errors.New("hopper: renew job lease: zero token")
	}
	st, err := l.c.exec.JobLeaseRenew(ctx, driver.JobLeaseRenewParams{ID: token.id, Lease: *token.fenceOf()})
	if err != nil {
		return nil, err
	}
	return &LeaseStatus{ExpiresAt: st.ExpiresAt, CancelRequested: st.CancelRequested}, nil
}

// RecordStep records that the leased attempt finished step, with an
// optional cursor, as RecordStep does for a worker, and returns the
// progress as stored. It returns ErrJobFenced once the lease is no longer
// the job's.
func (l *Leases[TTx]) RecordStep(ctx context.Context, token LeaseToken, step string, cursor any) (*Progress, error) {
	return l.record(ctx, l.c.exec, token, step, false, cursor)
}

// RecordStepTx is RecordStep inside the caller's transaction. Until tx ends
// it holds the job's row, which the calls on the pool for the attempt wait
// for.
func (l *Leases[TTx]) RecordStepTx(ctx context.Context, tx TTx, token LeaseToken, step string, cursor any) (*Progress, error) {
	return l.record(ctx, l.c.driver.UnwrapTx(tx), token, step, false, cursor)
}

// RecordCursor records a cursor within the step under way, keeping the last
// finished step, as RecordCursor does for a worker.
func (l *Leases[TTx]) RecordCursor(ctx context.Context, token LeaseToken, cursor any) (*Progress, error) {
	return l.record(ctx, l.c.exec, token, "", true, cursor)
}

// RecordCursorTx is RecordCursor inside the caller's transaction.
func (l *Leases[TTx]) RecordCursorTx(ctx context.Context, tx TTx, token LeaseToken, cursor any) (*Progress, error) {
	return l.record(ctx, l.c.driver.UnwrapTx(tx), token, "", true, cursor)
}

func (l *Leases[TTx]) record(ctx context.Context, exec driver.Executor, token LeaseToken, step string, keepStep bool, cursor any) (*Progress, error) {
	if token.IsZero() {
		return nil, errors.New("hopper: record job progress: zero lease token")
	}
	raw, err := progressCursor(token.id, step, keepStep, cursor)
	if err != nil {
		return nil, err
	}
	p, err := exec.JobRecordProgress(ctx, driver.JobRecordProgressParams{
		ID: token.id, Lease: token.fenceOf(), Step: step, KeepStep: keepStep, Cursor: raw,
	})
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, ErrJobFenced
	}
	return p, nil
}

// Finish ends the leased attempt with res, as if the job's worker had
// returned (see LeaseResult), and releases the lease. The result is written
// before Finish returns, so a worker's report is durable once acknowledged.
// It returns ErrJobFenced, and changes nothing, once the lease is no longer
// the job's. The job's batch and workflow dependents are updated as for any
// finished job, and the client's events and Await see the result.
// Config.OnError runs before the result is written, as for a worker, so it
// can run for a Finish that then returns ErrJobFenced, such as one whose
// token is stale or made up but names the current fence.
func (l *Leases[TTx]) Finish(ctx context.Context, token LeaseToken, res LeaseResult) error {
	row, f, err := l.result(ctx, l.c.exec, token, res)
	if err != nil {
		return err
	}
	if err := l.write(ctx, l.c.exec, row, f); err != nil {
		return err
	}
	c := l.c
	c.logOutcome(ctx, row, f, res.Err)
	c.finalized(row, f)
	if f.Delay == 0 && !f.State.Terminal() {
		c.wakeQueue(row.Queue)
		c.notifier.mark(row.Queue)
	}
	if f.State.Terminal() && !row.BatchID.IsZero() {
		// As the finalizer follows up a flush: of two last jobs of a batch
		// or a dependent finished at once, the one that follows up second
		// completes it.
		settleAndPromote(ctx, c.exec, c.logger, []JobID{row.BatchID}, []JobID{row.ID})
	}
	return nil
}

// FinishTx is Finish inside the caller's transaction, so that the attempt's
// result commits or rolls back with the caller's own writes. Until tx ends
// it holds the job's row. The job's batch and dependents are updated as
// for any finished job, the batch's completion and the dependents'
// promotion within a leader interval of the commit. As the client cannot
// know whether tx commits, it emits no event for the result.
func (l *Leases[TTx]) FinishTx(ctx context.Context, tx TTx, token LeaseToken, res LeaseResult) error {
	exec := l.c.driver.UnwrapTx(tx)
	row, f, err := l.result(ctx, exec, token, res)
	if err != nil {
		return err
	}
	if err := l.write(ctx, exec, row, f); err != nil {
		return err
	}
	if f.Delay == 0 && !f.State.Terminal() {
		return exec.Notify(ctx, driver.ChannelInsert, []string{row.Queue})
	}
	return nil
}

// result reads the leased job and turns res into its result, as the
// outcome of a worker's attempt.
func (l *Leases[TTx]) result(ctx context.Context, exec driver.Executor, token LeaseToken, res LeaseResult) (*driver.JobRow, driver.JobFinalize, error) {
	if token.IsZero() {
		return nil, driver.JobFinalize{}, errors.New("hopper: finish leased attempt: zero token")
	}
	row, err := exec.JobGet(ctx, token.id)
	if errors.Is(err, driver.ErrNotFound) {
		return nil, driver.JobFinalize{}, ErrJobFenced
	}
	if err != nil {
		return nil, driver.JobFinalize{}, err
	}
	if row.State != driver.JobStateRunning || row.AttemptedBy != 0 || row.Fence != token.fence {
		return nil, driver.JobFinalize{}, ErrJobFenced
	}
	c := l.c
	qcfg := c.cfg.Queues[row.Queue]
	c.mu.Lock()
	if p := c.producers[row.Queue]; p != nil {
		qcfg = p.cfg
	}
	c.mu.Unlock()
	// The attempt's context, for outcome: canceled, with JobCancel's
	// cause, if a cancel was requested. The lease generation is not the
	// caller's context, which ending must not read as the client stopping.
	jobCtx := context.Background()
	if !row.CancelRequestedAt.IsZero() {
		var cancel context.CancelCauseFunc
		jobCtx, cancel = context.WithCancelCause(jobCtx)
		cancel(ErrJobCanceled)
	}
	// The kind's worker, if this client has one, chooses the retry delay as
	// it would for its own attempt (NextRetry, and an ordered delivery's
	// cap); one whose args do not decode leaves it to the retry policies.
	// Only a failed attempt's retry asks it, so a success decodes nothing.
	var unit workUnit
	if info, ok := c.workers.lookup(row.Kind); ok && res.Err != nil {
		if u, err := info.newUnit(row, c.cfg.Codec); err == nil {
			unit = u
		}
	}
	f := c.outcome(context.WithoutCancel(ctx), jobCtx, row, qcfg, unit, res.Err, true)
	f.AttemptedBy, f.Lease = 0, token.fenceOf()
	if f.State.Terminal() && res.Output != nil {
		data, err := c.cfg.Codec.Marshal(res.Output)
		if err != nil {
			return nil, driver.JobFinalize{}, fmt.Errorf("hopper: encode output: %w", err)
		}
		f.Output = data
	}
	return row, f, nil
}

func (l *Leases[TTx]) write(ctx context.Context, exec driver.Executor, row *driver.JobRow, f driver.JobFinalize) error {
	applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{
		Jobs: []driver.JobFinalize{f}, NoBatches: row.BatchID.IsZero(), KeepLogs: l.c.cfg.JobLogs.MaxAttempts,
	})
	if err != nil {
		return err
	}
	if len(applied) == 0 {
		return ErrJobFenced
	}
	return nil
}
