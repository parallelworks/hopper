package hopper

import (
	"context"
	"encoding/hex"
	"encoding/json"
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
// TTL expires: the leader makes the job retryable at once, with an entry in
// its errors whose Rescue is RescueLeaseExpired and whose Fence is the
// lease's, or, on its last attempt, discards it, or cancels it if JobCancel
// was called. hopper never runs a leased attempt's work itself.
//
// The queue's pause applies to a claim by ID; its limits (GlobalLimit,
// PartitionLimit, RateLimit) do not, and leased jobs do not count toward
// them. The job's TTL and ordering key apply.
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

// Fence returns the fence of the claim the token belongs to.
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
	if n, err := hex.Decode(t.nonce[:], []byte(parts[2])); err != nil || n != len(t.nonce) || len(parts[2]) != 2*len(t.nonce) {
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
	// IfFence, when not zero, claims the job only if its fence still is
	// IfFence, and otherwise returns ErrJobFenced: a coordinator that read
	// the job and its own placement record claims only if no attempt ran
	// since, a superseded placement included.
	IfFence int64
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
// (Cancel) discards it; any other error retries it, with Config.OnError
// and the retry policies applied as for a worker's error, or discards it on
// its last attempt. After JobCancel, an error cancels the job.
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
// It returns ErrNotFound for an unknown job; ErrJobRunning for a running
// one, whether a client or another lease runs it, or whose result is still
// being written (try again shortly); ErrJobFenced when opts.IfFence is set
// and the job's fence is not it; and ErrJobNotClaimable for a job that is
// pending, finalized or past its TTL, whose ordering key holds it back, or
// whose queue is paused. It waits for the job's row if another transaction
// holds it.
func (l *Leases[TTx]) Claim(ctx context.Context, id JobID, opts *LeaseOpts) (*JobLease, error) {
	return claimByID(ctx, l.c.exec, id, opts)
}

// ClaimTx is Claim inside the caller's transaction, so that the claim
// commits or rolls back with the caller's own writes, such as a placement
// record locked in the same transaction. Until tx ends it holds the job's
// row.
func (l *Leases[TTx]) ClaimTx(ctx context.Context, tx TTx, id JobID, opts *LeaseOpts) (*JobLease, error) {
	return claimByID(ctx, l.c.driver.UnwrapTx(tx), id, opts)
}

func claimByID(ctx context.Context, exec driver.Executor, id JobID, opts *LeaseOpts) (*JobLease, error) {
	var o LeaseOpts
	if opts != nil {
		o = *opts
	}
	if o.TTL < 0 || o.IfFence < 0 {
		return nil, errors.New("hopper: claim job by ID: TTL and IfFence must not be negative")
	}
	if o.TTL == 0 {
		o.TTL = DefaultJobLeaseTTL
	}
	row, err := exec.JobClaimByID(ctx, driver.JobClaimByIDParams{ID: id, IfFence: o.IfFence, TTL: o.TTL})
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
	return l.record(ctx, l.c.exec, false, token, step, false, cursor)
}

// RecordStepTx is RecordStep inside the caller's transaction. Until tx ends
// it holds the job's row; a RecordStep or RecordCursor on the pool
// meanwhile returns ErrJobLocked.
func (l *Leases[TTx]) RecordStepTx(ctx context.Context, tx TTx, token LeaseToken, step string, cursor any) (*Progress, error) {
	return l.record(ctx, l.c.driver.UnwrapTx(tx), true, token, step, false, cursor)
}

// RecordCursor records a cursor within the step under way, keeping the last
// finished step, as RecordCursor does for a worker.
func (l *Leases[TTx]) RecordCursor(ctx context.Context, token LeaseToken, cursor any) (*Progress, error) {
	return l.record(ctx, l.c.exec, false, token, "", true, cursor)
}

// RecordCursorTx is RecordCursor inside the caller's transaction.
func (l *Leases[TTx]) RecordCursorTx(ctx context.Context, tx TTx, token LeaseToken, cursor any) (*Progress, error) {
	return l.record(ctx, l.c.driver.UnwrapTx(tx), true, token, "", true, cursor)
}

func (l *Leases[TTx]) record(ctx context.Context, exec driver.Executor, inTx bool, token LeaseToken, step string, keepStep bool, cursor any) (*Progress, error) {
	if token.IsZero() {
		return nil, errors.New("hopper: record job progress: zero lease token")
	}
	if !keepStep && !storableStep(step) {
		return nil, fmt.Errorf("hopper: record the progress of job %s: step %q holds a NUL or invalid UTF-8, which PostgreSQL cannot store", token.id, step)
	}
	var raw json.RawMessage
	if cursor != nil {
		b, err := json.Marshal(cursor)
		if err != nil {
			return nil, fmt.Errorf("hopper: record the progress of job %s: encode the cursor: %w", token.id, err)
		}
		if string(b) != "null" {
			raw = b
		}
	}
	p, err := exec.JobRecordProgress(ctx, driver.JobRecordProgressParams{
		ID: token.id, Lease: token.fenceOf(), Step: step, KeepStep: keepStep, Cursor: raw, NoWait: !inTx,
	})
	if errors.Is(err, driver.ErrJobLocked) {
		return nil, fmt.Errorf("hopper: record the progress of job %s: a transaction holds the job's row, such as one of RecordStepTx, RecordCursorTx or FinishTx, which must end first: %w", token.id, err)
	}
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
		opCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), flushTimeout)
		defer cancel()
		if _, err := c.exec.BatchSettle(opCtx, driver.BatchSettleParams{IDs: []JobID{row.BatchID}}); err != nil {
			c.logger.WarnContext(ctx, "hopper: settle batches; the leader will", "count", 1, "error", err)
		}
		if _, err := c.exec.WorkflowPromoteReady(opCtx, driver.WorkflowPromoteParams{DependsOn: []JobID{row.ID}}); err != nil {
			c.logger.WarnContext(ctx, "hopper: promote dependents; the leader will", "count", 1, "error", err)
		}
	}
	return nil
}

// FinishTx is Finish inside the caller's transaction, so that the attempt's
// result commits or rolls back with the caller's own writes. Until tx ends
// it holds the job's row. The job's batch and dependents are updated as
// for any finished job, the batch's completion and the dependents'
// promotion within a leader interval of the commit.
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
	// The attempt's context, for outcome: canceled, with JobCancel's
	// cause, if a cancel was requested. The lease generation is not the
	// caller's context, which ending must not read as the client stopping.
	jobCtx := context.Background()
	if !row.CancelRequestedAt.IsZero() {
		var cancel context.CancelCauseFunc
		jobCtx, cancel = context.WithCancelCause(jobCtx)
		cancel(ErrJobCanceled)
	}
	f := c.outcome(context.WithoutCancel(ctx), jobCtx, row, qcfg, nil, res.Err, true)
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
