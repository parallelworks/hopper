package pgsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// Job leases. A job claimed by ID runs with no owning client
// (attempted_by NULL) and a row in hopper_job_leases owns it: the token the
// claim generated, the claim's fence and the lease's expiry. Every statement
// on a leased job checks the lease, and every one that touches both rows
// locks the job's row first, then the lease's; a renewal locks only the
// lease's and reads the job's without a lock, so none of them can deadlock
// another.

// jobClaimByIDSQL claims one waiting job by ID under a new lease, whatever
// its schedule, unless its queue is paused, its TTL has passed or its
// ordering key holds it back, and only at the fence $2. A lease row
// whose job is not running in it, which only direct SQL leaves, is never
// read, and the job's next claim by ID replaces it. The queue's limits do
// not apply. The claim's time is the statement's, not now(): in a caller's
// transaction (ClaimTx) that began earlier, now() would shorten the lease
// by the transaction's age, or start it expired, date the attempt before
// the claim, and claim a job whose TTL passed while the transaction ran.
var jobClaimByIDSQL = fmt.Sprintf(`
WITH claimed AS (
  UPDATE {{schema}}.hopper_jobs j
  SET state = 'running', attempt = j.attempt + 1, fence = j.fence + 1, attempted_at = statement_timestamp(), attempted_by = NULL,
      snooze_reason = NULL
  WHERE j.id = $1 AND j.state IN ('available', 'scheduled', 'retryable')
    AND j.fence = $2
    AND (j.expires_at IS NULL OR j.expires_at > statement_timestamp())
    AND NOT EXISTS (SELECT 1 FROM {{schema}}.hopper_queues q WHERE q.name = j.queue AND q.paused_at IS NOT NULL)
    AND (j.ordering_key IS NULL OR (
      NOT EXISTS (SELECT 1 FROM {{schema}}.hopper_jobs r
                  WHERE r.queue = j.queue AND r.ordering_key = j.ordering_key AND r.state = 'running')
      AND j.seq = (SELECT min(o.seq) FROM {{schema}}.hopper_jobs o
                   WHERE o.queue = j.queue AND o.ordering_key = j.ordering_key
                     AND o.state IN ('available', 'scheduled', 'retryable'))))
  RETURNING %s
),
lease AS (
  INSERT INTO {{schema}}.hopper_job_leases (id, token, fence, ttl, renewed_at, expires_at)
  SELECT id, gen_random_uuid(), fence, make_interval(secs => $3::float8), statement_timestamp(),
    statement_timestamp() + make_interval(secs => $3::float8)
  FROM claimed
  ON CONFLICT (id) DO UPDATE SET token = EXCLUDED.token, fence = EXCLUDED.fence, ttl = EXCLUDED.ttl,
    renewed_at = EXCLUDED.renewed_at, expires_at = EXCLUDED.expires_at
  RETURNING id, token, expires_at
)
SELECT %s, `+claimDependenciesSQL+`, lease.token, lease.expires_at FROM claimed JOIN lease ON lease.id = claimed.id`,
	JobColumns("j."), JobColumns("claimed."))

// JobClaimByID implements driver.Executor.
func (e *Executor) JobClaimByID(ctx context.Context, params driver.JobClaimByIDParams) (*driver.JobLeaseRow, error) {
	if params.TTL < time.Microsecond {
		return nil, errors.New("hopper: claim job by ID: the lease TTL must be at least a microsecond")
	}
	var lease driver.JobLeaseRow
	job, err := scanClaimed(e.Conn.QueryRow(ctx, jobClaimByIDSQL, uuidParam(params.ID), params.Fence, params.TTL.Seconds()),
		&lease.Lease.Token, &lease.ExpiresAt)
	switch {
	case err == nil:
		lease.Job, lease.Lease.Fence = job, job.Fence
		return &lease, nil
	case isUniqueViolation(err):
		// Another job of the ordering key started running meanwhile.
		return nil, fmt.Errorf("%w: another job of its ordering key is running", driver.ErrJobNotClaimable)
	case !errors.Is(err, ErrNoRows):
		return nil, fmt.Errorf("hopper: claim job by ID: %w", err)
	}
	return nil, e.whyNotClaimed(ctx, params)
}

// whyNotClaimed tells why JobClaimByID claimed nothing, from the job as it
// is now, by the claim's clock.
func (e *Executor) whyNotClaimed(ctx context.Context, params driver.JobClaimByIDParams) error {
	var (
		state, queue string
		fence        int64
		expired      bool
		paused       bool
		orderingKey  sql.NullString
	)
	err := e.Conn.QueryRow(ctx, `
SELECT j.state, j.queue, j.fence, coalesce(j.expires_at <= statement_timestamp(), false), j.ordering_key,
  EXISTS (SELECT 1 FROM {{schema}}.hopper_queues q WHERE q.name = j.queue AND q.paused_at IS NOT NULL)
FROM {{schema}}.hopper_jobs j WHERE j.id = $1`, uuidParam(params.ID)).Scan(&state, &queue, &fence, &expired, &orderingKey, &paused)
	if errors.Is(err, ErrNoRows) {
		// JobGet reads history, and the live table again: a job moved back
		// into it between the two reads, only by direct SQL, reads as
		// finished.
		if _, err := e.JobGet(ctx, params.ID); err != nil {
			return err
		}
		return fmt.Errorf("%w: it has finished", driver.ErrJobNotClaimable)
	}
	if err != nil {
		return fmt.Errorf("hopper: claim job by ID: %w", err)
	}
	switch {
	case fence != params.Fence:
		// Before the running state: a job running at another fence runs a
		// later attempt than the caller's, not one to wait for.
		return fmt.Errorf("%w: its fence is %d, not %d", driver.ErrJobFenced, fence, params.Fence)
	case state == string(driver.JobStateRunning):
		return driver.ErrJobRunning
	case state == string(driver.JobStatePending):
		return fmt.Errorf("%w: it is pending", driver.ErrJobNotClaimable)
	case expired:
		return fmt.Errorf("%w: its TTL has passed", driver.ErrJobNotClaimable)
	case paused:
		return fmt.Errorf("%w: queue %q is paused", driver.ErrJobNotClaimable, queue)
	case orderingKey.Valid:
		// Assumed, not checked: a keyed job whose attempt at the fence
		// ended between the two statements, as below, reads as held back
		// rather than ErrJobRunning.
		return fmt.Errorf("%w: its ordering key holds it back", driver.ErrJobNotClaimable)
	}
	// A claim between the two statements would have moved the fence. So
	// the job ran at the fence when the claim read it, and its attempt
	// ended since keeping the fence (a snooze or a retry), or its queue
	// was resumed since: the caller tries again, as for a running job.
	return driver.ErrJobRunning
}

// jobLeaseRenewSQL extends a lease while it is the job's: the job runs with
// no client at the lease's fence. It locks the lease row alone. The new
// expiry counts from the statement's start, so a renewal that waited on
// the lease row for longer than the TTL, behind a FinishTx that then rolled
// back, returns an expiry already past.
var jobLeaseRenewSQL = `
UPDATE {{schema}}.hopper_job_leases l SET renewed_at = now(), expires_at = now() + l.ttl
FROM {{schema}}.hopper_jobs j
WHERE l.id = $1 AND l.token = $2::uuid AND l.fence = $3
  AND j.id = l.id AND ` + isRunning("j.state") + ` AND j.attempted_by IS NULL AND j.fence = l.fence
RETURNING l.expires_at, j.cancel_requested_at IS NOT NULL`

// JobLeaseRenew implements driver.Executor.
func (e *Executor) JobLeaseRenew(ctx context.Context, params driver.JobLeaseRenewParams) (*driver.JobLeaseStatus, error) {
	var st driver.JobLeaseStatus
	err := e.Conn.QueryRow(ctx, jobLeaseRenewSQL, uuidParam(params.ID), uuidParam(params.Lease.Token), params.Lease.Fence).
		Scan(&st.ExpiresAt, &st.CancelRequested)
	if errors.Is(err, ErrNoRows) {
		return nil, driver.ErrJobFenced
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: renew job lease: %w", err)
	}
	return &st, nil
}

// leaseDueSQL is the condition that the lease l is due. Only the starts of
// clients whose leases are live count: a client that started after the
// renewal and lapsed before the cluster went down keeps its row until a
// leader prunes it, after the first expiry once the cluster is back. A
// start later than now(), which only a clock set ahead and then corrected
// leaves, counts once the clock reaches it: counted before, it would hold
// every lease renewed before it until then.
const leaseDueSQL = `l.expires_at <= now()
    AND coalesce((SELECT min(c.started_at) FROM {{schema}}.hopper_clients c
                  WHERE c.started_at > l.renewed_at AND c.started_at <= now() AND c.expires_at > now())
      + greatest(l.ttl, make_interval(secs => $5::float8)), l.expires_at) <= now()`

// jobLeaseExpiredError is the error entry of an attempt whose lease expired.
const jobLeaseExpiredError = `jsonb_build_object('at', now(), 'attempt', j.attempt, 'fence', j.fence,
      'error', 'hopper: job lease expired', 'rescue', 'lease_expired')`

// jobExpireLeasesSQL ends the attempts of jobs whose leases expired, in one
// statement. It locks the jobs' rows, skipping those another statement
// holds (a Finish in flight), then deletes their leases, checking the
// expiry again on the lease row as it is once locked: a renewal that
// committed meanwhile keeps its lease, and its job is left running. A job
// with attempts left is retryable at once; one on its last attempt is
// discarded, and one with a cancel requested is canceled. Which of these
// is decided in due, from the job's row as locked: retry and finished scan
// the statement's snapshot, which a JobCancel committed after it began does
// not show, and deciding there would leave such a job in neither, running
// with its lease gone. A lease is due at its expiry or, if a client whose
// lease is live started after the lease's last renewal, at the first such
// start plus the larger of its TTL and the grace ($5), so that a holder who
// could not renew while no client ran gets that long from the cluster's
// restart; nothing is due unless the expiring client's own lease ($4) is
// live.
var jobExpireLeasesSQL = `
WITH RECURSIVE due AS (
  SELECT j.id, l.fence, j.attempt < j.max_attempts AND j.cancel_requested_at IS NULL AS retry
  FROM {{schema}}.hopper_job_leases l
  JOIN {{schema}}.hopper_jobs j ON j.id = l.id
  WHERE ` + leaseDueSQL + ` AND ` + isRunning("j.state") + ` AND j.attempted_by IS NULL AND j.fence = l.fence
    AND EXISTS (SELECT 1 FROM {{schema}}.hopper_clients me WHERE me.id = $4 AND me.expires_at > now())
  ORDER BY l.expires_at
  LIMIT $1
  FOR UPDATE OF j SKIP LOCKED
),
released AS (
  DELETE FROM {{schema}}.hopper_job_leases l USING due
  WHERE l.id = due.id AND l.fence = due.fence AND ` + leaseDueSQL + `
  RETURNING l.id, l.fence, due.retry
),
retry AS (
  UPDATE {{schema}}.hopper_jobs j
  SET state = 'retryable', scheduled_at = now(), wake_requested_at = NULL,
      errors = j.errors || ` + jobLeaseExpiredError + `
  FROM released r
  WHERE j.id = r.id AND j.fence = r.fence AND j.attempted_by IS NULL AND r.retry
  RETURNING ` + JobColumns("j.") + `
),
finished AS (
  DELETE FROM {{schema}}.hopper_jobs j
  USING released r
  WHERE j.id = r.id AND j.fence = r.fence AND j.attempted_by IS NULL AND NOT r.retry
  RETURNING ` + finishedColumns("j.errors || "+jobLeaseExpiredError, "j.logs", "NULL::jsonb", "true",
	"CASE WHEN j.cancel_requested_at IS NOT NULL THEN 'canceled' ELSE 'discarded' END", "$2") + `
),
` + finalizeTailSQL("$2", "$3") + `
SELECT ` + finishedSelectColumns + ` FROM finished
UNION ALL
SELECT ` + strings.Replace(JobColumns(""), ", state,", ", state::text,", 1) + `, NULL::timestamptz, NULL::jsonb FROM retry`

// JobExpireLeases implements driver.Executor. It waits only briefly for
// rows another transaction holds (briefly), expiring nothing if it has to.
func (e *Executor) JobExpireLeases(ctx context.Context, params driver.JobExpireLeasesParams) ([]*driver.JobRow, error) {
	jobs, err := briefly(ctx, e, func(c Conn) ([]*driver.JobRow, error) {
		return historyJobs(c.Query(ctx, jobExpireLeasesSQL, max(params.Limit, 1), driver.ChannelDone, driver.ChannelInsert,
			params.ClientID, max(params.Grace, 0).Seconds()))
	})
	if errors.Is(err, driver.ErrRowsHeld) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: expire job leases: %w", err)
	}
	return jobs, nil
}
