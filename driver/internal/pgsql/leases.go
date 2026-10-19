package pgsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

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
// ordering key holds it back, and, with $2, only at that fence. A lease row
// whose job is not running in it, which only direct SQL leaves, is never
// read, and the job's next claim by ID replaces it. The queue's limits do
// not apply.
var jobClaimByIDSQL = fmt.Sprintf(`
WITH claimed AS (
  UPDATE {{schema}}.hopper_jobs j
  SET state = 'running', attempt = j.attempt + 1, fence = j.fence + 1, attempted_at = now(), attempted_by = NULL,
      snooze_reason = NULL
  WHERE j.id = $1 AND j.state IN ('available', 'scheduled', 'retryable')
    AND ($2::bigint = 0 OR j.fence = $2)
    AND (j.expires_at IS NULL OR j.expires_at > now())
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
  SELECT id, gen_random_uuid(), fence, make_interval(secs => $3::float8), now(), now() + make_interval(secs => $3::float8)
  FROM claimed
  ON CONFLICT (id) DO UPDATE SET token = EXCLUDED.token, fence = EXCLUDED.fence, ttl = EXCLUDED.ttl,
    renewed_at = EXCLUDED.renewed_at, expires_at = EXCLUDED.expires_at
  RETURNING id, token, expires_at
)
SELECT %s, lease.token, lease.expires_at FROM claimed JOIN lease ON lease.id = claimed.id`, JobColumns("j."), JobColumns("claimed."))

// JobClaimByID implements driver.Executor.
func (e *Executor) JobClaimByID(ctx context.Context, params driver.JobClaimByIDParams) (*driver.JobLeaseRow, error) {
	if params.TTL <= 0 {
		return nil, errors.New("hopper: claim job by ID: the lease TTL must be positive")
	}
	var lease driver.JobLeaseRow
	job, err := scanJob(e.Conn.QueryRow(ctx, jobClaimByIDSQL, uuidParam(params.ID), params.IfFence, params.TTL.Seconds()),
		false, &lease.Lease.Token, &lease.ExpiresAt)
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
// is now.
func (e *Executor) whyNotClaimed(ctx context.Context, params driver.JobClaimByIDParams) error {
	var (
		state, queue string
		fence        int64
		expired      bool
		paused       bool
		orderingKey  sql.NullString
	)
	err := e.Conn.QueryRow(ctx, `
SELECT j.state, j.queue, j.fence, coalesce(j.expires_at <= now(), false), j.ordering_key,
  EXISTS (SELECT 1 FROM {{schema}}.hopper_queues q WHERE q.name = j.queue AND q.paused_at IS NOT NULL)
FROM {{schema}}.hopper_jobs j WHERE j.id = $1`, uuidParam(params.ID)).Scan(&state, &queue, &fence, &expired, &orderingKey, &paused)
	if errors.Is(err, ErrNoRows) {
		if _, err := e.JobGet(ctx, params.ID); err != nil {
			return err
		}
		return fmt.Errorf("%w: it has finished", driver.ErrJobNotClaimable)
	}
	if err != nil {
		return fmt.Errorf("hopper: claim job by ID: %w", err)
	}
	switch {
	case state == string(driver.JobStateRunning):
		return driver.ErrJobRunning
	case state == string(driver.JobStatePending):
		return fmt.Errorf("%w: it is pending", driver.ErrJobNotClaimable)
	case params.IfFence != 0 && fence != params.IfFence:
		return driver.ErrJobFenced
	case expired:
		return fmt.Errorf("%w: its TTL has passed", driver.ErrJobNotClaimable)
	case paused:
		return fmt.Errorf("%w: queue %q is paused", driver.ErrJobNotClaimable, queue)
	case orderingKey.Valid:
		return fmt.Errorf("%w: its ordering key holds it back", driver.ErrJobNotClaimable)
	}
	// It was claimed and finished, or claimed, between the two statements.
	return driver.ErrJobRunning
}

// jobLeaseRenewSQL extends a lease while it is the job's: the job runs with
// no client at the lease's fence. It locks the lease row alone.
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

// jobLeaseExpiredError is the error entry of an attempt whose lease expired.
const jobLeaseExpiredError = `jsonb_build_object('at', now(), 'attempt', j.attempt, 'fence', j.fence,
      'error', 'hopper: job lease expired', 'rescue', 'lease_expired')`

// jobExpireLeasesSQL ends the attempts of jobs whose leases expired, in one
// statement. It locks the jobs' rows, skipping those another statement
// holds (a Finish in flight), then deletes their leases, checking the
// expiry again on the lease row as it is once locked: a renewal that
// committed meanwhile keeps its lease, and its job is left running. A job
// with attempts left is retryable at once; one on its last attempt is
// discarded, and one with a cancel requested is canceled.
var jobExpireLeasesSQL = `
WITH RECURSIVE due AS (
  SELECT j.id, l.fence FROM {{schema}}.hopper_job_leases l
  JOIN {{schema}}.hopper_jobs j ON j.id = l.id
  WHERE l.expires_at <= now() AND ` + isRunning("j.state") + ` AND j.attempted_by IS NULL AND j.fence = l.fence
  ORDER BY l.expires_at
  LIMIT $1
  FOR UPDATE OF j SKIP LOCKED
),
released AS (
  DELETE FROM {{schema}}.hopper_job_leases l USING due
  WHERE l.id = due.id AND l.fence = due.fence AND l.expires_at <= now()
  RETURNING l.id, l.fence
),
retry AS (
  UPDATE {{schema}}.hopper_jobs j
  SET state = 'retryable', scheduled_at = now(), wake_requested_at = NULL,
      errors = j.errors || ` + jobLeaseExpiredError + `
  FROM released r
  WHERE j.id = r.id AND j.fence = r.fence AND j.attempted_by IS NULL
    AND j.attempt < j.max_attempts AND j.cancel_requested_at IS NULL
  RETURNING ` + JobColumns("j.") + `
),
finished AS (
  DELETE FROM {{schema}}.hopper_jobs j
  USING released r
  WHERE j.id = r.id AND j.fence = r.fence AND j.attempted_by IS NULL
    AND (j.attempt >= j.max_attempts OR j.cancel_requested_at IS NOT NULL)
  RETURNING ` + finishedColumns("j.errors || "+jobLeaseExpiredError, "j.logs", "NULL::jsonb", "true",
	"CASE WHEN j.cancel_requested_at IS NOT NULL THEN 'canceled' ELSE 'discarded' END", "$2") + `
),
` + finalizeTailSQL("$2", "$3") + `
SELECT ` + finishedSelectColumns + ` FROM finished
UNION ALL
SELECT ` + strings.Replace(JobColumns(""), ", state,", ", state::text,", 1) + `, NULL::timestamptz, NULL::jsonb FROM retry`

// JobExpireLeases implements driver.Executor.
func (e *Executor) JobExpireLeases(ctx context.Context, limit int) ([]*driver.JobRow, error) {
	jobs, err := historyJobs(e.Conn.Query(ctx, jobExpireLeasesSQL, max(limit, 1), driver.ChannelDone, driver.ChannelInsert))
	if err != nil {
		return nil, fmt.Errorf("hopper: expire job leases: %w", err)
	}
	return jobs, nil
}
