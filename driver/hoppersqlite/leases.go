package hoppersqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// Job leases. A job claimed by ID runs with no owning client
// (attempted_by NULL) and a row in hopper_job_leases owns it: the token the
// claim generated with randomblob, the claim's fence and the lease's
// expiry. Every write on a leased job checks the lease in a transaction
// that holds the write lock, so a renewal, a result and an expiry of one
// lease apply one after another, each seeing what the one before wrote.

// leaseOwnerSQL is the condition, on a hopper_jobs row aliased j, that the
// job runs in the job lease whose token is ?2 at the fence ?3.
const leaseOwnerSQL = `j.state = 'running' AND j.attempted_by IS NULL AND j.fence = ?3
  AND EXISTS (SELECT 1 FROM hopper_job_leases l WHERE l.id = j.id AND l.token = ?2 AND l.fence = ?3)`

// JobClaimByID implements driver.Executor.
func (e *executor) JobClaimByID(ctx context.Context, params driver.JobClaimByIDParams) (*driver.JobLeaseRow, error) {
	if params.TTL < time.Microsecond {
		return nil, errors.New("hopper: claim job by ID: the lease TTL must be at least a microsecond")
	}
	var lease *driver.JobLeaseRow
	err := e.write(ctx, func(x *w) error {
		if err := x.claimableByID(ctx, params); err != nil {
			return err
		}
		job, err := scanJob(x.QueryRowContext(ctx, `
			UPDATE hopper_jobs SET state = 'running', attempt = attempt + 1, fence = fence + 1, attempted_at = ?1,
			  attempted_by = NULL, snooze_reason = NULL
			WHERE id = ?2 AND state IN ('available', 'scheduled', 'retryable') RETURNING `+jobColumns, x.us(), idParam(params.ID)), false)
		if err != nil {
			return err
		}
		l := &driver.JobLeaseRow{Job: job, Lease: driver.LeaseFence{Fence: job.Fence}}
		if err := x.QueryRowContext(ctx, `
			INSERT INTO hopper_job_leases (id, token, fence, ttl, renewed_at, expires_at) VALUES (?1, randomblob(16), ?2, ?3, ?4, ?5)
			ON CONFLICT (id) DO UPDATE SET token = excluded.token, fence = excluded.fence, ttl = excluded.ttl,
			  renewed_at = excluded.renewed_at, expires_at = excluded.expires_at
			RETURNING token`, idParam(params.ID), job.Fence, params.TTL.Microseconds(), x.us(), x.at(params.TTL)).Scan(&l.Lease.Token); err != nil {
			return err
		}
		l.ExpiresAt = micros{sql.NullInt64{Int64: x.at(params.TTL), Valid: true}}.time()
		lease = l
		return nil
	})
	if err != nil {
		if isDriverError(err) {
			return nil, err
		}
		return nil, fmt.Errorf("hopper: claim job by ID: %w", err)
	}
	return lease, nil
}

func isDriverError(err error) bool {
	return errors.Is(err, driver.ErrNotFound) || errors.Is(err, driver.ErrJobRunning) ||
		errors.Is(err, driver.ErrJobFenced) || errors.Is(err, driver.ErrJobNotClaimable)
}

// claimableByID returns nil if the job can be claimed by ID now, and the
// error JobClaimByID returns if not.
func (x *w) claimableByID(ctx context.Context, params driver.JobClaimByIDParams) error {
	var (
		state, queue string
		fence        int64
		expiresAt    micros
		orderingKey  sql.NullString
		paused, busy bool
	)
	// busy compares with IS NOT, which is NULL-safe: a pending job's key
	// can have no waiting job, and a NULL would fail the scan.
	err := x.QueryRowContext(ctx, `
		SELECT j.state, j.queue, j.fence, j.expires_at, j.ordering_key,
		  EXISTS (SELECT 1 FROM hopper_queues q WHERE q.name = j.queue AND q.paused_at IS NOT NULL),
		  j.ordering_key IS NOT NULL AND (
		    EXISTS (SELECT 1 FROM hopper_jobs r WHERE r.queue = j.queue AND r.ordering_key = j.ordering_key AND r.state = 'running')
		    OR j.seq IS NOT (SELECT min(o.seq) FROM hopper_jobs o WHERE o.queue = j.queue AND o.ordering_key = j.ordering_key
		                   AND o.state IN ('available', 'scheduled', 'retryable')))
		FROM hopper_jobs j WHERE j.id = ?`, idParam(params.ID)).Scan(&state, &queue, &fence, &expiresAt, &orderingKey, &paused, &busy)
	if errors.Is(err, sql.ErrNoRows) {
		var n int
		if err := x.QueryRowContext(ctx, "SELECT count(*) FROM hopper_job_history WHERE id = ?", idParam(params.ID)).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			return driver.ErrNotFound
		}
		return fmt.Errorf("%w: it has finished", driver.ErrJobNotClaimable)
	}
	if err != nil {
		return err
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
	case expiresAt.Valid && expiresAt.Int64 <= x.us():
		return fmt.Errorf("%w: its TTL has passed", driver.ErrJobNotClaimable)
	case paused:
		return fmt.Errorf("%w: queue %q is paused", driver.ErrJobNotClaimable, queue)
	case busy:
		return fmt.Errorf("%w: its ordering key holds it back", driver.ErrJobNotClaimable)
	}
	return nil
}

// JobLeaseRenew implements driver.Executor.
func (e *executor) JobLeaseRenew(ctx context.Context, params driver.JobLeaseRenewParams) (*driver.JobLeaseStatus, error) {
	var st *driver.JobLeaseStatus
	err := e.write(ctx, func(x *w) error {
		var expiresAt micros
		var canceled bool
		err := x.QueryRowContext(ctx, `
			UPDATE hopper_job_leases SET renewed_at = ?4, expires_at = ?4 + ttl
			WHERE id = ?1 AND token = ?2 AND fence = ?3
			  AND EXISTS (SELECT 1 FROM hopper_jobs j WHERE j.id = ?1 AND `+leaseOwnerSQL+`)
			RETURNING expires_at, (SELECT cancel_requested_at IS NOT NULL FROM hopper_jobs WHERE id = ?1)`,
			idParam(params.ID), idParam(params.Lease.Token), params.Lease.Fence, x.us()).Scan(&expiresAt, &canceled)
		if errors.Is(err, sql.ErrNoRows) {
			return driver.ErrJobFenced
		}
		if err != nil {
			return err
		}
		st = &driver.JobLeaseStatus{ExpiresAt: expiresAt.time(), CancelRequested: canceled}
		return nil
	})
	if errors.Is(err, driver.ErrJobFenced) {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: renew job lease: %w", err)
	}
	return st, nil
}

// leaseOwns reports whether the job of a live row runs in lease l.
func (x *w) leaseOwns(ctx context.Context, row *liveRow, l *driver.LeaseFence) (bool, error) {
	j := row.job
	if j.State != driver.JobStateRunning || j.AttemptedBy != 0 || j.Fence != l.Fence {
		return false, nil
	}
	var n int
	err := x.QueryRowContext(ctx, "SELECT count(*) FROM hopper_job_leases WHERE id = ? AND token = ? AND fence = ?",
		idParam(j.ID), idParam(l.Token), l.Fence).Scan(&n)
	return n == 1, err
}

// releaseLease deletes a job's lease row, once its attempt has ended.
func (x *w) releaseLease(ctx context.Context, id driver.JobID) error {
	_, err := x.ExecContext(ctx, "DELETE FROM hopper_job_leases WHERE id = ?", idParam(id))
	return err
}

// JobExpireLeases implements driver.Executor.
func (e *executor) JobExpireLeases(ctx context.Context, params driver.JobExpireLeasesParams) ([]*driver.JobRow, error) {
	var jobs []*driver.JobRow
	// As the rescue's finalize skips held rows, the expiry waits only
	// briefly for a write lock another transaction holds, such as a
	// FinishTx's, and leaves the leases to the next pass.
	err := e.writeBrief(ctx, func(x *w) error {
		jobs = jobs[:0]
		// A client's start later than now, which a host clock set ahead and
		// then corrected leaves (see clock), counts once the clock
		// reaches it, as on PostgreSQL.
		rows, err := queryAll(ctx, x, scanLive, `SELECT `+prefixed(liveRowColumns, "j")+` FROM hopper_job_leases l
			JOIN hopper_jobs j ON j.id = l.id
			WHERE l.expires_at <= ?1
			  AND coalesce((SELECT min(c.started_at) FROM hopper_clients c
			                WHERE c.started_at > l.renewed_at AND c.started_at <= ?1 AND c.expires_at > ?1)
			    + max(l.ttl, ?3), l.expires_at) <= ?1
			  AND j.state = 'running' AND j.attempted_by IS NULL AND j.fence = l.fence
			  AND EXISTS (SELECT 1 FROM hopper_clients me WHERE me.id = ?4 AND me.expires_at > ?1)
			ORDER BY l.expires_at LIMIT ?2`, x.us(), max(params.Limit, 1), max(params.Grace, 0).Microseconds(), params.ClientID)
		if err != nil {
			return err
		}
		done := make([]finished, 0, len(rows))
		for _, row := range rows {
			j := row.job
			errs, err := withErrorAt(row.errors, &driver.AttemptError{
				Attempt: j.Attempt, Fence: j.Fence, Error: "hopper: job lease expired", Rescue: driver.RescueLeaseExpired,
			}, x.now)
			if err != nil {
				return err
			}
			if err := x.releaseLease(ctx, j.ID); err != nil {
				return err
			}
			if j.Attempt < j.MaxAttempts && j.CancelRequestedAt.IsZero() {
				job, err := scanJob(x.QueryRowContext(ctx, `
					UPDATE hopper_jobs SET state = 'retryable', scheduled_at = ?1, errors = ?2, wake_requested_at = NULL
					WHERE id = ?3 RETURNING `+jobColumns, x.us(), errs, idParam(j.ID)), false)
				if err != nil {
					return err
				}
				jobs = append(jobs, job)
				continue
			}
			state := driver.JobStateDiscarded
			if !j.CancelRequestedAt.IsZero() {
				state = driver.JobStateCanceled
			}
			f := finished{row: row, state: state, errors: errs, logs: row.logs, archive: true}
			if err := x.finish(ctx, f); err != nil {
				return err
			}
			done = append(done, f)
			job, err := f.jobRow(x.now)
			if err != nil {
				return err
			}
			jobs = append(jobs, job)
		}
		_, err = x.tail(ctx, done)
		return err
	})
	if errors.Is(err, errLockBusy) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: expire job leases: %w", err)
	}
	return jobs, nil
}
