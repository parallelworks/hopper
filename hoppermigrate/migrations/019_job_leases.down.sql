-- The previous release knows no job leases, and would see a leased job as
-- running with no owner forever, so leased jobs go back to retryable first,
-- keeping their attempt, as an expired lease leaves a job with attempts left.
-- One on its last attempt, or with a cancel requested, is not ended as an
-- expiry would end it: the previous release runs it once more, or cancels
-- it once a client claims it.

UPDATE hopper_jobs j
SET state = 'retryable', scheduled_at = now(), attempted_by = NULL
FROM hopper_job_leases l
WHERE j.id = l.id AND j.fence = l.fence AND j.state = 'running' AND j.attempted_by IS NULL;

DROP TABLE hopper_job_leases;
