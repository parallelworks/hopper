-- The previous release knows no job leases, and would see a leased job as
-- running with no owner forever, so leased jobs go back to retryable first,
-- keeping their attempt, as an expired lease leaves a job with attempts left.
-- One on its last attempt, or with a cancel requested, is not ended as an
-- expiry would end it: the previous release runs it once more, or cancels
-- it once a client claims it.

UPDATE hopper_jobs
SET state = 'retryable', scheduled_at = (SELECT renewed_at FROM hopper_job_leases l WHERE l.id = hopper_jobs.id), attempted_by = NULL
WHERE state = 'running' AND attempted_by IS NULL
  AND EXISTS (SELECT 1 FROM hopper_job_leases l WHERE l.id = hopper_jobs.id AND l.fence = hopper_jobs.fence);

DROP TABLE hopper_job_leases;
