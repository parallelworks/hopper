-- The previous release knows no job leases, and would see a leased job as
-- running with no owner forever, so leased jobs go back to retryable first,
-- keeping their attempt, as an expired lease leaves them.

UPDATE hopper_jobs j
SET state = 'retryable', scheduled_at = now(), attempted_by = NULL
FROM hopper_job_leases l
WHERE j.id = l.id AND j.fence = l.fence AND j.state = 'running' AND j.attempted_by IS NULL;

DROP TABLE hopper_job_leases;
