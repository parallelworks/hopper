-- hopper schema v18: a fencing number per claim.
--
-- Every claim increments a job's fence, so that a result, a progress write
-- or a rescue applies only to the claim it belongs to: a job rescued and
-- claimed again by the same client, or snoozed and claimed again at the
-- same attempt number, has a new fence. Snoozes, retries and the archive
-- keep it, and a JobRetry from history carries it back, so for a given job
-- it never repeats. History has the column for that reason.

ALTER TABLE hopper_jobs ADD COLUMN fence INTEGER NOT NULL DEFAULT 0;
ALTER TABLE hopper_job_history ADD COLUMN fence INTEGER NOT NULL DEFAULT 0;
