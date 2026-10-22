-- hopper schema v15: a job's progress through its steps.
--
-- A worker records the last step it finished, and optionally a cursor within
-- the next, so that a later attempt resumes there. The progress survives
-- retries, snoozes and a JobRetry from history, so history keeps it too.

ALTER TABLE hopper_job_history ADD COLUMN progress TEXT;
ALTER TABLE hopper_jobs ADD COLUMN progress TEXT;
