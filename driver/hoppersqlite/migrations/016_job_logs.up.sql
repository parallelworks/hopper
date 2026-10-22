-- hopper schema v16: the log lines of a job's latest attempts, written by
-- the transaction that finalizes each attempt and kept through retries and
-- a JobRetry from history, so history keeps them too.

ALTER TABLE hopper_job_history ADD COLUMN logs TEXT;
ALTER TABLE hopper_jobs ADD COLUMN logs TEXT;
