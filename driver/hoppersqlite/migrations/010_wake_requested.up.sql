-- hopper schema v10: a wake that finds its key's job running.
--
-- JobRetryOrderingKey brings a key's waiting jobs forward, and marks a job
-- of the key that is running at the time. If that job then snoozes or
-- fails, the finalize makes it runnable at once instead of after its delay,
-- and clears the mark. Only a mark set since the job's current attempt
-- started counts. History has no such column.

ALTER TABLE hopper_jobs ADD COLUMN wake_requested_at INTEGER;
