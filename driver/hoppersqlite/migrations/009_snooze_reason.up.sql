-- hopper schema v9: why a snoozed job is waiting.
--
-- A worker can give a reason when it snoozes a job. The finalize stores it
-- with the snooze, and the claim, every other finalize, a retry and a
-- replacing insert clear it, so it only ever describes the current wait.
-- History has no such column: a finalized job is not waiting.

ALTER TABLE hopper_jobs ADD COLUMN snooze_reason TEXT;
