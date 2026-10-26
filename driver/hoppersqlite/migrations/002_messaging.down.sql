DROP INDEX hopper_jobs_ordering;
DROP INDEX hopper_jobs_ordering_running;
ALTER TABLE hopper_subscriptions DROP COLUMN metadata;
ALTER TABLE hopper_subscriptions DROP COLUMN max_attempts;
