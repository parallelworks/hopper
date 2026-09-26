DROP INDEX hopper_jobs_batch;
DROP TABLE hopper_batches;
DROP INDEX hopper_jobs_partition_running;
ALTER TABLE hopper_queues DROP COLUMN aging_seconds, DROP COLUMN partition_limit;
