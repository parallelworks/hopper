DROP INDEX hopper_job_history_batch;
DROP TABLE hopper_job_deps;
ALTER TABLE hopper_batches DROP COLUMN edges;
ALTER TABLE hopper_batches DROP COLUMN name;
