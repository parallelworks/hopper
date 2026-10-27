ALTER TABLE hopper_batches DROP COLUMN steps;
ALTER TABLE hopper_job_deps DROP COLUMN step;
DROP TABLE hopper_job_results;
