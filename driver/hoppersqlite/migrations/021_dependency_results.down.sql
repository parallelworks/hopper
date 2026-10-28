DROP INDEX hopper_job_deps_reads_depends_on;
DROP INDEX hopper_job_deps_reads_job;
ALTER TABLE hopper_batches DROP COLUMN reads;
ALTER TABLE hopper_batches DROP COLUMN steps;
ALTER TABLE hopper_job_deps DROP COLUMN reads;
ALTER TABLE hopper_job_deps DROP COLUMN step;
DROP TABLE hopper_job_results;
