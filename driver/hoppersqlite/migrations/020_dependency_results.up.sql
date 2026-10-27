-- hopper schema v20: dependency results, for workflow steps.
--
-- A claimed workflow step carries its dependencies' final states and
-- outputs. The transaction that finalizes a job that still has dependents
-- copies its state and output here, so history's retention and
-- DeleteCompleted do not reach them. A workflow's results are deleted when
-- it completes. hopper_job_deps.step is the name of the edge's dependency,
-- and hopper_batches.steps maps a workflow's job IDs to their names.

CREATE TABLE hopper_job_results (
  job_id       BLOB    PRIMARY KEY CHECK (length(job_id) = 16),
  batch_id     BLOB    NOT NULL,
  state        TEXT    NOT NULL,
  output       TEXT,
  finalized_at INTEGER NOT NULL
) WITHOUT ROWID;

CREATE INDEX hopper_job_results_batch ON hopper_job_results (batch_id);

ALTER TABLE hopper_job_deps ADD COLUMN step TEXT;
ALTER TABLE hopper_batches ADD COLUMN steps TEXT;
