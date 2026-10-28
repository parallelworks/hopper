-- hopper schema v21: dependency results, for workflow steps.
--
-- A claimed workflow step carries its dependencies' final states and
-- outputs, when it asks for them. The transaction that finalizes a job
-- with a dependent that asked (hopper_job_deps.reads) copies its state and
-- output here, so history's retention and DeleteCompleted do not reach
-- them. A workflow's results are deleted when it completes, and
-- hopper_batches.reads marks a workflow with any dependent that asked.
-- hopper_job_deps.step is the name of an edge's dependency, on the edges
-- that read, and hopper_batches.steps maps a workflow's job IDs to their
-- names.

CREATE TABLE hopper_job_results (
  batch_id     BLOB    NOT NULL,
  job_id       BLOB    NOT NULL CHECK (length(job_id) = 16),
  state        TEXT    NOT NULL,
  output       TEXT,
  finalized_at INTEGER NOT NULL,
  PRIMARY KEY (batch_id, job_id)
) WITHOUT ROWID;

ALTER TABLE hopper_job_deps ADD COLUMN step TEXT;
ALTER TABLE hopper_job_deps ADD COLUMN reads INTEGER;
ALTER TABLE hopper_batches ADD COLUMN steps TEXT;
ALTER TABLE hopper_batches ADD COLUMN reads INTEGER;
CREATE INDEX hopper_job_deps_reads_job ON hopper_job_deps (job_id) WHERE reads;
CREATE INDEX hopper_job_deps_reads_depends_on ON hopper_job_deps (depends_on) WHERE reads;
