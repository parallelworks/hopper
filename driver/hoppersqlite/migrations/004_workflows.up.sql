-- hopper schema v4: workflows.
--
-- A workflow is a batch whose jobs depend on each other. Dependents wait in
-- the 'pending' state; the transaction that finalizes a job promotes the
-- dependents whose dependencies are all finalized, and cancels the
-- dependents of a failed job (transitively) unless their edge says to
-- ignore the failure. hopper_job_deps is the working set of edges, deleted
-- as dependents finalize; the batch row keeps the whole graph.

ALTER TABLE hopper_batches ADD COLUMN name TEXT;
ALTER TABLE hopper_batches ADD COLUMN edges TEXT;   -- [[job_id, depends_on], ...] for workflows

CREATE TABLE hopper_job_deps (
  job_id     BLOB NOT NULL,                -- the dependent, pending until depends_on is finalized
  depends_on BLOB NOT NULL,
  on_failure TEXT NOT NULL DEFAULT 'cancel', -- 'cancel' the dependent when depends_on fails, or 'ignore'
  PRIMARY KEY (job_id, depends_on)
) WITHOUT ROWID;

CREATE INDEX hopper_job_deps_depends_on ON hopper_job_deps (depends_on);

-- Workflow inspection reads a batch's finished jobs from history.
CREATE INDEX hopper_job_history_batch ON hopper_job_history (batch_id) WHERE batch_id IS NOT NULL;
