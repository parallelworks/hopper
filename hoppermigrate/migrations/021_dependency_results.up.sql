-- hopper schema v21: dependency results, for workflow steps.
--
-- A workflow step that asks for them (StepOpts.Dependencies) is claimed
-- with its dependencies' final states and outputs. The statement that
-- finalizes a job with such a dependent copies its state and output here,
-- in the statement that moves it out of the live table, so every snapshot
-- sees a dependency either live or with its result; history's retention
-- and DeleteCompleted do not reach it. A workflow's results are deleted
-- when it completes, after all its jobs, dependents included, have
-- finished.
--
-- hopper_job_deps.step is the name of the edge's dependency, copied per
-- edge, so a claim reads names without the workflow's row, and
-- hopper_batches.steps maps a workflow's job IDs to their names, for
-- WorkflowGet. hopper_job_deps.reads marks the edges of a dependent that
-- asked, and hopper_batches.reads a workflow with any, NULL for none. The
-- partial indexes on the marked edges are empty for workflows that never
-- ask, so their finalizes and claims probe nothing.
--
-- Adding the columns changes only the catalog, and the partial indexes
-- index none of the existing edges, but both need an ACCESS
-- EXCLUSIVE lock on hopper_job_deps and hopper_batches, which finalizes
-- use, so it tries as v17 and v18 do: with a short lock_timeout per try, in
-- a loop, locking hopper_job_deps before hopper_batches, the order a
-- finalize names them in. Neither hopper_jobs nor history is locked, so
-- claims never wait for this migration.

-- Keyed by workflow first, so that one index serves both a claim's lookup
-- of a dependency (in the dependent's own workflow) and the delete of a
-- completed workflow's results.
CREATE TABLE hopper_job_results (
  batch_id     uuid             NOT NULL,      -- the workflow; its rows go when it completes
  job_id       uuid             NOT NULL,      -- the finished dependency
  state        hopper_job_state NOT NULL,      -- completed, canceled or discarded
  output       jsonb,
  finalized_at timestamptz      NOT NULL DEFAULT now(),
  PRIMARY KEY (batch_id, job_id)
) WITH (vacuum_truncate = false);

DO $$
DECLARE
  deadline constant timestamptz := clock_timestamp() + interval '10 minutes';
  next_notice timestamptz := clock_timestamp() + interval '10 seconds';
  tries int := 0;
  prior_lock_timeout constant text := current_setting('lock_timeout');
BEGIN
  PERFORM set_config('lock_timeout', '50ms', true);
  LOOP
    BEGIN
      LOCK TABLE hopper_job_deps, hopper_batches IN ACCESS EXCLUSIVE MODE;
      ALTER TABLE hopper_job_deps ADD COLUMN step text, ADD COLUMN reads boolean;
      ALTER TABLE hopper_batches ADD COLUMN steps jsonb, ADD COLUMN reads boolean;
      CREATE INDEX hopper_job_deps_reads_job ON hopper_job_deps (job_id) WHERE reads;
      CREATE INDEX hopper_job_deps_reads_depends_on ON hopper_job_deps (depends_on) WHERE reads;
      EXIT;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock hopper_job_deps and hopper_batches to add step names in 10 minutes (% tries)', tries
        USING ERRCODE = 'lock_not_available',
              HINT = 'A long transaction holds a lock on hopper_job_deps or hopper_batches; find it in pg_stat_activity, end it, and run the migration again.';
    END IF;
    IF clock_timestamp() >= next_notice THEN
      RAISE NOTICE 'hopper: still waiting to lock hopper_job_deps and hopper_batches (% tries)', tries;
      next_notice := clock_timestamp() + interval '10 seconds';
    END IF;
    PERFORM pg_sleep(random() * least(1.0, 0.05 * 2 ^ least(tries, 5)));
  END LOOP;
  PERFORM set_config('lock_timeout', prior_lock_timeout, true);
END
$$;
