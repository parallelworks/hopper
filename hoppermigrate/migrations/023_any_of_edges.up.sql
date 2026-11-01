-- hopper schema v23: any-of groups of workflow edges.
--
-- A workflow step can wait for one job of a group to complete rather than
-- for every job to finish (StepOpts.AfterAny). hopper_job_deps.any_of
-- numbers the dependent's group an edge is in, NULL for an ordinary edge.
-- A group's edges store on_failure as 'any_cancel' or 'any_ignore', the
-- dependent's policy, never 'cancel': a client of the previous release,
-- which cancels a dependent as soon as a job it depends on by a 'cancel'
-- edge fails, leaves them alone, and treats them as edges of a dependent
-- that runs once they have all finished.
--
-- The finalize of a job with a dependent in an any-of group records its
-- final state in hopper_job_results, as for a dependent that reads it, so
-- that every snapshot sees whether a member that left the live table
-- completed. The partial index on the edges that read, by dependency,
-- which that finalize probes, is rebuilt to cover the edges of groups too,
-- so it stays one probe; the previous release's probe of the edges that
-- read still matches it.
--
-- Adding the column changes only the catalog, and building the index
-- reads hopper_job_deps, which holds only the edges of the workflows in
-- flight. Both need an ACCESS EXCLUSIVE lock on hopper_job_deps, which
-- finalizes use and every claim reads, so it tries as v21 does: with a
-- short lock_timeout per try, in a loop. Neither hopper_jobs nor history
-- is locked, so claims of jobs outside workflows never wait for it.

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
      LOCK TABLE hopper_job_deps IN ACCESS EXCLUSIVE MODE;
      ALTER TABLE hopper_job_deps ADD COLUMN any_of smallint;
      CREATE INDEX hopper_job_deps_results_depends_on ON hopper_job_deps (depends_on) WHERE reads OR any_of IS NOT NULL;
      DROP INDEX hopper_job_deps_reads_depends_on;
      EXIT;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock hopper_job_deps to add any-of groups in 10 minutes (% tries)', tries
        USING ERRCODE = 'lock_not_available',
              HINT = 'A long transaction holds a lock on hopper_job_deps; find it in pg_stat_activity, end it, and run the migration again.';
    END IF;
    IF clock_timestamp() >= next_notice THEN
      RAISE NOTICE 'hopper: still waiting to lock hopper_job_deps (% tries)', tries;
      next_notice := clock_timestamp() + interval '10 seconds';
    END IF;
    PERFORM pg_sleep(random() * least(1.0, 0.05 * 2 ^ least(tries, 5)));
  END LOOP;
  PERFORM set_config('lock_timeout', prior_lock_timeout, true);
END
$$;
