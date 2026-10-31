-- hopper schema v22: the partial index on the edges that read, by dependent,
-- is dropped.
--
-- A claim reads a claimed job's edges by the primary key, which leads with
-- job_id, and no other statement looks the edges that read up by
-- dependent. Left in place, the index also made the finalize's probe for a
-- dependent that reads (by depends_on, through the partial index by
-- dependency) ambiguous: statistics taken while no edge read make both
-- partial indexes look empty, and the planner then walked this one whole,
-- filtering it by the dependency, on every finalize of a workflow job.
--
-- DROP INDEX takes an ACCESS EXCLUSIVE lock on hopper_job_deps, which
-- finalizes use, so it tries as v21 does: with a short lock_timeout per
-- try, in a loop. Neither hopper_jobs nor history is locked, so claims
-- never wait for this migration.

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
      DROP INDEX hopper_job_deps_reads_job;
      EXIT;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock hopper_job_deps to drop an index in 10 minutes (% tries)', tries
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
