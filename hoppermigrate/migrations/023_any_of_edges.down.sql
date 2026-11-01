-- The edges of any-of groups become edges a dependent ignores the failure
-- of, which is how v22's finalizes treated them, and the index comes back
-- as v21 built it. It locks hopper_job_deps as the up script does.

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
      UPDATE hopper_job_deps SET on_failure = 'ignore' WHERE any_of IS NOT NULL;
      CREATE INDEX hopper_job_deps_reads_depends_on ON hopper_job_deps (depends_on) WHERE reads;
      DROP INDEX hopper_job_deps_results_depends_on;
      ALTER TABLE hopper_job_deps DROP COLUMN any_of;
      EXIT;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock hopper_job_deps to drop any-of groups in 10 minutes (% tries)', tries
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
