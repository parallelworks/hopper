-- Dropping the columns needs the same locks as adding them, so it tries the
-- same way as the up script. The results table is dropped in the same
-- tries, locked after the other two, as a finalize names them: dropped
-- after the loop, it would wait, with no lock_timeout and holding
-- hopper_job_deps and hopper_batches, for any transaction on it alone,
-- such as the leader's analyze of it, with every finalize of a workflow
-- step waiting behind; and the leader's sweep of orphaned results, which
-- locks it before hopper_batches, would deadlock with it.

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
      LOCK TABLE hopper_job_deps, hopper_batches, hopper_job_results IN ACCESS EXCLUSIVE MODE;
      ALTER TABLE hopper_batches DROP COLUMN steps, DROP COLUMN reads;
      ALTER TABLE hopper_job_deps DROP COLUMN step, DROP COLUMN reads;
      DROP TABLE hopper_job_results;
      EXIT;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock hopper_job_deps, hopper_batches and hopper_job_results to drop dependency results in 10 minutes (% tries)', tries
        USING ERRCODE = 'lock_not_available',
              HINT = 'A long transaction holds a lock on hopper_job_deps, hopper_batches or hopper_job_results; find it in pg_stat_activity, end it, and run the migration again.';
    END IF;
    IF clock_timestamp() >= next_notice THEN
      RAISE NOTICE 'hopper: still waiting to lock hopper_job_deps, hopper_batches and hopper_job_results (% tries)', tries;
      next_notice := clock_timestamp() + interval '10 seconds';
    END IF;
    PERFORM pg_sleep(random() * least(1.0, 0.05 * 2 ^ least(tries, 5)));
  END LOOP;
  PERFORM set_config('lock_timeout', prior_lock_timeout, true);
END
$$;
