-- Dropping the columns needs the same locks as adding them, so it tries the
-- same way as the up script.

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
      ALTER TABLE hopper_batches DROP COLUMN steps;
      ALTER TABLE hopper_job_deps DROP COLUMN step;
      EXIT;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock hopper_job_deps and hopper_batches to drop step names in 10 minutes (% tries)', tries
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

DROP TABLE hopper_job_results;
