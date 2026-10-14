-- Dropping the column needs the same ACCESS EXCLUSIVE lock as adding it,
-- so it tries the same way as the up script.

SELECT set_config('lock_timeout', '50ms', true);

DO $$
DECLARE
  deadline constant timestamptz := clock_timestamp() + interval '10 minutes';
  next_notice timestamptz := clock_timestamp() + interval '10 seconds';
  tries int := 0;
BEGIN
  LOOP
    BEGIN
      ALTER TABLE hopper_job_history DROP COLUMN logs;
      ALTER TABLE hopper_jobs DROP COLUMN logs;
      RETURN;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock hopper_jobs and hopper_job_history to drop logs in 10 minutes (% tries)', tries
        USING ERRCODE = 'lock_not_available',
              HINT = 'A long transaction holds a lock on hopper_jobs or hopper_job_history; find it in pg_stat_activity, end it, and run the migration again.';
    END IF;
    IF clock_timestamp() >= next_notice THEN
      RAISE NOTICE 'hopper: still waiting to lock hopper_jobs and hopper_job_history (% tries)', tries;
      next_notice := clock_timestamp() + interval '10 seconds';
    END IF;
    PERFORM pg_sleep(random() * least(1.0, 0.05 * 2 ^ least(tries, 5)));
  END LOOP;
END
$$;
