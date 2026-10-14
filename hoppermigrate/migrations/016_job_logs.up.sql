-- hopper schema v16: the log lines of a job's latest attempts.
--
-- A worker's lines are buffered in memory for the attempt and written by
-- the statement that finalizes it, so they cost no writes of their own.
-- They are kept through retries and a JobRetry from history, so history
-- keeps them too, and leave with the history partition that holds the job.
--
-- The column is added like v15's progress: with a short lock_timeout per
-- try, in a loop, history first.

SELECT set_config('lock_timeout', '50ms', true);

DO $$
DECLARE
  deadline constant timestamptz := clock_timestamp() + interval '10 minutes';
  next_notice timestamptz := clock_timestamp() + interval '10 seconds';
  tries int := 0;
BEGIN
  LOOP
    BEGIN
      ALTER TABLE hopper_job_history ADD COLUMN logs jsonb;
      ALTER TABLE hopper_jobs ADD COLUMN logs jsonb;
      RETURN;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock hopper_jobs and hopper_job_history to add logs in 10 minutes (% tries)', tries
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
