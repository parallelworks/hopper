-- hopper schema v10: a wake that finds its key's job running.
--
-- JobRetryOrderingKey brings a key's waiting jobs forward, and marks a job
-- of the key that is running at the time. If that job then snoozes or
-- fails, the finalize makes it runnable at once instead of after its delay,
-- and clears the mark. Only a mark set since the job's current attempt
-- started counts. History has no such column.
--
-- The column is added the way v9 added snooze_reason: with a short
-- lock_timeout per try, so claims wait at most one short try at a time.

SELECT set_config('lock_timeout', '50ms', true);

DO $$
DECLARE
  deadline constant timestamptz := clock_timestamp() + interval '10 minutes';
  next_notice timestamptz := clock_timestamp() + interval '10 seconds';
  tries int := 0;
BEGIN
  LOOP
    BEGIN
      ALTER TABLE hopper_jobs ADD COLUMN wake_requested_at timestamptz;
      RETURN;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock hopper_jobs to add wake_requested_at in 10 minutes (% tries)', tries
        USING ERRCODE = 'lock_not_available',
              HINT = 'A long transaction holds a lock on hopper_jobs; find it in pg_stat_activity, end it, and run the migration again.';
    END IF;
    IF clock_timestamp() >= next_notice THEN
      RAISE NOTICE 'hopper: still waiting to lock hopper_jobs (% tries)', tries;
      next_notice := clock_timestamp() + interval '10 seconds';
    END IF;
    PERFORM pg_sleep(random() * least(1.0, 0.05 * 2 ^ least(tries, 5)));
  END LOOP;
END
$$;
