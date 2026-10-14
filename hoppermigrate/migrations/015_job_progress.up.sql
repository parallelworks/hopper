-- hopper schema v15: a job's progress through its steps.
--
-- A worker records the last step it finished, and optionally a cursor within
-- the next, so that a later attempt resumes there. The progress survives
-- retries, snoozes and a JobRetry from history, so history keeps it too.
--
-- Adding a nullable column with no default changes only the catalog, but it
-- needs an ACCESS EXCLUSIVE lock on each table, every history partition
-- included, so it is added the way v9 added snooze_reason: with a short
-- lock_timeout per try, in a loop.

SELECT set_config('lock_timeout', '50ms', true);

DO $$
DECLARE
  deadline constant timestamptz := clock_timestamp() + interval '10 minutes';
  next_notice timestamptz := clock_timestamp() + interval '10 seconds';
  tries int := 0;
BEGIN
  LOOP
    BEGIN
      ALTER TABLE hopper_jobs ADD COLUMN progress jsonb;
      ALTER TABLE hopper_job_history ADD COLUMN progress jsonb;
      RETURN;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock hopper_jobs and hopper_job_history to add progress in 10 minutes (% tries)', tries
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
