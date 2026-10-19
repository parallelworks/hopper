-- hopper schema v18: a fencing number per claim.
--
-- Every claim increments a job's fence, so that a result, a progress write
-- or a rescue applies only to the claim it belongs to: a job rescued and
-- claimed again by the same client, or snoozed and claimed again at the
-- same attempt number, has a new fence. Snoozes, retries and the archive
-- keep it, and a JobRetry from history carries it back, so for a given job
-- it never repeats. History has the column for that reason.
--
-- A NOT NULL column with a constant default changes only the catalog, but
-- it needs an ACCESS EXCLUSIVE lock on each table, every history partition
-- included, so it is added like v17's tags: with a short lock_timeout per
-- try, in a loop, set inside the loop and restored after it. Each try locks
-- hopper_jobs and then history, the order a finalize takes them in.

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
      LOCK TABLE hopper_jobs, hopper_job_history IN ACCESS EXCLUSIVE MODE;
      ALTER TABLE hopper_jobs ADD COLUMN fence bigint NOT NULL DEFAULT 0;
      ALTER TABLE hopper_job_history ADD COLUMN fence bigint NOT NULL DEFAULT 0;
      EXIT;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock hopper_jobs and hopper_job_history to add fence in 10 minutes (% tries)', tries
        USING ERRCODE = 'lock_not_available',
              HINT = 'A long transaction holds a lock on hopper_jobs or hopper_job_history; find it in pg_stat_activity, end it, and run the migration again.';
    END IF;
    IF clock_timestamp() >= next_notice THEN
      RAISE NOTICE 'hopper: still waiting to lock hopper_jobs and hopper_job_history (% tries)', tries;
      next_notice := clock_timestamp() + interval '10 seconds';
    END IF;
    PERFORM pg_sleep(random() * least(1.0, 0.05 * 2 ^ least(tries, 5)));
  END LOOP;
  PERFORM set_config('lock_timeout', prior_lock_timeout, true);
END
$$;
