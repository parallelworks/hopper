-- The previous release counts a batch down in pending, which this release
-- does not maintain, so open batches get their counts back from their jobs.
-- Dropping the index needs an ACCESS EXCLUSIVE lock on hopper_batches, so
-- it tries the same way as the up script.

UPDATE hopper_batches b
SET pending = (SELECT count(*) FROM hopper_jobs j WHERE j.batch_id = b.id),
    failed = (SELECT count(*) FROM hopper_job_history h
              WHERE h.batch_id = b.id AND h.state IN ('cancelled', 'discarded'))
WHERE b.completed_at IS NULL;

SELECT set_config('lock_timeout', '50ms', true);

DO $$
DECLARE
  deadline constant timestamptz := clock_timestamp() + interval '10 minutes';
  next_notice timestamptz := clock_timestamp() + interval '10 seconds';
  tries int := 0;
BEGIN
  LOOP
    BEGIN
      DROP INDEX hopper_batches_open;
      RETURN;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock hopper_batches to drop hopper_batches_open in 10 minutes (% tries)', tries
        USING ERRCODE = 'lock_not_available',
              HINT = 'A long transaction holds a lock on hopper_batches; find it in pg_stat_activity, end it, and run the migration again.';
    END IF;
    IF clock_timestamp() >= next_notice THEN
      RAISE NOTICE 'hopper: still waiting to lock hopper_batches (% tries)', tries;
      next_notice := clock_timestamp() + interval '10 seconds';
    END IF;
    PERFORM pg_sleep(random() * least(1.0, 0.05 * 2 ^ least(tries, 5)));
  END LOOP;
END
$$;
