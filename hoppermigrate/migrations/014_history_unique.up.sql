-- hopper schema v14: find a unique key's finished jobs without reading all
-- history.
--
-- An insert whose UniqueOpts.Finished names finished states looks for the
-- key in history through this index. It is declared ON ONLY each outcome
-- table and built per partition by the leader, as v13 does for the
-- ordering-key index and for the same reasons; see there.

SELECT set_config('lock_timeout', '50ms', true);

DO $$
DECLARE
  deadline constant timestamptz := clock_timestamp() + interval '10 minutes';
  next_notice timestamptz := clock_timestamp() + interval '10 seconds';
  tries int := 0;
BEGIN
  LOOP
    BEGIN
      CREATE INDEX hopper_job_history_completed_unique ON ONLY hopper_job_history_completed (kind, unique_key, finalized_at)
        WHERE unique_key IS NOT NULL;
      CREATE INDEX hopper_job_history_failed_unique ON ONLY hopper_job_history_failed (kind, unique_key, finalized_at)
        WHERE unique_key IS NOT NULL;
      RETURN;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock the history tables to declare the unique-key index in 10 minutes (% tries)', tries
        USING ERRCODE = 'lock_not_available',
              HINT = 'A long transaction holds a lock on hopper_job_history; find it in pg_stat_activity, end it, and run the migration again.';
    END IF;
    IF clock_timestamp() >= next_notice THEN
      RAISE NOTICE 'hopper: still waiting to lock the history tables (% tries)', tries;
      next_notice := clock_timestamp() + interval '10 seconds';
    END IF;
    PERFORM pg_sleep(random() * least(1.0, 0.05 * 2 ^ least(tries, 5)));
  END LOOP;
END
$$;
