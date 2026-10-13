-- hopper schema v9: why a snoozed job is waiting.
--
-- A worker can give a reason when it snoozes a job. The finalize statement
-- stores it with the snooze, and the claim, every other finalize, a retry
-- and a replacing insert clear it, so it only ever describes the current
-- wait. History has no such column: a finalized job is not waiting.
--
-- Adding a nullable column with no default changes only the catalog, but
-- it needs an ACCESS EXCLUSIVE lock on hopper_jobs. A request for that lock
-- waits behind any open transaction that has touched the table, such as a
-- producer's InsertTx, and every claim queues behind the request. Each try
-- therefore gives up after a short lock_timeout, so claims wait at most one
-- short try at a time. Between tries the loop sleeps a jittered, growing
-- interval (up to a second), so it neither hammers a busy table nor falls
-- into step with periodic traffic. It reports progress every 10 seconds
-- with a NOTICE and gives up after 10 minutes, naming what to look for.

SELECT set_config('lock_timeout', '50ms', true);

DO $$
DECLARE
  deadline constant timestamptz := clock_timestamp() + interval '10 minutes';
  next_notice timestamptz := clock_timestamp() + interval '10 seconds';
  tries int := 0;
BEGIN
  LOOP
    BEGIN
      ALTER TABLE hopper_jobs ADD COLUMN snooze_reason text;
      RETURN;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock hopper_jobs to add snooze_reason in 10 minutes (% tries)', tries
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
