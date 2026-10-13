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
-- therefore gives up after a short lock_timeout, and the loop tries again
-- until it gets the lock, so claims wait at most one short try at a time.

SELECT set_config('lock_timeout', '50ms', true);

DO $$
BEGIN
  LOOP
    BEGIN
      ALTER TABLE hopper_jobs ADD COLUMN snooze_reason text;
      RETURN;
    EXCEPTION WHEN lock_not_available THEN
      PERFORM pg_sleep(0.05);
    END;
  END LOOP;
END
$$;
