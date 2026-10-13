-- hopper schema v11: batches complete without a shared counter.
--
-- A batch is complete when none of its jobs is left in the live table. The
-- statement that finalizes a batch's last live job completes it and inserts
-- its callbacks, guarded by completed_at, so finalizes no longer update the
-- batch row once per batch per statement, and a transaction that finalizes
-- a job and stays open (CompleteTx) holds up none of its batch siblings.
-- Two jobs finalized in concurrent transactions each see the other still
-- live, so neither completes the batch; the leader completes open batches
-- whose jobs are all gone, and this index finds them.
--
-- The index build holds a SHARE lock on hopper_batches, which a previous
-- release's finalizes of batch jobs wait for, so the lock is taken with a
-- short lock_timeout per try, as v9 and v10 add their columns.

SELECT set_config('lock_timeout', '50ms', true);

DO $$
DECLARE
  deadline constant timestamptz := clock_timestamp() + interval '10 minutes';
  next_notice timestamptz := clock_timestamp() + interval '10 seconds';
  tries int := 0;
BEGIN
  LOOP
    BEGIN
      CREATE INDEX hopper_batches_open ON hopper_batches (created_at) WHERE completed_at IS NULL;
      EXIT;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock hopper_batches to index open batches in 10 minutes (% tries)', tries
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

-- A batch the previous release left open with none of its jobs live never
-- completed there (a job left out of it as a duplicate, or an insert that
-- failed part way), so its callbacks never ran. Close it as it was, without
-- running them now. A batch younger than an hour may be one whose jobs a
-- client of the previous release is inserting, and is left to complete.
UPDATE hopper_batches b SET completed_at = now()
WHERE b.completed_at IS NULL AND b.created_at < now() - interval '1 hour'
  AND NOT EXISTS (SELECT 1 FROM hopper_jobs j WHERE j.batch_id = b.id);
