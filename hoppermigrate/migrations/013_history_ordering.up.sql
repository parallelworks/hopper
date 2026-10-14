-- hopper schema v13: list a key's finished jobs without reading all history.
--
-- JobList with an ordering key reads history through this index. Building it
-- on the history tables directly would lock every history partition against
-- finalizes for as long as the build takes, and CREATE INDEX CONCURRENTLY
-- cannot run on a partitioned table or inside a migration's transaction. So
-- the index is declared ON ONLY each outcome table, which reads no rows, and
-- the leader's history maintenance builds each existing partition's index
-- concurrently and attaches it. A partition created after this has the index
-- made for it when it is attached, while it is still empty. An outcome
-- table's index becomes valid once every partition's index is attached.
--
-- There is no index on hopper_job_history itself: the attach that completes
-- an outcome table's index would then also mark that one valid, which takes
-- an ACCESS EXCLUSIVE lock on it while holding the partition's index, and
-- deadlocks with every query on hopper_job_history that holds the first and
-- waits for the second. Queries on hopper_job_history use the partitions'
-- indexes either way.
--
-- Declaring the index takes a SHARE lock on the outcome table, which every
-- finalize that archives a job waits for. A request queued behind an open
-- transaction that wrote history would hold them all up, so the indexes are
-- declared the way v9 added snooze_reason: with a short lock_timeout per try.

SELECT set_config('lock_timeout', '50ms', true);

DO $$
DECLARE
  deadline constant timestamptz := clock_timestamp() + interval '10 minutes';
  next_notice timestamptz := clock_timestamp() + interval '10 seconds';
  tries int := 0;
BEGIN
  LOOP
    BEGIN
      CREATE INDEX hopper_job_history_completed_ordering ON ONLY hopper_job_history_completed (ordering_key, id)
        WHERE ordering_key IS NOT NULL;
      CREATE INDEX hopper_job_history_failed_ordering ON ONLY hopper_job_history_failed (ordering_key, id)
        WHERE ordering_key IS NOT NULL;
      RETURN;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock the history tables to declare the ordering-key index in 10 minutes (% tries)', tries
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
