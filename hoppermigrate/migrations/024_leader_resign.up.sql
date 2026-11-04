-- hopper schema v24: a request that the leader give up its term.
--
-- hopper_leader.resign_client_id names the client a resign request
-- (Client.LeaderResign) asked to give the lease up: the client that held
-- it, live, when the request was made. The holder's renewal reports a
-- request that names it, and the holder then ends its term and deletes
-- the row, as on a graceful stop. A client that takes the lease over
-- clears it; one of the previous release does not, but it never reads the
-- column, and a request names its holder only, so it cannot ask a later
-- holder to resign. Adding a nullable column with no default rewrites
-- nothing.
--
-- The column is added the way v10 added wake_requested_at: with a short
-- lock_timeout per try. The ACCESS EXCLUSIVE lock it needs would otherwise
-- queue behind any session that holds a lock on hopper_leader for long, a
-- pg_dump say, and every leader renewal and Stats would wait behind it.

SELECT set_config('lock_timeout', '50ms', true);

DO $$
DECLARE
  deadline constant timestamptz := clock_timestamp() + interval '10 minutes';
  next_notice timestamptz := clock_timestamp() + interval '10 seconds';
  tries int := 0;
BEGIN
  LOOP
    BEGIN
      ALTER TABLE hopper_leader ADD COLUMN resign_client_id bigint;
      RETURN;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock hopper_leader to add resign_client_id in 10 minutes (% tries)', tries
        USING ERRCODE = 'lock_not_available',
              HINT = 'A long transaction holds a lock on hopper_leader; find it in pg_stat_activity, end it, and run the migration again.';
    END IF;
    IF clock_timestamp() >= next_notice THEN
      RAISE NOTICE 'hopper: still waiting to lock hopper_leader (% tries)', tries;
      next_notice := clock_timestamp() + interval '10 seconds';
    END IF;
    PERFORM pg_sleep(random() * least(1.0, 0.05 * 2 ^ least(tries, 5)));
  END LOOP;
END
$$;
