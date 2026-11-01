-- The index comes back as v21 built it. CREATE INDEX takes a SHARE lock on
-- hopper_job_deps, which finalizes' writes wait for, so it tries the same
-- way as the up script; the build reads only the edges of the workflows in
-- flight.

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
      CREATE INDEX hopper_job_deps_reads_job ON hopper_job_deps (job_id) WHERE reads;
      EXIT;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock hopper_job_deps to build an index in 10 minutes (% tries)', tries
        USING ERRCODE = 'lock_not_available',
              HINT = 'A long transaction holds a lock on hopper_job_deps; find it in pg_stat_activity, end it, and run the migration again.';
    END IF;
    IF clock_timestamp() >= next_notice THEN
      RAISE NOTICE 'hopper: still waiting to lock hopper_job_deps (% tries)', tries;
      next_notice := clock_timestamp() + interval '10 seconds';
    END IF;
    PERFORM pg_sleep(random() * least(1.0, 0.05 * 2 ^ least(tries, 5)));
  END LOOP;
  PERFORM set_config('lock_timeout', prior_lock_timeout, true);
END
$$;
