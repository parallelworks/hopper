-- hopper_insert as it was before v17, without tags.
CREATE OR REPLACE FUNCTION hopper_insert(kind text, args jsonb, opts jsonb DEFAULT '{}') RETURNS uuid
LANGUAGE sql VOLATILE
SET search_path FROM CURRENT
AS $$
  INSERT INTO hopper_jobs (kind, queue, state, priority, max_attempts, scheduled_at, args, metadata, unique_key,
                           ordering_key, expires_at, await)
  VALUES (
    kind,
    coalesce(opts->>'queue', 'default'),
    (CASE WHEN (opts->>'scheduled_at')::timestamptz > now() THEN 'scheduled' ELSE 'available' END)::hopper_job_state,
    coalesce((opts->>'priority')::smallint, 2),
    coalesce((opts->>'max_attempts')::smallint, 25),
    coalesce((opts->>'scheduled_at')::timestamptz, now()),
    coalesce(args, '{}'),
    coalesce(opts->'metadata', '{}'),
    opts->>'unique_key',
    opts->>'ordering_key',
    CASE WHEN (opts->>'ttl_seconds')::float8 > 0 THEN now() + make_interval(secs => (opts->>'ttl_seconds')::float8) END,
    coalesce((opts->>'await')::boolean, false))
  ON CONFLICT (kind, unique_key) WHERE unique_key IS NOT NULL DO UPDATE SET kind = EXCLUDED.kind
  RETURNING id
$$;
DROP FUNCTION hopper_tags(jsonb);

-- Dropping the column and the triggers needs the same locks as adding
-- them, so it tries the same way as the up script. The CHECK goes with the
-- column. The tags tables are dropped in the same tries: a listing by tag
-- that started meanwhile holds a lock on one while it waits for
-- hopper_jobs, and would otherwise fail the drop on the lock_timeout. Each
-- try locks the tables first, in the order a finalize takes them:
-- hopper_jobs, history, then the tags tables its triggers write. Dropping
-- history's triggers first would leave a finalize that has hopper_jobs
-- waiting for history, and the try waiting for that finalize until its
-- lock_timeout, as in the up script. As there, the timeout is the loop's
-- alone.

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
      LOCK TABLE hopper_jobs, hopper_job_history, hopper_job_history_tags, hopper_job_tags IN ACCESS EXCLUSIVE MODE;
      DROP TRIGGER hopper_job_history_tags_insert ON hopper_job_history;
      DROP TRIGGER hopper_job_history_tags_delete ON hopper_job_history;
      DROP TRIGGER hopper_job_history_tags_update ON hopper_job_history;
      DROP TRIGGER hopper_job_history_tags_truncate ON hopper_job_history;
      ALTER TABLE hopper_job_history DROP COLUMN tags;
      DROP TRIGGER hopper_jobs_tags_insert ON hopper_jobs;
      DROP TRIGGER hopper_jobs_tags_delete ON hopper_jobs;
      DROP TRIGGER hopper_jobs_tags_update ON hopper_jobs;
      DROP TRIGGER hopper_jobs_tags_truncate ON hopper_jobs;
      ALTER TABLE hopper_jobs DROP COLUMN tags;
      -- The partitions, the leader's time partitions among them, go with it.
      DROP TABLE hopper_job_history_tags;
      DROP TABLE hopper_job_tags;
      EXIT;
    EXCEPTION WHEN lock_not_available THEN
      tries := tries + 1;
    END;
    IF clock_timestamp() >= deadline THEN
      RAISE EXCEPTION 'hopper: could not lock hopper_jobs, hopper_job_history and the tags tables to drop tags in 10 minutes (% tries)', tries
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

DROP FUNCTION hopper_job_tags_inserted();
DROP FUNCTION hopper_job_tags_deleted();
DROP FUNCTION hopper_job_tags_updated();
DROP FUNCTION hopper_job_history_tags_inserted();
DROP FUNCTION hopper_job_history_tags_deleted();
DROP FUNCTION hopper_job_history_tags_updated();
DROP FUNCTION hopper_job_tags_truncated();
DROP FUNCTION hopper_job_history_tags_truncated();
DROP FUNCTION hopper_tags_of(jsonb);
