-- hopper schema v2: messaging.
--
-- Subscriptions carry their own retry budget, and deliveries with an
-- ordering key run one at a time per key. The SQL insert and publish
-- contract lets producers in other languages enqueue work inside their own
-- transactions.

ALTER TABLE hopper_subscriptions
  ADD COLUMN max_attempts smallint,                       -- NULL: the client default (10 for ordered deliveries)
  ADD COLUMN metadata     jsonb NOT NULL DEFAULT '{}';

-- Backstop for ordering keys: the claim already selects at most one delivery
-- per (queue, ordering_key), and this makes two concurrent claims of the same
-- key impossible rather than merely unlikely.
CREATE UNIQUE INDEX hopper_jobs_ordering_running ON hopper_jobs (queue, ordering_key)
  WHERE state = 'running' AND ordering_key IS NOT NULL;

-- Oldest-first lookup per ordering key for the claim.
CREATE INDEX hopper_jobs_ordering ON hopper_jobs (queue, ordering_key, seq)
  WHERE ordering_key IS NOT NULL AND state IN ('available', 'scheduled', 'retryable');

-- hopper_topic_regex turns an AMQP topic pattern into a regular expression:
-- words are separated by dots, '*' matches one word and '#' zero or more.
CREATE FUNCTION hopper_topic_regex(pattern text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $$
  SELECT '^' ||
    replace(replace(replace(replace(replace(
      regexp_replace(pattern, '([.+?^${}()|\[\]\\])', '\\\1', 'g'),
      '*', '[^.]+'),            -- one word (before '#', whose rules introduce '.*')
      '\.#\.', '(\..*)?\.'),   -- a.#.b: zero or more words between
      '\.#', '(\..*)?'),        -- a.#: the word and anything under it
      '#\.', '(.*\.)?'),        -- #.b: anything above the word
      '#', '.*')                -- #: everything
    || '$'
$$;

-- hopper_insert enqueues a job from SQL, for producers not written in Go. It
-- returns the job ID. opts is a JSON object with any of: queue, priority,
-- max_attempts, scheduled_at, metadata, unique_key, ttl_seconds,
-- ordering_key, await. A unique_key conflict returns the existing job's ID.
CREATE FUNCTION hopper_insert(kind text, args jsonb, opts jsonb DEFAULT '{}') RETURNS uuid
LANGUAGE sql VOLATILE
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

-- hopper_publish delivers a message to every subscription whose pattern
-- matches the topic, as one job each, and returns the delivery IDs. opts is
-- a JSON object with any of: ordering_key, dedup_key, ttl_seconds, delay_seconds,
-- priority, headers (an object), await. A dedup_key makes the publish
-- idempotent while an earlier delivery is live. The notification is sent
-- once per queue on commit.
CREATE FUNCTION hopper_publish(topic text, payload jsonb, opts jsonb DEFAULT '{}') RETURNS SETOF uuid
LANGUAGE sql VOLATILE
AS $$
  WITH msg AS (SELECT hopper_uuidv7()::text AS id),
  ins AS (
    INSERT INTO hopper_jobs (kind, queue, state, priority, max_attempts, scheduled_at, args, metadata, unique_key,
                             ordering_key, expires_at, await)
    SELECT s.kind, s.queue,
      (CASE WHEN (opts->>'delay_seconds')::float8 > 0 THEN 'scheduled' ELSE 'available' END)::hopper_job_state,
      coalesce((opts->>'priority')::smallint, 2),
      coalesce(s.max_attempts, CASE WHEN opts->>'ordering_key' IS NOT NULL THEN 10 ELSE 25 END),
      now() + make_interval(secs => coalesce((opts->>'delay_seconds')::float8, 0)),
      coalesce(payload, 'null'),
      jsonb_build_object('topic', topic, 'message_id', msg.id, 'headers', coalesce(opts->'headers', '{}')) || s.metadata,
      CASE WHEN opts->>'dedup_key' IS NOT NULL THEN 'msg:' || (opts->>'dedup_key') END,
      opts->>'ordering_key',
      CASE WHEN (opts->>'ttl_seconds')::float8 > 0 THEN now() + make_interval(secs => (opts->>'ttl_seconds')::float8) END,
      coalesce((opts->>'await')::boolean, false)
    FROM hopper_subscriptions s, msg
    WHERE topic ~ hopper_topic_regex(s.pattern)
    ON CONFLICT (kind, unique_key) WHERE unique_key IS NOT NULL DO UPDATE SET kind = EXCLUDED.kind
    RETURNING id, queue, (xmax <> 0) AS duplicate
  ),
  notified AS (SELECT pg_notify('hopper_insert', queue) FROM (SELECT DISTINCT queue FROM ins WHERE NOT duplicate) q)
  SELECT id FROM ins
$$;
