-- hopper schema v1. Objects are created in the first schema of the
-- connection's search_path, so hopper can be isolated in its own schema.

-- hopper_uuidv7() generates time-ordered job IDs in the database, so Go
-- inserts, COPY and the SQL insert contract all get the same IDs and hopper's
-- Go code needs no random number generator. Postgres 18 has uuidv7() built in;
-- earlier versions build one from clock_timestamp() and gen_random_uuid().
DO $do$
BEGIN
  IF current_setting('server_version_num')::int >= 180000 THEN
    EXECUTE $fn$
      CREATE FUNCTION hopper_uuidv7() RETURNS uuid
      LANGUAGE sql VOLATILE PARALLEL SAFE
      AS 'SELECT uuidv7()'
    $fn$;
  ELSE
    EXECUTE $fn$
      CREATE FUNCTION hopper_uuidv7() RETURNS uuid
      LANGUAGE sql VOLATILE PARALLEL SAFE
      AS $body$
        SELECT encode(
          set_bit(
            set_bit(
              overlay(uuid_send(gen_random_uuid())
                placing substring(int8send((extract(epoch FROM clock_timestamp()) * 1000)::bigint) FROM 3)
                FROM 1 FOR 6),
              52, 1),
            53, 1),
          'hex')::uuid
      $body$
    $fn$;
  END IF;
END
$do$;

CREATE TYPE hopper_job_state AS ENUM (
  'pending',     -- waiting on workflow dependencies (reserved for workflows)
  'available',   -- ready once scheduled_at <= now()
  'scheduled',   -- inserted or snoozed with a future scheduled_at
  'running',
  'retryable',   -- failed; will run again at scheduled_at
  'completed',
  'cancelled',
  'discarded'    -- out of attempts, expired, or cancelled by the worker: the dead-letter state
);

-- Live jobs only. Finalized jobs move to hopper_job_history, so this table and
-- its indexes stay sized to the backlog, not to all-time volume.
CREATE TABLE hopper_jobs (
  id                  uuid        NOT NULL DEFAULT hopper_uuidv7() PRIMARY KEY,
  seq                 bigint      GENERATED ALWAYS AS IDENTITY,  -- internal FIFO tie-break
  kind                text        NOT NULL,
  queue               text        NOT NULL DEFAULT 'default',
  state               hopper_job_state NOT NULL DEFAULT 'available',
  priority            smallint    NOT NULL DEFAULT 2 CHECK (priority BETWEEN 1 AND 4),
  attempt             smallint    NOT NULL DEFAULT 0,
  max_attempts        smallint    NOT NULL DEFAULT 25,
  scheduled_at        timestamptz NOT NULL DEFAULT now(),
  attempted_at        timestamptz,
  attempted_by        bigint,                    -- hopper_clients.id holding the claim
  args                jsonb       NOT NULL DEFAULT '{}',
  metadata            jsonb       NOT NULL DEFAULT '{}',   -- trace context, topic, message ID
  errors              jsonb       NOT NULL DEFAULT '[]',   -- [{at, attempt, error, trace}]
  unique_key          text,
  ordering_key        text,
  partition_key       text,                      -- for partitioned rate/concurrency limits
  batch_id            uuid,
  expires_at          timestamptz,               -- TTL: discarded if not started by then
  cancel_requested_at timestamptz,
  await               boolean     NOT NULL DEFAULT false,   -- notify waiters on finalize
  created_at          timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 70);

-- Claim path: the only index the hot query touches.
CREATE INDEX hopper_jobs_claim ON hopper_jobs (queue, priority, scheduled_at, seq)
  WHERE state IN ('available', 'scheduled', 'retryable');

-- Running set: rescue, cancel delivery, global limits. Bounded by total concurrency.
CREATE INDEX hopper_jobs_running ON hopper_jobs (queue, attempted_by) WHERE state = 'running';

-- Uniqueness applies only while a job is live.
CREATE UNIQUE INDEX hopper_jobs_unique ON hopper_jobs (kind, unique_key) WHERE unique_key IS NOT NULL;

-- Finalized jobs. Two-level partitioning: by outcome, then by time, so
-- retention is DROP TABLE on an old partition, with no DELETE and no vacuum
-- debt. Time partitions are managed by the leader; the DEFAULT partitions
-- catch rows until then.
CREATE TABLE hopper_job_history (
  LIKE hopper_jobs,
  finalized_at timestamptz NOT NULL,
  output       jsonb
) PARTITION BY LIST (state);

CREATE TABLE hopper_job_history_completed PARTITION OF hopper_job_history
  FOR VALUES IN ('completed') PARTITION BY RANGE (finalized_at);
CREATE TABLE hopper_job_history_failed PARTITION OF hopper_job_history
  FOR VALUES IN ('cancelled', 'discarded') PARTITION BY RANGE (finalized_at);
CREATE TABLE hopper_job_history_completed_default PARTITION OF hopper_job_history_completed DEFAULT;
CREATE TABLE hopper_job_history_failed_default PARTITION OF hopper_job_history_failed DEFAULT;

CREATE INDEX hopper_job_history_id ON hopper_job_history (id);
CREATE INDEX hopper_job_history_queue ON hopper_job_history (queue, finalized_at);

-- Liveness: one lease row per running client process.
CREATE TABLE hopper_clients (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  hostname    text        NOT NULL,
  started_at  timestamptz NOT NULL DEFAULT now(),
  expires_at  timestamptz NOT NULL,
  info        jsonb       NOT NULL DEFAULT '{}'    -- version, queues, pid
);

CREATE TABLE hopper_leader (
  name       text PRIMARY KEY DEFAULT 'default',
  client_id  bigint      NOT NULL,
  elected_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL
);

CREATE TABLE hopper_queues (
  name          text PRIMARY KEY,
  paused_at     timestamptz,
  global_limit  int,                    -- NULL = unlimited
  rate_per_sec  double precision,       -- NULL = unlimited
  rate_burst    int,
  tokens        double precision,
  refilled_at   timestamptz,
  metadata      jsonb       NOT NULL DEFAULT '{}',
  updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE hopper_periodic (          -- last inserted slot per periodic job
  name      text PRIMARY KEY,
  last_slot timestamptz NOT NULL
);

CREATE TABLE hopper_subscriptions (
  name       text PRIMARY KEY,
  pattern    text NOT NULL,             -- AMQP topic syntax
  kind       text NOT NULL,
  queue      text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE hopper_schema (
  version    int PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);
