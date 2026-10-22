-- hopper schema v1 on SQLite.
--
-- SQLite's scripts follow hoppermigrate's Postgres scripts version for
-- version, so that a version names the same tables and columns on both.
-- What only Postgres has (partitions, enum types, autovacuum settings, SQL
-- functions) is left out, and a version that changes only those is a script
-- of comments here.
--
-- Times are INTEGER microseconds since the Unix epoch, which is Postgres's
-- timestamptz precision; IDs are 16-byte BLOBs, which sort as UUIDs do; JSON
-- values are TEXT.

-- Live jobs only. Finalized jobs move to hopper_job_history, so this table and
-- its indexes stay sized to the backlog, not to all-time volume. seq is the
-- rowid, which AUTOINCREMENT never reuses: history keeps it, and it orders a
-- workflow's jobs.
CREATE TABLE hopper_jobs (
  seq                 INTEGER PRIMARY KEY AUTOINCREMENT,
  id                  BLOB    NOT NULL UNIQUE CHECK (length(id) = 16),
  kind                TEXT    NOT NULL,
  queue               TEXT    NOT NULL DEFAULT 'default',
  state               TEXT    NOT NULL DEFAULT 'available'
                      CHECK (state IN ('pending', 'available', 'scheduled', 'running', 'retryable')),
  priority            INTEGER NOT NULL DEFAULT 2 CHECK (priority BETWEEN 1 AND 4),
  attempt             INTEGER NOT NULL DEFAULT 0,
  max_attempts        INTEGER NOT NULL DEFAULT 25,
  scheduled_at        INTEGER NOT NULL,
  attempted_at        INTEGER,
  attempted_by        INTEGER,                   -- hopper_clients.id holding the claim
  args                TEXT    NOT NULL DEFAULT '{}',
  metadata            TEXT    NOT NULL DEFAULT '{}',
  errors              TEXT    NOT NULL DEFAULT '[]',
  unique_key          TEXT,
  ordering_key        TEXT,
  partition_key       TEXT,
  batch_id            BLOB,
  expires_at          INTEGER,                   -- TTL: discarded if not started by then
  cancel_requested_at INTEGER,
  await               INTEGER NOT NULL DEFAULT 0,  -- notify waiters on finalize
  created_at          INTEGER NOT NULL
);

-- Claim path: the only index the hot query touches.
CREATE INDEX hopper_jobs_claim ON hopper_jobs (queue, priority, scheduled_at, seq)
  WHERE state IN ('available', 'scheduled', 'retryable');

-- Running set: rescue, cancel delivery, global limits. Bounded by total concurrency.
CREATE INDEX hopper_jobs_running ON hopper_jobs (queue, attempted_by) WHERE state = 'running';

-- Uniqueness applies only while a job is live.
CREATE UNIQUE INDEX hopper_jobs_unique ON hopper_jobs (kind, unique_key) WHERE unique_key IS NOT NULL;

-- The expiry sweep's walk.
CREATE INDEX hopper_jobs_expires ON hopper_jobs (expires_at) WHERE expires_at IS NOT NULL;

-- Finalized jobs, in one table: retention deletes them in batches.
CREATE TABLE hopper_job_history (
  id                  BLOB    NOT NULL,
  seq                 INTEGER NOT NULL,
  kind                TEXT    NOT NULL,
  queue               TEXT    NOT NULL,
  state               TEXT    NOT NULL CHECK (state IN ('completed', 'canceled', 'discarded')),
  priority            INTEGER NOT NULL,
  attempt             INTEGER NOT NULL,
  max_attempts        INTEGER NOT NULL,
  scheduled_at        INTEGER NOT NULL,
  attempted_at        INTEGER,
  attempted_by        INTEGER,
  args                TEXT    NOT NULL DEFAULT '{}',
  metadata            TEXT    NOT NULL DEFAULT '{}',
  errors              TEXT    NOT NULL DEFAULT '[]',
  unique_key          TEXT,
  ordering_key        TEXT,
  partition_key       TEXT,
  batch_id            BLOB,
  expires_at          INTEGER,
  cancel_requested_at INTEGER,
  await               INTEGER NOT NULL DEFAULT 0,
  created_at          INTEGER NOT NULL,
  finalized_at        INTEGER NOT NULL,
  output              TEXT
);

CREATE INDEX hopper_job_history_id ON hopper_job_history (id);
CREATE INDEX hopper_job_history_queue ON hopper_job_history (queue, finalized_at);
-- Retention's walk, by outcome.
CREATE INDEX hopper_job_history_retention ON hopper_job_history (state, finalized_at);

-- Liveness: one lease row per running client process.
CREATE TABLE hopper_clients (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  hostname    TEXT    NOT NULL,
  started_at  INTEGER NOT NULL,
  expires_at  INTEGER NOT NULL,
  info        TEXT    NOT NULL DEFAULT '{}'    -- version, queues, pid
);

CREATE TABLE hopper_leader (
  name       TEXT PRIMARY KEY DEFAULT 'default',
  client_id  INTEGER NOT NULL,
  elected_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL
);

CREATE TABLE hopper_queues (
  name          TEXT PRIMARY KEY,
  paused_at     INTEGER,
  global_limit  INTEGER,                -- NULL = unlimited
  rate_per_sec  REAL,                   -- NULL = unlimited
  rate_burst    INTEGER,
  tokens        REAL,
  refilled_at   INTEGER,
  metadata      TEXT    NOT NULL DEFAULT '{}',
  updated_at    INTEGER NOT NULL
);

CREATE TABLE hopper_periodic (          -- last inserted slot per periodic job
  name      TEXT PRIMARY KEY,
  last_slot INTEGER NOT NULL
);

CREATE TABLE hopper_subscriptions (
  name       TEXT PRIMARY KEY,
  pattern    TEXT    NOT NULL,          -- AMQP topic syntax
  kind       TEXT    NOT NULL,
  queue      TEXT    NOT NULL,
  created_at INTEGER NOT NULL
);

-- Notifications, which SQLite has no LISTEN for: a notifying transaction
-- writes its rows, which commit with it, and listeners in every process that
-- opens the database read the ones after the last they saw. Rows a minute
-- old are deleted as new ones are written.
CREATE TABLE hopper_notifications (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  channel    TEXT    NOT NULL,
  payload    TEXT    NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE INDEX hopper_notifications_created ON hopper_notifications (created_at);

CREATE TABLE hopper_schema (
  version    INTEGER PRIMARY KEY,
  applied_at INTEGER NOT NULL
);
