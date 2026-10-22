-- hopper schema v3: flow control and batches.

ALTER TABLE hopper_queues ADD COLUMN partition_limit INTEGER;  -- max running jobs per partition_key, NULL = unlimited
ALTER TABLE hopper_queues ADD COLUMN aging_seconds INTEGER;    -- raise a waiting job's priority after this long, NULL = off

-- Partitioned limits count running jobs per partition key.
CREATE INDEX hopper_jobs_partition_running ON hopper_jobs (queue, partition_key)
  WHERE state = 'running' AND partition_key IS NOT NULL;

-- A batch counts its pending jobs down in the transaction that finalizes
-- them, which no other writes at the same time, and enqueues its callbacks
-- when the count reaches zero.
CREATE TABLE hopper_batches (
  id           BLOB    PRIMARY KEY CHECK (length(id) = 16),
  pending      INTEGER NOT NULL,
  failed       INTEGER NOT NULL DEFAULT 0,
  total        INTEGER NOT NULL,
  on_success   TEXT,                        -- {kind, queue, args, priority, max_attempts, metadata, tags}
  on_failure   TEXT,
  on_complete  TEXT,
  metadata     TEXT    NOT NULL DEFAULT '{}',
  created_at   INTEGER NOT NULL,
  completed_at INTEGER
);

CREATE INDEX hopper_jobs_batch ON hopper_jobs (batch_id) WHERE batch_id IS NOT NULL;
