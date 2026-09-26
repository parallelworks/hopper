-- hopper schema v3: flow control and batches.
--
-- Queue limits live on the queue row so they hold across every client and
-- survive restarts. Batches count their pending jobs down in the finalize
-- statement and enqueue their callbacks when the count reaches zero.

ALTER TABLE hopper_queues
  ADD COLUMN partition_limit int,          -- max running jobs per partition_key, NULL = unlimited
  ADD COLUMN aging_seconds   int;          -- raise a waiting job's priority after this long, NULL = off

-- Partitioned limits count running jobs per partition key.
CREATE INDEX hopper_jobs_partition_running ON hopper_jobs (queue, partition_key)
  WHERE state = 'running' AND partition_key IS NOT NULL;

CREATE TABLE hopper_batches (
  id           uuid        NOT NULL DEFAULT hopper_uuidv7() PRIMARY KEY,
  pending      int         NOT NULL,
  failed       int         NOT NULL DEFAULT 0,
  total        int         NOT NULL,
  on_success   jsonb,                       -- {kind, queue, args, priority, max_attempts, metadata}
  on_failure   jsonb,
  on_complete  jsonb,
  metadata     jsonb       NOT NULL DEFAULT '{}',
  created_at   timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz
);

CREATE INDEX hopper_jobs_batch ON hopper_jobs (batch_id) WHERE batch_id IS NOT NULL;
