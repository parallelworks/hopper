-- hopper schema v2: messaging.
--
-- Subscriptions carry their own retry budget, and deliveries with an
-- ordering key run one at a time per key. Postgres's SQL insert and publish
-- functions have no SQLite counterpart.

ALTER TABLE hopper_subscriptions ADD COLUMN max_attempts INTEGER;  -- NULL: the client default (10 for ordered deliveries)
ALTER TABLE hopper_subscriptions ADD COLUMN metadata TEXT NOT NULL DEFAULT '{}';

-- Backstop for ordering keys: the claim selects at most one delivery per
-- (queue, ordering_key), and this makes running two of one key impossible.
CREATE UNIQUE INDEX hopper_jobs_ordering_running ON hopper_jobs (queue, ordering_key)
  WHERE state = 'running' AND ordering_key IS NOT NULL;

-- Oldest-first lookup per ordering key for the claim.
CREATE INDEX hopper_jobs_ordering ON hopper_jobs (queue, ordering_key, seq)
  WHERE ordering_key IS NOT NULL AND state IN ('available', 'scheduled', 'retryable');
