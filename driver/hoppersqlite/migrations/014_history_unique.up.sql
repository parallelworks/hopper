-- hopper schema v14: find a unique key's finished jobs without reading all
-- history, for an insert whose UniqueOpts.Finished names finished states.

CREATE INDEX hopper_job_history_unique ON hopper_job_history (kind, unique_key, finalized_at)
  WHERE unique_key IS NOT NULL;
