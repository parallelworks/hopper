-- hopper schema v13: list a key's finished jobs without reading all history.

CREATE INDEX hopper_job_history_ordering ON hopper_job_history (ordering_key, id)
  WHERE ordering_key IS NOT NULL;
