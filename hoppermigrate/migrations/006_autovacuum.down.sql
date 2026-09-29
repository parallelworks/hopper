ALTER TABLE hopper_jobs RESET (
  autovacuum_vacuum_scale_factor,
  autovacuum_vacuum_threshold,
  autovacuum_vacuum_cost_delay,
  autovacuum_analyze_scale_factor,
  autovacuum_vacuum_insert_scale_factor
);
