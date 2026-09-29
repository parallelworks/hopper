-- hopper schema v6: autovacuum settings for the live table.
--
-- hopper_jobs is small but churns at the job rate, and every claim walks
-- the claim index from its head, past the entries of jobs that have since
-- run and left. Those entries go away with vacuum, so claim latency grows
-- between vacuums; with Postgres's defaults (a 20% dead-tuple threshold and
-- a cost delay) the table can go a minute between passes at high rates,
-- and pickup latency was seen to climb into seconds before each pass. The
-- table settings below make autovacuum start after 1% churn and run at
-- full speed. How often it can start at all is autovacuum_naptime, a
-- server setting (60s by default); docs/operations.md recommends 5-10s
-- for high-rate installations.

ALTER TABLE hopper_jobs SET (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_threshold = 1000,
  autovacuum_vacuum_cost_delay = 0,
  autovacuum_analyze_scale_factor = 0.02,
  autovacuum_vacuum_insert_scale_factor = 0.05
);
