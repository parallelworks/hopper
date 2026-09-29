-- hopper schema v7: no heap truncation on the live table.
--
-- A vacuum ends by trying to give back empty pages at the end of the table,
-- for which it needs an exclusive lock: it retries for up to five seconds
-- and, whenever it briefly succeeds, every claim and finalize waits behind
-- it while it inspects the tail. hopper_jobs churns at the job rate and
-- reuses freed pages within seconds, so the truncation gains nothing and
-- cost the reference run pickup-latency spikes of one to two seconds on
-- every pass. This applies to autovacuum; the leader's own vacuum passes
-- the equivalent option.

ALTER TABLE hopper_jobs SET (vacuum_truncate = false);
