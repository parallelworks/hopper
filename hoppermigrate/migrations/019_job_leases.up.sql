-- hopper schema v19: job leases, for attempts that run outside a client.
--
-- A claim by job ID (Leases.Claim) moves a waiting job to running with no
-- owning client and writes a lease row here: the job's ID, a random token
-- the database generates, the claim's fence, and the lease's TTL and
-- expiry. Renewals, progress writes and the result name the token and the
-- fence, and every one of them is conditional on the lease row; the leader
-- expires a lease that was not renewed in time. The client rescue leaves
-- leased jobs alone.
--
-- The lease lives in a table of its own rather than in hopper_jobs columns,
-- so that a renewal rewrites a narrow row in a small table, never the wide
-- job row, and so that this migration creates a table and takes no lock on
-- hopper_jobs or history. There is no index on expires_at, which would make
-- every renewal a non-HOT update; the leader scans this table instead.
-- There is no foreign key to hopper_jobs either, which would lock job rows
-- and tax every delete: a claim writes the lease in the statement that
-- moves the job, and the statements that end the attempt delete it.

CREATE TABLE hopper_job_leases (
  id         uuid        PRIMARY KEY,
  token      uuid        NOT NULL,
  fence      bigint      NOT NULL,
  ttl        interval    NOT NULL,
  renewed_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL
) WITH (fillfactor = 50, autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_threshold = 1000,
        autovacuum_vacuum_cost_delay = 0, vacuum_truncate = false);
