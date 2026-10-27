-- hopper schema v19: job leases, for attempts that run outside a client.
--
-- A claim by job ID moves a waiting job to running with no owning client
-- and writes a lease row here: the job's ID, a random token the database
-- generates, the claim's fence, and the lease's TTL and expiry, in
-- microseconds. Every call on the attempt names the token and the fence;
-- the leader expires a lease that was not renewed in time.

CREATE TABLE hopper_job_leases (
  id         BLOB    PRIMARY KEY CHECK (length(id) = 16),
  token      BLOB    NOT NULL CHECK (length(token) = 16),
  fence      INTEGER NOT NULL,
  ttl        INTEGER NOT NULL,
  renewed_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL
) WITHOUT ROWID;
