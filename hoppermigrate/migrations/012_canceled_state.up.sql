-- hopper schema v12: the canceled job state is spelled canceled.
--
-- Renaming an enum label rewrites only its pg_enum row: stored rows and the
-- history partition bound refer to the label's OID, so they follow without
-- a table rewrite or a lock on hopper_jobs. Statements of a release before
-- v12 still write 'cancelled' and fail against this schema.

ALTER TYPE hopper_job_state RENAME VALUE 'cancelled' TO 'canceled';
