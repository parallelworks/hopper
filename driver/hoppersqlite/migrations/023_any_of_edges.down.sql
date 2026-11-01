-- The edges of any-of groups become edges a dependent ignores the failure
-- of, which is how v22's finalize treated them.
UPDATE hopper_job_deps SET on_failure = 'ignore' WHERE any_of IS NOT NULL;
ALTER TABLE hopper_job_deps DROP COLUMN any_of;
