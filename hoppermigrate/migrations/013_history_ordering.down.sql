-- Dropping an outcome table's index drops every partition's index attached to it.
DROP INDEX hopper_job_history_completed_ordering;
DROP INDEX hopper_job_history_failed_ordering;
