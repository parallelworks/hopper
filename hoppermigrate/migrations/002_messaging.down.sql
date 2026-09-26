DROP FUNCTION hopper_publish(text, jsonb, jsonb);
DROP FUNCTION hopper_insert(text, jsonb, jsonb);
DROP FUNCTION hopper_topic_regex(text);
DROP INDEX hopper_jobs_ordering;
DROP INDEX hopper_jobs_ordering_running;
ALTER TABLE hopper_subscriptions DROP COLUMN metadata, DROP COLUMN max_attempts;
