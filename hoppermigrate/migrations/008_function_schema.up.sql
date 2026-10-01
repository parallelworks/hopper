-- Bind SQL producers to the installation's namespace, even when callers
-- have a different search_path. The migrator sets the local path first.
ALTER FUNCTION hopper_insert(text, jsonb, jsonb) SET search_path FROM CURRENT;
ALTER FUNCTION hopper_publish(text, jsonb, jsonb) SET search_path FROM CURRENT;
