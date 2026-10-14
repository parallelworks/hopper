-- Dropping an outcome table's index drops every partition's index attached to it.
DROP INDEX hopper_job_history_completed_unique;
DROP INDEX hopper_job_history_failed_unique;

-- A partition's index that was built but never attached, such as one an
-- interrupted build left invalid, is not dropped with the outcome table's.
DO $$
DECLARE
  leftover regclass;
BEGIN
  FOR leftover IN
    SELECT c.oid::regclass FROM pg_class c
    WHERE c.relkind = 'i' AND c.relnamespace = current_schema()::regnamespace
      AND c.relname LIKE 'hopper\_job\_history\_%\_unique'
  LOOP
    EXECUTE 'DROP INDEX ' || leftover;
  END LOOP;
END
$$;
