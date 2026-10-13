-- Dropping the column needs the same ACCESS EXCLUSIVE lock as adding it,
-- so it tries under the same short lock_timeout in a loop.

SELECT set_config('lock_timeout', '50ms', true);

DO $$
BEGIN
  LOOP
    BEGIN
      ALTER TABLE hopper_jobs DROP COLUMN snooze_reason;
      RETURN;
    EXCEPTION WHEN lock_not_available THEN
      PERFORM pg_sleep(0.05);
    END;
  END LOOP;
END
$$;
