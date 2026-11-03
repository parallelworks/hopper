-- A pending resign request is dropped with its column.

ALTER TABLE hopper_leader DROP COLUMN resign_client_id;
