-- hopper schema v19: periodic jobs registered by each client.
--
-- A client records the periodic jobs it registers, so that the leader
-- schedules the jobs of every live client, not only its own. A registration
-- lasts as long as its client's lease: the leader reads only those of
-- unexpired clients, and deleting a client's row deletes its registrations.
-- definition is the job as the client built it, slot-independent parts only.
-- It is text, not jsonb, so that it reads back byte for byte: the leader
-- compares it with its own jobs' definitions to tell a changed job from the
-- same job registered by another client, and jsonb reorders keys.

CREATE TABLE hopper_periodic_registrations (
  client_id  bigint NOT NULL,
  name       text   NOT NULL,
  definition text   NOT NULL,
  PRIMARY KEY (client_id, name)
);
