-- hopper schema v20: periodic jobs registered by each client.
--
-- A client records the periodic jobs it registers, so that the leader
-- schedules the jobs of every live client, not only its own. A registration
-- lasts as long as its client's lease: the leader reads only those of
-- unexpired clients, and deleting a client's row deletes its registrations.
-- definition is the job as the client built it, slot-independent parts only.
-- jsonb stores it reformatted, so digest, which the client computes from the
-- bytes it encoded, is what tells one definition from another.

CREATE TABLE hopper_periodic_registrations (
  client_id  bigint NOT NULL,
  name       text   NOT NULL,
  definition jsonb  NOT NULL,
  digest     text   NOT NULL,
  PRIMARY KEY (client_id, name)
);
