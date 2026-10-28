-- hopper schema v20: periodic jobs registered by each client.
--
-- A client records the periodic jobs it registers, so that the leader
-- schedules the jobs of every live client, not only its own. A registration
-- lasts as long as its client's lease: the leader reads only those of
-- unexpired clients, and deleting a client's row deletes its registrations.
-- definition is the job as the client built it, slot-independent parts only;
-- digest, which the client computes from the bytes it encoded, tells one
-- definition from another.

CREATE TABLE hopper_periodic_registrations (
  client_id  INTEGER NOT NULL,
  name       TEXT    NOT NULL,
  definition TEXT    NOT NULL CHECK (json_valid(definition)),
  digest     TEXT    NOT NULL,
  PRIMARY KEY (client_id, name)
);
