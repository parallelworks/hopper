-- hopper schema v5: streams.
--
-- A stream is an append-only log of events, partitioned by day and dropped
-- by retention like history. Events are identified by (xid, seq): the
-- transaction that wrote them, then the event within it. A consumer reads
-- the log by snapshot deltas: the events of transactions committed since
-- the snapshot it last read to, whatever their xid, so a transaction that
-- commits late is delivered when it commits and never skipped, and a long
-- transaction never holds the others back. The leader turns the events of
-- each delta into delivery jobs.

CREATE TABLE hopper_stream_events (
  xid        xid8        NOT NULL DEFAULT pg_current_xact_id(),
  seq        bigint      GENERATED ALWAYS AS IDENTITY,
  topic      text        NOT NULL,
  key        text,                                  -- becomes the delivery's ordering key
  payload    jsonb       NOT NULL DEFAULT 'null',
  headers    jsonb       NOT NULL DEFAULT '{}',
  message_id uuid        NOT NULL DEFAULT hopper_uuidv7(),
  created_at timestamptz NOT NULL DEFAULT now()
) PARTITION BY RANGE (created_at);

CREATE INDEX hopper_stream_events_position ON hopper_stream_events (xid, seq);
CREATE TABLE hopper_stream_events_default PARTITION OF hopper_stream_events DEFAULT;

CREATE TABLE hopper_stream_consumers (
  name         text PRIMARY KEY,
  pattern      text        NOT NULL,               -- AMQP topic syntax
  kind         text        NOT NULL,               -- the delivery job kind
  queue        text        NOT NULL,
  max_attempts smallint,
  metadata     jsonb       NOT NULL DEFAULT '{}',
  seen         pg_snapshot NOT NULL DEFAULT '3:3:', -- every transaction visible here has been delivered
  reading      pg_snapshot,                        -- the delta being delivered, when a pump stopped part way
  xid          xid8        NOT NULL DEFAULT '0',   -- the last event delivered of that delta
  seq          bigint      NOT NULL DEFAULT 0,
  delivered_at timestamptz,
  created_at   timestamptz NOT NULL DEFAULT now()
);
