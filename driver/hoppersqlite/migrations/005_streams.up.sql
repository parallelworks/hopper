-- hopper schema v5: streams.
--
-- A stream is an append-only log of events. SQLite runs one writer at a
-- time, so events commit in the order they are appended, and an event's
-- position is its seq alone: a consumer reads the log past the last seq it
-- has seen, and nothing committed later can sort before it.

CREATE TABLE hopper_stream_events (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  topic      TEXT    NOT NULL,
  key        TEXT,                                  -- becomes the delivery's ordering key
  payload    TEXT    NOT NULL DEFAULT 'null',
  headers    TEXT    NOT NULL DEFAULT '{}',
  message_id BLOB    NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX hopper_stream_events_created ON hopper_stream_events (created_at);

CREATE TABLE hopper_stream_consumers (
  name         TEXT PRIMARY KEY,
  pattern      TEXT    NOT NULL,               -- AMQP topic syntax
  kind         TEXT    NOT NULL,               -- the delivery job kind
  queue        TEXT    NOT NULL,
  max_attempts INTEGER,
  metadata     TEXT    NOT NULL DEFAULT '{}',
  seen         INTEGER NOT NULL DEFAULT 0,     -- every event up to this seq has been read
  position     INTEGER NOT NULL DEFAULT 0,     -- the last event delivered
  delivered_at INTEGER,
  created_at   INTEGER NOT NULL
);
