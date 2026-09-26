# The SQL contract

Producers not written in Go enqueue work with plain SQL, inside their own
transactions. Two functions ship with the migrations and are versioned with
the schema (`hopper_schema`). They change only with a deprecation window.

## `hopper_insert(kind text, args jsonb, opts jsonb DEFAULT '{}') RETURNS uuid`

Inserts one job and returns its ID.

```sql
SELECT hopper_insert('send_email', '{"user_id": 42, "tmpl": "welcome"}',
                     '{"queue": "email", "priority": 1, "max_attempts": 5}');
```

| `opts` key | Type | Meaning |
| --- | --- | --- |
| `queue` | string | Defaults to `default`. |
| `priority` | 1–4 | 1 runs first; defaults to 2. |
| `max_attempts` | int | Defaults to 25. |
| `scheduled_at` | timestamptz | Run no earlier than this. |
| `metadata` | object | Stored with the job; trace context goes here. |
| `unique_key` | string | At most one live job per (kind, key); a conflict returns the existing job's ID. |
| `ttl_seconds` | number | Discard the job if it has not started within this long. |
| `ordering_key` | string | Serialize with other jobs of the same key in the queue. |
| `await` | bool | Announce the finalize on `hopper_done`. |

Unique keys built by the Go client have the form documented in
`hopper.UniqueOpts`; a producer that must share a key with Go code should
build the same text.

## `hopper_publish(topic text, payload jsonb, opts jsonb DEFAULT '{}') RETURNS SETOF uuid`

Delivers a message to every subscription whose pattern matches the topic,
one job per subscription, and returns the delivery IDs. Workers are notified
once per queue when the transaction commits.

```sql
SELECT hopper_publish('allocation.created', '{"id": 42}',
                      '{"ordering_key": "allocation:42", "dedup_key": "evt-7f3a"}');
```

| `opts` key | Type | Meaning |
| --- | --- | --- |
| `ordering_key` | string | FIFO per key, per queue. |
| `dedup_key` | string | Idempotent publish while an earlier delivery is live. |
| `delay_seconds` | number | Deliver later. |
| `ttl_seconds` | number | Discard deliveries not started within this long. |
| `priority` | 1–4 | Defaults to 2. |
| `headers` | object of strings | Available to consumers as `Message.Headers`. |
| `await` | bool | Announce each delivery's finalize on `hopper_done`. |

Each delivery's `metadata` holds `topic`, `message_id` and `headers`, merged
with the subscription's own metadata. The job kind is `sub:<subscription>`
and the args are the payload.

## `hopper_topic_regex(pattern text) RETURNS text`

Converts an AMQP topic pattern (`*` one word, `#` zero or more words) to a
regular expression; `topic ~ hopper_topic_regex(pattern)` is the match used
by `hopper_publish`.

## Direct inserts

The functions are the supported path. Inserting into `hopper_jobs` directly
works too, as long as `state` is `available` (or `scheduled` when
`scheduled_at` is in the future) and `scheduled_at` uses the database clock;
the columns' defaults cover the rest. Rows in other states are the client's
to manage.
