# hopper: design and implementation plan

hopper is a job queue and message broker for Go that stores everything in
PostgreSQL. The aim is to replace both an in-process job framework (such as
River) and a separate broker (such as RabbitMQ) for applications that already
run on Postgres, with one small, auditable, Apache-2.0 library.

- **Module:** `github.com/parallelworks/hopper`
- **License:** Apache-2.0
- **Dependencies:** the Go standard library and `github.com/jackc/pgx/v5`. Nothing else in the core module.
- **Status:** design. This document is the plan of record, and changes to it go through PRs.

The name refers to a feed hopper, which releases work into a machine one piece
at a time, and to RADM Grace Hopper. It is also a fitting name for something
meant to replace a *rabbit*.

---

## 1. Goals

1. **Transactional enqueue.** A job or message is inserted in the same transaction
   as the business data that caused it. Either both commit or neither does. This is
   the main reason to put the queue in Postgres.
2. **At-least-once execution, with exactly-once in the common case.** Every job runs
   to completion or ends in a terminal state with a recorded reason. A job runs twice
   only when a worker dies mid-job, so workers must be idempotent. The docs will
   say this prominently.
3. **Horizontal scale with no coordination service.** Any number of processes can run
   hopper clients against one database. Work is claimed with `FOR UPDATE SKIP LOCKED`.
   Singleton duties (periodic jobs, maintenance) run on a single leader elected
   through the database.
4. **Runs inside the application process.** hopper is a library, not a server.
5. **Small and auditable.** The target is under 5k lines of non-test Go for the core,
   written for people who have to review it.
6. **FIPS-friendly.** hopper does no cryptography of its own, so it works under
   `GODEBUG=fips140=only`. Tests run in that mode.
7. **Messaging (phase 2).** Topics, subscriptions and fan-out with acks, retries and
   dead-lettering, covering the RabbitMQ features that application code actually uses.

## 2. Non-goals

- The AMQP, STOMP or MQTT wire protocols. hopper is a Go API over SQL.
- High-throughput streaming. Postgres-backed queues comfortably handle thousands of
  jobs per second, not hundreds of thousands. Kafka, NATS and RabbitMQ remain the
  right tools at that scale. We will publish benchmarks, not promises.
- Cross-database support. hopper is Postgres only.
- A hosted UI in the core module. An optional `hopperui` module may come later (§12).
- Workflow orchestration (DAGs, sagas). This might become a separate module later,
  never part of the core.

## 3. Concepts

| Term | Meaning |
| --- | --- |
| **Job** | One unit of work: a kind, JSON args, a queue, a priority, a schedule and an attempt history. |
| **Kind** | A string naming the job type (`"send_email"`). It maps to exactly one worker. |
| **Args** | A Go struct implementing `Kind() string`, stored as `jsonb`. |
| **Queue** | A named lane with its own concurrency limit on each client (`"default"`, `"email"`). |
| **Worker** | Code that performs one kind of job. |
| **Client** | The per-process runtime: it inserts, fetches, works, heartbeats and completes jobs. |
| **Leader** | The one client, elected through the database, that runs periodic jobs and maintenance. |
| **Topic / Subscription** (phase 2) | A message published to a topic is delivered as one job to each matching subscription. |

## 4. Public API sketch

The API is illustrative; names get settled in the M1 PR.

```go
// Args and workers are strongly typed with generics.
type SendEmail struct {
    UserID int64  `json:"user_id"`
    Tmpl   string `json:"tmpl"`
}

func (SendEmail) Kind() string { return "send_email" }

type SendEmailWorker struct{ hopper.WorkerDefaults[SendEmail] }

func (w *SendEmailWorker) Work(ctx context.Context, job *hopper.Job[SendEmail]) error {
    // A returned error schedules a retry with backoff.
    // hopper.Snooze(d) reschedules without using up an attempt.
    // hopper.Cancel(err) discards the job immediately with no retry.
    return nil
}

workers := hopper.NewWorkers()
hopper.AddWorker(workers, &SendEmailWorker{})

client, err := hopper.NewClient(pool, hopper.Config{
    Queues:  map[string]hopper.QueueConfig{"default": {MaxWorkers: 25}},
    Workers: workers,
    Periodic: []hopper.PeriodicJob{
        hopper.Every(15*time.Minute, "sync_allocations", func() hopper.JobArgs { return SyncAllocations{} }),
    },
    Logger: slog.Default(),
})

// Transactional enqueue: the job exists only if tx commits.
_, err = client.InsertTx(ctx, tx, SendEmail{UserID: 42, Tmpl: "welcome"}, &hopper.InsertOpts{
    Queue:       "email",
    Priority:    1,                                   // 1 = highest … 4
    ScheduledAt: time.Now().Add(time.Hour),
    MaxAttempts: 10,
    UniqueKey:   "welcome:42",                         // deduplicates while the job is live
})

err = client.Start(ctx) // begins fetching and working; returns immediately
err = client.Stop(ctx)  // stops fetching and drains; when ctx expires, cancels job contexts

// Management
client.JobCancel(ctx, id)
client.JobRetry(ctx, id)          // move a discarded or cancelled job back to available
client.QueuePause(ctx, "email")
client.Stats(ctx)                 // counts by queue and state, for metrics and health checks

// Messaging (phase 2)
client.Subscribe(ctx, hopper.Subscription{Name: "billing", Topic: "allocation.*", Kind: "billing_on_allocation"})
client.PublishTx(ctx, tx, "allocation.created", payload)
```

A `hopper.Pool` interface covers `*pgxpool.Pool`. A `database/sql` adapter can be
added later without touching the core.

## 5. Schema

Tables are prefixed with `hopper_` and live in whatever schema is first in the
connection's `search_path`, so users can isolate hopper in its own schema.

```sql
CREATE TYPE hopper_job_state AS ENUM (
  'available',   -- ready to run once scheduled_at <= now()
  'scheduled',   -- inserted with a future scheduled_at
  'running',
  'retryable',   -- failed; will run again at scheduled_at
  'completed',
  'cancelled',
  'discarded'    -- out of attempts or cancelled by the worker; the dead-letter state
);

CREATE TABLE hopper_jobs (
  id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  kind           text        NOT NULL,
  queue          text        NOT NULL DEFAULT 'default',
  args           jsonb       NOT NULL DEFAULT '{}',
  metadata       jsonb       NOT NULL DEFAULT '{}',    -- tracing IDs, publish topic, ...
  state          hopper_job_state NOT NULL DEFAULT 'available',
  priority       smallint    NOT NULL DEFAULT 1 CHECK (priority BETWEEN 1 AND 4),
  attempt        smallint    NOT NULL DEFAULT 0,
  max_attempts   smallint    NOT NULL DEFAULT 25,
  scheduled_at   timestamptz NOT NULL DEFAULT now(),
  attempted_at   timestamptz,
  attempted_by   text,                                  -- client ID
  heartbeat_at   timestamptz,
  finalized_at   timestamptz,
  errors         jsonb[]     NOT NULL DEFAULT '{}',     -- {at, attempt, error, trace} per failure
  unique_key     text,
  cancel_requested boolean   NOT NULL DEFAULT false,
  created_at     timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 80);   -- leaves room for HOT updates on a table that is updated constantly

-- Fetch path: the only index the hot query uses.
CREATE INDEX hopper_jobs_fetch ON hopper_jobs (queue, priority, scheduled_at, id)
  WHERE state IN ('available', 'scheduled', 'retryable');

-- Rescuer: running jobs, ordered by heartbeat.
CREATE INDEX hopper_jobs_running ON hopper_jobs (heartbeat_at) WHERE state = 'running';

-- Pruner.
CREATE INDEX hopper_jobs_finalized ON hopper_jobs (state, finalized_at) WHERE finalized_at IS NOT NULL;

-- Uniqueness applies only while a job is live.
CREATE UNIQUE INDEX hopper_jobs_unique ON hopper_jobs (kind, unique_key)
  WHERE unique_key IS NOT NULL AND state IN ('available', 'scheduled', 'running', 'retryable');

CREATE TABLE hopper_leader (
  name       text PRIMARY KEY DEFAULT 'default',
  leader_id  text        NOT NULL,
  elected_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL
);

CREATE TABLE hopper_queues (             -- pause state and future per-queue settings
  name       text PRIMARY KEY,
  paused_at  timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE hopper_schema (version int PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());
```

Phase 2 adds `hopper_subscriptions (name, topic_pattern, kind, queue, created_at)`.

### State machine

```
insert ──► available ─┐                     ┌──► completed
insert ──► scheduled ─┼─► (fetch) running ──┼──► retryable ──(scheduled_at)──► fetchable again
           retryable ─┘        │            ├──► discarded   (attempts exhausted or hopper.Cancel)
                               │            └──► cancelled   (JobCancel while running)
      JobCancel ◄──────────────┴─ also applies to available, scheduled and retryable jobs
      rescuer: running with stale heartbeat ──► retryable (or discarded if out of attempts)
```

`available`, `scheduled` and `retryable` all mean "fetch once `scheduled_at <= now()`".
They stay separate states only so that operators can see why a job is waiting. As a
result there is **no scheduler loop** moving jobs between states, which removes a whole
maintenance process.

## 6. Core algorithms

### 6.1 Clock
All time comparisons use the database's `now()`, never the application clock, so clock
skew between pods cannot cause early or duplicate execution.

### 6.2 Insert
`INSERT … ON CONFLICT (kind, unique_key) WHERE … DO NOTHING RETURNING id`. A conflict
returns the existing job's ID with `Duplicate: true`. The insert then calls
`pg_notify('hopper_insert', queue)` in the same statement or transaction. Postgres
delivers the notification only when the transaction commits, so no worker is ever woken
for an uncommitted job. `InsertMany` uses `COPY` or a multi-row `INSERT` and sends one
notify per queue.

### 6.3 Fetch (claim)
Each queue has a producer loop. When it has free worker slots, it runs:

```sql
UPDATE hopper_jobs SET state = 'running', attempt = attempt + 1,
       attempted_at = now(), attempted_by = $client, heartbeat_at = now()
WHERE id IN (
  SELECT id FROM hopper_jobs
  WHERE queue = $queue AND state IN ('available','scheduled','retryable')
    AND scheduled_at <= now()
  ORDER BY priority, scheduled_at, id
  LIMIT $free_slots
  FOR UPDATE SKIP LOCKED)
RETURNING *;
```

The producer runs this query when:
1. a `hopper_insert` notification arrives for its queue (debounced by about 50ms, so a
   burst of inserts triggers one fetch, not a flood of queries);
2. a job finishes and frees a slot; or
3. the poll interval elapses (default 1s). This is the safety net for missed
   notifications, poolers that don't support LISTEN, and jobs whose `scheduled_at`
   has just arrived.

A paused queue skips fetching. Each client learns the pause state from a
`hopper_control` notification and re-reads it on every poll.

### 6.4 Execute
- Workers run with a bounded number of goroutines per queue.
- Each job gets a context that is cancelled on timeout (worker `Timeout()` or the
  client default), on `JobCancel`, or on a hard stop.
- A panic is recovered and recorded as an error with its stack trace, then treated as
  a normal failure.
- Middleware (`func(ctx, job, next) error`) provides logging, metrics and tracing
  hooks.

### 6.5 Complete
Results are buffered and written in batches. The completer flushes every 50ms or every
100 results, whichever comes first, with one `UPDATE … FROM unnest($ids, $states, …)`.
The update is conditional on `state = 'running' AND attempted_by = $client`. If the
rescuer has already reclaimed the job, the stale result is dropped and logged, so a
completion can never overwrite a newer attempt.

Outcomes:
- **success:** `completed`, `finalized_at = now()`.
- **error:** `retryable`, `scheduled_at = now() + backoff(attempt)`, error appended. If
  `attempt >= max_attempts`, the job becomes `discarded` instead.
- **`hopper.Snooze(d)`:** `scheduled`, `attempt = attempt - 1`, `scheduled_at = now() + d`.
- **`hopper.Cancel(err)`:** `discarded` immediately, with the error recorded.

`backoff(n) = n^4 + 5s ± 10% jitter`, capped at 24h. Workers can override it with a
`NextRetry(job) time.Time` method.

`CompleteTx(ctx, tx, job)` finalizes a job inside the worker's own transaction, so
"do the work and mark it done" can be atomic.

### 6.6 Heartbeat and rescue
Every 10s, each client runs one `UPDATE hopper_jobs SET heartbeat_at = now() WHERE id =
ANY($running_ids)` for all of its running jobs. The same round trip returns
`cancel_requested` flags, which is how `JobCancel` reaches a job running on another
pod: the heartbeat sees the flag and cancels the job's context.

The leader's rescuer treats `running` jobs whose `heartbeat_at < now() - 1m` (the
configurable `RescueAfter`) as orphaned. It moves them to `retryable` with the error
"worker lost", or to `discarded` if they are out of attempts.

This is the one intentional design difference from River. River rescues jobs by age,
with a default of 1h. Heartbeats let hopper rescue a job from a dead pod in about a
minute without mistaking a slow, healthy job for a dead one.

### 6.7 Leader election
Leadership is a lease row in `hopper_leader`:

```sql
INSERT INTO hopper_leader (name, leader_id, elected_at, expires_at)
VALUES ('default', $me, now(), now() + $ttl)
ON CONFLICT (name) DO UPDATE SET leader_id = $me, elected_at = now(), expires_at = now() + $ttl
WHERE hopper_leader.expires_at < now() OR hopper_leader.leader_id = $me
RETURNING leader_id = $me;
```

- The TTL is 15s, and the leader renews every 5s.
- On graceful stop the leader deletes its row and sends a `hopper_leader` notification,
  so a replacement takes over immediately.

A lease table is used instead of `pg_advisory_lock` for three reasons: it works behind
PgBouncer in transaction mode and behind RDS Proxy, it can be inspected with plain SQL,
and it doesn't pin a connection. Leader-only duties must be idempotent anyway, because
two leaders can briefly overlap during a network partition. §6.8 is designed with that
in mind.

### 6.8 Periodic jobs
Each periodic job has a name and a schedule: `Every(d)` in v0.1, with cron expressions
in a later phase (§12).

On each tick, the leader computes the current slot boundary and inserts the job with
`unique_key = "periodic:<name>:<slot RFC3339>"`. The unique index ensures that one slot
produces at most one job, even if two leaders overlap or leadership changes hands
mid-slot. `RunOnStart` inserts the current slot immediately. Missed slots are not
back-filled by default; this is configurable.

### 6.9 Pruner
The leader deletes finalized jobs past their retention in batches of 1,000
(`DELETE … WHERE id IN (SELECT … LIMIT 1000)`), so it never holds long locks. Default
retention: 24h for `completed`, 7d for `cancelled` and `discarded`.

### 6.10 Shutdown
`Stop(ctx)` works in three steps:
1. Stop fetching and give up leadership.
2. Wait for running jobs to finish and flush the completer.
3. If `ctx` expires first, cancel all job contexts, wait a short grace period, flush
   whatever finished, and return `ctx.Err()`. Jobs still unfinished at that point keep
   `state = 'running'` and are rescued by another client, which is at-least-once
   behavior.

### 6.11 Errors that become useless without a fix
If a `Work` method keeps failing because of a bug, retries only add noise.
`hopper.Cancel(err)` gives workers an explicit way out. The `discarded` state is the
dead-letter queue, and `JobRetry` re-drives jobs once the bug is fixed.

## 7. Messaging (phase 2, the RabbitMQ replacement)

Messaging is modeled on RabbitMQ's topic exchange, built from job primitives so that it
inherits acks, retries, dead-lettering, transactional publish and observability without
new machinery.

- **Subscriptions** are rows: `(name, topic_pattern, kind, queue)`. Patterns use AMQP
  syntax: `.`-separated words, where `*` matches one word and `#` matches zero or more.
- **`PublishTx(ctx, tx, topic, payload)`** inserts one job per matching subscription in
  the same transaction, via a single `INSERT … SELECT` that matches patterns in SQL.
  Each job's `metadata` records the topic and a message ID. Fan-out happens at publish
  time, so a subscription created later does not receive earlier messages. That matches
  RabbitMQ.
- **Consumers** are ordinary workers for the subscription's `kind`. Returning nil acks
  the message; returning an error nacks it with backoff; when attempts run out, it is
  dead-lettered.
- **Competing consumers** come for free, because every replica's client consumes the
  same queue with SKIP LOCKED.
- **Ordering keys** (phase 3). This is optional FIFO per key: at most one `running` job
  per `(queue, ordering_key)` at a time, enforced by a partial unique index on running
  jobs plus fetch-time filtering. This gives per-entity ordering, for example "events for
  allocation 42 in order", without global ordering.
- **Cross-language producers.** The insert SQL is a documented, stable contract, so
  Python or shell scripts can enqueue jobs with plain SQL. That is useful on HPC systems.

This covers the RabbitMQ features typically used between application components. It
does not replace RabbitMQ as an external, multi-protocol broker.

## 8. Migrations

- SQL files are embedded in the `hoppermigrate` package, versioned, and forward-only by
  default. Down migrations exist for development.
- `hoppermigrate.Up(ctx, pool)` applies them under an advisory lock taken on a dedicated
  connection, not a pooled one. This avoids the pool-starvation deadlock we hit in pie
  (see pie#1).
- The same SQL files are published for teams that use goose, atlas or Flyway. The
  `hopper_schema` table records the applied version either way.
- Every schema change ships with an upgrade test that migrates from the previous release
  while jobs are live.

## 9. Observability

- `log/slog` is used throughout, and `Config.Logger` is required.
- An **event subscription API** (`client.Subscribe(events...)`) streams job completed,
  failed, discarded and similar events for tests and custom metrics.
- `Stats(ctx)` returns counts by queue and state, the oldest available job's age, and
  the current leader, for dashboards and readiness checks.
- An OpenTelemetry middleware lives in a separate `hopperotel` module, so the core
  stays dependency-free.

## 10. Testing strategy

| Layer | What |
| --- | --- |
| Unit | Backoff, pattern matching, the state transition table, option validation. |
| Integration | Against real Postgres 14–18 in CI (a service container per version), always with `-race` and `GODEBUG=fips140=only`. Each test gets its own schema, so tests run in parallel. |
| Concurrency | N clients by M jobs: every job is completed exactly once when nothing crashes. Uniqueness under concurrent inserts. Periodic slots are unique across two leaders. |
| Chaos | `pg_terminate_backend` on a client mid-job, followed by rescue and re-run. Leader killed, then a new leader within the TTL. Stop with an expired context. Listener connection dropped, then polling continues and the listener reconnects. |
| Upgrade | Migrate from release N-1 with jobs in every state. |
| Benchmarks | Insert (single, batch, transactional) and end-to-end throughput for no-op jobs with 1, 4 and 16 clients. Results are published in the README. |
| Test helpers | A `hoppertest` package for users: `RequireInserted`, `WorkOne`, and a transaction-scoped client. |

## 11. Compatibility

- **Postgres:** 14 and later, which covers all current AWS RDS and Cloud SQL major
  versions.
- **Go:** the two most recent releases, matching Go's own support policy.
- **Poolers:** PgBouncer in transaction mode and RDS Proxy work. LISTEN/NOTIFY needs a
  session connection, so behind a transaction pooler hopper falls back to polling (and
  logs that once).
- **Stability:** API stability starts at v1.0.0. Until then, minor versions may break
  the API, with changes noted in the CHANGELOG.

## 12. Milestones

The estimates assume one engineer. Each milestone is one or more PRs.

| # | Milestone | Scope | Est. |
| --- | --- | --- | --- |
| M0 | Scaffold | go.mod, CI (lint, test matrix, fips140=only), PR-title check, Dependabot, CONTRIBUTING | 0.5d |
| M1 | Core loop | Schema v1, `hoppermigrate`, Insert/InsertTx/InsertMany, typed workers, fetch/work/complete with batching, retries and backoff, graceful Stop | 3–4d |
| M2 | Reliability | Heartbeats and rescuer, leader lease, pruner, LISTEN/NOTIFY with poll fallback, unique jobs | 3d |
| M3 | Control | Periodic `Every`, Snooze, Cancel (in-flight via heartbeat), JobRetry, queue pause, per-worker timeouts, middleware | 2d |
| M4 | Harden and release | Chaos and upgrade tests, benchmarks, `hoppertest`, docs and examples, **v0.1.0** | 2–3d |
| M5 | Adopt in pie | Replace River in pie's `internal/jobs`; drain and drop River's tables | 1d |
| M6 | Messaging | Subscriptions, AMQP-style topic patterns, PublishTx fan-out, docs, **v0.2.0** | 3–5d |
| M7 | Later | Cron schedules, ordering keys, `hopperotel`, read-only `hopperui`, `database/sql` adapter | as needed |

M0–M4 total roughly 2–2.5 weeks to a production-ready v0.1.0.

## 13. Risks

| Risk | Mitigation |
| --- | --- |
| Subtle concurrency bugs: double execution, lost jobs | Chaos and concurrency tests from M1 onward. Every state transition is a conditional `UPDATE` that checks the prior state and owner. |
| Bloat on the hot jobs table | fillfactor 80 for HOT updates, a partial fetch index, batched completion, the pruner, and documented autovacuum settings for the table. |
| Notification storms at high insert rates | Debounced listeners, one notify per queue per transaction, and a fixed poll interval as the backstop. |
| Ongoing maintenance burden | A small scope with explicit non-goals, and the core kept to stdlib plus pgx. |
| Scale expectations set by the "replace RabbitMQ" framing | Publish benchmarks and document the ceiling clearly (§2). |

## 14. Open questions

1. Should args support optional application-level encryption, through a user-supplied
   `crypto/cipher.AEAD`? That would keep hopper out of the key-management business
   while allowing sensitive args. Default: no; document "don't put secrets in args".
2. Should queues be declared up front in `Config` only, or also be creatable at runtime
   for multi-tenant use? Default: config only in v0.1.
3. Should job IDs be `bigint` identity (compact, ordered) or UUIDv7 (safe to expose
   externally)? The plan says bigint. Revisit if IDs need to cross trust boundaries.
4. Do we need an `InsertMany` fast path using `COPY` in v0.1, or is batch `INSERT`
   enough for pie's volumes?
