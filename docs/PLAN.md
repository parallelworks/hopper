# hopper: design and implementation plan

hopper is a high-performance job queue and message broker for Go. It gives
applications durable background jobs, scheduled and periodic work, and pub/sub
messaging with transactional guarantees, using the database they already run.
PostgreSQL is the first engine. One Apache-2.0 library replaces both an
in-process job framework and a separate message broker.

- **Module:** `github.com/parallelworks/hopper`
- **License:** Apache-2.0
- **Dependencies:** the Go standard library and `github.com/jackc/pgx/v5`. Nothing else in the core module.
- **Status:** M0 through M4, M6 and M7 (except the `hoppersql` driver) are implemented (§16); the §8.2 targets still need a run on the reference hardware before v0.1.0 is tagged. This document is the plan of record, and changes to it go through PRs.

The name refers to a feed hopper, which releases work into a machine one piece
at a time, and to RADM Grace Hopper. It is also a fitting name for something
meant to replace a *rabbit*.

---

## 1. Goals

1. **Performance under load.** hopper is built for production traffic at its
   heaviest. Every hot-path operation works on sets: claim N jobs, finalize N
   results and insert N rows, each in one round trip. The live jobs table stays
   small no matter how much history accumulates, so its indexes stay in memory.
   Performance targets are release gates, measured in CI (§8).
2. **Transactional enqueue and publish.** A job or message is written in the same
   transaction as the business data that caused it. Either both commit or neither
   does, so no outbox table or dual-write reconciliation is needed.
3. **At-least-once execution, exactly-once in the common case.** Every job runs to
   completion or ends in a terminal state with a recorded reason. A job runs twice only
   when its process is lost mid-job. Workers must be idempotent, and the docs will
   say so prominently. `CompleteTx` makes "do the work and mark it done" atomic when
   the work is in the same database.
4. **Horizontal scale with no coordination service.** Any number of processes run
   hopper clients against one database. Work is claimed with `FOR UPDATE SKIP LOCKED`.
   Singleton duties run on a leader elected through the database. Adding replicas
   adds throughput.
5. **An idiomatic, modern Go API.** Generic, type-safe args and workers. `context`
   everywhere. `log/slog`. Range-over-func iterators for listing. Errors that work with
   `errors.Is` and `errors.As`. Zero-config defaults that are correct in production.
   The type system catches mistakes at compile time wherever possible.
6. **A complete feature set.** Priorities, scheduling, cron, retries with custom
   backoff, unique jobs and debouncing, snooze, cancellation, timeouts, awaitable
   results, global concurrency and rate limits, batches, workflows, topic-routed
   pub/sub, ordering keys, dead-lettering and replay. These are the features teams
   reach for in job frameworks and message brokers, all in one library.
7. **Engine-agnostic by design.** Postgres is the first engine. The core talks to
   storage through a `Driver` interface defined at the level of queue operations, not
   SQL strings, and every engine must pass a shared conformance, concurrency and chaos
   suite (§5).
8. **Runs inside the application process.** hopper is a library, not a server. It
   scales out with the application.
9. **Lean core, layered features.** The core module depends only on the standard
   library and pgx. Integrations (OpenTelemetry, the web UI, other engines) are separate
   modules, so applications pay only for what they import.
10. **FIPS-friendly.** hopper does no cryptography of its own, so it works under
    `GODEBUG=fips140=only`. Tests run in that mode.

## 2. Non-goals

- **Broker wire protocols** (AMQP, STOMP, MQTT). hopper is a Go API. Non-Go producers
  use the documented SQL insert contract (§10).
- **Network ingress** (HTTP or gRPC endpoints for enqueueing). Applications that want
  one can wrap `client.Insert` in their own handler with their own authentication.
  The docs include a short example.
- **A hosted UI in the core module.** The web UI is the separate `hopperui` module (§13).
- **Application-level cryptography.** hopper does not encrypt args. The `Codec` hook
  (§7.13) lets applications encrypt payloads themselves if they need to.

## 3. Concepts

| Term | Meaning |
| --- | --- |
| **Job** | One unit of work: a kind, args, a queue, a priority, a schedule and an attempt history. |
| **Kind** | A string naming the job type (`"send_email"`). It maps to exactly one worker. |
| **Args** | A Go struct implementing `Kind() string`, encoded by the configured `Codec` (JSON by default). |
| **Queue** | A named lane with a per-client worker count and optional cluster-wide concurrency and rate limits. Queues can be declared in config or added at runtime. |
| **Worker** | Code that performs one kind of job: a type implementing `Worker[T]`, or a plain function. |
| **Client** | The per-process runtime. It inserts, claims, works and finalizes jobs, and holds a liveness lease. |
| **Driver** | The storage engine binding (`hopperpgx` for Postgres). It is generic over the engine's transaction type. |
| **Leader** | The one client, elected through the database, that runs periodic jobs and maintenance. |
| **History** | Finalized jobs, moved out of the live table into time-partitioned storage for inspection and replay. |
| **Topic / Subscription** | A message published to a topic is delivered as one job to each matching subscription. |
| **Ordering key** | An optional key that makes delivery FIFO per key (for example, per customer) without global ordering. |
| **Batch / Workflow** | Groups of jobs with completion callbacks (batch) or dependencies between them (workflow). |

## 4. Public API sketch

This sketch is illustrative. Names in the M1 surface (jobs, workers, client lifecycle,
inserting) are settled; later sections are still sketches.

### 4.1 Jobs and workers

```go
type SendEmail struct {
    UserID int64  `json:"user_id" hopper:"unique"` // part of the unique key
    Tmpl   string `json:"tmpl"`
}

func (SendEmail) Kind() string { return "send_email" }

// Optional: per-kind insert defaults, overridable per call.
func (SendEmail) InsertOpts() hopper.InsertOpts {
    return hopper.InsertOpts{Queue: "email", MaxAttempts: 10}
}

type SendEmailWorker struct {
    hopper.WorkerDefaults[SendEmail] // default Timeout, NextRetry, etc.
    Mailer *mail.Client
}

func (w *SendEmailWorker) Work(ctx context.Context, job *hopper.Job[SendEmail]) error {
    // A returned error schedules a retry with backoff.
    // return hopper.Snooze(time.Minute)  // reschedule without using up an attempt
    // return hopper.Cancel(err)          // discard now, with no retry
    return w.Mailer.Send(ctx, job.Args.UserID, job.Args.Tmpl)
}

func (w *SendEmailWorker) Timeout(*hopper.Job[SendEmail]) time.Duration { return 30 * time.Second }

workers := hopper.NewWorkers()
hopper.AddWorker(workers, &SendEmailWorker{Mailer: m})
hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[ResizeImage]) error {
    return resize(ctx, job.Args)
})
```

`AddWorker` panics if a kind is registered twice. It is called at startup, like
`http.Handle`. Calling `Insert` with an args type that has no registered worker on this
client is allowed (another service may work it), but `Config.StrictKinds` rejects it.

### 4.2 Client

```go
client, err := hopper.NewClient(hopperpgx.New(pool), &hopper.Config{
    Queues: map[string]hopper.QueueConfig{
        hopper.QueueDefault: {MaxWorkers: 100},
        "email": {
            MaxWorkers:  50,                            // per client
            GlobalLimit: 200,                           // across all clients
            RateLimit:   hopper.PerSecond(500),         // across all clients
        },
    },
    Workers: workers,
    Periodic: []hopper.PeriodicJob{
        hopper.Cron("*/15 * * * *", SyncAllocations{}, nil),
        hopper.Every(30*time.Second, RefreshCache{}, &hopper.PeriodicOpts{RunOnStart: true}),
    },
    Middleware: []hopper.Middleware{hopperotel.Middleware(otel.GetTracerProvider())},
    // Logger defaults to slog.Default(); every other field has a production default.
})
// client is a *hopper.Client[pgx.Tx]. The driver fixes the transaction type,
// so InsertTx only accepts a transaction from the right engine.

// Run blocks until ctx is cancelled, then drains gracefully. It fits errgroup.
g.Go(func() error { return client.Run(ctx) })

// Or manage the lifecycle explicitly:
err = client.Start(ctx)
err = client.Stop(ctx) // stop claiming, drain, flush; when ctx expires, cancel job contexts
```

### 4.3 Inserting

```go
// Transactional: the job exists only if tx commits.
res, err := client.InsertTx(ctx, tx, SendEmail{UserID: 42, Tmpl: "welcome"}, &hopper.InsertOpts{
    Priority:    hopper.PriorityHigh,
    ScheduledAt: time.Now().Add(time.Hour),
    Unique:      &hopper.UniqueOpts{ByArgs: true, ByPeriod: 24 * time.Hour, OnConflict: hopper.UniqueReplace},
})
res.Job.ID; res.Duplicate

// Bulk: a single statement, or COPY for large non-unique batches.
results, err := client.InsertManyTx(ctx, tx, []hopper.InsertParams{
    {Args: SendEmail{UserID: 1}}, {Args: SendEmail{UserID: 2}},
})

// Awaitable results (request/reply). Await: true makes the finalize announce
// itself, so Await returns as soon as it commits rather than on its next poll.
res, err = client.Insert(ctx, RenderReport{ID: 7}, &hopper.InsertOpts{Await: true})
report, err := hopper.Await[ReportURL](ctx, client, res.Job.ID) // worker called hopper.SetOutput(ctx, url)
```

### 4.4 Managing and observing

```go
client.JobCancel(ctx, id)            // works on any live job, including one running on another pod
client.JobRetry(ctx, id)             // re-drive a discarded or cancelled job from history
job, err := client.JobGet(ctx, id)   // live or historical

for job, err := range client.Jobs(ctx, hopper.JobFilter{
    Queue: "email", States: []hopper.JobState{hopper.JobStateDiscarded},
}) { ... }                           // iter.Seq2, pages transparently

client.Queues().Pause(ctx, "email")
client.Queues().Add(ctx, "tenant_42", hopper.QueueConfig{MaxWorkers: 5}) // runtime, multi-tenant

for ev := range client.Events(ctx, hopper.EventJobFailed, hopper.EventJobDiscarded) { ... }

stats, err := client.Stats(ctx)      // depth, age of oldest job, throughput and leader, by queue
```

### 4.5 Messaging

```go
type AllocationCreated struct{ ID int64 `json:"id"` }

func (AllocationCreated) Topic() string { return "allocation.created" }

// Declarative: the subscription row is upserted when the client starts.
hopper.Subscribe(workers, hopper.Subscription{Name: "billing", Pattern: "allocation.*", Queue: "billing"},
    func(ctx context.Context, msg *hopper.Message[AllocationCreated]) error {
        return bill(ctx, msg.Payload.ID) // nil acks; an error nacks with backoff
    })

res, err := client.PublishTx(ctx, tx, AllocationCreated{ID: 42}, &hopper.PublishOpts{
    OrderingKey: "allocation:42",   // FIFO per key
    DedupKey:    "evt-7f3a",        // idempotent publish
    TTL:         10 * time.Minute,  // expire undelivered
})
res.MessageID; res.Deliveries      // one delivery per matching subscription
```

### 4.6 Batches and workflows

```go
b := client.NewBatch(hopper.BatchOpts{OnSuccess: ReportDone{RunID: 9}, OnFailure: AlertOps{RunID: 9}})
b.Add(ProcessShard{Shard: 0}, nil)
b.Add(ProcessShard{Shard: 1}, nil)
err = b.InsertTx(ctx, tx)

wf := hopper.NewWorkflow("ingest-9")
fetch := wf.Add(Fetch{URL: u})
parse := wf.Add(Parse{}, hopper.After(fetch))
wf.Add(Index{}, hopper.After(parse))
wf.Add(Notify{}, hopper.After(parse))
err = client.InsertWorkflowTx(ctx, tx, wf)
```

### 4.7 API conventions

- Options are pointer-to-struct and `nil` means "defaults". There are no functional
  options.
- Sentinel and typed errors: `hopper.ErrNotFound`, `hopper.ErrClientStopped`,
  `*hopper.UniqueConflictError`, and so on.
- Anything that lists returns `iter.Seq2[T, error]`. Anything that streams returns
  `iter.Seq[T]` bound to a context.
- IDs are typed: `hopper.JobID` (UUIDv7, §6.1), so a job ID can't be confused with an
  application's own integer keys.
- No package-level mutable state. Everything hangs off `Workers` and `Client`, so
  tests can run many clients in one process.

## 5. Architecture

```
            ┌──────────────────────────── hopper.Client[TTx] ─────────────────────────────┐
 Insert ───►│ inserter ──────────────────────────────────────────────┐                     │
            │                                                        ▼                     │
            │ per-queue producer ── claim(N) ──► worker pool ──► finalizer (batched) ──────┼──► Driver ──► Postgres
            │        ▲                                                                     │
            │   notify / poll       lease keeper (liveness, cancel signals)                │
            │                       leader: periodic · rescuer · retention · partitions    │
            └──────────────────────────────────────────────────────────────────────────────┘
```

### 5.1 The driver boundary

The core package (`hopper`) imports only the standard library. It defines:

```go
type Driver[TTx any] interface {
    Executor() Executor                 // pool-scoped operations
    UnwrapTx(tx TTx) Executor           // operations inside the caller's transaction
    Listener(ctx context.Context) (Listener, error) // ErrNotSupported → polling only
    Capabilities() Capabilities         // e.g. COPY, LISTEN, advisory locks
}

type Executor interface {
    JobInsertMany(ctx context.Context, p []JobInsertParams) ([]JobInsertResult, error)
    JobClaim(ctx context.Context, p ClaimParams) ([]*JobRow, error)
    JobFinalizeMany(ctx context.Context, p FinalizeParams) (FinalizeResult, error)
    LeaseRenew(ctx context.Context, p LeaseParams) (LeaseResult, error)
    LeaderAttempt(ctx context.Context, p LeaderParams) (bool, error)
    // ... rescue, retention, queues, subscriptions, listing
}
```

The methods are queue operations, not SQL. Each engine is free to implement them in
the way that performs best on that engine.

| Package | Engine | Module |
| --- | --- | --- |
| `hopper/driver/hopperpgx` | Postgres via pgx v5 (the reference driver) | core |
| `hopper/driver/hoppersql` | Postgres via `database/sql` (lib/pq, pgx stdlib); polling only | core |
| `hopper/drivertest` | Conformance, concurrency and chaos suite every driver must pass | core |
| `hoppersqlite` (later) | SQLite: embedded, single-node and local development | separate module |
| `hoppermongo` (later) | MongoDB (replica set, for transactions) | separate module |

An engine needs atomic claims, transactions for transactional enqueue, and a server
clock. Push notifications are optional, because polling is always available. The two
planned engines meet these requirements differently, which is why the interface is
defined as operations rather than SQL:

- **SQLite** has a single writer, so a claim is an `UPDATE … RETURNING` inside a
  `BEGIN IMMEDIATE` transaction, and SKIP LOCKED is unnecessary. There is no LISTEN,
  so clients in the same process wake each other directly and other processes poll.
  Retention is a batched `DELETE`.
- **MongoDB** claims with atomic `findOneAndUpdate` or `updateMany` (tagging a batch
  with a claim token, then reading it back). It uses `$$NOW` for server time, change
  streams for notifications, and TTL indexes or dropped collections for retention.
  `TTx` is a session context.

`Capabilities` tells the core which strategies an engine supports, for example batch
claims, push notifications and retention by partition drop. Postgres-specific parts
of this document (the schema, the SQL insert contract, and partition retention) are
the Postgres driver's implementation of these operations, not requirements on every
engine.

## 6. Schema (Postgres)

Tables are prefixed with `hopper_` and live in whatever schema is first in the
connection's `search_path`, so users can isolate hopper in its own schema.

```sql
CREATE TYPE hopper_job_state AS ENUM (
  'pending',     -- waiting on workflow dependencies (M8); reserved now to avoid an enum migration
  'available',   -- ready once scheduled_at <= now()
  'scheduled',   -- inserted or snoozed with a future scheduled_at
  'running',
  'retryable',   -- failed; will run again at scheduled_at
  'completed',
  'cancelled',
  'discarded'    -- out of attempts, expired, or cancelled by the worker: the dead-letter state
);

-- Live jobs only. Finalized jobs move to hopper_job_history, so this table
-- (and its indexes) stay sized to the backlog, not to all-time volume.
CREATE TABLE hopper_jobs (
  id                  uuid        NOT NULL DEFAULT hopper_uuidv7() PRIMARY KEY,  -- public job ID (§6.1)
  seq                 bigint      GENERATED ALWAYS AS IDENTITY,  -- internal FIFO tie-break
  kind                text        NOT NULL,
  queue               text        NOT NULL DEFAULT 'default',
  state               hopper_job_state NOT NULL DEFAULT 'available',
  priority            smallint    NOT NULL DEFAULT 2 CHECK (priority BETWEEN 1 AND 4),
  attempt             smallint    NOT NULL DEFAULT 0,
  max_attempts        smallint    NOT NULL DEFAULT 25,
  scheduled_at        timestamptz NOT NULL DEFAULT now(),
  attempted_at        timestamptz,
  attempted_by        bigint,                    -- hopper_clients.id holding the claim
  args                jsonb       NOT NULL DEFAULT '{}',
  metadata            jsonb       NOT NULL DEFAULT '{}',   -- trace context, topic, message ID
  errors              jsonb       NOT NULL DEFAULT '[]',   -- [{at, attempt, error, trace}]
  unique_key          text,
  ordering_key        text,
  partition_key       text,                      -- for partitioned rate/concurrency limits (M7)
  batch_id            uuid,
  expires_at          timestamptz,               -- TTL: discarded if not started by then
  cancel_requested_at timestamptz,
  await               boolean     NOT NULL DEFAULT false,   -- notify waiters on finalize
  created_at          timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 70);

-- Claim path: the only index the hot query touches.
CREATE INDEX hopper_jobs_claim ON hopper_jobs (queue, priority, scheduled_at, seq)
  WHERE state IN ('available', 'scheduled', 'retryable');

-- Running set: rescue, cancel delivery, global limits. Bounded by total concurrency.
CREATE INDEX hopper_jobs_running ON hopper_jobs (queue, attempted_by) WHERE state = 'running';

-- Uniqueness applies only while a job is live.
CREATE UNIQUE INDEX hopper_jobs_unique ON hopper_jobs (kind, unique_key) WHERE unique_key IS NOT NULL;

-- Finalized jobs. Two-level partitioning: by outcome, then by time, so retention is
-- DROP TABLE on an old partition, with no DELETE and no vacuum debt.
CREATE TABLE hopper_job_history (
  LIKE hopper_jobs,
  finalized_at timestamptz NOT NULL,
  output       jsonb
) PARTITION BY LIST (state);
CREATE TABLE hopper_job_history_completed PARTITION OF hopper_job_history
  FOR VALUES IN ('completed') PARTITION BY RANGE (finalized_at);            -- hourly partitions
CREATE TABLE hopper_job_history_failed PARTITION OF hopper_job_history
  FOR VALUES IN ('cancelled', 'discarded') PARTITION BY RANGE (finalized_at); -- daily partitions
-- Schema v1 gives each a DEFAULT partition so finalize works before the leader
-- (M2) manages time partitions. The leader creates future time partitions only,
-- because attaching a partition scans the DEFAULT partition for overlapping rows.

-- Liveness: one lease row per running client process.
CREATE TABLE hopper_clients (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  hostname    text        NOT NULL,
  started_at  timestamptz NOT NULL DEFAULT now(),
  expires_at  timestamptz NOT NULL,
  info        jsonb       NOT NULL DEFAULT '{}'    -- version, queues, pid
);

CREATE TABLE hopper_leader (
  name       text PRIMARY KEY DEFAULT 'default',
  client_id  bigint      NOT NULL,
  elected_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL
);

CREATE TABLE hopper_queues (
  name          text PRIMARY KEY,
  paused_at     timestamptz,
  global_limit  int,                    -- NULL = unlimited
  rate_per_sec  double precision,       -- NULL = unlimited
  rate_burst    int,
  tokens        double precision,
  refilled_at   timestamptz,
  metadata      jsonb       NOT NULL DEFAULT '{}',
  updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE hopper_periodic (          -- last inserted slot per periodic job
  name      text PRIMARY KEY,
  last_slot timestamptz NOT NULL
);

CREATE TABLE hopper_subscriptions (
  name       text PRIMARY KEY,
  pattern    text NOT NULL,             -- AMQP topic syntax
  kind       text NOT NULL,
  queue      text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE hopper_schema (version int PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());
```

Schema v2 (M6) adds `max_attempts` and `metadata` to `hopper_subscriptions`, the
ordering-key indexes (§10) and the SQL contract functions. Schema v3 (M7) adds
`partition_limit` and `aging_seconds` to `hopper_queues`, the partition-key running
index, `hopper_batches` and the batch index. M8 adds `hopper_job_deps` (§11).

### 6.1 Job IDs
Job IDs are UUIDv7. They are safe to expose outside the application (in URLs, APIs
and logs shared with customers), they don't reveal volume, and producers on other
systems can generate them without coordination. Because v7 IDs are time-ordered,
index inserts stay local like a sequence.

- **Generated in the database.** `hopper_uuidv7()` is installed by the migrations.
  On Postgres 18 and later it calls the built-in `uuidv7()`. On 14–17 it is a SQL
  function that builds a v7 UUID from `clock_timestamp()` and the built-in
  `gen_random_uuid()`. Generation therefore works the same for Go inserts, `COPY`
  and the SQL contract, and hopper's Go code needs no random number generator or
  cryptography. Engines without a database-side generator (MongoDB) generate IDs in
  the driver.
- **Ordering uses `seq`, not the ID.** Several IDs created in the same millisecond
  are not guaranteed to sort in insertion order on every Postgres version. An
  internal identity column, `seq`, provides the tie-break for claim order and for
  strict ordering keys. It is never exposed in the API.
- **In Go**, IDs are `hopper.JobID`, a `[16]byte` type in the core package with
  `String`, `ParseJobID`, text and JSON marshalling, and `sql.Scanner` /
  `driver.Valuer` support. The core adds no UUID dependency.
- **Internal IDs stay compact.** `hopper_clients.id` remains a `bigint`, because it
  never leaves the database and is written into every running job.

### 6.2 State machine

```
insert ──► available ─┐                        ┌──► completed  ─┐
insert ──► scheduled ─┼─► (claim) running ─────┼──► cancelled   ├──► moved to hopper_job_history
           retryable ─┘        │    │          └──► discarded  ─┘        │
               ▲               │    └──► retryable / scheduled (snooze)  │
               └───────────────┘                                        │
  rescuer: running, owner's lease expired ──► retryable (or discarded)   │
  JobRetry ◄─────────────────────────────────────────────────────────────┘
  JobCancel: any live state; running jobs are signalled and finalize as cancelled
```

`available`, `scheduled` and `retryable` all mean "claim once `scheduled_at <= now()`".
They are separate states only so operators can see why a job is waiting. As a result
there is **no scheduler loop** moving jobs between states.

## 7. Core algorithms

### 7.1 Clock
All time comparisons use the database's `now()`, never the application clock, so clock
skew between pods cannot cause early or duplicate execution.

### 7.2 Insert
- **Single and small batches:** one `INSERT … SELECT FROM unnest($kinds, $args, …)
  ON CONFLICT (kind, unique_key) WHERE unique_key IS NOT NULL DO NOTHING RETURNING …`.
  Conflicting rows are resolved to the existing job, and the result reports
  `Duplicate: true`. Any batch size costs one round trip.
- **Bulk:** `InsertMany` switches to `COPY` for batches of non-unique jobs above a
  threshold (256 by default). This is the fastest way to load rows into Postgres.
- **Unique jobs:** a job with `UniqueOpts` is unique among live jobs (those not yet
  finalized) of its kind. The key is built from the parts the options select: the args
  (`ByArgs`: the fields tagged `hopper:"unique"`, or the whole args if none is tagged),
  the queue (`ByQueue`) and a time period (`ByPeriod`). The zero `UniqueOpts` is unique
  by kind alone. The key is stored as canonical text, with no hashing, so it is
  FIPS-clean and readable in SQL. Keys over 1 KiB are rejected at insert. The insert
  statement's `ON CONFLICT ... DO UPDATE` returns the existing job for a duplicate;
  `UniqueReplace` updates the args, metadata, priority, attempts and `scheduled_at` of a
  non-running duplicate, which gives debouncing. Within one batch, later inputs with the
  same key resolve to the first.
- **Wake-up:** an insert inside a caller's transaction sends
  `pg_notify('hopper_insert', queue)` in the same pipelined batch as the insert, once
  per queue. Postgres delivers notifications only on commit and collapses identical
  ones within a transaction, so no worker wakes for an uncommitted job (§7.11). An
  insert outside a transaction is committed when it returns; the client then wakes its
  local producer directly, with no database round trip, and notifies other processes
  through a per-process coalescer (§7.11).

### 7.3 Claim
Each queue has a producer. The claim is a single statement:

```sql
UPDATE hopper_jobs j
SET state = 'running', attempt = attempt + 1, attempted_at = now(), attempted_by = $client
FROM (
  SELECT id FROM hopper_jobs
  WHERE queue = $queue AND state IN ('available','scheduled','retryable')
    AND scheduled_at <= now()
  ORDER BY priority, scheduled_at, seq
  LIMIT $n
  FOR UPDATE SKIP LOCKED
) c
WHERE j.id = c.id
RETURNING j.*;
```

- **Batching.** A producer claims only when at least `min(MaxWorkers/4, free slots)`
  slots are free or a short cooldown (20ms by default) has passed. Busy queues are
  claimed in batches rather than one row at a time, while idle queues still start
  jobs immediately.
- **Triggers.** A producer claims when (1) an insert notification or local wake-up
  arrives, debounced; (2) enough slots free up; (3) the adaptive poll timer fires. That
  timer is 1s when the queue is idle and becomes continuous while claims come back
  full. Polling is also the safety net for missed notifications and for jobs whose
  `scheduled_at` has just arrived.
- **Pipelining.** When the finalizer has pending results and the producer wants work,
  both statements are sent in one pgx batch, which is one network round trip.
- **Expiry.** Rows past `expires_at` (set from `InsertOpts.TTL`, as `now() + TTL`) are
  skipped by the claim, and the leader moves them to history as `discarded` with the
  error `hopper: expired`.
- **Limits.** Queues with a `GlobalLimit` or `RateLimit` claim inside a short
  transaction that locks their `hopper_queues` row (§9). Unlimited queues never touch
  that row, so they pay nothing for the feature.
- **Paused queues** skip claiming. `Queues().Pause` upserts the queue row and sends
  `pg_notify('hopper_control', 'pause:<queue>')`; clients apply it at once and re-read
  the set of paused queues on every lease renewal, in the same round trip as the
  renewal. `Queues().Add` and `Remove` start and stop working a queue on one client at
  runtime. Clients record the queues they work in `hopper_queues` on start.

### 7.4 Execute
- Each queue runs a bounded pool of goroutines. Workers are started only once a slot is
  free, so there is no unbounded fan-out.
- Each job gets a context that is cancelled on timeout (the worker's `Timeout()` or the
  queue default), on `JobCancel`, on lease loss (§7.6), or on a hard stop.
- A panic is recovered and recorded as an error with its stack trace, then treated as
  a normal failure.
- `Middleware` (an interface with `Work` and `Insert` methods; `WorkMiddleware` and
  `InsertMiddleware` wrap a function) wraps execution for logging, metrics, tracing
  and custom policy, and wraps inserts, for example to inject trace context into
  `metadata`. Middleware runs in `Config.Middleware` order, outermost first, inside
  the panic guard.
- `hopper.SetOutput(ctx, v)` records a result, which is stored with the finalized job
  and returned by `Await` and `JobGet`. Output is kept only for archived jobs.

### 7.5 Finalize
Results are buffered and written by a per-client finalizer. It flushes every 25ms or
every 500 results, whichever comes first, in **one statement**. Retries and snoozes are
updated in place. Terminal outcomes are moved to history.

```sql
WITH r AS (SELECT * FROM unnest($ids, $attempted_by, $states, $delays, $snoozes, $errors, $outputs, $archives) AS r(...)),
retry AS (
  UPDATE hopper_jobs j SET state = r.state, scheduled_at = now() + r.delay,
         attempt = CASE WHEN r.snooze THEN j.attempt - 1 ELSE j.attempt END,
         errors = j.errors || jsonb_set(r.error, '{at}', to_jsonb(now())), attempted_by = NULL
  FROM r WHERE j.id = r.id AND r.state IN ('retryable','scheduled')
    AND j.state = 'running' AND j.attempted_by = r.attempted_by
  RETURNING j.id),
done AS (
  DELETE FROM hopper_jobs j USING r
  WHERE j.id = r.id AND r.state IN ('completed','cancelled','discarded')
    AND j.state = 'running' AND j.attempted_by = r.attempted_by
  RETURNING j.*, r.state AS final_state, r.error, r.output, r.archive),
archived AS (INSERT INTO hopper_job_history SELECT … , now() FROM done WHERE archive)
SELECT id FROM retry UNION ALL SELECT id FROM done;
```

- The `state = 'running' AND attempted_by = r.attempted_by` condition fences out
  stale results. Each result carries the client ID that claimed its attempt, so a
  client that re-registered under a new ID still finalizes the attempts it claimed
  under the old one. If the job was rescued and reclaimed, the result is dropped and
  logged, so a finalize can never overwrite a newer attempt. The statement returns
  the IDs it applied; the client logs the rest.
- Retry delays are sent as durations and applied to database `now()`; error
  timestamps are set by the database too (§7.1).
- Queues with `DeleteCompleted: true` delete completed jobs without writing history.
  Cancelled and discarded jobs are always archived, because history is the
  dead-letter queue (§7.10).
- Outcomes:
  - **success:** `completed`.
  - **error:** `retryable` at `now() + backoff(attempt)`, or `discarded` when
    `attempt >= max_attempts`.
  - **`hopper.Snooze(d)`:** `scheduled` at `now() + d`, without using up an attempt.
  - **`hopper.Cancel(err)`:** `discarded` immediately.
- `backoff(n) = n^4 + 5s ± 10% jitter`, capped at 24h. It can be overridden per worker
  with `NextRetry(job) time.Time` or per queue with a `RetryPolicy`.
- `CompleteTx(ctx, tx, job)` finalizes inside the worker's own transaction, so the work
  and its completion commit together. It bypasses the buffer.
- The finalizer's write runs on a context the stop signal cannot cancel: a result that
  reached the finalizer belongs to a job that has finished. While the client is running,
  a failed flush is retried with backoff and the buffered results apply back-pressure
  to claiming. Once the client is stopping, each batch gets one more attempt.
- If a job has `await = true`, the finalize statement sends `pg_notify('hopper_done', id)`
  from its `RETURNING` clause, and `Await` returns as soon as the transaction commits.
  `Await` on a client without a listener (one that is not started) polls at
  `PollInterval`. A cancelled or discarded job surfaces as `*JobFailedError`.

### 7.6 Liveness and rescue
Liveness is tracked **per client, not per job**. Each client holds a lease row in
`hopper_clients` and renews it every 5s (the TTL is 15s):

```sql
UPDATE hopper_clients SET expires_at = now() + $ttl WHERE id = $me AND expires_at > now();
```

A heartbeat therefore costs one row per process regardless of how many jobs it is
running. Running jobs are never rewritten just to prove they are alive, which removes
the largest source of write amplification in heartbeat-based designs.

- **Rescue.** The leader's rescuer finds `running` jobs whose `attempted_by` has no
  unexpired lease and moves them to `retryable` with the error `client lost` (or to
  `discarded` if they are out of attempts). A crashed pod's jobs run again within about
  20s.
- **Fencing.** If a renewal matches zero rows (after a long GC pause or a partition),
  the client has lost its lease. It cancels every in-flight job context and re-registers
  under a new ID. Results still in flight are submitted anyway: the finalize fence (§7.5)
  rejects any whose job was rescued meanwhile and applies the rest, which is equivalent
  to a rescue. A leader that finds its own jobs among the rescue candidates fences
  itself first, so it does not re-claim them under the lapsed ID before its lease loop
  notices.
- **Hung jobs.** A live client with a stuck goroutine is handled by timeouts. An
  optional `RescueStuckAfter` also rescues jobs that are still running past an absolute
  age.
- **Graceful stop** deletes the lease after the finalizer flushes, so nothing needs
  rescuing.

### 7.7 Cancellation
`JobCancel` on a waiting job moves it straight to history as `cancelled`. On a
running job, it sets `cancel_requested_at` and sends
`pg_notify('hopper_control', 'cancel:<id>')`. The owning client cancels the job's
context immediately, with cause `hopper.ErrJobCancelled`. Each lease renewal also
returns any cancel requests for that client's running jobs (through the small running
index), which covers missed notifications. The job finalizes as `cancelled` if the
worker returns an error after the request, and as `completed` if it finishes anyway.
`JobRetry` re-drives a cancelled or discarded job from history (with one more attempt
allowed if it had run out, and a fresh `seq`) or brings a waiting job forward; it
refuses a running job and a job whose unique key is held by a live job.

### 7.8 Leader election
Leadership is a lease row in `hopper_leader`:

```sql
INSERT INTO hopper_leader (name, client_id, elected_at, expires_at)
VALUES ('default', $me, now(), now() + $ttl)
ON CONFLICT (name) DO UPDATE SET client_id = $me, elected_at = now(), expires_at = now() + $ttl
WHERE hopper_leader.expires_at < now() OR hopper_leader.client_id = $me
RETURNING client_id = $me;
```

- The TTL is 15s, and the leader renews every 5s.
- On graceful stop the leader deletes its row and sends a `hopper_leader` notification,
  so a replacement takes over immediately.

A lease table is used instead of `pg_advisory_lock` for three reasons: it works behind
PgBouncer in transaction mode and behind RDS Proxy, it can be inspected with plain SQL,
and it doesn't pin a connection. Leader-only duties are idempotent, because two leaders
can briefly overlap during a partition.

Leader duties: periodic jobs, the rescuer, expiring TTL'd jobs, creating history
partitions ahead of time and dropping expired ones, removing stale `hopper_clients`
rows, and resolving batch and workflow completion.

### 7.9 Periodic jobs
`hopper.Every(d, …)` and `hopper.Cron(spec, …)` are both available from v0.1. Cron
supports standard five-field syntax, an optional seconds field, `@hourly`-style
descriptors, and an IANA time zone per job (a `CRON_TZ=`/`TZ=` prefix or
`PeriodicOpts.Location`; UTC by default). The parser is in the core, using only the
standard library. `Every` slots are aligned to the interval, so every leader computes
the same slots.

The leader runs a periodic loop for the duration of its term. It sleeps until the
soonest slot, reads the database clock, and for each slot that has come due upserts
`hopper_periodic.last_slot` with `ON CONFLICT DO UPDATE … WHERE last_slot < $slot` and
inserts the job in the same transaction only if that upsert returned a row. A slot
therefore produces at most one job, even if two leaders overlap or leadership changes
mid-slot, and even after the first job has finished and left the live table.
`RunOnStart` inserts a job when a leader starts scheduling. Missed slots are skipped by
default, except slots due since the last recorded one within the last lease TTL, which
covers a failover; `CatchUp: n` back-fills up to the n most recent missed slots
instead. A schedule with no recorded slot starts from its next slot.

### 7.10 Retention
Finalized jobs live in `hopper_job_history`, partitioned by outcome and then by time.
Retention is enforced by dropping whole partitions, so there is no `DELETE`, no dead
tuples and no vacuum work. The leader creates partitions several intervals ahead
(three hours for `completed`, two days for `failed`), on election and every five
minutes. A partition is created standalone and then attached, which takes only a
`SHARE UPDATE EXCLUSIVE` lock on the parent, so finalizes are never blocked by
maintenance. Only future periods are created and no row is ever moved: rows finalized
before their period's partition existed (the first period after installation, or
after a long leader outage) land in a `DEFAULT` partition and are deleted individually
once past retention. Dropping a partition takes a brief `ACCESS EXCLUSIVE` lock on the
parent, once per period. Retention periods are set in `Config` and enforced by the
leader, so the leader's settings apply cluster-wide.

The defaults follow common practice: job queues keep successful jobs briefly and
failures longer, while brokers such as RabbitMQ and SQS delete a message once it is
acknowledged.

| Outcome | Job queues | Subscription queues |
| --- | --- | --- |
| `completed` | archived for 24h | deleted on ack (`DeleteCompleted: true`) |
| `cancelled`, `discarded` | archived for 7d (the dead-letter queue) | archived for 7d (the dead-letter queue) |

`DeleteCompleted` is configurable per queue; the two retention periods
(`CompletedRetention`, `FailedRetention`) are per cluster, because partitions are cut
by time, not by queue. Setting `DeleteCompleted: true` on a job queue gives maximum
throughput by skipping the history insert.

### 7.11 Notifications
LISTEN/NOTIFY gives low-latency wake-ups, and hopper keeps it cheap at high insert
rates:
- **One notify per queue per transaction**, no matter how many jobs the transaction
  inserts.
- **Per-process coalescing** for inserts outside a caller's transaction. Each client
  sends at most one insert notification per queue per 10ms, on the trailing edge of
  the window, after the window's inserts have committed, so a woken worker always
  finds the jobs. A busy queue's producers are claiming continuously anyway, so extra
  notifications would add nothing.
- **One listener connection** per client, multiplexing every channel. It can be pointed
  at a direct Postgres address (`hopperpgx.Config.ListenConn`) when the pool goes
  through a transaction pooler.
- **Polling fallback.** If LISTEN is unavailable or the connection drops, the client
  polls and keeps working, and it reconnects the listener with backoff. The listener
  connection is named `hopper-listener:<schema>` in `pg_stat_activity`.
- **Channels are per database**, not per schema. Two hopper schemas in one database
  see each other's notifications, which only causes spurious wake-ups.

### 7.12 Shutdown
`Stop(ctx)` and `Run` (when its context is cancelled) shut down in three steps:
1. Stop claiming and give up leadership.
2. Wait for running jobs to finish, then flush the finalizer.
3. If `ctx` expires first, cancel all job contexts, wait a short grace period, flush
   whatever finished, release the lease and return `ctx.Err()`. Jobs that are still
   unfinished are rescued by another client (at-least-once).

A job that returns because its context was cancelled by the stop is finalized as
`retryable` with no delay, so it runs again immediately on another client. The
interrupted attempt still counts, as it does for a rescued job. The lease keeps being
renewed until the finalizer has flushed, so a long drain cannot let it lapse.

### 7.13 Encoding
Args, outputs and message payloads go through a `Codec`. The default is
`encoding/json`. Applications can plug in a faster JSON implementation, or wrap the
codec to encrypt payloads with their own keys, in which case the payload is stored as
a JSON string. hopper itself never encrypts.

## 8. Performance

### 8.1 Principles
- **Everything on the hot path is a set operation.** Claim, finalize, insert and lease
  renewal each cost one statement regardless of how many jobs they cover.
- **A small live table.** `hopper_jobs` holds only the backlog and running jobs, so the
  claim index stays in shared buffers and autovacuum finishes quickly.
- **Per-client liveness.** There are no per-job heartbeat writes (§7.6).
- **Partition-drop retention.** History never produces dead tuples (§7.10).
- **Few round trips.** Statements are prepared and cached, finalize and claim are
  pipelined into one batch, and local wake-ups skip the database.
- **Pay only for what you use.** Rate limits, global limits, ordering keys and
  awaitable results cost nothing on queues and jobs that don't use them.

### 8.2 Targets
These are release gates on reference hardware (Postgres 17, 8 vCPU / 32 GB, NVMe,
`synchronous_commit = on`, clients on a separate host in the same zone). The results,
the hardware details and the benchmark harness are published with each release.

| Metric | Target |
| --- | --- |
| End-to-end throughput, no-op jobs, 4 clients | ≥ 50,000 jobs/s sustained |
| Bulk insert (`InsertMany`, COPY path) | ≥ 250,000 jobs/s |
| Batched insert (`InsertMany`, unique path) | ≥ 100,000 jobs/s |
| Pickup latency on an idle queue (commit → `Work` called) | p50 < 5 ms, p99 < 25 ms |
| Pickup latency at 80% of peak throughput | p99 < 100 ms |
| Publish fan-out cost | one statement, independent of subscriber count |
| Recovery after a process crash | ≤ 20 s |
| Throughput with a 10M-row history | within 5% of an empty history |

### 8.3 Benchmark harness
- `hopperbench` is a command in the repo that drives the scenarios above and prints
  one JSON line per scenario. It has the no-op baseline, mixed-priority, scheduled-heavy
  and retry-heavy workloads, the two insert paths and pickup latency, and `-history N`
  to preload history rows. It warms up before measuring, because the first leader
  election creates history partitions, which briefly blocks finalizes. Fan-out arrives
  with messaging (M6).
- CI runs a reduced benchmark on every PR that touches the insert, claim or finalize
  paths, on the PR and on its base alternately on the same runner, scenario by scenario
  for several rounds, comparing the average of the faster half of each side's runs (noise
  on a shared runner only slows runs down). `hopperbench -compare` fails the check on a
  regression of more than 10%.
- A nightly soak runs the full scenario set at volume with five million history rows
  and keeps the results as an artifact. The 24h soak at 70% of peak, tracking table and
  index size, autovacuum activity and latency drift, runs on the release hardware.

### 8.4 Scaling further
- **Vertical:** the tuning guide covers autovacuum settings for `hopper_jobs`
  (aggressive scale factors, no cost delay), `max_connections`, and pool sizing per
  client.
- **Horizontal:** queues can be spread across several databases, with one client per
  database per process. Clients are cheap, and routing by queue is a configuration
  choice.

## 9. Flow control

| Feature | Scope | Mechanism |
| --- | --- | --- |
| `MaxWorkers` | per client, per queue | Size of the goroutine pool. |
| `GlobalLimit` | cluster, per queue | The claim locks the queue row, then counts the running jobs through the running index in a second statement (a statement that waited for the lock keeps the snapshot it started with, so the count must come after), and claims `min(free, limit − running)`. |
| `RateLimit` | cluster, per queue | Token bucket on the queue row (`tokens`, `refilled_at`), refilled from `now()` inside the claim transaction. When the bucket is empty the claim returns how long until the next token, and the producer sleeps that long instead of polling. `RateBurst` is the bucket size, one second's worth by default. |
| `PartitionLimit` | cluster, per `partition_key` | For example, "at most 5 concurrent per customer". The claim ranks candidates with `row_number() OVER (PARTITION BY partition_key)` plus the key's running count, and admits those that fit. The ranking scans the queue's waiting jobs, so only partition-limited queues pay for it. A per-key rate is not implemented. |
| Priorities | per job | Four levels in the claim order. `PriorityAging` makes the leader promote a waiting job one level each time it has waited that long (`JobAge`), so low priorities cannot starve; the claim order itself stays index-only. |
| Pause / resume | cluster, per queue | Row flag with a control notification. |

Limit state lives in the database, so limits hold across any number of replicas and
survive restarts. Limits declared in `QueueConfig` are recorded on the queue row when the
client starts; `Queues().SetLimits` (and `hopper queues limit`) change them at runtime,
announced on `hopper_control` and re-read on every lease renewal. Producers start with
the recorded pause and limit state, so a paused or limited queue is never claimed freely
in the window before the first renewal. Unlimited queues never touch the queue row.

## 10. Messaging

Messaging is modeled on RabbitMQ's topic exchange and built from job primitives. It
inherits acks, retries, dead-lettering, transactional publish, scheduling and
observability without new machinery.

- **Subscriptions** are rows: `(name, pattern, kind, queue, max_attempts, metadata)`,
  upserted by name when a client whose `Workers` declared them starts. The delivery kind
  is `sub:<name>`. Patterns use AMQP syntax: `.`-separated words, where `*` matches one
  word and `#` matches zero or more. `#` alone is a fanout exchange, and a literal topic
  is a direct exchange. `hopper_topic_regex(pattern)` turns a pattern into a regular
  expression; the match is `topic ~ hopper_topic_regex(pattern)`.
- **`PublishTx`** inserts one job per matching subscription in a single
  `INSERT … SELECT` that matches patterns in SQL, in the publisher's transaction. Each
  delivery's `metadata` records the topic, a message ID generated once by the database,
  and the headers, merged with the subscription's metadata. Fan-out happens at publish
  time, so a subscription created later does not receive earlier messages, as in
  RabbitMQ. A topic with no matching subscription inserts nothing. Pool publishes wake
  local producers and notify through the coalescer; transactional ones notify from the
  transaction, deriving the queues from the matching subscriptions.
- **Typed consumers.** `Message[T]` carries the payload, topic, message ID, delivery
  attempt and headers. A subscription that matches several topics with different
  types can use `Message[hopper.Raw]` and dispatch on `msg.Topic`.
- **Acks and dead-lettering.** Returning nil acks. An error nacks with backoff. When
  attempts run out the delivery is dead-lettered in history, where it can be inspected
  and replayed with `JobRetry` or, for a whole subscription, `ReplayDiscarded`.
- **Competing consumers** come for free, because every replica claims from the same
  queue with SKIP LOCKED.
- **Delayed delivery, TTL and priority** use the standard job fields.
- **Idempotent publish.** `DedupKey` becomes each delivery's unique key (`msg:<key>`,
  scoped by kind, so subscriptions deduplicate independently), so a retried publish
  inserts nothing while the earlier delivery is still live and reports the duplicate.
- **Ordering keys.** Strict FIFO per key, with the same semantics as SQS FIFO message
  groups, Azure Service Bus sessions and Google Pub/Sub ordering keys:
  - At most one delivery per `(queue, ordering_key)` runs at a time, oldest first. The
    claim selects only the oldest live delivery for each key (by `seq`, through an index
    on `(queue, ordering_key, seq)`) and skips keys with a running job; jobs without a
    key pay nothing for the check. A partial unique index on running ordering keys is the
    backstop: if two clients pass the check at once, one claim fails and is retried.
    Ordering keys are available on plain jobs too, through `InsertOpts.OrderingKey`.
  - A failing delivery blocks its key while it retries. Later messages for that key
    wait until it succeeds or is dead-lettered, and dead-lettering unblocks the key.
  - Ordered deliveries get a shorter default retry budget (10 attempts, with backoff
    capped at 5 minutes), so a poison message is dead-lettered within about an hour
    instead of blocking its key for days. The budget is configurable per subscription
    (`Subscription.MaxAttempts`).
  - Keys are independent, so a blocked key never delays other keys. Throughput scales
    with the number of distinct keys.
- **Request/reply.** Publish with `Await: true`, and the consumer's
  `hopper.SetOutput` becomes the reply.
- **Cross-language producers.** The insert and publish SQL is a documented, versioned
  contract ([docs/sql-contract.md](sql-contract.md)). `hopper_insert(kind, args, opts)`
  and `hopper_publish(topic, payload, opts)` SQL functions ship with schema v2, so
  Python, shell or HPC batch scripts can enqueue work with plain SQL, inside their own
  transactions. This is the supported path for producers not written in Go (§2).
- **Streams (M8).** An append-only, time-partitioned topic log with consumer groups
  that track offsets, for replay and late subscribers. Readers only see events below
  the current snapshot's `xmin` (`pg_snapshot_xmin(pg_current_snapshot())`), so
  transactions that commit out of order can never make a reader skip an event.

## 11. Batches and workflows

- **Batches (M7).** `hopper_batches (id, pending, failed, total, on_success, on_failure,
  on_complete, metadata, completed_at)`. Jobs carry `batch_id`. Every statement that
  finalizes jobs (the batched finalize, cancelling a waiting job, expiring) decrements
  `pending` once per batch per statement, not once per job, which bounds contention on
  the batch row, and counts cancelled and discarded jobs as failures. The statement that
  reaches zero inserts the callbacks in the same statement: `on_success` when nothing
  failed, `on_failure` otherwise, `on_complete` either way, each with `batch_id` and
  `batch_failed` in its metadata, and notifies their queues. `client.NewBatch(opts)`,
  `Add`, `Insert`/`InsertTx` and `BatchGet` are the API.
- **Workflows (M8).** Jobs with dependencies are inserted as `pending`, with edges in
  `hopper_job_deps`. When a job completes, the same finalize statement promotes
  dependents whose dependencies are all complete to `available`. Failure policies are
  `cancel dependents` (the default) and `ignore`. Workflows are inspectable as a DAG in
  `hopperui`.

## 12. Migrations

- SQL files are embedded in the `hoppermigrate` package, versioned, and forward-only by
  default. Down migrations exist for development.
- `hoppermigrate.Up(ctx, driver)` applies them under an advisory lock taken on a
  dedicated connection, not a pooled one. This avoids a pool-starvation deadlock:
  processes waiting for the lock must not hold the connections the migration needs.
- `hopper migrate` in the CLI runs the same migrations. The raw SQL files are also
  published for teams that use goose, atlas or Flyway. `hopper_schema` records the
  applied version either way.
- Every schema change ships with an upgrade test that migrates from the previous version
  to the latest while a client works jobs in every state and inserts keep arriving
  (`TestUpgradeUnderTraffic`). Migrations that touch `hopper_jobs` must not take locks that
  block claims for longer than one statement (`CREATE INDEX CONCURRENTLY`, `NOT VALID`
  constraints, and so on).

## 13. Observability and operations

- **Logging:** `log/slog` throughout, defaulting to `slog.Default()`.
- **Events:** `client.Events(ctx, kinds...)` streams job lifecycle, leadership and lease
  events as an `iter.Seq[Event]`, for tests and custom metrics. Delivery is best-effort:
  a consumer that falls behind by more than a buffer misses events rather than slowing
  the client.
- **Stats:** `Stats(ctx)` returns per-queue depth by state, the age of the oldest
  claimable job, completions in the last minute (through the `(queue, finalized_at)`
  history index, per known queue), the running count by client, live clients and the
  current leader, in one statement. It is suitable for readiness checks and autoscaling
  signals, such as scaling replicas on queue latency.
- **OpenTelemetry (`hopperotel` module):** `hopperotel.New` (or `Middleware(tp)`) is a
  `hopper.Middleware` that propagates trace context from insert to work through
  `metadata`, records a producer span per insert batch and a consumer span per attempt,
  and counts attempts by outcome and their duration. `RegisterStats` adds observable
  gauges for queue depth by state, the oldest claimable job's age and live clients from
  `Stats`. Prometheus users export through the OTel exporter.
- **CLI (`cmd/hopper`, core module):** `migrate up|down|version`, `jobs list|get|retry|cancel`,
  `queues list|pause|resume`, `clients list` and `stats`, all with `-json`. `queues limit`
  and `subscriptions list` arrive with M7 and M6. Benchmarks are the separate
  `hopperbench` command.
- **Web UI (`hopperui` module):** an embeddable `http.Handler` for browsing queues,
  jobs, history, subscriptions and workflows, with retry, cancel and pause actions
  behind an application-supplied authorization hook.

## 14. Testing strategy

| Layer | What |
| --- | --- |
| Unit | Backoff, cron parsing and time zones, pattern matching, unique-key construction, the state transition table, option validation. Timers are tested with `testing/synctest`. |
| Integration | Against real Postgres 14–18 in CI (a service container per version), always with `-race` and `GODEBUG=fips140=only`. Each test gets its own schema, so tests run in parallel. |
| Driver conformance | `drivertest.Run(t, fixture)` runs the behavioral suite (inserting, unique keys, claiming, every finalize transition and fence, leases, leader, rescue, cancel, retry, TTL, listing, queues, periodic slots, stats, notifications, history maintenance) against every driver. A `Fixture` supplies isolated databases and the few engine-specific hooks (expire a lease, backdate an attempt, count rows). Engine internals such as partition management stay in the driver's own tests. |
| Concurrency | N clients by M jobs: every job is finalized exactly once when nothing crashes. Uniqueness under concurrent inserts. Periodic slots unique across two leaders. Global and rate limits never exceeded. Ordering keys never run two at once. |
| Chaos | `pg_terminate_backend` on a client mid-job, followed by rescue and re-run. A frozen client that loses its lease and must fence itself. Leader killed, new leader within the TTL. Stop with an expired context. Listener dropped, polling continues and the listener reconnects. |
| Upgrade | Migrate from release N-1 with jobs in every state and live traffic. |
| Performance | `hopperbench` regression gate on PRs, and the nightly soak (§8.3). |
| Test helpers | The `hoppertest` package: `RequireInserted[T]`, `RequireManyInserted[T]`, `RequireNotInserted[T]` and their `Tx` variants (which look inside the caller's transaction), and `Work[T]`, which runs a worker inline on a synthetic job with no database round trip. |

## 15. Compatibility

- **Postgres:** 14 and later, which covers all current AWS RDS, Aurora, Cloud SQL,
  AlloyDB, Azure and Neon major versions.
- **Go:** the two most recent releases, matching Go's own support policy.
- **Poolers:** PgBouncer in transaction mode (1.21+ for prepared statements) and RDS
  Proxy work. LISTEN needs a session connection, so it can be given a direct
  connection (§7.11). Otherwise the client uses adaptive polling and logs that once.
- **Stability:** API stability starts at v1.0.0. Until then, minor versions may break
  the API, with changes noted in the CHANGELOG. The SQL insert contract (§10) is
  versioned separately and changes only with a deprecation window.

## 16. Milestones

The estimates assume one engineer. Each milestone is one or more PRs.

| # | Milestone | Scope | Est. |
| --- | --- | --- | --- |
| M0 | Scaffold | go.mod, CI (lint, Postgres test matrix, `fips140=only`), PR-title check, Dependabot, CONTRIBUTING. **Done.** | 0.5d |
| M1 | Core engine | `Driver` interface and `hopperpgx`, schema v1, `hoppermigrate`, Insert/InsertTx/InsertMany (unnest and COPY), typed workers and `WorkFunc`, batched claim, batched finalize with history move, retries and backoff, `Snooze`/`Cancel` from workers, client lease registration and renewal, `Run`/`Stop`, a first `hopperbench`. **Done.** | 5d |
| M2 | Reliability | Client leases, rescuer and fencing, leader lease, partitioned retention, LISTEN/NOTIFY with coalescing and adaptive polling, unique jobs (skip and replace), `RescueStuckAfter`. **Done.** | 4d |
| M3 | Control | Cron and `Every` with time zones, Snooze, Cancel (in-flight), JobRetry, TTL, pause, runtime queues, timeouts, middleware, `SetOutput`/`Await`, `Jobs` iterator, events, `Stats`. **Done.** | 4d |
| M4 | Performance and release | `hopperbench` scenarios and `-compare`, CI perf gate and nightly soak, `drivertest`, `hoppertest`, CLI, `hopperotel`, docs and examples, CHANGELOG. **Done**, except the §8.2 run on the reference hardware that gates the **v0.1.0** tag. The upgrade suite starts with the first schema change (there is one schema version so far). | 5d |
| M5 | First adoption | Move an internal service's `internal/jobs` package to hopper; drain and drop its old queue tables | 1d |
| M6 | Messaging | Subscriptions, AMQP topic patterns, typed `Message[T]`, PublishTx fan-out, dedup, ordering keys, request/reply, SQL publish contract, `ReplayDiscarded`, the upgrade test. **Done**; **v0.2.0** follows v0.1.0. | 5d |
| M7 | Flow control and batches | Global limits, rate limits, partitioned limits, priority aging, batches with callbacks, `hoppersql` driver, **v0.3.0**. **Done** except `hoppersql`, which lands in its own PR. | 5d |
| M8 | Workflows, streams, UI | Job dependencies and DAG workflows, streams with consumer groups, `hopperui` | 2–3w |
| M9 | More engines (later) | `hoppersqlite`, then `hoppermongo`, each in its own module and passing `drivertest`. Not scheduled yet. | per engine |

M0–M4 take roughly four weeks to a production-ready v0.1.0 that meets its performance
targets. v1.0.0 follows M8, once the API has been proven in production.

## 17. Risks

| Risk | Mitigation |
| --- | --- |
| Subtle concurrency bugs: double execution, lost jobs | Every state transition is a conditional statement that checks the prior state and the owning client. Chaos and concurrency suites run from M1 onward, on every driver. |
| Vacuum and bloat at sustained peak rates | A small live table, fillfactor 70, partition-drop retention, no per-job heartbeats, batched finalize, a documented autovacuum profile, and the 24h soak test. |
| NOTIFY contention at high commit rates (Postgres serializes notifying commits) | One notify per queue per transaction, per-process coalescing, and adaptive polling that makes notifications unnecessary on busy queues. |
| Claim contention with many clients on one hot queue | Batched claims with a cooldown, so each client claims less often but more at a time. Benchmarks cover 1–64 clients. |
| Postgres assumptions leaking into the driver interface | Operation-level driver methods, and `drivertest` as the contract. A second driver (`hoppersql`) lands before v1.0 to prove the boundary. |
| Feature breadth diluting the core | Features are layered: each milestone ships only once the core perf and chaos suites pass. Integrations live in separate modules. |

## 18. Open questions

None at the moment. New questions are added here through PRs.
