# hopper: design and implementation plan

hopper is a high-performance job queue and message broker for Go. It gives
applications durable background jobs, scheduled and periodic work, and pub/sub
messaging with transactional guarantees, using the database they already run.
PostgreSQL is the first engine. One Apache-2.0 library replaces both an
in-process job framework and a separate message broker.

- **Module:** `github.com/parallelworks/hopper`
- **License:** Apache-2.0
- **Dependencies:** the Go standard library and `github.com/jackc/pgx/v5`. Nothing else in the core module.
- **Status:** M0 through M4 and M6 through M8 are implemented (§16), the §8.2 targets have been run on the reference hardware, and everything ships together as one version across the core module, `hopperotel` and `hopperui` ([docs/releasing.md](releasing.md)); **v0.1.1** is current. This document is the plan of record, and changes to it go through PRs.

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
    // return &hopper.SnoozeError{Duration: time.Minute, Reason: "mailer rate limit"} // and say why
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

A kind is renamed with aliases: args that implement `KindAliases() []string`
(`JobArgsWithKindAliases`) register their worker under each earlier kind too, so jobs
stored under the old kind still run while the new one is inserted. The alias lives on
the args, beside `Kind`, and is resolved only in the registry, when a job is worked
(and by `StrictKinds`, which accepts an aliased kind). Claiming selects by queue, never
by kind, so nothing in the database changes: a job keeps its stored kind through
retries and snoozes, and that is `job.Kind`. The worker decodes the stored args into
the new args type, so the rename must keep their encoding: with JSON, a field renamed
at the same time decodes to its zero value, and the job runs with it. An alias that
is empty, repeated, the args' own kind, or another registered kind or alias panics,
and a rejected registration adds none of its names. Everything that keys on the stored
kind keeps doing so: a unique key does not match across the old and new kinds, so a duplicate can
be inserted during the rename; `JobFilter.Kinds` and the UI list each kind by its own
name; middleware, `OnError` and `hopperotel` see the stored kind, so spans and metrics
carry both names until the old jobs are gone; and a periodic job's default name is its
kind, so one renamed without a `Name` starts a new schedule. Ordering keys and limits
are per queue and do not see kinds.

In a rolling deploy, a client of the previous release has no worker for the new kind,
so a job of it that client claims fails with `UnknownKindError` and uses an attempt (a
job on its last attempt is discarded). A rename that must not spend attempts takes two
releases: the first keeps the old `Kind` and adds the new kind as its alias, and the
second, once no client runs an older release, swaps them. A job of the old kind kept in
history fails the same way if `JobRetry` re-drives it after the alias is removed, and
so does a callback of the old kind that a batch or workflow still open when the alias
is removed inserts on completion: the callback is stored with the batch, not as a job,
so a search for jobs of the old kind does not find it.

A worker whose work is in the same database can complete its job in the same
transaction, so the job runs exactly once even if the process dies right after:

```go
func (w *TransferWorker) Work(ctx context.Context, job *hopper.Job[Transfer]) error {
    tx, err := w.pool.Begin(ctx)
    if err != nil {
        return err
    }
    defer tx.Rollback(ctx)
    if err := w.ledger.Apply(ctx, tx, job.Args); err != nil {
        return err
    }
    if err := hopper.CompleteTx(ctx, tx, job); err != nil {
        return err
    }
    return tx.Commit(ctx)
}
```

A long job made of several steps resumes where it left off. `hopper.RunSteps` runs
named steps in order, starting after the last one the job's progress records as
finished, and records each as it finishes; a step that works through many items
records a cursor within itself with `hopper.RecordCursor`, which the next attempt reads
with `hopper.StepCursor[C]`. `RecordStepTx` and `RecordCursorTx` record in the worker's
own transaction, so a step's writes and the record that it finished commit together.
The progress is `JobRow.Progress` (last step, cursor, time), kept through retries,
snoozes and `JobRetry`, unless `hopper.FromStart()` is passed to it (§7.5).

```go
func (w *Importer) Work(ctx context.Context, job *hopper.Job[ImportArgs]) error {
    return hopper.RunSteps(ctx, job,
        hopper.NamedStep("download", w.download),
        hopper.NamedStep("load", w.load), // RecordCursor(ctx, job, page) as it goes
        hopper.NamedStep("notify", w.notify),
    )
}
```

A worker logs through `hopper.Logger(ctx)`, a `*slog.Logger` whose lines go to
`Config.Logger` with the job's ID, kind and attempt, and are also kept with the job:
each attempt's lines, in text form, are written with its result, bounded per attempt,
and a job keeps those of its latest attempts (§7.5). `client.JobLogs(ctx, id)` returns
them, and `hopper jobs get` and the web UI show them; both still show a job whose logs
cannot be read, and say why.

```go
func (w *Importer) Work(ctx context.Context, job *hopper.Job[ImportArgs]) error {
    log := hopper.Logger(ctx)
    log.Info("downloading", "url", job.Args.URL)
    ...
}
```

Workers are registered before the client that runs them exists, so a worker or
subscriber that needs the client, to insert follow-up jobs, publish or query, takes it
from its context: `hopper.ClientFromContext[pgx.Tx](ctx)` returns the client running the
job, and `false` outside a job or for another transaction type. This avoids wiring the
client back into components after construction. Tests that run a worker inline with
`hoppertest.Work` supply one with `hopper.ContextWithClient`.

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
    RetryPolicy: hopper.ExponentialRetry{Base: time.Second, Max: time.Hour}, // unless a worker's NextRetry says otherwise
    OnError: func(ctx context.Context, job *hopper.JobRow, err error) hopper.ErrorOutcome {
        if errors.Is(err, errRateLimited) {
            return hopper.ErrorOutcome{Snooze: time.Minute} // any kind; doesn't use up an attempt
        }
        return hopper.ErrorOutcome{} // keep the outcome
    },
    // Logger defaults to slog.Default(); every other field has a production default.
    JobLogs: hopper.JobLogsConfig{MaxBytes: 64 << 10}, // per attempt; defaults 32 KiB, latest 10 attempts; or Store: a LogStore
})
// client is a *hopper.Client[pgx.Tx]. The driver fixes the transaction type,
// so InsertTx only accepts a transaction from the right engine.

// Run blocks until ctx is canceled, then drains gracefully. It fits errgroup.
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
    Tags:        []string{"tenant:acme", "onboarding"}, // for finding the job later
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
client.JobRetry(ctx, id)             // re-drive a discarded or canceled job from history
client.JobRetry(ctx, id, hopper.FromStart()) // ... clearing its progress, to run all its steps again
client.JobRetryOrderingKey(ctx, "billing", "customer:42") // wake a key's snoozed and retrying jobs
client.JobDelete(ctx, id)            // remove a job that is not running, live or from history
client.JobLogs(ctx, id)              // the lines its worker kept with hopper.Logger, per attempt
client.JobCancelTx(ctx, tx, id)      // ...Tx variants commit with the caller's own changes
job, err := client.JobGet(ctx, id)   // live or historical

for job, err := range client.Jobs(ctx, hopper.JobFilter{
    Queue: "email", States: []hopper.JobState{hopper.JobStateDiscarded},
}) { ... }                           // iter.Seq2, pages transparently

for job, err := range client.Jobs(ctx, hopper.JobFilter{
    Queue: "billing", OrderingKey: "customer:42", NewestFirst: true,
}) { ... }                           // one key's lane, latest first

for job, err := range client.Jobs(ctx, hopper.JobFilter{
    TagsAll: []string{"tenant:acme"}, Metadata: json.RawMessage(`{"region": "eu"}`),
    Priorities: []hopper.Priority{hopper.PriorityHigh},
}) { ... }                           // by tags (any or all), metadata containment, priority or ID

client.Queues().Pause(ctx, "email")
client.Queues().Add(ctx, "tenant_42", hopper.QueueConfig{MaxWorkers: 5}) // runtime, multi-tenant
client.PeriodicJobs().Add(hopper.Cron("0 * * * *", TenantReport{ID: 42}, &hopper.PeriodicOpts{Name: "report:42"})) // on every client that can lead

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
    TTL:         10 * time.Minute,  // discard deliveries still waiting to run after this
})
res.MessageID; res.Deliveries      // one delivery per matching subscription

// Streams: the retained log. A consumer reads from a position of its own.
hopper.Consume(workers, hopper.Consumer{Name: "audit", Pattern: "#", Start: hopper.StreamStartEarliest},
    func(ctx context.Context, msg *hopper.Message[hopper.Raw]) error { return audit(ctx, msg) })
ev, err := client.Streams().AppendTx(ctx, tx, AllocationCreated{ID: 42}, &hopper.AppendOpts{Key: "allocation:42"})
err = client.Streams().Seek(ctx, "audit", hopper.SeekOpts{Time: yesterday})   // replay
```

### 4.6 Batches and workflows

```go
b := client.NewBatch(hopper.BatchOpts{OnSuccess: ReportDone{RunID: 9}, OnFailure: AlertOps{RunID: 9}})
b.Add(ProcessShard{Shard: 0}, nil)
b.Add(ProcessShard{Shard: 1}, nil)
err = b.InsertTx(ctx, tx)

wf := hopper.NewWorkflow("ingest-9", &hopper.WorkflowOpts{OnFailure: AlertOps{RunID: 9}})
fetch := wf.Add(Fetch{URL: u}, nil)
parse := wf.Add(Parse{}, hopper.After(fetch))
wf.Add(Index{}, hopper.After(parse))
wf.Add(Notify{}, hopper.After(parse))
res, err := client.InsertWorkflowTx(ctx, tx, wf)
row, err := client.WorkflowGet(ctx, res.ID)   // progress, jobs and edges
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
| `hopper/driver/hopperpgx` | Postgres via pgx v5 (the reference driver): COPY, LISTEN, pipelined statements | core |
| `hopper/driver/hoppersql` | Postgres via `database/sql` (lib/pq, pgx stdlib); polling only | core |
| `hopper/drivertest` | Conformance, concurrency and chaos suite every driver must pass | core |
| `hoppersqlite` (later) | SQLite: embedded, single-node and local development | separate module |
| `hoppermongo` (later) | MongoDB (replica set, for transactions) | separate module |

The two Postgres drivers share one implementation of the SQL and the logic around it
(`driver/internal/pgsql`), written against a five-method transport (`Exec`, `Query`,
`QueryRow`, `QueryExec`, `Begin`) that carries only portable values: strings, numbers,
times, plain slices of them, and pointers for NULL; every statement casts its parameters,
so a transport that cannot encode slices (`database/sql`) renders them as array
literals instead. Each driver is the adapter from its
library to that transport, plus what the library alone offers; `hopperpgx` adds COPY,
LISTEN and pipelining, and `hoppersql` emulates savepoints and buffers a result set
before the statement that follows it.

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

Tables are prefixed with `hopper_` and live in the PostgreSQL schema configured
on `hopperpgx.Config.Schema` or `hoppersql.Config.Schema`, defaulting to `hopper`.
Both drivers qualify tables, functions, enum types, COPY destinations and
maintenance objects explicitly, including inside caller-owned transactions.
They never change the application's pool or transaction `search_path`.
The CLI and UI accept `-schema` with the same default. Existing installations
must configure their existing schema explicitly; changing Schema selects a
separate installation and does not relocate data.

```sql
CREATE TYPE hopper_job_state AS ENUM (
  'pending',     -- waiting on workflow dependencies (M8); reserved now to avoid an enum migration
  'available',   -- ready once scheduled_at <= now()
  'scheduled',   -- inserted or snoozed with a future scheduled_at
  'running',
  'retryable',   -- failed; will run again at scheduled_at
  'completed',
  'canceled',
  'discarded'    -- out of attempts, expired, or canceled by the worker: the dead-letter state
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
  expires_at          timestamptz,               -- TTL: discarded if still waiting to run by then
  cancel_requested_at timestamptz,
  await               boolean     NOT NULL DEFAULT false,   -- notify waiters on finalize
  created_at          timestamptz NOT NULL DEFAULT now(),
  snooze_reason       text,                      -- why a snoozed job waits (schema v9); live table only
  wake_requested_at   timestamptz,               -- JobRetryOrderingKey reached the job while it ran (schema v10); live table only
  progress            jsonb,                     -- last step finished, cursor and time (schema v15); kept in history
  logs                jsonb,                     -- the latest attempts' log lines (schema v16); kept in history
  tags                jsonb,                     -- array of strings, NULL for none (schema v17); kept in history, with the same CHECK
    CHECK (jsonb_typeof(tags) = 'array' AND NOT jsonb_path_exists(tags, 'strict $[*] ? (@.type() != "string")')) NOT VALID,
  fence               bigint      NOT NULL DEFAULT 0  -- numbers the job's claims (schema v18, §7.3); kept in history
) WITH (fillfactor = 70);

-- Claim path: the only index the hot query touches.
CREATE INDEX hopper_jobs_claim ON hopper_jobs (queue, priority, scheduled_at, seq)
  WHERE state IN ('available', 'scheduled', 'retryable');

-- Running set: rescue, cancel delivery, global limits. Bounded by total concurrency.
CREATE INDEX hopper_jobs_running ON hopper_jobs (queue, attempted_by) WHERE state = 'running';

-- Uniqueness among live jobs. History counts only when UniqueOpts.Finished asks.
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
  FOR VALUES IN ('canceled', 'discarded') PARTITION BY RANGE (finalized_at); -- daily partitions
-- Schema v1 gives each a DEFAULT partition so finalize works before the leader
-- (M2) manages time partitions. The leader creates future time partitions only,
-- because attaching a partition scans the DEFAULT partition for overlapping rows.

-- A key's lane in history, for JobList with an ordering key (schema v13). Declared
-- ON ONLY each outcome table; the leader builds each existing partition's index
-- CONCURRENTLY and attaches it, and a partition created later gets its index when
-- it is attached, while still empty. hopper_job_history itself has none: the attach
-- that completed an outcome table's index would also mark that one valid, under an
-- ACCESS EXCLUSIVE lock taken while holding the partition's index, which deadlocks
-- with readers of hopper_job_history.
CREATE INDEX hopper_job_history_completed_ordering ON ONLY hopper_job_history_completed (ordering_key, id)
  WHERE ordering_key IS NOT NULL;
CREATE INDEX hopper_job_history_failed_ordering ON ONLY hopper_job_history_failed (ordering_key, id)
  WHERE ordering_key IS NOT NULL;
-- A unique key's finished jobs, for UniqueOpts.Finished (schema v14). Declared and
-- built like the ordering-key index.
CREATE INDEX hopper_job_history_completed_unique ON ONLY hopper_job_history_completed (kind, unique_key, finalized_at)
  WHERE unique_key IS NOT NULL;
CREATE INDEX hopper_job_history_failed_unique ON ONLY hopper_job_history_failed (kind, unique_key, finalized_at)
  WHERE unique_key IS NOT NULL;

-- A tag's jobs in ID order, for JobList by tag (schema v17): one row per tag of each
-- tagged job, kept in step with the tags column by triggers (§7.2).
-- Tags compare by their bytes (COLLATE "C"), whatever the database's collation.
CREATE TABLE hopper_job_tags (tag text COLLATE "C" NOT NULL, id uuid NOT NULL, PRIMARY KEY (tag, id))
  WITH (vacuum_truncate = false);                -- and hopper_jobs' autovacuum settings
CREATE TABLE hopper_job_history_tags (
  state hopper_job_state NOT NULL, finalized_at timestamptz NOT NULL, tag text COLLATE "C" NOT NULL,
  id uuid NOT NULL
) PARTITION BY LIST (state);                    -- then by day, for both outcomes (§7.10)
CREATE UNIQUE INDEX hopper_job_history_tags_tag                -- a job's tags rows are unique
  ON hopper_job_history_tags (tag, id, state, finalized_at);

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
index, `hopper_batches` and the batch index. Schema v4 (M8) adds `hopper_job_deps`, the
name and edges of a batch, and a `batch_id` index on history (§11). Schema v5 (M8) adds
the stream log `hopper_stream_events` (partitioned by day) and `hopper_stream_consumers`
(§10). Schema v11 adds `hopper_batch_progress (id, batch_id, done, failed)`, where
finalizing statements record batch progress (§11), with `vacuum_truncate = false`
like `hopper_jobs`, since every finalize of a batch's jobs writes to it. Schema v12
spells the canceled state `canceled` (§12). Schema v13 adds the history index on
`(ordering_key, id)` for listing one key's lane (§4.4), declared
`ON ONLY` the completed and failed outcome tables and built per partition by the
leader. On the live table, a key's lane is found through the claim's two ordering-key
indexes, for its waiting jobs and its running one, and its pending jobs through their
dependency edges, which only live workflow jobs have, so a key listing reads neither
the whole queue nor a new index on the hot table. Schema v14 adds the history index on
`(kind, unique_key, finalized_at)` for finding a unique key's finished jobs (§7.2),
declared and built the same way. Schema v15 adds `progress` to `hopper_jobs` and
history (§7.5), and v16 adds `logs` the same way (§7.5). Schema v17 adds `tags` the
same way, with `hopper_job_tags`, `hopper_job_history_tags` (whose time partitions the
leader keeps with history's) and the triggers that maintain them (§7.2). Schema v18
adds `fence` to `hopper_jobs` and history (§7.3), in the loop v17's columns took.

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
  `String`, `ParseJobID`, text and JSON marshaling, and `sql.Scanner` /
  `driver.Valuer` support. The core adds no UUID dependency.
- **Internal IDs stay compact.** `hopper_clients.id` remains a `bigint`, because it
  never leaves the database and is written into every running job.

### 6.2 State machine

```
insert ──► available ─┐                        ┌──► completed  ─┐
insert ──► scheduled ─┼─► (claim) running ─────┼──► canceled   ├──► moved to hopper_job_history
           retryable ─┘        │    │          └──► discarded  ─┘        │
               ▲               │    └──► retryable / scheduled (snooze)  │
               └───────────────┘                                        │
  rescuer: running, owner's lease expired ──► retryable (or discarded)   │
  JobRetry ◄─────────────────────────────────────────────────────────────┘
  JobCancel: any live state; running jobs are signaled and finalize as canceled
  JobDelete: any state but running; a waiting job is canceled without a history row
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
  `UniqueReplace` updates the args, metadata, tags, priority, attempts and `scheduled_at` of a
  non-running duplicate, which gives debouncing. Within one batch, later inputs with the
  same key resolve to the first.
- **Finished duplicates:** `UniqueOpts.Finished` names finished states (completed,
  canceled, discarded) whose jobs in history also count as duplicates, for as long as
  retention keeps them; with `ByPeriod` that is the rest of the period, which is part
  of the key. The plain insert statement is unchanged, so jobs without `Finished` pay
  nothing. A batch with such jobs is inserted as usual, which reports or replaces a
  live duplicate as before, and a second statement in the same transaction then looks
  up each job it inserted in history through the v14 index, deleting the job and
  reporting the finished one (never replaced) where it finds a duplicate. Looking
  after the insert rather than before also covers a duplicate finalized while the
  insert ran: its finalize deletes the live row and writes the history row in one
  transaction, so the insert's snapshot sees neither, but the insert's conflict check
  waits for that transaction on the live row, and the next statement's snapshot at
  read committed includes it. A duplicate already in history costs a row inserted and
  deleted, less than a second lookup. The lookup reads only the outcome table holding
  the counted states, and descends one index per partition of it, so it costs more as
  retention keeps more partitions: on Postgres 17, a batch of 100 jobs counting
  completed duplicates took about 9 ms with a day of hourly partitions (4.5 ms
  without `Finished`), and about 30 ms with seven days. A pool insert runs the insert
  and the lookup in a read committed transaction of its own, whatever the server's
  default isolation, so no worker claims a job about to be deleted. A caller's
  transaction at repeatable read or serializable keeps its snapshot, and misses a
  duplicate finalized after it. A job of a batch found finished counts toward the
  batch as that job's outcome (done, and failed unless it completed), recorded with
  the insert, so the batch still completes. Within a batch, a later input with the same
  key that counts different finished states is inserted in a statement after the
  earlier one's, as if inserted on its own.
- **Tags:** `InsertOpts.Tags` labels a job for listings, for example by tenant or
  release. Tags are strings of 1 to 255 bytes without control characters, at most 64
  distinct per job; repeats are dropped and the order kept. `hopper_insert` takes a
  `tags` array, checked the same way by `hopper_tags` (§10), which names the control
  characters by code point, as Go's `unicode.IsControl` does, rather than with
  `[[:cntrl:]]`, whose meaning varies with the database's collation, and looks for them
  in the tag's UTF-8 bytes, since a regex reads a single-byte encoding's bytes as code
  points (WIN1252's euro sign is byte 0x80). They are stored on the
  job's row as a jsonb array, NULL for none, rather than `text[]`: both drivers read
  jsonb the same way, while database/sql transports return arrays in formats that
  differ. The insert sends them as one more `jsonb[]` element per job, NULL for an
  untagged one. A `NOT VALID` CHECK (adding it reads no rows) refuses anything but an
  array of strings, in `hopper_jobs` and in history alike, so that a direct SQL write
  to either cannot store a value the readers must guess at.
- **Finding jobs by tag:** a listing pages in ID order from a cursor, which no index on
  the tags column can serve: a GIN index finds a tag's rows in no particular order, so
  a page would read every match past the cursor, and Postgres keeps no statistics on
  jsonb elements to choose between that and walking the primary key, which reads the
  whole table for a rare tag. So each tag of each job also has a row in
  `hopper_job_tags (tag, id)`, or in `hopper_job_history_tags (state, finalized_at,
  tag, id)` once the job is in history, whose btree walks one tag's jobs in ID order;
  a page costs about its size, for a rare tag or a common one. Triggers on
  `hopper_jobs` and `hopper_job_history` keep the rows in step with the column on every
  write, whoever makes it: COPY, the SQL contract, a replacing insert, a retry from
  history, a delete, and on history a write aimed at an outcome table or a partition.
  An insert uses a row trigger whose `WHEN (NEW.tags IS NOT NULL)`
  is checked before the trigger is queued, so an untagged insert pays nothing; a
  statement trigger would copy every new row, payload and all, into its transition
  table. A delete from `hopper_jobs` uses a statement trigger, whose transition table keeps large values
  where they are stored, so a batch of finalizes costs one statement, not one per job;
  it is planned on each call that has tagged rows, since a plan cached for one batch's
  size is wrong for another's. A `TRUNCATE`, which fires no delete trigger, truncates
  the table's tags table from a `TRUNCATE` trigger, unless the same statement has
  already emptied it. An update of a live job's `tags`, which only a replacing
  insert makes, or of its `id`, uses a row trigger. History's triggers are all row
  triggers, which Postgres gives each of a partitioned table's partitions, present and
  future, so they fire for a write aimed at an outcome table or a partition too; a
  statement trigger fires only for its own table. History is deleted from and updated
  only by retries, retention's prune of its `DEFAULT` partitions and direct SQL, so a
  row trigger there costs little, and nothing for an untagged job. History's tags rows
  are keyed by the job's `state` and `finalized_at` too, and an update of either can move the job to
  another partition, which Postgres runs as a delete and an insert, firing those
  triggers; an update within its partition fires the update trigger, which replaces
  the job's tags rows. A `TRUNCATE` of one of history's partitions fires no trigger of
  history's: the tags rows it leaves go with their partition, or with the `DEFAULT`
  partition's prune, at retention, and listings skip them. History's tags rows are
  unique, as `hopper_job_tags`' are, so reinserting such a partition's rows fails
  (23505) rather than adding a second copy of each job's tags rows, which would list
  the job twice. A tagged job pays an index insert per tag when inserted and a
  delete and an insert per tag when finalized, and a `completed` one a delete per tag
  when retention drops its hour before its day. History's table is partitioned by
  outcome as history is and then by day, and the leader creates and drops its
  partitions with history's (§7.10), so retention of its rows is a partition drop,
  less the hours of `completed` history that go before their day. A listing by
  tag has an arm per table (live and history): for each of its tags (`unnest`) a
  `LATERAL` subquery walks the tag's rows in ID order from the cursor and stops at the
  page size, and the arm keeps each job once, in ID order, up to the page. On the live
  table the walk looks each row's job up by key in a `LATERAL` subquery fenced with
  `OFFSET 0`. In history that lookup is a `LATERAL` reference to a partition key, which
  no plan can prune, so a page planned every one of history's partitions, once per tag:
  at 30 days of hourly `completed` partitions (725) a page by 8 tags took 830 ms to plan
  and one by 64 tags 8.5 s, against 15 ms and 81 ms at the default day. So outside a
  transaction the listing reads history in two statements: the walk returns the page's
  keys (ID, state and `finalized_at`), and a second statement reads the jobs by those
  values (`state = ANY('{...}') AND finalized_at = ANY($m) AND id = ANY($n)`), which the
  planner prunes to the partitions the page's jobs are in, or the `DEFAULT` partition
  for a period that has none. The times are a parameter, so that the statement's text
  is the same from page to page: pgx and its database/sql adapter prepare each text
  they run and keep up to 512 a connection on the server, and with the times written
  in, each page prepared another. The listing's transaction sets `plan_cache_mode` to
  `force_custom_plan`, so that each statement is planned for its values and the times
  prune as constants would; a generic plan would plan every partition. The states are
  written in, sorted, since a cast parameter of their type would not prune (its input
  function is not immutable), and there are few sets of them. The live arm goes in
  that second statement, so a page is two statements. Both run in one `READ ONLY`
  `REPEATABLE READ` transaction: under read committed a job finalized between them is in neither
  the live table nor the keys the walk read, and a page would skip it; under one
  snapshot a job moved after the walk is read where the walk found it, once. A
  history partition that retention drops between the statements is no longer read,
  since every snapshot sees a table's drop at once: its jobs, past retention, are left out
  of the page, which the walk fills from later keys. The second statement applies the listing's other filters, so it can
  return fewer jobs than the walk found. The page's jobs are known in full up to the
  end of each arm that came back full, the live arm's last job and the walk's last key;
  when the walk ends them short of a page, the listing walks history on from that key
  in the same transaction, keeping the live jobs it has. Each walk after the first
  takes twice the keys of the one before, up to 16 pages of them, so that a filter that
  keeps few of a tag's finished jobs, as `Metadata` beside a common tag can, costs a
  few round trips a page: with a page of keys each time, a page of 10 by a tag and a
  kind that only 4 of the tag's 404 finished jobs had took 41 walks, and now takes 6.
  The statement that reads a walk's jobs returns at most a page of them, the first in
  listing order, however many of its keys the filter keeps: a walk of 160 keys whose
  jobs all had the kind returned all 160 for a page of 10. The walk counts the jobs it
  found, so a job with two tags rows for one tag under different (state,
  `finalized_at`) counts once: only a direct SQL write can give it them, by reinserting
  into history at another time the rows of a partition a `TRUNCATE` emptied, whose
  stale tags rows stay. A walk cut off at the page size then reads as exhausted, the
  page can come back short, and `Jobs` stops at it; the rows go at retention. Inside a caller's transaction (`JobsTx`) the listing cannot set the
  isolation level, so its history arm looks jobs up inline as the live one does; it is
  correct, and plans every partition. A statement leaves out a parameter it does not
  use, since the server cannot type one it never sees (42P18), and numbers the rest in
  order. The lookup checks the walked tag on the job's row too (`tags ? t.tag`), so
  that a tags row the column no longer holds lists nothing, whatever left it: a
  `TRUNCATE` of a partition, or a write with the triggers disabled. The walk is narrowed by what the tags table holds beside the tag, since the
  fence keeps the job query's filter out of it: the listing's `IDs`, and in history its
  states, walking only an outcome's partition of the tags table when the states are all
  of one outcome. Without that, a page by IDs, or by `discarded` for a tag on many
  completed jobs, walked every row of the tag and looked each job up only to drop it.
  The page's jobs are among each arm's, and the arms are merged. The fence keeps
  the planner from joining the walk to a scan of the whole job table, which it prefers
  when it cannot tell how common a tag is; ordering and limiting the walk in the same
  query as the lookup makes it merge history's partitions in index order even when it
  takes the tag for a rare one, as a generic plan does, where ordering the walk in a
  subquery sorted every row of the tag. A job with several of `TagsAny`'s tags comes
  from each of their walks, and the arm keeps one (`DISTINCT ON (id)`), so each walk
  reads at most a page however much the tags overlap; leaving an earlier tag's jobs out
  of each later walk instead walked the whole range of a tag whose jobs mostly had an
  earlier one (seconds a page for two tags on the same 600,000 jobs). With one walk per
  tag in one arm, the plan is the same for one tag as for 64. Repeated tags are
  dropped, in `TagsAll` as in `TagsAny`, so that a filter repeating one tag costs no more
  than one naming it once. `TagsAll` walks its rarest tag and checks
  the rest on the job's row (`tags ?& $n`): the listing's statement counts each tag up
  to 1,000 rows in each table it reads, caps the sum at 1,000 too (a tag at the cap
  in one table is common however few rows it has in the other, and an uncapped sum
  ranked it below a tag just past the cap in both), and tells tags that both reach
  the cap apart by their estimated rows in the tables it walks: each leaf's most common frequency in
  `pg_stats` times its rows, the live tags table's kept by the leader's analyze and
  history's partitions' by autovacuum, so that a listing of history is not decided by
  the live table's. The leaves and the estimates are read once a statement, since
  inlined per tag pg_stats' join listed the partitions again for each of its rows, a
  cost that grows with the square of history's partitions. Each page chooses again: the count ignores the cursor, so every page of a
  listing picks the same tag while the jobs stay put, and it reads at most 1,000 index
  entries per tag and table, a fraction of a millisecond. Carrying the choice from one
  page to the next would need `JobList` to report it, a change to the driver interface
  that saves less than a page's own lookups cost. The listing's limit is a constant in its SQL, as the claim's is (§7.3): a
  cached generic plan for a parameter would cost the page as a tenth of the table and
  choose parallel scans and JIT compilation. On Postgres 18 with a million live and a
  million finished jobs, under generic plans, a page of 20 took 0.3 ms for a tag on
  every job, 0.1 ms for one on 18, 0.4 ms for both with `TagsAny`, and 1.2 ms with
  `TagsAll` for the common tag and one on 2,000 jobs. With 300,000 jobs (200,000 in
  history) tagged with one of 64 tags and 8 more, a page of 100, planned for its values
  as the listing's transaction plans it, took 3.2 to 4.7 ms to plan, both statements
  together, at the default day of `completed` partitions (29), for 1 to 64 tags of
  `TagsAny` and 2 or 8 of `TagsAll`. At 30 days (725) it took 2.0 to 7.9 ms when the
  page's jobs had finished within hours of each other, as jobs listed in ID order
  mostly have, and 12 to 17 ms with `finalized_at` random across the 30 days, where
  each job of the page was in a partition of its own: a cost bounded by the page size.
  The walk of the 33 daily tags partitions is about a millisecond of that.
- **Other filters:** `JobFilter.IDs` and `Priorities` match any of the given values,
  and `Metadata`, a JSON object, matches jobs whose metadata contains it (jsonb `@>`).
  Metadata has no index, since one would cost every insert; it narrows what the other
  filters find. Each filter is added to the listing's SQL only when set, as a plain
  condition. A filter that cannot list (an ordering key without a queue, an
  out-of-range priority, `Metadata` that is not a JSON object or that jsonb's input
  refuses, a queue, kind, state or tag that the database's text refuses, a tag no job could
  have, or more distinct `TagsAny` or `TagsAll` than `MaxTags`, which bounds a
  listing's walks) fails with `ErrInvalidFilter`. What the server's input refuses is
  left to it: text with a NUL or a character the database's encoding lacks, and for
  jsonb also bytes that are not UTF-8, a `\u0000` escape, an unpaired surrogate, a
  number beyond numeric's range, and nesting past the server's stack. No statement
  checks a filter before the listing, so a paged listing costs one statement a page.
  A data exception (class 22) or 54001 from the listing can also come from a stored
  row, such as one whose text has no UTF-8 form, so only then are the filters' values
  given to the server again on their own: if it refuses them, the listing fails with
  `ErrInvalidFilter`, wrapping the server's error, which names the value, and
  otherwise with the listing's own error.
  In the caller's transaction (`JobsTx`), a listing whose filters the server could
  refuse runs under a savepoint, so that a refusal leaves the transaction usable
  rather than aborted. Only `Metadata` and text with a NUL or beyond ASCII can be
  refused, since ASCII is valid in every server encoding, so other listings skip the
  savepoint's two extra round trips.
- **Tags in a rolling deploy:** a client of the previous release drops a job's tags
  wherever it writes the row anew, since its statements name the columns it knows:
  when it moves a tagged job it finalizes to history, retries one from history, or
  inserts a batch's callbacks. The triggers keep the tags tables in step with what it
  writes. The empty string and repeats, which the CHECK allows and only a direct SQL
  write stores, are left out wherever tags are read, they get no tags rows, and a
  retry from history brings the job back without them. A batch callback's stored tags
  read the same way, less any longer than `MaxTagBytes`: only a direct SQL write to the
  batch row stores one, and one past the tags index's largest row would fail the
  finalize that completes the batch. A job's own tags are not filtered so: a direct
  SQL write of a tag longer than the live tags index takes fails that write, and one
  of 2,666 to 2,676 incompressible bytes, which the live index takes, fails the
  finalize that moves the job to history, whose tags index also holds the state and
  `finalized_at`, with every job of that finalize. A direct SQL write keeps to
  `MaxTagBytes`, as `hopper_tags` checks.
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
WITH c AS MATERIALIZED (
  SELECT id, row_number() OVER (ORDER BY priority, scheduled_at, seq) AS n FROM (
    SELECT id, priority, scheduled_at, seq FROM hopper_jobs
    WHERE queue = $queue AND state IN ('available','scheduled','retryable')
      AND scheduled_at <= now()
    ORDER BY priority, scheduled_at, seq
    LIMIT 64                      -- a constant: one statement per bucket 1, 2, 4 … 512
    FOR UPDATE SKIP LOCKED
  ) cand
)
UPDATE hopper_jobs j
SET state = 'running', attempt = attempt + 1, fence = fence + 1, attempted_at = now(), attempted_by = $client
FROM c WHERE j.id = c.id AND c.n <= $n
RETURNING j.*;
```

- **Fence.** Every claim increments the job's `fence`, returned as `JobRow.Fence`, and
  each result, progress write and rescue names the fence of the claim it belongs to
  (§7.5). The attempt number cannot serve: a snooze gives it back, so the claim after
  a snooze has the same number, and a job rescued while its client is alive
  (`RescueStuckAfter`) can be claimed again by that same client. Snoozes, retries and
  the archive keep the fence, and `JobRetry` carries it back from history, so for a
  given job it never repeats; the error recorded for a failed attempt carries it too.
  It is a counter on the row rather than a sequence, which would be a hotspot shared
  by every claim and, cached per session, would not increase per job. `fence` is in no
  index, so incrementing it adds no index write to the claim. A client of the previous release claims
  without incrementing it, archives a job with `fence` 0 and drops it in `JobRetry` from
  history; the finalize fence still holds then against other clients, since it checks
  the client too, and progress writes check the attempt number as well. A fence reset
  that way can come round to the same value for the same client, and a late result of
  that client's earlier claim at that value, still running all that while, would
  finalize the later one: this needs the reset and a stuck attempt that outlives as
  many claims of its job, and is accepted rather than adding the attempt number to the
  finalize. A fence passed outside hopper only increases once every client runs this
  release.
- **Plan stability.** Drivers cache prepared statements, and Postgres keeps a
  statement's generic plan until something invalidates it. A plan made while
  `hopper_jobs` was empty (an idle queue, drained and vacuumed to zero pages) costs every
  scan at zero, and with the candidates as a plain subquery the planner nested them
  inside a scan of the table and re-executed the locking subquery once per row: after a
  burst, one claim ran for minutes holding locks while every finalizer queued behind
  it. The candidates are therefore a `MATERIALIZED` CTE, evaluated exactly once
  whatever the join order, which bounds a stale plan to one scan of the table per
  claim; and the leader's live-table maintenance (below) refreshes the table's
  statistics as soon as the planner's row count is an order of magnitude off, which
  invalidates cached plans cluster-wide within one leader interval. Planning every execution afresh instead
  (pgx's cache-describe mode) was measured at 40% of throughput and rejected. The claim's LIMIT is a constant for the same reason, one prepared statement per bucket
  (1, 2, 4 … 512) with the count applied through row numbers: with the limit as a
  parameter the planner's generic plan assumes it selects a tenth of the table and joins
  the update to it with a sequential scan and a hash, so every claim scanned the table
  (a 94% loss under `force_generic_plan`), and in the automatic mode the planner rejected
  that plan and replanned every execution instead (−27% on the CI runner). Rows locked
  beyond the count are released when the statement ends. The finalize statement finds
  its results' rows by primary key whatever the statistics say, comparing the running
  state as text, which no partial index's predicate matches: compared as the enum,
  statistics taken while few jobs ran (an analyze just after a backlog was inserted)
  made a scan of all of `hopper_jobs_running` look like a page, and the statement
  compared every running row with every result, 0.6 s a statement at 40,000 running
  rows, which piled up as the finalizes fell behind, so a workflow drain of 3 s took
  20–48 s. The statements that look up one running job by ID check it the same way.
  The cast stays because Postgres (14–18) uses a partial index only for a condition on
  the identical expression, and no index but the primary key serves a lookup by ID;
  matching by primary key with the state checked after, `= ANY ('{running}')` or a
  scalar subquery all still matched `hopper_jobs_running` in EXPLAIN.
  The finalize tail and the dependents' promotion look up rows keyed by the
  statement's own rows (the finished jobs, the completed batches, the candidates) as
  `= ANY (ARRAY(...))`, which the planner takes for ten keys and so probes an index
  for: joined to the CTEs, it guessed hundreds of rows for them, and scanned
  `hopper_job_deps`, `hopper_jobs` and `hopper_batches` whole on every finalize, at a
  cost that grew with the workflows in flight. The counted batches are looked up one by
  one by primary key, in a `LATERAL` subquery fenced with `OFFSET 0`: as `= ANY` the
  planner rescanned the counts once per batch row, quadratic in the batches a statement
  touches.
- **Live-table maintenance.** Every claim walks the claim index past the entries of
  jobs that have run and left, which only a vacuum removes, so pickup latency climbs
  between vacuums: at 30,000 jobs/s on the reference hardware, from 10 ms to seconds
  within a minute. Autovacuum looks only every `autovacuum_naptime` (a minute by
  default), so the leader keeps the table fit itself, in a loop of its own since a pass
  over a large backlog takes seconds: every leader interval it vacuums `hopper_jobs`
  once a hundred thousand dead rows have accumulated (`VACUUM (SKIP_LOCKED, TRUNCATE
  false)`: heap truncation needs an exclusive lock the vacuum retries for five seconds,
  and every claim waits behind it whenever it briefly succeeds, so schema v7 disables it
  on the live table for autovacuum too;
  what matters is their number, about five hundred index pages, not their share of the
  table, so a short burst never triggers one) and re-analyzes it, with a small sample,
  at once when the row count the planner holds is ten times off the live count, and
  otherwise once a tenth of the table has changed but at most every thirty seconds: an
  analyze invalidates every connection's cached plans, and one in the middle of a burst
  cost a CPU-starved server a fifth of its throughput. The live tags table and
  `hopper_job_deps` are kept the same way, with the server's sample size, except that
  churn re-analyzes the dependency table at most every ten minutes (at once if it was
  never analyzed; a vacuum gives the planner its row count but no statistics): until it
  has statistics, the planner takes each lookup of a job's edges for a two-hundredth of
  the table, and scans it instead, while what it reads from them (edges per job,
  dependents per job) changes only with the shape of the workflows, and each analyze has
  every connection replan the finalize statement. The maintenance never
  waits for a lock. Schema v6 also sets `hopper_jobs`' autovacuum thresholds low with no
  cost delay.

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
- **Expiry.** `InsertOpts.TTL` sets `expires_at` to `now() + TTL` at insert. It bounds
  how long the job may wait to run, not only its first wait: a waiting row
  (`available`, `scheduled` or `retryable`) past `expires_at` is skipped by the claim,
  and the leader moves it to history as `discarded` with the error `hopper: expired`
  (with `while snoozed: <reason>` appended for a job snoozed with a reason).
  So a job that has run and is retrying or snoozed is discarded too once its deadline
  passes, without running again. A running job is not interrupted, but if its attempt
  ends in a retry or a snooze past the deadline, it is discarded. `JobRetry` clears the
  deadline, live or from history: an explicit retry is the operator overriding the
  wait, so a job past its TTL still runs. `JobRetryOrderingKey` keeps it: a wake is a
  producer saying a key's work is due, not an operator overriding a deadline, so a
  woken job keeps its TTL and one already past it is left to expire.
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
- Each job gets a context that is canceled on timeout (the worker's `Timeout()` or the
  queue default), on `JobCancel`, on lease loss (§7.6), or on a hard stop.
- A panic is recovered and recorded as an error with its stack trace, then treated as
  a normal failure, which `Config.OnError` sees as a `*PanicError`.
- `Middleware` (an interface with `Work` and `Insert` methods; `WorkMiddleware` and
  `InsertMiddleware` wrap a function) wraps execution for logging, metrics, tracing
  and custom policy, and wraps inserts, for example to inject trace context into
  `metadata`. Middleware runs in `Config.Middleware` order, outermost first, inside
  the panic guard.
- `hopper.SetOutput(ctx, v)` records a result, which is stored with the finalized job
  and returned by `Await` and `JobGet`. Output is kept only for archived jobs.

### 7.5 Finalize
Results are buffered and written by a per-client finalizer. It flushes every 25ms or
every 500 results, whichever comes first, in **one statement**. Up to 10,000 results
wait in the buffer, so a commit that is slow for a while is absorbed while the client
keeps claiming; only when the buffer is full do job goroutines block on submit, which
stops the producers. A flush that hits its 30 s timeout is retried as two halves,
down to single results, so one statement the database cannot finish in time degrades
into smaller ones instead of being sent again as it was. Retries and snoozes are
updated in place, a snooze with its reason. Terminal outcomes are moved to history.

```sql
WITH r AS (SELECT * FROM unnest($ids, $attempted_by, $states, $delays, $snoozes, $errors, $outputs, $archives,
                                 $snooze_reasons, $fences) AS r(...)),
retry AS (
  UPDATE hopper_jobs j SET state = r.state,
         scheduled_at = CASE WHEN j.wake_requested_at >= j.attempted_at THEN now() ELSE now() + r.delay END,
         attempt = CASE WHEN r.snooze THEN j.attempt - 1 ELSE j.attempt END,
         errors = j.errors || jsonb_set(r.error, '{at}', to_jsonb(now())), attempted_by = NULL,
         snooze_reason = CASE WHEN r.snooze AND NOT coalesce(j.wake_requested_at >= j.attempted_at, false) THEN r.snooze_reason END,
         wake_requested_at = NULL
  FROM r WHERE j.id = r.id AND r.state IN ('retryable','scheduled')
    AND j.state::text = 'running' AND j.attempted_by = r.attempted_by AND j.fence = r.fence
  RETURNING j.id, j.fence),
done AS (
  DELETE FROM hopper_jobs j USING r
  WHERE j.id = r.id AND r.state IN ('completed','canceled','discarded')
    AND j.state::text = 'running' AND j.attempted_by = r.attempted_by AND j.fence = r.fence
  RETURNING j.*, r.state AS final_state, r.error, r.output, r.archive),
archived AS (INSERT INTO hopper_job_history SELECT … , now() FROM done WHERE archive)
SELECT id, fence FROM retry UNION ALL SELECT id, fence FROM done;
```

- The `state = 'running' AND attempted_by = r.attempted_by AND fence = r.fence`
  condition fences out stale results. Each result carries the client ID and the fence
  of the claim it ran in, so a client that re-registered under a new ID still
  finalizes the attempts it claimed under the old one. If the job was rescued and
  claimed again, by another client or by the same one, the result is dropped and
  logged, so a finalize can never overwrite a newer attempt. The statement returns
  the job and fence of each result it applied; the client logs the rest. A flush can
  hold two results of one job, a rescued attempt's late one and that of the claim
  after it: at most one applies, its fence says which, and the late result emits no
  event. The client keeps the
  cancel function of each running job's latest claim, and an attempt that returns
  removes only its own claim's, so that the return of a rescued attempt does not take
  away the cancel of the claim after it. A claim that replaces another cancels the
  earlier attempt with `ErrJobFenced`, since its result can no longer apply; that
  attempt's failure, like a rescued job's, goes to neither `Config.OnError` nor the log.
- Retry delays are sent as durations and applied to database `now()`; error
  timestamps are set by the database too (§7.1).
- Queues with `DeleteCompleted: true` delete completed jobs without writing history.
  Canceled and discarded jobs are always archived, because history is the
  dead-letter queue (§7.10).
- Outcomes:
  - **success:** `completed`.
  - **error:** `retryable` at `now() + backoff(attempt)`, or `discarded` when
    `attempt >= max_attempts`.
  - **`hopper.Snooze(d)`:** `scheduled` at `now() + d`, without using up an attempt.
    A worker that returns a `*hopper.SnoozeError` with a `Reason` has the reason stored
    in `snooze_reason` and shown as `JobRow.SnoozeReason`, so operators (`hopper jobs
    get`, `hopperui`) can see why the job waits; `hopperotel` records it on the attempt's
    span, and the `job.snoozed` event carries it as `Event.Error`. The claim clears it
    in the statement that takes the job, and so do every other finalize, `JobRetry`,
    `JobRetryOrderingKey` and a replacing insert, so the reason only ever describes the
    current wait. A client of the previous release clears it in none of these, so
    during a rolling deploy a running or retrying row can still hold one; reads
    therefore report the reason only for a `scheduled` row. History has no such
    column, but a snoozed job that expires or is canceled keeps the reason in the
    error it is finalized with
    (`hopper: expired while snoozed: <reason>`, `hopper: canceled while snoozed:
    <reason>`).
  - **`hopper.Cancel(err)`:** `discarded` immediately.
  - `Config.OnError` can change the outcome of an error or a `Cancel` (below).
- `Config.OnError(ctx, job, err) ErrorOutcome` sees every failed attempt the client
  runs, of any kind, with the worker's error: a returned error, a recovered panic
  (`*PanicError`), a timeout, a `Cancel`, or a kind with no worker. It can change the
  outcome: `Discard` (as `Cancel`), `Snooze` (as `Snooze`, without using up the
  attempt, with the error as the snooze reason) or `RetryAt` (a retry at that time,
  using up the attempt; the last attempt is discarded all the same). The zero
  `ErrorOutcome` keeps the outcome, and a panic in the hook is logged and keeps it
  too. It is not called for a worker's `Snooze`, a `JobCancel` (even when the worker
  then returns `Cancel`), or a client that is stopping, none of which is the job
  failing, nor for a `CompleteTx` attempt whose job could not be read, which may have
  committed. A job the leader rescues (§7.6) is retried or discarded without it. It
  runs on the worker's goroutine before the result is buffered, so it should be quick.
  A `JobCancel` that arrives while it runs wins over what it returns. Its context
  carries the client (`ClientFromContext`), and it runs after `Middleware`:
  `hopperotel`'s span records what the worker returned, and the job's events the
  outcome the hook chose.
- `backoff(n) = n^4 + 5s ± 10% jitter`, capped at 24h. A failed job's next run is
  chosen by, in order: `Config.OnError`'s `RetryAt`, the worker's `NextRetry(job)`,
  the queue's `QueueConfig.RetryPolicy`, the client's `Config.RetryPolicy`, then
  `backoff`. A `RetryPolicy` (`NextRetry(job *JobRow) time.Time`) that returns the zero
  time passes to the next, and so does one that panics, which is logged.
  `hopper.ExponentialRetry{Base, Max}` doubles from `Base` (one second) with ± 10%
  jitter up to `Max` (24h). Like `MaxAttempts`, these are per client: each client
  applies its own to the attempts it runs.
- `hopper.CompleteTx(ctx, tx, job)` completes the job inside the worker's own
  transaction, so the work and its completion commit together. It runs the finalize
  statement above for one completed result through `Driver.UnwrapTx(tx)`, bypassing
  the buffer, with the output `SetOutput` recorded so far and the queue's
  `DeleteCompleted`; it takes the client from the job's context (§4.1), so it needs
  no client argument. The fence applies as to any finalize: if the attempt no longer
  owns the job, nothing is written and `ErrJobFenced` is returned. History, awaited
  jobs (`pg_notify` is transactional), batches and workflow dependents all take effect
  when `tx` commits.
- After the worker returns, the client reads the job (one primary-key read of the
  live table and history), because a fenced result cannot tell a commit from a rescue
  that applied once `tx` rolled back. The read matches the job to this attempt by its
  client, fence and attempt number: a `JobRetry` from history by a client of the
  previous release drops the fence, and the claim after it can repeat this one's.
  - **Committed.** If the job is in history as this attempt completed it, or gone (a
    completed job on a `DeleteCompleted` queue), `tx` committed, whatever the worker
    returned: the client emits `job.completed`, settles the job's batch and promotes
    its dependents (§11), and writes nothing. This is the common case. Neither the
    failure log nor the retry policy runs.
  - **Not committed.** Otherwise the attempt's other outcome applies: the worker's
    error, or `the worker returned nil, but the transaction it completed the job in did
    not commit` if it returned nil. The job retries (or is discarded on its last
    attempt), so no job is left running because a worker ignored a failed commit. The
    finalizer writes these results in their own statement with `SKIP LOCKED`, because
    a worker may return before `tx` ends: a job whose row is still held is read again,
    and if it is still running in this attempt, its result is retried every 250 ms
    (with one warning) while the client's other results keep flushing. Once `tx`
    ends, the job is reported completed if it committed, or the result applies. A job
    that moved on without this attempt (rescued) is dropped like any fenced result.
    Every other result goes in a statement that does not skip held rows, which is
    cheaper. If that statement has not finished within 2 s, which a row held by a
    caller's transaction (§7.7) causes, the batch is written again with `SKIP LOCKED`,
    and a result left out is read again and retried in the same way.
  - **Unknown.** If the read fails, the result goes the same way, and the fence and a
    second read decide.
- From `CompleteTx` until `tx` ends, `tx` holds the job's row and those of workflow
  dependents it promotes or cancels, and a lock on the history table that holds off
  the creation and drop of history partitions (which give up after 200 ms and retry
  on the next pass, §7.10). Its batch progress is an appended row (§11), so the job's
  siblings finalize without waiting for it, and the leader's rescue skips rows a
  transaction holds, so a lost client's open `tx` holds up neither. Once the worker
  returns and the client has seen `tx` commit, it settles the job's batch and promotes
  its dependents, which completes the batch or starts the dependent if a sibling
  finalized at the same time missed this job. `CompleteTx` belongs last, just before
  the commit.
- **Progress.** `RecordStep` and `RecordCursor` write the job's `progress` column
  (`{"step", "cursor", "at"}`, the time `statement_timestamp()`) with one `UPDATE`
  fenced like a finalize: `state = 'running'`, the attempt's client, its claim's
  fence and its attempt number, which guards a fence that a client of the previous
  release reset. A fenced write changes nothing and returns `ErrJobFenced`. They run on the
  pool and are not batched: a step is long next to its write, and the write must be
  durable before the worker goes on. `RecordStepTx` and `RecordCursorTx` run the same
  statement through `Driver.UnwrapTx(tx)`, so the progress commits with the step's
  writes. Until `tx` ends it holds the job's row. A write on the pool would then wait
  for the worker itself, so once a transaction of the attempt's has written the row
  (`CompleteTx`, `RecordStepTx`, `RecordCursorTx`), a pool write locks the row with
  `NOWAIT` first and fails with `ErrJobLocked` instead of waiting, and the attempt's
  result goes to the finalize statement that skips held rows. An attempt's writes are
  serialized, so `job.Progress` ends as the last one stored. The claim, retries,
  snoozes and the archive keep `progress`, and `JobRetry` carries it back from
  history, so a re-driven job resumes too; `JobRetry(ctx, id, hopper.FromStart())`
  (`hopper jobs retry -from-start`, and a button in the web UI) clears it, to run a
  finished job over or repair one whose recorded step was renamed. A replacing insert
  (`UniqueReplace`) clears it too, since the job's new args make its old progress
  meaningless; a running job is not replaced and keeps it. A value that is not in the
  form hopper writes, such as one set by hand, reads as what can be made of it (none,
  if it is not an object), so it never fails a claim. `RunSteps` resumes after
  `Progress.Step`, and discards the job (`Cancel`) if that step is not among the steps
  it was given, or if a step has no name or is listed twice, since every attempt would
  fail the same way. `hoppertest.Work` keeps progress in memory, and
  `WorkOpts.Progress` starts a job part way through.
- **Logs.** `hopper.Logger(ctx)` builds the attempt's logger on first use: a
  `slog.MultiHandler` over `Config.Logger`'s handler, with the job's ID, kind and
  attempt, and a `TextHandler` writing into the attempt's buffer at
  `Config.JobLogs.Level` (default Info), with `JobLogs.ReplaceAttr`: `Config.Logger`'s
  handler options do not reach it, so what that handler redacts is redacted here
  too, or it is stored with the job. The buffer keeps whole lines up to `MaxBytes`
  (default 32 KiB); the line that would pass it and every one after are dropped and
  counted, and the log ends with a line giving the count. The attempt's log
  (`{"attempt", "log"}`) rides on its result (`JobFinalize.Log`), and the finalize
  statement appends it to the job's `logs` array, keeping the latest
  `JobLogs.MaxAttempts` (default 10) with a `jsonpath` slice, in the same `UPDATE` or
  `DELETE ... RETURNING` that moves the job on: logs cost no statement or round trip
  of their own, and a job that logged nothing writes `NULL` and keeps what it has. A
  completed job that skips history (`DeleteCompleted`) sends no log. A snooze gives
  its attempt back, so the next attempt has the same number: its log is marked
  `snoozed`, and replaces the log of the previous snooze of that number, as a snooze
  that logged nothing removes it, so a job that polls by snoozing keeps at most one,
  not `MaxAttempts` of them. `MaxBytes` is at most
  1 MiB, `MaxAttempts` at most 100, and their product at most 4 MiB: each finalize
  that writes a log rewrites the job's whole `logs` value, so the product bounds a
  write's cost (to within each entry's dropped-lines line and JSON escaping), and with
  it how much a job that retries many times writes over its life. Larger logs go to a
  log store, which the database never holds. A finished attempt's lines wait in memory
  with its result: a flush starts once its results hold 8 MiB of logs, which keeps a statement's parameters small,
  and job goroutines block on submit while the buffered results hold 64 MiB, as they
  do when the buffer is full, so a slow database cannot make logs exhaust memory. A
  stop's final flush lets those submits through and takes their results, as it takes
  those waiting on a full buffer, and ends once no submit is in flight, counting the
  retry of a result whose row a transaction held. Once what was buffered at the stop
  is written, it takes results for at most 5 s more; past that, those handed in later,
  from jobs still finishing, are dropped with a warning, and their jobs rescued,
  rather than holding the stop (a result whose `CompleteTx` committed is applied).
  Results handed in before then are written, however long the writes before them took.
  Completed jobs that skip history hold no log, nor does a `CompleteTx` result, and
  once an attempt's log is full its later lines are counted without being formatted.
  `CompleteTx` writes the lines logged before it. The lines are not streamed to the
  database while the attempt runs: each write would rewrite the whole value on the
  hottest table, so a steadily logging job would write the square of its log's size,
  and live lines are already in `Config.Logger`. The cost is that the lines of an
  attempt whose result is not written are lost: its client crashed or lost its lease,
  or it was rescued for running past `RescueStuckAfter`. The rescue's error says which.
  Retries, snoozes, the archive, cancels, expiry and `JobRetry` from history keep
  `logs`, and the job's history partition takes them when it is dropped, so they live
  as long as the job: 24 hours completed, 7 days failed, by default. Only
  `JobLogs` (`Executor.JobLogs`) reads them: `JobRow` has no logs, so `JobGet`,
  `Await`, the read after a `CompleteTx`, the claim and listings never carry them.
  A worker run inline with `Workers.Work` inside an attempt logs as that attempt.
  A value not in the form hopper writes reads as the entries that are.
- **Log store.** With `Config.JobLogs.Store`, a `LogStore`, the attempt's logger
  writes its text lines to the store instead of the buffer. Logging never waits for
  the store: each line is queued, and one goroutine per attempt, started with the
  first line, calls `Writer(ctx, job)` and hands the lines to it in order, one per
  `Write`. An attempt that logs nothing opens no writer and starts no goroutine. Up
  to 256 KiB of lines wait for a store that falls behind; past that, lines are
  dropped and a line counting them follows, so a slow or hung store costs lines, never
  the worker's time. `job` is the row as claimed, a copy the worker cannot change
  under the store. The context carries the values of the context the worker first
  called `Logger` with (a trace span its middleware started, say), but not its
  cancellation, deadline or attempt (a store that logs with `hopper.Logger` must not
  re-enter the attempt's logger), and is canceled once the writer is closed or
  hopper stops waiting for it. The log is ended once the worker returns, and the
  attempt's result waits for the queued lines and `Close` for up to 5 seconds, so a
  store that saves on `Close` has the log when the job is finished; past that the
  context is canceled and the result goes ahead. An attempt canceled by a hard stop
  (or a fence) waits for at most half the stop's grace instead, so its result still
  reaches the finalizer before it stops. A line logged after the end, by a goroutine
  the worker left, goes to `Config.Logger` only. Nothing goes to the `logs` column
  (the result carries no `Log`), so the store costs the database nothing and keeps a
  crashed client's lines up to the crash, less those still queued. Logs are keyed by
  job ID and attempt, which a process that runs the work outside the client (a
  remote executor) knows from the claim, so it writes to the store directly.
  `Client.JobLogs` reads the job's row, its logs and the store's at once, and returns
  the row's logs (from before the store, or from clients without it during a rolling
  deploy, which are ordered before the store's of the same attempt number), then the
  store's, in attempt order. A snooze gives the attempt back, so the run after it
  opens a writer under the same ID and attempt, and the store keeps both: a store log
  that another of the same number follows, or whose number is past the job's
  current attempt, was a snooze. As in the row, only the latest snooze of each
  attempt number and the latest `MaxAttempts` logs are returned, so a job that
  polls by snoozing for a week renders as a few entries, whatever the store keeps.
  If the store fails, `JobLogs` returns the row's logs with the error (`ErrNotFound`
  still matches for a job that is gone), and hopperui shows the job with a note
  where the logs would be; a store's `ErrNotFound` means it has none. The CLI, which
  has no store, shows only the column. `MaxBytes` neither applies nor is checked, and
  retention is the store's: hopper does not remove a job's logs from it. A failed or
  panicking `Writer`, `Write` or `Close`, or a nil writer, is logged to
  `Config.Logger` once and drops the rest of that attempt's lines; it never changes
  the job's outcome. The interface stays small on purpose: a live tail is the
  store's to offer, from what it has been written.
- The finalizer's write runs on a context the stop signal cannot cancel: a result that
  reached the finalizer belongs to a job that has finished. While the client is running,
  a failed flush is retried with backoff and the buffered results apply back-pressure
  to claiming once the buffer is full. Once the client is stopping, each batch gets one
  more attempt.
- The deep buffer exists because of the v0.1.1 soak (§8.2): with a two-batch buffer,
  four clients' finalizers convoyed on the history partition's extension lock and the
  WAL-write lock, a flush then exceeded its 47 ms budget at 10,700 results/s per
  client, every worker blocked on submit, and the client stopped claiming for one to
  three seconds at a time (issue #20). Larger flushes do not help: the convoy's cost
  is per row, a 2,000-row statement held the locks four times longer and could hit
  the flush timeout, and a timed-out statement retried whole livelocked the client.
- If a job has `await = true`, the finalize statement sends `pg_notify('hopper_done', id)`
  from its `RETURNING` clause, and `Await` returns as soon as the transaction commits.
  `Await` on a client without a listener (one that is not started) polls at
  `PollInterval`. A canceled or discarded job surfaces as `*JobFailedError`.

### 7.6 Liveness and rescue
Liveness is tracked **per client, not per job**. Each client holds a lease row in
`hopper_clients` and renews it three times per TTL (`Config.LeaseTTL`, 15s by default,
so every 5s):

```sql
UPDATE hopper_clients SET expires_at = now() + $ttl WHERE id = $me AND expires_at > now();
```

A heartbeat therefore costs one row per process regardless of how many jobs it is
running. Running jobs are never rewritten just to prove they are alive, which removes
the largest source of write amplification in heartbeat-based designs.

- **Rescue.** The leader's rescuer finds `running` jobs whose `attempted_by` has no
  unexpired lease and moves them to `retryable` with the error `client lost` (or to
  `discarded` if they are out of attempts). A crashed pod's jobs run again within about
  20s. The rescue reads its candidates and then finalizes them, fenced to the claim
  each was found in, so a job its client finalized and claimed again in between is
  left to that claim. The rescue's finalize skips rows another transaction holds (a `CompleteTx` that
  has not ended, §7.5) and retries them on the next pass, so one such row cannot hold
  up the leader's loop and its lease renewal; a full batch that rescues nothing ends
  the pass rather than read the same rows again. The read takes the oldest candidates
  first, so while a full batch of them stays held, candidates behind them wait for
  those transactions to end; reading past held rows would change the rescue's query
  for a case that needs `RescueBatch` rows held at once.
- **Fencing.** If a renewal matches zero rows (after a long GC pause or a partition),
  the client has lost its lease. It cancels the context of every in-flight job that has
  attempts left and re-registers under a new ID. A job on its last attempt keeps
  running, because the rescuer discards it instead of running it again, so no second
  attempt can overlap it and canceling would only lose its work. Results still in flight are submitted anyway: the finalize fence (§7.5)
  rejects any whose job was rescued meanwhile and applies the rest, which is equivalent
  to a rescue. A leader that finds its own jobs among the rescue candidates fences
  itself first, so it does not re-claim them under the lapsed ID before its lease loop
  notices.
- **Hung jobs.** A live client with a stuck goroutine is handled by timeouts. An
  optional `RescueStuckAfter` also rescues jobs that are still running past an absolute
  age, measured by database time: the candidate query reports which jobs it found as
  stuck, and the rescue's error says so. The rescue does not reach the stuck attempt:
  it runs on until it returns, and its result is dropped, unless its own client claims
  the job again, which cancels it (§7.5).
- **Graceful stop** deletes the lease after the finalizer flushes, so nothing needs
  rescuing.

### 7.7 Cancellation
`JobCancel` on a waiting job moves it straight to history as `canceled`. On a
running job, it sets `cancel_requested_at` and sends
`pg_notify('hopper_control', 'cancel:<id>')`. The owning client cancels the job's
context immediately, with cause `hopper.ErrJobCanceled`. Each lease renewal also
returns any cancel requests for that client's running jobs (through the small running
index), which covers missed notifications. The job finalizes as `canceled` if the
worker returns an error after the request, and as `completed` if it finishes anyway.
`JobRetry` re-drives a canceled or discarded job from history (with one more attempt
allowed if it had run out, and a fresh `seq`) or brings a waiting job forward; either
way it clears the job's TTL (§7.3). It refuses a running job and a job whose unique key
is held by a live job.

`JobDelete` removes a job that is not running and refuses a running one (cancel it,
then delete it once it has finished). A finalized job's history row is deleted. A
waiting job is canceled by the statement `JobCancel` runs, except that it keeps no
history row: its batch counts it as failed, its
dependents react as to a cancel, and `Await` on it returns `ErrNotFound` instead of
waiting for a job that no longer exists. There is no bulk delete: retention drops
history by partition (§7.10), and an admin tool deletes chosen jobs with `JobDelete`
over `Jobs`.

`JobCancelTx`, `JobRetryTx`, `JobRetryOrderingKeyTx` and `JobDeleteTx` run the same
statements in the caller's transaction, so the control call commits or rolls back with
the caller's own changes. Nothing reaches other processes before the commit: the
cancel request of a running job is a `pg_notify` in the transaction (with the lease
renewal as the fallback), and a retry wakes its queue with a `pg_notify` in the
transaction, instead of the client's own wake-up that `JobRetry` uses. The mark a
key's wake leaves on its running job is dated by `statement_timestamp()`, not
`now()`, so that a transaction older than the attempt still marks it. Until the
transaction ends it holds the rows it changed, so a running job that `JobCancelTx` or
`JobRetryOrderingKeyTx` reached cannot finalize. Its result alone waits: a finalize
statement that does not skip held rows gets 2 s, after which the batch is written again
skipping them (the statement `CompleteTx` results use, §7.5), and a held job's result is
tried again every 250 ms while the job is still running in its attempt. These calls
still belong late in the transaction, as `CompleteTx` does.

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
partitions ahead of time and dropping expired ones, building the ordering-key and
unique-key indexes of any history partition that lacks them (after the v13 and v14
upgrades), removing stale
`hopper_clients` rows, sweeping the batch progress that clients did not settle, and
promoting workflow jobs whose last dependencies finished in concurrent transactions
(§11).

The index build (`HistoryIndex`) runs in its own loop of the leader's term, once the
term's first history maintenance has created the partitions ahead and then every
maintenance interval, not in the leader loop. A `CREATE INDEX CONCURRENTLY` takes as
long as the partition is large, and waits for every transaction in the database with
an older snapshot, so in the leader loop it would stop the lease renewal, and
leadership, and what runs once per term such as `RunOnStart`, would move between
clients until it finished. Each partition is built under an advisory lock
(`HistoryIndexLockKey`), so overlapping leaders don't build on one table at once, which
deadlocks. The build cannot run in a transaction, so the lock is held by a read
committed transaction of its own that stays open, idle, on another connection; a
session lock would not survive a transaction pooler. A build also holds the lock keyed
to its outcome table: exclusively on the `DEFAULT` partition, in share mode elsewhere.
Partition creation under the table tries it in share mode without waiting, deferring
the partition (§7.10) while the `DEFAULT` partition is being built: attaching a
partition scans the `DEFAULT` partition under a lock that the build holds, while the
build waits for the attach's transaction. A build elsewhere does not conflict with the
attach. Retention tries the same lock, and keeps the expired partitions
(`HistoryMaintainResult.Kept`) while a build holds it: a drop takes `ACCESS EXCLUSIVE`
locks on the table and then on the partition and the `DEFAULT` partition, so it would
hold every query of the table up while it waited for a build on either, which waits for
the drop's snapshot in turn. For a partition that has every index, which no build runs
on, retention tries the lock in share mode, so that only the `DEFAULT` partition's
build keeps it; for one that lacks some, which may be the one being built, it tries it
exclusively. Retention holds the lock only while it drops a partition, and creation
only while it creates one. A build waits for it, under the doubling lock timeout of
deferred partition work; one that times out, behind a transaction left open with the
lock, is reported deferred (`HistoryIndexResult.Deferred`) rather than stall the pass,
and every other leader's, behind the schema's build lock. A drop the deadlock
detector picks over a build waiting for its snapshot is deferred, and so is a build it
picks over the drop, which has not committed when the build returns: the next pass
finds the partition gone. A build cut short by
the end of a term leaves an invalid index, which the next build drops and builds
again. A build of a partition dropped since it was listed, or during the build, is
skipped. Postgres marks an outcome table's index valid only in the attach that
completes it, so retention does not drop the last partition without an index before
it is built: before Postgres's May 2026 releases (18.4, 17.10, 16.14, 15.18, 14.23),
nothing short of a catalog update could mark the index valid after that. While the
`DEFAULT` partition, which retention never drops, still lacks the index, its attach
completes it, and retention drops the partitions past it as usual. Once the `DEFAULT`
partition has it, retention keeps every partition that lacks it, not only the last,
until its index is attached. Keeping needs no lock: a partition only ever goes from
lacking an index to having it attached, so a list read before the drops keeps at
worst a partition indexed since, which the next pass drops. A drop that
counts on the `DEFAULT` partition rechecks that it still lacks the index under the
table's build lock, which the `DEFAULT` partition's build holds through its attach, at
read committed whatever the session's default, so that it sees an attach that
committed before the lock was free. A pass builds the partitions newest first and the
`DEFAULT` partition last, so that while any other partition lacks the index the
`DEFAULT` partition does too, and partitions that expire before the pass reaches them
are dropped rather than built. The cost is building, once after an upgrade declared
an index, the partitions past retention that a pass left out (timed out or deferred)
while it indexed the `DEFAULT` partition. Known costs, kept for the simpler lock model: while any build of the table runs, retention keeps every expired partition that lacks an index, so a long pass after an upgrade can build a partition that expires during it before dropping it (a lock per partition would let it drop instead, at the cost of a second lock protocol); retention keeps such partitions even where the server can revalidate the index, so that the rule does not depend on the probe below; and while the `DEFAULT` partition is being built, each expired partition that lacks an index still tries the table's lock once per pass before it is kept. A build the deadlock detector cancels leaves its invalid index until the next pass drops and builds it again, whichever statement it deadlocked with. Once every index is valid, or no partition lacks an invalid one (an index
an earlier release left invalid, below), nothing is kept. The leader logs kept
partitions at Info once per maintenance interval: a kept partition only
waits for its build, and every way a build fails already warns, a timeout each time,
an error each pass, and a deferred attach from its fifth pass in a row. An index an
earlier release left invalid is revalidated by attaching again the index of its
furthest partition ahead, which no finalize writes to yet: releases from May 2026 on
count the attached indexes anew. The pass first asks the server whether its
attach counts anew, on tables of its own in the schema in a transaction it rolls back,
rather than by its version, which forks number their own way, once per server start
(`pg_postmaster_start_time()`): a running server keeps its code until it restarts,
which a minor update needs, and each question leaves dead catalog rows behind. Where
the attach does not count anew, the index is reported invalid without locking the
partition. A repair that fails, its question included, is reported as the pass's
error once the indexes after it are built, and the index as invalid, so that the
leader backs its retries off as it does the indexes the server cannot repair; a failed
check of whether the index is invalid at all is reported only as the error. The question's tables are named the same
in every session, so another leader's question, or one that stalled, holds it off:
it waits no longer than the attach's lock timeout below, and the repair is deferred. The attach waits for its locks under the same
doubling timeout as deferred partition work (`HistoryIndexParams.LockTimeout`), since
the partition's queries queue behind it; so does the attach that ends a build. One
that cannot get them is reported (`HistoryIndexResult.Deferred`, naming the
partition's index for a build's, whose partition's other indexes wait for that pass
rather than be built for their attaches to wait behind the same holder) and retried
on the next pass, which attaches a built
index without building it again. The leader warns once some index work, of any index,
has been deferred five passes in a row; a pass that fails before it reaches that work
neither ends the run nor extends it. A pass that finds another session holding the
schema's build lock, another leader's pass or one that stalled, ends there and
reports the index it would have built (`HistoryIndexResult.Busy`), which the leader
warns about from the fifth such pass in a row; it waited for no lock, so it does not
lengthen the next pass's lock timeout as a deferral does, and like a failed pass it
may have ended before the deferred work, so it does not end their run either. A repair whose partition retention dropped meanwhile is left to
the next pass. An index the server cannot repair, or that the attach left invalid, is
reported (`HistoryIndexResult.Invalid`), and the leader warns at doubling intervals,
up to 8 maintenance intervals, that queries needing it read every partition, and that
Postgres before its May 2026 releases cannot repair it. On those releases hopper only
reports it: building a replacement index and renaming it into place would double the
index's storage and lock every partition for the swap, and declaring it again would
leave every partition without it until the leader rebuilt them all, so updating the
Postgres minor release is the repair. The update arrives without a new term, and the
next try after it repairs the index. A partition's
or index's backoff ends once a pass that tried it finishes without reporting it again,
so that a later failure starts over at the shortest wait. A build that runs past the session's
`statement_timeout` is canceled by it: its
leftover is dropped, the pass goes on to the other partitions, and the leader retries
that partition at doubling intervals, up to 64 maintenance intervals, warning each
time that the timeout should be raised for hopper's role. The build cannot lift the
timeout itself, because behind a transaction pooler a `SET` would land on whichever
server connection it ran on, and stay there for other clients.

A client with `Config.NeverLead` never attempts the lease. It claims and works jobs
like any other, so an install can include processes that should do work but not run
the install, such as a developer's process against a shared environment.

### 7.9 Periodic jobs
`hopper.Every(d, …)` and `hopper.Cron(spec, …)` are both available from v0.1. Cron
supports standard five-field syntax, an optional seconds field, `@hourly`-style
descriptors, and an IANA time zone per job (a `CRON_TZ=`/`TZ=` prefix or
`PeriodicOpts.Location`; UTC by default). The parser is in the core, using only the
standard library. `Every` slots are aligned to the interval, so every leader computes
the same slots.

The leader runs a periodic loop for the duration of its term. It sleeps until the
soonest slot, timed on the database clock, reads that clock, and for each slot that
has come due upserts `hopper_periodic.last_slot` with
`ON CONFLICT DO UPDATE … WHERE last_slot < $slot` and inserts the job in the same
transaction only if that upsert returned a row. A slot therefore produces at most one
job, even if two leaders overlap or leadership changes mid-slot, and even after the
first job has finished and left the live table.
`RunOnStart` inserts a job when a leader starts scheduling. Missed slots are skipped by
default, except slots due since the last recorded one within the last lease TTL, which
covers a failover; `CatchUp: n` back-fills up to the n most recent missed slots
instead, and never fewer than that window holds. Either way at most the 10,000 most
recent missed slots are inserted, so a failover window of a dense schedule is cut to
its most recent 10,000. A schedule with no recorded slot starts from its next slot.
`RunOnStart`'s slot is the database time, so it is inserted after the back-filled slots, which the
upsert would otherwise reject as older than the last one.

`Config.Periodic` is a client's initial set of periodic jobs. `Client.PeriodicJobs()`
changes the set at runtime with `Add`, `AddMany`, `Remove` and `Clear`, and lists it with
`All`. A job is identified by its name, the key of `hopper_periodic`; adding a name that
is already registered fails, so a job is changed by removing it and adding it again. The
set is held in memory, per client, and is not persisted or shared: the leader schedules
only its own set, so **every client that can lead must register the job**, as with
`Config.Periodic`. A job added only on the leader stops when leadership moves, a job
added only on a follower starts when that client leads, and a job removed only on the
leader comes back with a leader that still has it. Overlapping leaders with different
sets still insert a slot at most once. A change wakes the leader's loop, which schedules
an added job as if its term had just started: `RunOnStart` inserts a job, `CatchUp`
back-fills, and otherwise slots since the last recorded one within the lease TTL are
inserted, so a job removed and added again within the lease TTL inserts the slots it
missed. Rows are kept, so a name whose recorded slot is older, however long ago it was
removed, inserts the slots of the last lease TTL, or back-fills up to `CatchUp`, as a
term start after an outage does. A `RunOnStart` added on every client inserts one job because only the leader
schedules; its slot is the leader's database time, so two overlapping leaders each
insert one, as at the start of a term. `Remove` applies only to the client it is called
on, so a job is removed on every client that can lead, or a failover to a client that
still has it brings it back. A removed job stops being inserted; a slot being inserted as
it is removed may still be. Its `hopper_periodic` row is kept, because another client may
still have the job and the row is what keeps a slot from being inserted twice, so the
table holds a row for every name ever scheduled. Starting added jobs reads the whole
table, once per loop pass that has jobs to start, as the start of a term does. The loop
retries a failed read of the clock or the last slots after a leader interval. It also
retries a failed insert after a leader interval, from the same slot, so a transient
error does not lose a slot; a job that fails to start is started again from the slots
then recorded, `RunOnStart` included. A job that keeps failing waits a leader interval
between tries, whatever else wakes the loop. A slot that falls more than a lease TTL
behind because the database failed, as while it cannot be reached, or because the loop
did not run, as while the process was suspended, is treated on recovery as missed slots
are at the start of a term: skipped beyond the lease TTL window, or back-filled up to
`CatchUp`, rather than inserted all at once. A slot behind only because a
pass was slow, such as one that started a job with many slots to back-fill, is still
inserted, and the jobs already scheduled insert their due slots before added jobs
start. `NewClient`, for
`Config.Periodic`, and `Add` build each job's insert as `Insert` would and fail if it
cannot be built, such as for invalid `InsertOpts`. `Add` also applies `StrictKinds`, as
an `Insert` at that moment would; `NewClient` leaves it to each insert, since workers may
be registered after it. A periodic job's `UniqueReplace` acts as `UniqueSkip`. A job's `UniqueOpts.ByPeriod` key is computed from its slot, so each
back-filled or retried slot keys its own period. A `RunOnStart` insert whose reply is
lost after it committed is inserted again when retried, since its slot is the database
time of the try. A client
that never leads (`NeverLead`, or one with no queues) accepts the jobs but never
schedules them.

### 7.10 Retention
Finalized jobs live in `hopper_job_history`, partitioned by outcome and then by time.
Retention is enforced by dropping whole partitions, so there is no `DELETE`, no dead
tuples and no vacuum work, but for the rows of the `DEFAULT` partitions, below, and the
tags rows of `completed` history's hours, each deleted with its hour. Their tags rows (schema v17, §7.2) live in `hopper_job_history_tags`,
partitioned by outcome and then by day for both outcomes, whose partitions the leader
creates (two days ahead) and drops beside history's, each in a transaction of its own.
Daily partitions keep a listing's walk, which reads every tags partition of its
outcome, to one per day of retention rather than one per hour. A tags partition is
dropped only once every history partition covering its day is gone, after them in the
same pass or in a later one, so that a job in history always keeps its tags rows. An
hour of `completed` history goes up to a day before its tags partition, so its drop
first deletes its jobs' tags rows, in the drop's transaction, by the tags index's key:
left in the day's partition, they were the first rows a listing in ID order walked,
and every page of them cost a walk and a lookup that found no job (20,000 of them
took 2,001 round trips, 2.6 s, for a page of 10). An hour whose tags partition is past
retention too, as a day's last hour's always is, and as every hour's of a day that a
pass catches up on is, deletes none: the same pass drops that partition after the
hour. When that partition has to wait instead, for another hour of its day that is
kept or deferred, for its own deferred drop, or while history's retention fails (as a
`DEFAULT` prune that outlasts `statement_timeout` does on every pass), the hour's rows
stay in it until it goes, and listings walk past them. After a leader outage, a pass
that catches up on whole days while that prune fails leaves the rows of every hour it
drops. The delete names its hour's partition, so its bounds are written into it too, and
it runs unprepared rather than leave a statement an hour in the connection's cache. A
deferred drop rolls the delete back
with it, and its retry, on the next leader pass, deletes again: an hour whose drop is
deferred pass after pass pays its delete each time. A delete the server cancels, as a `statement_timeout` shorter than a busy hour's
delete does, is rolled back to a savepoint and the hour dropped without it, as a previous
release's leader drops one: its rows stay in the day's partition until it goes, rather
than the hour, and every later one, staying for each pass to try again. A delete that
finds the hour gone, which an overlapping leader's pass dropped while it waited for the
hour, is rolled back the same way, and the hour is reported dropped, as one gone before
the drop's `DROP TABLE IF EXISTS` is, rather than fail the pass. A `transaction_timeout`
(Postgres 17 on) shorter than a busy hour's delete instead ends the drop's session,
which no savepoint outlives: the pass fails, and the hour and every later one stay,
for each pass to fail the same way, so a role that sets one sets it past that delete. A `failed` partition's period is its tags partition's, which goes in the same
pass. A period that never had a history
partition has its jobs in history's `DEFAULT` partition, so tags partitions are dropped
only in a pass whose history outcome group has pruned its `DEFAULT` partition; while
that prune fails, they wait. Neither creation nor a drop takes an exclusive lock on
both tables: a finalize locks history before its tags and a listing by tag the tags before
history, so a statement that held one parent while it waited for the other could
deadlock with either. The delete of an hour's tags rows takes on the tags tables only
the `ROW EXCLUSIVE` lock that finalizes take, which neither a finalize nor a listing
waits for, and the row locks of the rows it deletes. A retry or a delete of one of the
hour's jobs, past retention, that comes while the drop runs does wait for those, while
the drop waits for that call's lock on history: the drop's lock timeout ends the drop,
which is deferred, or the deadlock detector ends either of them, the call failing with
`deadlock_detected` (40P01). A history partition that has no tags partition beside it, as one
a previous release's leader creates, sends its jobs' tags rows to the tags table's
`DEFAULT` partition, whose prune keeps the rows of jobs still in history. v17 gives
each day that history's partitions for the current period and those ahead cover its
tags partition, so that a job's tags rows are in their day's partition from the upgrade
on, and today and tomorrow theirs for both outcomes whatever history has, as on a new
installation, whose history has no time partitions yet: the leader creates only days
ahead, and a first day without a tags partition left a day of tags rows in the
`DEFAULT` partition, which the attach of each later day scans under an exclusive lock
that holds listings by tag off. It counts the days in UTC, as the leader does, whatever the session's time zone: a
day added in a zone whose clocks change that day is 23 or 25 hours, and the partition
would leave a gap before the next day's or overlap it, failing the upgrade. A past
period's partition
gets none: jobs are finalized into history at `now()`, so only a direct SQL write
could give one tags rows, which the `DEFAULT` partition keeps, and a long retention
would otherwise create a table and index per period in the migration's transaction. The leader creates partitions several intervals ahead
(three hours for `completed`, two days for `failed`), on election and every five
minutes. A partition is created standalone and then attached, which takes only a
`SHARE UPDATE EXCLUSIVE` lock on the parent. Only future periods are created and no
row is ever moved: rows finalized before their period's partition existed (the first
period after installation, or after a long leader outage) land in a `DEFAULT`
partition and are deleted individually once past retention. Attaching scans the
`DEFAULT` partition under an `ACCESS EXCLUSIVE` lock on it, and dropping a partition
takes a brief `ACCESS EXCLUSIVE` lock on the parent, once per period. A transaction
that wrote history and is still open (a worker's `CompleteTx`) holds either lock off,
and while the DDL waits, every finalize that writes to the same table queues behind
it. Both therefore run under a `lock_timeout`: one that cannot get its lock is
reported as deferred, and so are the rest of its kind in the same group, untried,
since they wait for the same lock. Deferred work is retried on the next leader pass
rather than the next maintenance interval, with the timeout doubling on each
consecutive deferral from 200 ms to at most 2 s, so that transactions that keep
overlapping cannot hold retention, or the partitions ahead, off for good. Maintenance
therefore holds up a group's finalizes for at most that timeout per kind of DDL each
pass. A creation's wait for the history maintenance lock, which an overlapping leader's
creation or index attach holds, runs under the same timeout, so a leader that
stalls holding it defers the partitions ahead rather than stall this leader's loop and
its lease renewal. A session `statement_timeout` that runs out during one
of these statements, which it can whatever its length since `lock_timeout` bounds each of
a statement's lock waits rather than their sum, defers the work the same way rather than
fail the pass. A creation takes its attach's locks first, in statements of their own,
so a `statement_timeout` that ends the attach itself, scanning a DEFAULT partition grown
large, fails the pass, an error each maintenance interval, rather than be retried as a
wait on every leader pass. A group whose maintenance fails, retention's read of its
partitions' indexes included, leaves the other groups to theirs, and within a group a
failed step leaves only the rest of that step to the next pass: a failed creation
leaves the partitions after it, not the group's retention, a failed read of the
indexes or a failed drop leaves the partitions past retention, not its `DEFAULT`
prune, and a failed listing of the group's partitions leaves everything but that
prune, so that a failure that lasts, such as an attach whose scan of a `DEFAULT`
partition grown large outlasts `statement_timeout`, does not also stop the prune that
lets it recover. The errors are reported together, and the leader logs what the
pass did nonetheless. A failed pass is retried at the next maintenance interval, its
deferred work with it, but work it deferred lengthens the next try's lock timeout as
on a pass without a failure, so that a failure that lasts does not hold the other
groups' deferred work at the shortest timeout for good. Deferrals are logged at debug level, and as a warning from the fifth pass in a
row. Retention periods are set in `Config` and enforced by the leader, so the leader's
settings apply cluster-wide. The groups maintained this way are history's outcome
tables, the stream log and history's tags tables.

The defaults follow common practice: job queues keep successful jobs briefly and
failures longer, while brokers such as RabbitMQ and SQS delete a message once it is
acknowledged.

| Outcome | Job queues | Subscription queues |
| --- | --- | --- |
| `completed` | archived for 24h | deleted on ack (`DeleteCompleted: true`) |
| `canceled`, `discarded` | archived for 7d (the dead-letter queue) | archived for 7d (the dead-letter queue) |

`DeleteCompleted` is configurable per queue; the retention periods
(`CompletedRetention`, `FailedRetention`, and `StreamRetention` for the stream log,
7 days by default) are per cluster, because partitions are cut by time, not by queue. Setting `DeleteCompleted: true` on a job queue gives maximum
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
`Stop(ctx)` and `Run` (when its context is canceled) shut down in three steps:
1. Stop claiming and give up leadership.
2. Wait for running jobs to finish, then flush the finalizer.
3. If `ctx` expires first, cancel all job contexts, wait a short grace period, flush
   whatever finished, release the lease and return `ctx.Err()`. Jobs that are still
   unfinished are rescued by another client (at-least-once).

A job that returns because its context was canceled by the stop is finalized as
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

**Results (2026-09-28, the v0.1.0 run).** Postgres 17.11 in a container capped to 8 vCPU
and 32 GB on a cloud VM whose data disk is a network-attached persistent disk, not local
NVMe (`shared_buffers` 8 GB, `synchronous_commit = on`, default autovacuum settings),
clients on an 8-vCPU host 1.8 ms away, `hopperbench` at `-jobs 300000` with four clients:

| Metric | Result |
| --- | --- |
| End-to-end throughput, no-op jobs, 4 clients | 48,700 jobs/s at 100 workers per client; **62,000 jobs/s** at 200 |
| Mixed priorities / scheduled / retry-heavy (100 workers) | 50,900 / 48,800 / 25,000 jobs/s |
| Bulk insert (COPY path) | **496,000 jobs/s** |
| Batched insert (unnest path) | **173,000 jobs/s** |
| Pickup latency, idle queue | **p50 3.3 ms, p99 3.9 ms** |
| Pickup latency at 80% of peak (49,000 jobs/s, 200 workers, 60 s) | **p50 10 ms, p99 53 ms** |
| Publish fan-out cost | one statement (§10) |
| Recovery after a process crash | lease TTL 15 s plus one leader interval, ≤ 20 s (§7.6; `drivertest` Rescue) |
| Throughput with a 10M-row history | 47,800 jobs/s, **−1.9%** |

A 3,000,000-job backlog drains at 30,600 jobs/s: the leader's vacuum passes over a
large live table (§7.3) cost throughput there, by design, to keep the claim index
short. The 100-worker throughput figure is bound by the client's concurrency, not the
database, so the default per-client `MaxWorkers` for a queue meant to saturate a
database of this size is 200.

**Soak (2026-09-30, v0.1.1, 3 hours).** Run on a different machine, a Kubernetes node
with a 96-core AMD EPYC 9655P and local NVMe in RAID 1 (md, XFS): Postgres 17.11 with
the settings above, pinned to 8 physical cores (16 threads) and 32 GB, the bench client
on the same node, four clients of 200 workers and the paced inserter at 43,000 jobs/s
(70% of peak) for 10,800 s: 381,900,000 jobs, no errors, no lease loss, no rescue. The
run had two regimes. For the first 30 minutes the inserter held 42,700 jobs/s with p50
5 ms and a **p99 of 1.1–2.4 s** in every window: on this node the four clients'
finalizers convoy on the history partition's extension lock and the WAL-write lock
every few seconds, a 500-row flush then exceeds its 47 ms budget, the finalizer's
two-batch buffer fills, all 200 workers block on submit and the client stops claiming
until the convoy breaks. Splitting the samples shows the insert call at p99 10 ms and
the commit-to-`Work` wait at p99 1.4–1.7 s; a shorter poll interval changes nothing,
removing the leader's vacuum collapses the run within two minutes, and a 20-batch
buffer roughly halves the tail (issue #20; the fix is a finalizer that keeps claiming
while results queue, or several flushes in flight). From minute 31 the inserter fell to
32,000–36,000 jobs/s and stayed there: its in-flight transactions waited 14–50 ms on
the WAL-write lock, and the harness's four inserter goroutines cannot exceed that rate
at such a commit latency, although WAL writes averaged 0.2 ms, syncs 0.04 ms and the
WAL writer was 17% busy. With the queue shallow, the remaining 150 minutes ran at **p50
3.1 ms and a median window p99 of 150–180 ms**, hour three within 5% of hour two, so
no drift. Table sizes stayed bounded: the live heap was 116–133 MB and the claim index
23–26 MB for the first 90 minutes, dead tuples between the leader's passes (one every
4.7 s) were 200,000 at the median and 453,000 at most, autovacuum's 180 passes on the
live table took at most 0.06 s each, the 108 checkpoints wrote a median 114,000
buffers over 90 s, and the 79 GB, 381,000,000-row history had no measurable effect on
the live table. One window at 02:25 UTC had a 17 s stall caused by the measurement
itself, a 15 s fdatasync latency probe on the data volume at 9,000 syncs/s; the
198,000-row backlog it left tripled the live heap to 313 MB and the claim index to
59 MB, which by design (no truncation, §7.3) stayed that size for the remaining
90 minutes without any effect on throughput or latency. A first attempt used an
`emptyDir` with a `sizeLimit`, which the kubelet enforces with an XFS project quota;
quota accounting made 8 KB appends 27 times slower and serialized the finalizers on
the partition's extension lock, so the data volume has no size limit.

### 8.3 Benchmark harness
- `hopperbench` is a command in the repo that drives the scenarios above and prints
  one JSON line per scenario. It has the no-op baseline, mixed-priority, scheduled-heavy,
  tagged and retry-heavy workloads, a workflow workload (ten-step workflows, one step,
  then eight after it, then one after those, each step recording a small output, which
  measures the finalize tail, dependency promotion and output), the two insert paths,
  pickup latency on an idle queue and under a paced load (`loaded`, at `-rate` jobs/s),
  and `-history N` to preload history rows. It warms up before measuring, because the
  first leader election creates history partitions, which briefly blocks finalizes.
  Fan-out arrives with messaging (M6).
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
  - `JobRetryOrderingKey(queue, key)` wakes a key, for a producer whose newer job should
    not wait behind an older one's delay, without canceling the job in it. It makes the
    key's waiting jobs (scheduled or retryable, TTL not passed) runnable now, in batches
    of 1000 locked in `seq` order through the same index. A job that was already due
    keeps its `scheduled_at`, so it keeps its claim order and aging. It does not
    interrupt a running job, but marks it (`wake_requested_at`, schema v10): if that
    attempt ends in a snooze or a retry, the finalize makes the job runnable at once
    and clears the mark, and a mark older than the attempt is ignored. The mark is a
    statement of its own, before the wake, so the wake's snapshot is taken after it: a
    job that finishes its attempt before the mark reaches it is waiting by then and is
    woken, and one that finishes after sees the mark. The claim, the aging pass
    and the expiry sweep skip locked rows, so the wake's fixed lock order cannot deadlock
    with them; a replacing insert of several jobs of one key still can, and Postgres
    then aborts one of the two statements.
  - Ordered deliveries get a shorter default retry budget (10 attempts, with backoff
    capped at 5 minutes), so a poison message is dead-lettered within about an hour
    instead of blocking its key for days. A `RetryPolicy` on the queue or the client
    applies to them too, under the same cap; only `OnError`'s `RetryAt` can wait
    longer. The budget is configurable per subscription
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
- **Streams (M8).** `hopper_stream_events` is an append-only, time-partitioned log:
  `Streams().Append`/`AppendTx` write one event (topic, optional key, payload, headers,
  message ID) and consumers read it. An event is identified by `(xid, seq)`, the
  transaction that wrote it and the event within it. A **consumer** (`hopper.Consume`)
  is a named position on the log plus a topic pattern; the leader turns the events
  after its position into delivery jobs of kind `stream:<name>` (with the event's key
  as ordering key), so a consumer inherits everything a subscription has: typed
  handlers, retries, dead-lettering, competing workers. Unlike a subscription, which is
  fanned out to at publish time, a consumer can start at the earliest retained event
  (`StreamStartEarliest`) and be moved (`Seek` to a position, a time, the start or
  now), so late subscribers and replays see past events.
  - **Reads never skip a late commit.** A consumer's row records the `pg_snapshot` it
    has read to. A pump takes a fresh snapshot and delivers the events of the
    transactions visible in the new one but not in the old (`pg_visible_in_snapshot`),
    in `(xid, seq)` order, then stores the new snapshot; a pump that stops part way
    through keeps both snapshots and the last event delivered, and continues the same
    delta next time. An event whose transaction commits late is delivered when it
    commits, whatever its xid, and a long-running transaction holds nothing back. Events
    of one transaction are delivered together; across transactions the order is commit
    order per delta, which is the only order two overlapping transactions have.
  - Pool appends notify `hopper_stream` through the coalescer and poke the local
    leader; transactional ones notify from the transaction. The leader also pumps every
    `PollInterval`, for clients without a listener. Two leaders pumping the same consumer
    serialize on its row.
  - `Streams().Read` pages the committed log by position, for browsing and the UI.
  - Retention drops daily partitions after `StreamRetention` (§7.10).

## 11. Batches and workflows

- **Batches (M7).** `hopper_batches (id, pending, failed, total, on_success, on_failure,
  on_complete, metadata, completed_at)`. Jobs carry `batch_id`. `client.NewBatch(opts)`,
  `Add`, `Insert`/`InsertTx` and `BatchGet` are the API.
  - **Progress is appended.** Every statement that finalizes jobs (the batched
    finalize, canceling a waiting job, expiring) inserts one `hopper_batch_progress`
    row per batch among them: how many of its jobs it finalized, and how many of those
    were canceled or discarded (failures). It takes no lock another statement waits
    for. Until schema v10 it counted the batch down on the batch row instead, so every
    finalize of a batch's jobs queued on that row, and a transaction that finalized a
    job and stayed open, as `CompleteTx` does, held up all of its siblings' finalizes
    and the finalizer flushes they were in.
  - **Completion.** A batch's remaining count is `pending` less the progress rows'
    `done`. The statement that finds nothing remaining after its own jobs completes
    the batch instead of appending: it sets `completed_at` and the total `failed`,
    zeroes `pending` (which the previous release reads as is), deletes the batch's
    progress rows, and inserts the callbacks in the same
    statement: `on_success` when nothing failed, `on_failure` otherwise, `on_complete`
    either way, each with `batch_id` and `batch_failed` in its metadata, and notifies
    their queues. It decides from its snapshot alone, and the update re-checks only
    `completed_at IS NULL`, so callbacks run once and a fold committed meanwhile
    (below) cannot be counted twice. Failures are counted here rather than read from
    history, which retention prunes.
  - **Settling, and concurrent last jobs.** Two statements that finalize a batch's last
    jobs at the same time each miss the other's uncommitted row, so neither completes
    the batch. `BatchSettle` locks batch rows that have progress rows, folds the rows
    into `pending` and `failed`, and completes those with nothing remaining, with
    their callbacks. The finalizer runs it on the batches of a flush once the flush
    has committed, waiting for a batch row another settle holds, so of the two the
    one settling second sees the other's progress and completes the batch. The wait
    is bounded (`lock_timeout` 500 ms, below `deadlock_timeout`, so a settle that
    would deadlock with a finalize completing several batches gives up first); a
    batch it cannot get is left to the leader, which on every pass sweeps batches
    with progress rows in ID order (`SKIP LOCKED`, resuming where a pass stopped)
    and deletes progress rows whose batch no longer exists. The sweep covers a
    client that stopped between commit and settle, and results the leader itself
    applies (rescue). Settling after every flush keeps the progress table to the
    flushes in flight. An uncommitted progress row is invisible to a settle and
    stays until it commits.
  - **Jobs outside batches.** A flush whose jobs all have no batch (and therefore no
    workflow dependents) sets `JobFinalizeParams.NoBatches`, and the engine runs a
    finalize without the batch and dependency steps, which roughly halves the cost of
    a small flush.
  - **Reading progress.** `BatchGet` and `WorkflowGet` report `pending` and `failed`
    with the progress rows folded in.
  - **Rolling deploys.** A client of the previous release counts down `pending`, which
    the remaining count includes, and completes the batch when `pending` reaches zero,
    which it does only if it finalized every job. Otherwise this release completes it;
    the `completed_at` guard keeps the two from both running the callbacks. A client
    of this release settles its batches after each flush, so the previous release's
    countdown sees its progress folded; only a batch whose settle was left to the
    sweep waits, while a client of the previous release holds the leader lease, until
    a client of this release is elected. Going back below v11 locks the progress table, folds
    the rows into `pending` and completes, with callbacks, the batches left with
    nothing remaining.
  - **Callbacks are validated at insert.** A callback that the completing statement
    could not insert (a priority outside 1..4, `max_attempts` beyond `smallint`) is
    rejected by `BatchInsert`, because the completing statement would fail, and in
    `BatchSettle` that would fail every other batch's fold with it. Rows written
    before this check have the values clamped into range when the callback is
    inserted.
- **Workflows (M8).** A workflow is a batch whose jobs depend on each other, so it has
  a name, the batch's progress and callbacks, and its graph (`hopper_batches.edges`).
  `NewWorkflow` collects steps; a step can only depend on steps added before it, so the
  graph is a DAG by construction. `InsertWorkflow` generates the IDs up front and
  inserts the batch, the jobs (`pending` when they have dependencies, otherwise
  available) and the edges in one transaction. `hopper_job_deps` is the working set of
  unsatisfied edges, deleted as dependents finalize, and the statement that finalizes
  a job is the one that acts on its dependents: it promotes those whose dependencies
  have all left the live table (including the ones finalized in the same statement)
  to `available` and notifies their queues, and it cancels, transitively, the
  dependents of a canceled or discarded job whose edge says `cancel` (the default;
  `ignore` lets the dependent run once its dependencies have finished, whatever their
  outcome). Canceled dependents go to history with a "dependency failed" error and
  count as batch failures. Every finalizing statement (the batched finalize,
  canceling a waiting job, expiring) ends with the same tail, so batches and
  dependents behave the same however a job finishes.
  - **Concurrent dependencies.** Two dependencies of one job finalized by concurrent
    transactions (two clients, or a `CompleteTx` held open while a sibling
    finalizes) each see the other still live, so neither statement promotes the
    dependent. `WorkflowPromoteReady` promotes pending jobs none of whose dependencies
    is still live, skipping rows a finalize holds. After each flush commits, the
    finalizer runs it on the dependents of the jobs the flush finished (and the client
    does for a job it saw `CompleteTx` commit), so of the two, the one that follows up
    second promotes the dependent at once. The leader sweeps up what clients left (a
    client that stopped between commit and follow-up): it walks `hopper_job_deps` a
    chunk at a time in ID order, for at most one leader interval per pass, resuming
    where it stopped, so it costs nothing without unfinished workflows and a bounded
    amount per pass however many pending steps they have.
  Workflows are inspectable with
  `WorkflowGet`, `hopper workflows get` and, as a DAG, in `hopperui`.

## 12. Migrations

- SQL files are embedded in the `hoppermigrate` package, versioned, and forward-only by
  default. Down migrations exist for development.
- `hoppermigrate.Up(ctx, driver)` applies them under an advisory lock taken on a
  dedicated connection, not a pooled one. This avoids a pool-starvation deadlock:
  processes waiting for the lock must not hold the connections the migration needs.
- `hopper migrate` in the CLI runs the same migrations. The raw SQL files are also
  published for teams that use goose, atlas or Flyway. `hopper_schema` records the
  applied version either way.
- Up creates the configured namespace if absent. Each migration sets a
  transaction-local `search_path` for the embedded unqualified DDL, and schema v8
  binds SQL producer functions to that namespace. Down to zero leaves the
  namespace intact. Raw SQL users must set the target path themselves.
- Every schema change ships with an upgrade test that migrates to the latest version
  from the previous one, and from every schema a release shipped, while jobs wait in every state,
  inserts keep arriving, claims keep polling, finalizes keep moving jobs into history
  and a transaction that wrote both stays open, then works them all with a client
  (`TestUpgradeUnderTraffic`). Migration versions are numbers only, so the latest is
  also pinned in `TestMigrationsAreWellFormed`: two branches that each add the next
  version conflict there instead of both merging. Migrations that touch `hopper_jobs`
  or history must not take locks that block claims or finalizes for longer than one
  statement (`CREATE INDEX CONCURRENTLY`, `NOT VALID`
  constraints, and so on). A migration runs in a transaction, where `CONCURRENTLY` is
  not allowed and a partitioned table cannot take it at all, so an index on history
  is declared `ON ONLY` and built per partition by the leader (schema v13 and v14).
  A migration first takes the lock the leader builds those indexes under
  (`HistoryIndexLockKey`), cancelling a build under way to get it, which the leader
  redoes on a later pass: a build and a migration would otherwise wait on each other
  unseen, the migration for the table lock the build holds and the build, in its last
  phase, for the migration's statement to end. Every migration cancels, whether or not
  it touches history: an upgrade's migrations run back to back, so a build is
  restarted at most once an upgrade, which costs less than a rule for which scripts may
  leave builds running. A build the migration's role may not cancel (pg_cancel_backend
  fails for it) fails the migration with an error saying which role may, rather than
  being waited for: the build, in its last phase, waits for every older snapshot,
  among them those of migrations queued for the migration lock this one holds, so the
  wait would never end. It looks for builds every 50 ms until it has the lock, each time
  after `pg_stat_clear_snapshot()`: the server shows a transaction the activity it first
  read for the rest of it, so a build the leader started after the first look (once it
  had dropped an invalid index, say) would never be seen. While it cancels,
  the migration holds `HistoryIndexCancelKey`, which the leader tries after a build
  fails with SQLSTATE 57014: held, the failure was the migration's cancel; free, it was
  the session's `statement_timeout` or an operator's cancel, which are not told apart.
  The error's message would tell them apart only in the server's language, and the
  `statement_timeout` a query on a pooled connection reads need not be the one the
  build ran under. After the migration's cancel the pass ends, so that the leader
  starts nothing while the migration runs, and the next pass drops the invalid index
  the build left; after a timeout or an operator's cancel the leader drops it at once,
  reports the partition, which it backs off and warns about, and goes on to the next.
  The migration's transaction is read committed, and keeps no snapshot between its statements, so the build never waits
  for it while it waits for the lock. A change that needs `ACCESS EXCLUSIVE`, such as adding a
  column, tries under a 50 ms `lock_timeout` in a loop (schema v9, v10, v15 to v18, and v13 and v14
  for the `SHARE` lock that declaring a history index takes, which finalizes wait for), because a lock
  request queued behind an open transaction on the table would hold up every claim
  behind it; claims wait at most one try at a time. A try that changes both `hopper_jobs`
  and history (v17, v18) locks them first, `hopper_jobs` before history, the order a finalize
  takes them in: a finalize that has `hopper_jobs` and waits for history would otherwise
  hold up the try until its timeout, and under steady finalizes almost every try would
  fail that way. From v17 the short timeout is set inside the loop and restored after
  it, so that the migration's other statements wait for a lock as any statement does
  rather than fail on a brief conflict; v17's listing of history's time partitions
  also reads their bounds from the catalog alone, without opening them, as a scan of
  the catalog may reach any schema's partitions. Between tries it sleeps a jittered,
  growing interval capped at a second, raises a `NOTICE` every 10 seconds while it
  waits, and gives up with an error after 10 minutes, so a transaction held open for
  longer fails the migration (end it and run `Up` again) rather than stalling it
  forever.
- Each release's statements target its own schema version: the code is deployed after
  `Up` (the client never runs against an older schema), and the previous release keeps
  working on the new schema while it is rolled out. `Client.Start` enforces the first
  half: it reads the newest version in `hopper_schema` (without the migration lock, so
  it does not wait for a migration in progress) and fails with `ErrSchemaOutdated`,
  naming the missing migrations, when it is older than the release's latest. When the latest version adds
  something this release's statements use, the upgrade test produces the jobs and
  traffic before and during the migration through the SQL producer contract and
  version-independent statements, and starts this release's client once it is done.
  Schema v12, which renames the `cancelled` state to `canceled`, is the one exception:
  a release before it fails to cancel jobs on the new schema, so all clients move to it
  together.

## 13. Observability and operations

- **Logging:** `log/slog` throughout, defaulting to `slog.Default()`. A worker's
  `hopper.Logger(ctx)` lines are also kept with its job, per attempt (§7.5).
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
- **CLI (`cmd/hopper`, core module):** `migrate up|down|version`, `jobs list|get|retry|cancel|delete`,
  `queues list|pause|resume|limit`, `clients list`, `workflows get`, `subscriptions list`,
  `streams consumers|seek` and `stats`, all with `-json`. Benchmarks are the separate
  `hopperbench` command.
- **Web UI (`hopperui` module):** `hopperui.New(client, cfg)` is an embeddable
  `http.Handler`, mounted under a prefix with `http.StripPrefix`, for browsing queues
  (depths, limits, paused state), jobs and history (filtered by queue, state, kind and
  tag, paged by ID), a job's tags, args, metadata, output and errors, workflows as a DAG (SVG, laid
  out by dependency depth), subscriptions, stream consumers and clients. Retry, cancel,
  delete (behind a confirmation), pause, resume and seek are offered only when the application supplies
  `Config.Authorize`, which is asked before every action; without it the UI is
  read-only. Actions are POSTs, and a browser's cross-site request to one is refused
  (`http.CrossOriginProtection`). It is server-rendered HTML with no scripts and no external assets, so it
  works behind any proxy and needs no build step. The client it is given need not be
  started.

## 14. Testing strategy

| Layer | What |
| --- | --- |
| Unit | Backoff, cron parsing and time zones, pattern matching, unique-key construction, the state transition table, option validation. Timers are tested with `testing/synctest`. |
| Integration | Against real Postgres 14–18 in CI (a service container per version), always with `-race` and `GODEBUG=fips140=only`. Each test gets its own schema, so tests run in parallel. |
| Driver conformance | `drivertest.Run(t, fixture)` runs the behavioral suite (inserting, unique keys, claiming, every finalize transition and fence, leases, leader, rescue, cancel, retry, TTL, listing, queues, periodic slots, stats, notifications, history maintenance) against every driver. A `Fixture` supplies isolated databases and the few engine-specific hooks (expire a lease, backdate an attempt, count rows). Engine internals such as partition management stay in the driver's own tests. |
| Concurrency | N clients by M jobs: every job is finalized exactly once when nothing crashes. A finalize held open in a caller's transaction (`CompleteTx`) against a competing fenced finalize: exactly one applies, whichever way the transaction ends. Uniqueness under concurrent inserts. Periodic slots unique across two leaders. An insert counting finished duplicates against its duplicate's finalize held open: the insert reports the finished job and leaves no live one. A progress write held open in a caller's transaction against the attempt's competing finalize: a write that commits first is kept, one that comes second is fenced. A stuck attempt rescued and claimed again by the same client: its late result does not finalize the new claim, and a cancel still reaches the new claim. Global and rate limits never exceeded. Ordering keys never run two at once. |
| Chaos | `pg_terminate_backend` on a client mid-job, followed by rescue and re-run. A client that crashes after its workers committed `CompleteTx` transactions but before its finalizer flushed: none of those jobs runs again. A frozen client that loses its lease and must fence itself. Leader killed, new leader within the TTL. Stop with an expired context. Listener dropped, polling continues and the listener reconnects. |
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
  the API, with changes noted in the CHANGELOG. Releases are cut by release-please from
  the Conventional Commit titles of merged PRs, one version across the three modules,
  with goreleaser attaching the CLI binaries ([docs/releasing.md](releasing.md)). The SQL insert contract (§10) is
  versioned separately and changes only with a deprecation window.

## 16. Milestones

The estimates assume one engineer. Each milestone is one or more PRs.

| # | Milestone | Scope | Est. |
| --- | --- | --- | --- |
| M0 | Scaffold | go.mod, CI (lint, Postgres test matrix, `fips140=only`), PR-title check, Dependabot, CONTRIBUTING. **Done.** | 0.5d |
| M1 | Core engine | `Driver` interface and `hopperpgx`, schema v1, `hoppermigrate`, Insert/InsertTx/InsertMany (unnest and COPY), typed workers and `WorkFunc`, batched claim, batched finalize with history move, retries and backoff, `Snooze`/`Cancel` from workers, client lease registration and renewal, `Run`/`Stop`, a first `hopperbench`. **Done.** | 5d |
| M2 | Reliability | Client leases, rescuer and fencing, leader lease, partitioned retention, LISTEN/NOTIFY with coalescing and adaptive polling, unique jobs (skip and replace), `RescueStuckAfter`. **Done.** | 4d |
| M3 | Control | Cron and `Every` with time zones, Snooze, Cancel (in-flight), JobRetry, TTL, pause, runtime queues, timeouts, middleware, `SetOutput`/`Await`, `Jobs` iterator, events, `Stats`. **Done.** | 4d |
| M4 | Performance and release | `hopperbench` scenarios and `-compare`, CI perf gate and nightly soak, `drivertest`, `hoppertest`, CLI, `hopperotel`, docs and examples, CHANGELOG. **Done**, including the §8.2 run on the reference hardware. | 5d |
| M5 | First adoption | Move an internal service's `internal/jobs` package to hopper; drain and drop its old queue tables. **Done** on v0.1.0. | 1d |
| M6 | Messaging | Subscriptions, AMQP topic patterns, typed `Message[T]`, PublishTx fan-out, dedup, ordering keys, request/reply, SQL publish contract, `ReplayDiscarded`, the upgrade test. **Done.** | 5d |
| M7 | Flow control and batches | Global limits, rate limits, partitioned limits, priority aging, batches with callbacks, `hoppersql` driver. **Done.** | 5d |
| M8 | Workflows, streams, UI | Job dependencies and DAG workflows, streams with consumers, `hopperui`. **Done**, one PR each. | 2–3w |
| M9 | More engines (later) | `hoppersqlite`, then `hoppermongo`, each in its own module and passing `drivertest`. Not scheduled yet. | per engine |

M0–M4 take roughly four weeks to a production-ready job queue that meets its performance
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
