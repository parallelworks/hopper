# Getting started

hopper is a library: it runs inside your Go application against the Postgres
database you already have. This guide takes you from an empty module to a
running worker. [examples/basic](../examples/basic/main.go) is the same steps as
one program.

## Install

```sh
go get github.com/parallelworks/hopper
```

hopper needs Go (the version in `go.mod`) and PostgreSQL 14 or later. The core
module depends only on the standard library and [pgx](https://github.com/jackc/pgx).

## Install the schema

hopper's tables are prefixed `hopper_` and live in the `hopper` PostgreSQL
schema by default. Migrations create the schema if it does not exist.
Install them once per schema, from any replica;
migrations take a cross-process lock, so concurrent starts are fine.
When upgrading hopper, run the new version's migrations before its clients
start; clients of the previous version keep working on the new schema while
they are replaced. A client's `Start` checks the schema and fails with
`hopper.ErrSchemaOutdated`, naming the migrations to apply, if they have not
run.

A migration that adds or drops a column on `hopper_jobs` (such as v9) needs a
brief exclusive lock on the table. It takes the lock only when no transaction
holds one, trying every so often, so claims are never held up for more than
50 ms; it reports that it is still waiting with a `NOTICE` every 10 seconds.
If a transaction keeps the table locked for 10 minutes, the migration fails
with an error that says so: find the long transaction in
`pg_stat_activity`, end it, and run the migrations again.

```go
pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
driver := hopperpgx.New(pool)
if _, err := hoppermigrate.Up(ctx, driver, nil); err != nil {
    return err
}
```

Or from the shell:

```sh
go run github.com/parallelworks/hopper/cmd/hopper -database-url "$DATABASE_URL" migrate up
```

An application that already holds a `*sql.DB` (through pgx's `stdlib` adapter
or lib/pq) can use `hoppersql.New(db)` in place of `hopperpgx.New(pool)`
everywhere below; its transactions are then `*sql.Tx`. It polls instead of
listening, so pickup takes up to `PollInterval` instead of a millisecond.

To choose another schema, configure the driver and use it for both migrations
and clients:

```go
driver := hopperpgx.NewWithConfig(pool, &hopperpgx.Config{Schema: "background_jobs"})
// For database/sql: hoppersql.NewWithConfig(db, &hoppersql.Config{Schema: "background_jobs"})
```

The CLI accepts `-schema background_jobs` before the command. Driver queries,
COPY, and caller-owned transactions qualify Hopper objects explicitly; the
application's `search_path` is unchanged, so the pool can be shared.

**Existing installations:** set `Schema` to the schema where your Hopper tables
already live (for example `public`) before upgrading. Changing this setting
selects another installation; it does not move existing jobs or tables.

Migration credentials need permission to create the schema, or you can create
it beforehand. Migrating down to zero removes Hopper objects and leaves the
namespace itself intact.

The SQL files are in `hoppermigrate/migrations` for teams that run migrations
with their own tool. Set a transaction-local `search_path` to the target schema
when running the raw SQL. `hopper_schema` records the applied versions either way.

### SQLite

An application that runs on one machine, such as a command-line tool or a
single-node service, can keep hopper in a SQLite file instead, with no server
to run. The driver is pure Go (`modernc.org/sqlite`), so the program still
builds with `CGO_ENABLED=0`, and it is a module of its own,
`github.com/parallelworks/hopper/driver/hoppersqlite`:

```go
driver, err := hoppersqlite.Open(filepath.Join(stateDir, "hopper.db"), nil)
if err != nil {
    return err // hoppersqlite.ErrNetworkFilesystem for a file on NFS and the like
}
defer driver.Close()
if _, err := hoppermigrate.Up(ctx, driver, nil); err != nil {
    return err
}
client, err := hopper.NewClient(driver, cfg)
```

Its transactions are `*sql.Tx`; begin them on `driver.DB()`, whose
connections take the database's write lock when a transaction runs its first
statement. SQLite runs one writing transaction at a time, so keep them short:
one held open holds up every client sharing the file, lease renewals
included, and one held longer than the lease costs them their running jobs.
Write through the transaction (`InsertTx`, `CompleteTx`) rather than the pool
while one is open. In a worker, begin it with the job's context: a write on
the pool then fails at once with `hoppersqlite.ErrLockHeldByOwner`, rather
than wait for the worker's own transaction until the context ends. After an error that makes SQLite roll a
transaction back (an interrupted write, a full disk), its later statements
fail with `hoppersqlite.ErrTransactionAborted`; roll it back.

Several processes can share the file: clients in other processes are woken
within 50ms of an insert. The file must be on a local disk; `Open` refuses
one on a network filesystem or a virtual machine's shared folder (Docker
Desktop's bind mounts among them), where SQLite's locks do not hold.
`hoppersqlite.Migrations` returns the SQLite scripts for teams that run
migrations with their own tool. Upgrade hopper and hoppersqlite together:
the driver uses hopper's internal packages.

## Define a job and its worker

Args are a struct with a `Kind` method. The kind names the job type and maps
to exactly one worker; the fields are stored as JSON.

```go
type SendEmail struct {
    UserID int    `json:"user_id"`
    Tmpl   string `json:"tmpl"`
}

func (SendEmail) Kind() string { return "send_email" }

type SendEmailWorker struct {
    hopper.WorkerDefaults[SendEmail] // default Timeout and NextRetry
    Mailer *mail.Client
}

func (w *SendEmailWorker) Work(ctx context.Context, job *hopper.Job[SendEmail]) error {
    return w.Mailer.Send(ctx, job.Args.UserID, job.Args.Tmpl)
}
```

A returned error schedules a retry with backoff (`n^4 + 5s`, capped at a day),
until `MaxAttempts` (25 by default) is used up and the job is discarded to the
dead-letter queue. Two other returns are special:

```go
return hopper.Snooze(10 * time.Minute) // run again later; does not use an attempt
return hopper.Cancel(err)              // discard now, with err recorded
```

`Config.RetryPolicy`, for example `hopper.ExponentialRetry{Base: time.Second,
Max: time.Hour}`, replaces the backoff for workers without their own
`NextRetry`. `Config.OnError` sees every failed attempt and recovered panic, of
any kind, and can discard, snooze or reschedule the job.

To show operators why a job is waiting, return the snooze with a reason. It is
stored on the job as `SnoozeReason` until the job runs again:

```go
return &hopper.SnoozeError{Duration: 10 * time.Minute, Reason: "export not ready"}
```

Workers must be idempotent: a job runs twice only when the process running it
is lost mid-job, but it can happen.

When the work is writes to the same database, complete the job in the same
transaction. The job is then finished exactly when the work commits, and a
crash afterwards cannot run it again:

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
        return err // hopper.ErrJobFenced: this attempt no longer owns the job
    }
    return tx.Commit(ctx)
}
```

If the transaction does not commit, return an error so the job is retried.

For a function instead of a type:

```go
hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[ResizeImage]) error {
    return resize(ctx, job.Args)
})
```

To rename a kind, keep its old name as an alias until no job of it is left,
and no open batch has a callback of it. The worker for the args then also
works jobs stored under the old kind, decoding their args into the renamed
args, so keep the fields' encoded names. `job.Kind` is the kind the job was
stored with:

```go
func (SendEmail) Kind() string          { return "send_email" }
func (SendEmail) KindAliases() []string { return []string{"send_mail"} }
```

A job keeps its stored kind, so during the rename a unique key does not match
across the two kinds, and filtering jobs by kind needs both names. A periodic
job is named after its kind unless it has a `Name`; give it the old kind as
its name to keep its schedule.

In a rolling deploy, a process still on the previous release cannot work the
new kind: a job of it that process claims fails and uses an attempt. To avoid
that, rename in two releases: first keep `Kind` as `"send_mail"` and return
`"send_email"` from `KindAliases`, then, once every process runs that release,
swap them as above.

## Start a client

```go
workers := hopper.NewWorkers()
hopper.AddWorker(workers, &SendEmailWorker{Mailer: m})

client, err := hopper.NewClient(driver, &hopper.Config{
    Queues: map[string]hopper.QueueConfig{
        hopper.QueueDefault: {MaxWorkers: 100},
        "email":             {MaxWorkers: 20},
    },
    Workers: workers,
})

// Run blocks until ctx is canceled, then drains. It fits errgroup.
g.Go(func() error { return client.Run(ctx) })
```

`MaxWorkers` is per client per queue. Run the same program on several
replicas and they share the queues: work is claimed with `FOR UPDATE SKIP
LOCKED`, and one replica is elected leader for periodic jobs and maintenance.

A client with no `Queues` only inserts. That is the right shape for an API
server that enqueues work for a separate worker deployment.

## Insert jobs

```go
res, err := client.Insert(ctx, SendEmail{UserID: 42, Tmpl: "welcome"}, nil)
res.Job.ID // a UUIDv7, safe to show to users

// Inside your transaction: the job exists only if tx commits.
res, err = client.InsertTx(ctx, tx, SendEmail{UserID: 42, Tmpl: "welcome"}, &hopper.InsertOpts{
    Queue:       "email",
    Priority:    hopper.PriorityHigh,
    ScheduledAt: time.Now().Add(time.Hour),
    MaxAttempts: 5,
})

// Many at once: one statement, or COPY for large batches.
results, err := client.InsertMany(ctx, []hopper.InsertParams{
    {Args: SendEmail{UserID: 1}},
    {Args: SendEmail{UserID: 2}, Opts: &hopper.InsertOpts{Priority: hopper.PriorityLow}},
})
```

A kind can carry its own defaults, overridden per call:

```go
func (SendEmail) InsertOpts() hopper.InsertOpts {
    return hopper.InsertOpts{Queue: "email", MaxAttempts: 5}
}
```

## Unique jobs

```go
res, err := client.Insert(ctx, SyncAccount{AccountID: 7}, &hopper.InsertOpts{
    Unique: &hopper.UniqueOpts{ByArgs: true, OnConflict: hopper.UniqueReplace},
})
res.Duplicate // true if a live job with the same key existed
```

`UniqueOpts{}` alone allows one live job of the kind. `ByArgs` adds the
fields tagged `hopper:"unique"` (or all fields), `ByQueue` the queue, and
`ByPeriod` a time bucket ("one per hour"). `UniqueReplace` updates the
existing job's args and schedule, which gives debouncing.

Only live jobs count unless `Finished` names finished states too. To run
a report at most once per day, even when the first run has already
completed:

```go
Unique: &hopper.UniqueOpts{
    ByArgs:   true,
    ByPeriod: 24 * time.Hour,
    Finished: []hopper.JobState{hopper.JobStateCompleted},
},
```

A finished job counts while history keeps it (`CompletedRetention`,
`FailedRetention`), and is reported as the duplicate, never replaced.

## Tags and finding jobs

Tags label a job so you can find it later, for example by tenant:

```go
client.Insert(ctx, SyncAccount{AccountID: 7}, &hopper.InsertOpts{
    Tags: []string{"tenant:acme", "backfill"},
})

for job, err := range client.Jobs(ctx, hopper.JobFilter{TagsAll: []string{"tenant:acme"}}) {
    ...
}
```

`TagsAny` finds jobs with any of the tags and `TagsAll` jobs with all of
them. Both read an index of each tag's jobs in ID order, live and in
history, so a page costs about the same for a rare tag or a common one.
`JobFilter` also takes job `IDs`, `Priorities`, and `Metadata`, a JSON
object the job's metadata must contain (`{"region": "eu"}`). Metadata
has no index, so combine it with filters that do. `hopper jobs list -tag
tenant:acme` and the web UI filter by tag too.

## Periodic jobs

```go
Periodic: []hopper.PeriodicJob{
    hopper.Cron("*/15 * * * *", SyncAllocations{}, nil),
    hopper.Cron("CRON_TZ=America/Chicago 0 9 * * MON-FRI", DailyDigest{}, nil),
    hopper.Every(30*time.Second, RefreshCache{}, &hopper.PeriodicOpts{RunOnStart: true}),
},
```

The leader inserts each slot exactly once, whichever replica is leader.

`client.PeriodicJobs()` adds and removes periodic jobs at runtime:

```go
err := client.PeriodicJobs().Add(hopper.Cron("0 * * * *", TenantReport{ID: 42}, &hopper.PeriodicOpts{Name: "report:42"}))
err = client.PeriodicJobs().Remove("report:42")
```

The set is per client and kept in memory, and only the leader's set is
scheduled. Make the same change on every replica that can lead, or the job
stops or starts when leadership moves.

## Batches and workflows

```go
b := client.NewBatch(hopper.BatchOpts{OnSuccess: ReportDone{RunID: 9}, OnFailure: AlertOps{RunID: 9}})
b.Add(ProcessShard{Shard: 0}, nil)
b.Add(ProcessShard{Shard: 1}, nil)
res, err := b.InsertTx(ctx, tx)

wf := hopper.NewWorkflow("ingest-9", &hopper.WorkflowOpts{OnFailure: AlertOps{RunID: 9}})
fetch := wf.Add(Fetch{URL: u}, nil)
parse := wf.Add(Parse{}, hopper.After(fetch))
wf.Add(Index{}, hopper.After(parse))
wf.Add(Notify{}, hopper.After(parse))
res, err := client.InsertWorkflowTx(ctx, tx, wf)
```

A batch runs its callbacks when its last job finishes. A workflow's steps
run as their dependencies finish; a failed step cancels the steps that
depend on it (set `StepOpts.OnDependencyFailure` to `DependencyIgnore` to run
anyway), and the workflow is a batch, so it has the same callbacks.
`client.WorkflowGet(ctx, res.ID)` and `hopper workflows get <id>` show its
progress and graph.

## Streams

Messaging fans out at publish time; a stream keeps the events, so a consumer
can start from the beginning or be moved back.

```go
hopper.Consume(workers, hopper.Consumer{Name: "audit", Pattern: "#", Start: hopper.StreamStartEarliest},
    func(ctx context.Context, msg *hopper.Message[hopper.Raw]) error {
        return record(ctx, msg.Topic, msg.Payload, msg.Position)
    })

ev, err := client.Streams().AppendTx(ctx, tx, AllocationCreated{ID: 42}, &hopper.AppendOpts{Key: "allocation:42"})
err = client.Streams().Seek(ctx, "audit", hopper.SeekOpts{Time: time.Now().Add(-time.Hour)})
```

Events with the same key are handled one at a time, in order. The log keeps
`Config.StreamRetention` (7 days) of events; `hopper streams consumers` shows
where each consumer is.

## Results and waiting

```go
func (w *RenderWorker) Work(ctx context.Context, job *hopper.Job[RenderReport]) error {
    url, err := render(ctx, job.Args)
    if err != nil {
        return err
    }
    return hopper.SetOutput(ctx, url)
}

res, err := client.Insert(ctx, RenderReport{ID: 7}, &hopper.InsertOpts{Await: true})
url, err := hopper.Await[string](ctx, client, res.Job.ID)
```

## Resumable steps

A long job made of several steps resumes where it left off when it is
retried, snoozed or re-driven with `JobRetry`:

```go
func (w *ImportWorker) Work(ctx context.Context, job *hopper.Job[Import]) error {
    return hopper.RunSteps(ctx, job,
        hopper.NamedStep("download", w.download),
        hopper.NamedStep("load", w.load),
        hopper.NamedStep("notify", w.notify),
    )
}

func (w *ImportWorker) load(ctx context.Context, job *hopper.Job[Import]) error {
    page, _, err := hopper.StepCursor[int](job.JobRow) // 0 on the first try
    if err != nil {
        return err
    }
    for ; page < job.Args.Pages; page++ {
        if err := w.loadPage(ctx, page); err != nil {
            return err
        }
        if err := hopper.RecordCursor(ctx, job, page+1); err != nil {
            return err
        }
    }
    return nil
}
```

`RunSteps` skips the steps the job already finished and records each one as
it finishes. `RecordStepTx` and `RecordCursorTx` record in your own
transaction, so a step's writes and its progress commit together. The
progress is `job.Progress`, shown by `hopper jobs get` and the web UI.
`client.JobRetry(ctx, id, hopper.FromStart())` re-runs a job from its first
step.

## Job logs

Log through `hopper.Logger(ctx)` to keep a job's lines with the job:

```go
func (w *ImportWorker) Work(ctx context.Context, job *hopper.Job[Import]) error {
    log := hopper.Logger(ctx)
    log.Info("loading", "pages", job.Args.Pages)
    ...
}
```

The lines go to `Config.Logger`, with the job's ID, kind and attempt, as
they happen. Each attempt's lines are also stored with the job when the
attempt finishes, up to 32 KiB per attempt for the latest 10 attempts
(`Config.JobLogs`), and kept for as long as the job is: `hopper jobs get`
and the web UI show them, and `client.JobLogs(ctx, id)` returns them.

To keep logs elsewhere, such as in object storage, or to read them while
the job runs, give the client a `hopper.LogStore`:

```go
type LogStore interface {
    Writer(ctx context.Context, job *hopper.JobRow) (io.WriteCloser, error)
    Logs(ctx context.Context, id hopper.JobID) ([]hopper.AttemptLog, error)
}

client, err := hopper.NewClient(driver, &hopper.Config{
    JobLogs: hopper.JobLogsConfig{Store: blobLogs},
    ...
})
```

Each attempt's lines are written to its `Writer` as they are logged, and
the writer is closed when the worker returns. Nothing is written to the
database, so the store also keeps the lines of a client that crashed, and
a process that runs the work elsewhere can write to the same store under
the job's ID and attempt. `client.JobLogs` and the web UI read the store.
`hopper jobs get` shows only logs kept in the database. The store decides
how long to keep logs.

## Operate

```go
client.JobCancel(ctx, id)            // any live job, on any replica
client.JobRetry(ctx, id)             // re-drive from the dead-letter queue
client.JobRetryOrderingKey(ctx, queue, key) // run a key's waiting jobs now
client.JobDelete(ctx, id)            // remove a job that is not running
job, err := client.JobGet(ctx, id)   // live or historical
client.Queues().Pause(ctx, "email")
stats, err := client.Stats(ctx)

for job, err := range client.Jobs(ctx, hopper.JobFilter{States: []hopper.JobState{hopper.JobStateDiscarded}}) {
    ...
}
```

The same operations are available from the shell with `cmd/hopper`. See
[operations.md](operations.md) for running hopper in production.

## Test your code

The `hoppertest` package checks what your code inserted, inside a transaction
if you like, and runs a worker inline without a database round trip:

```go
job := hoppertest.RequireInserted[SendEmail](ctx, t, client, nil)
if job.Args.UserID != 42 { ... }

hoppertest.RequireNotInsertedTx[SendEmail](ctx, t, client, tx, nil)

err := hoppertest.Work(ctx, t, workers, SendEmail{UserID: 42}, nil)

// A step worker, resumed after its "download" step:
err = hoppertest.Work(ctx, t, workers, Import{Pages: 3},
    &hoppertest.WorkOpts{Progress: &hopper.Progress{Step: "download"}})
```
