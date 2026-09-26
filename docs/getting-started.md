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

hopper's tables are prefixed `hopper_` and live in the first schema of the
connection's `search_path`. Install them once per database, from any replica;
migrations take a cross-process lock, so concurrent starts are fine.

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

The SQL files are in `hoppermigrate/migrations` for teams that run migrations
with their own tool. `hopper_schema` records the applied versions either way.

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

Workers must be idempotent: a job runs twice only when the process running it
is lost mid-job, but it can happen.

For a function instead of a type:

```go
hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[ResizeImage]) error {
    return resize(ctx, job.Args)
})
```

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

// Run blocks until ctx is cancelled, then drains. It fits errgroup.
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

## Periodic jobs

```go
Periodic: []hopper.PeriodicJob{
    hopper.Cron("*/15 * * * *", SyncAllocations{}, nil),
    hopper.Cron("CRON_TZ=America/Chicago 0 9 * * MON-FRI", DailyDigest{}, nil),
    hopper.Every(30*time.Second, RefreshCache{}, &hopper.PeriodicOpts{RunOnStart: true}),
},
```

The leader inserts each slot exactly once, whichever replica is leader.

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

## Operate

```go
client.JobCancel(ctx, id)            // any live job, on any replica
client.JobRetry(ctx, id)             // re-drive from the dead-letter queue
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
```
