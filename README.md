# hopper

**A high-performance job queue and message broker for Go, built on Postgres.**

hopper runs background jobs and delivers messages using the PostgreSQL database
your application already has. There's no broker to operate and no separate worker
service: it is a library that runs inside your process and scales out with it.
Postgres is the first engine, behind a driver interface designed for more.

- **Fast:** set-based claim, finalize and insert (including `COPY`), a live
  table that stays small however much history accumulates, and retention by
  dropping partitions. Throughput and latency targets are release gates.
- **Transactional enqueue:** jobs and messages commit or roll back with your data.
- **Scale by adding replicas:** work is claimed with `FOR UPDATE SKIP LOCKED`, and
  periodic jobs and maintenance run on one elected leader.
- **Reliable:** retries with backoff, dead-lettering and replay, unique jobs, and
  lease-based rescue of jobs from crashed processes in seconds.
- **Complete:** priorities, cron with time zones, snooze, cancellation, timeouts,
  awaitable results, global concurrency and rate limits, batches and workflows.
- **Messaging:** topics, subscriptions and fan-out with AMQP-style routing,
  ordering keys, TTLs, idempotent publish and request/reply.
- **Idiomatic Go:** generic, type-safe workers and messages, `context`, `slog`,
  and iterators. The core depends only on the standard library and
  [pgx](https://github.com/jackc/pgx), and it runs under `GODEBUG=fips140=only`.

> **Status: pre-release.** The job queue is implemented: schema and migrations,
> the Postgres driver, transactional and bulk inserts, batched claim and finalize,
> typed workers, retries with backoff, graceful shutdown, lease-based rescue of
> jobs from crashed processes, leader election, partition-drop retention,
> LISTEN/NOTIFY wake-ups, unique jobs, cron and interval schedules, cancellation,
> retry from the dead-letter queue, TTLs, pause and runtime queues, middleware,
> awaitable results, listing, events and stats, plus the `hopper` CLI,
> `hoppertest`, `hopperotel`, the `hopperui` web UI and the `drivertest`
> conformance suite. So are messaging (subscriptions with topic patterns,
> fan-out, dedup and ordering keys, a SQL contract for producers in other
> languages, streams with consumers), flow control (global, rate and
> partitioned limits, priority aging), batches and workflows, and a
> `database/sql` driver, in the order laid out in [docs/PLAN.md](docs/PLAN.md),
> the plan of record. Start with
> [docs/getting-started.md](docs/getting-started.md). Feedback is welcome
> through issues and PRs.

```go
_, err := client.InsertTx(ctx, tx, SendEmail{UserID: 42}, nil)        // commits with tx
_, err = client.PublishTx(ctx, tx, AllocationCreated{ID: 42}, nil)  // fans out to subscribers
```

## The name

A feed hopper releases work into a machine one piece at a time. The name is
also a tribute to RADM Grace Hopper, and it's a good name for something meant
to replace a rabbit.

## License

[Apache-2.0](LICENSE)
