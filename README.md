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

> **Status: pre-release.** The core engine is implemented: schema and migrations,
> the Postgres driver, transactional and bulk inserts, batched claim and finalize,
> typed workers, retries with backoff, and graceful shutdown. Rescue of jobs from
> crashed processes, LISTEN/NOTIFY wake-ups, unique jobs, cron, cancellation and
> messaging follow, in the order laid out in [docs/PLAN.md](docs/PLAN.md), the
> plan of record. Feedback is welcome through issues and PRs.

```go
_, err := client.InsertTx(ctx, tx, SendEmail{UserID: 42}, nil)   // commits with tx
err = client.PublishTx(ctx, tx, "allocation.created", payload)   // fans out to subscribers
```

## Why another queue?

[River](https://riverqueue.com) showed how well a Postgres-native queue fits Go
applications, and we started with it. hopper exists because we want:

- a permissive, single-license (Apache-2.0) project with no commercial tier;
- a lean core with integrations in separate modules;
- lease-based rescue that recovers from crashed pods in seconds, with no
  per-job heartbeat writes;
- the features of commercial queue tiers (global limits, batches, workflows)
  in the open-source library;
- built-in pub/sub messaging, so one dependency replaces both a job framework
  and a broker.

## The name

A feed hopper releases work into a machine one piece at a time. The name is
also a tribute to RADM Grace Hopper, and it's a good name for something meant
to replace a rabbit.

## License

[Apache-2.0](LICENSE)
