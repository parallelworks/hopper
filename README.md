# hopper

**A Postgres-backed job queue and message broker for Go.**

hopper runs background jobs and delivers messages using only the PostgreSQL
database your application already has. There's no broker to operate and no
separate worker service: it is a library that runs inside your process and
scales out with it.

- **Transactional enqueue:** jobs and messages commit or roll back with your data.
- **Scale by adding replicas:** work is claimed with `FOR UPDATE SKIP LOCKED`, and
  periodic jobs and maintenance run on one elected leader.
- **Reliable:** retries with backoff, dead-lettering, heartbeat-based rescue of
  jobs from crashed processes, and unique jobs.
- **Messaging:** topics, subscriptions and fan-out with AMQP-style routing
  patterns, for the RabbitMQ features application code actually uses.
- **Small and auditable:** the core depends on the Go standard library and
  [pgx](https://github.com/jackc/pgx) only. hopper does no cryptography of its
  own, so it runs under `GODEBUG=fips140=only`.

> **Status: design.** Nothing is implemented yet. The plan of record is
> [docs/PLAN.md](docs/PLAN.md). Feedback is welcome through issues and PRs.

```go
_, err := client.InsertTx(ctx, tx, SendEmail{UserID: 42}, nil)   // commits with tx
err = client.PublishTx(ctx, tx, "allocation.created", payload)   // fans out to subscribers
```

## Why another queue?

[River](https://riverqueue.com) showed how well a Postgres-native queue fits Go
applications, and we started with it. hopper exists because we want:

- a permissive, single-license (Apache-2.0) project with no commercial tier;
- a core small enough to audit line by line;
- heartbeat-based rescue that recovers from crashed pods in about a minute;
- built-in pub/sub messaging, so one dependency replaces both a job framework
  and a broker.

## The name

A feed hopper releases work into a machine one piece at a time. The name is
also a tribute to RADM Grace Hopper, and it's a good name for something meant
to replace a rabbit.

## License

[Apache-2.0](LICENSE)
