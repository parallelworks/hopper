# Changelog

All notable changes to hopper are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Until v1.0.0, minor
versions may change the API.

## [Unreleased]

### Added

- Messaging: `Subscribe` with AMQP topic patterns, typed `Message[T]`,
  `Publish`/`PublishTx` fan-out in one statement, dedup keys, ordering keys
  (also on plain jobs through `InsertOpts.OrderingKey`), request/reply, and
  `ReplayDiscarded`.
- Schema v2: subscription retry budgets and metadata, ordering-key indexes,
  and the SQL contract functions `hopper_insert` and `hopper_publish`.
- `TestUpgradeUnderTraffic`, which migrates to the latest schema while a
  client works jobs.
- Flow control: cluster-wide `GlobalLimit`, `RateLimit`/`RateBurst` and
  `PartitionLimit` per queue (declared in `QueueConfig` or set at runtime
  with `Queues().SetLimits` and `hopper queues limit`), `PriorityAging`,
  and `InsertOpts.PartitionKey`.
- Batches: `NewBatch`, `Add`, `Insert`/`InsertTx`, `BatchGet`, with
  `OnSuccess`, `OnFailure` and `OnComplete` callbacks inserted by the
  finalizing statement.
- Schema v3: queue limit columns, `hopper_batches`, and the partition and
  batch indexes.
- `hoppersql`, a driver for `database/sql` (pgx's stdlib adapter or lib/pq)
  that passes the same conformance suite as `hopperpgx`. It polls instead of
  listening and inserts without COPY.
- Workflows: `NewWorkflow`, `Add` with `After`, `InsertWorkflow`/
  `InsertWorkflowTx`, `WorkflowGet` and `hopper workflows get`. Steps with
  dependencies wait pending and are promoted by the statement that finalizes
  the last of them; a failed step cancels its dependents unless they opt to
  `DependencyIgnore`. Schema v4 adds `hopper_job_deps`.
- Streams: `Streams().Append`/`AppendTx` write to a retained, time-partitioned
  log; `hopper.Consume` registers a consumer that delivers matching events as
  jobs from a position of its own, starting at the earliest or latest event
  and movable with `Seek`; `Read` pages the log. Consumers read by snapshot
  deltas, so a late-committing transaction is delivered when it commits and
  never skipped. `Config.StreamRetention`, `hopper streams consumers|seek`
  and `hopper subscriptions list`. Schema v5 adds `hopper_stream_events` and
  `hopper_stream_consumers`.
- `hopperui`, a separate module: an embeddable web UI for queues, jobs,
  workflows (as a DAG), subscriptions, stream consumers and clients, with
  actions behind an `Authorize` hook.
- `JobFilter.After`, to page job listings.

### Changed

- The Postgres SQL and the logic around it moved to a package shared by both
  drivers; `hopperpgx` keeps its COPY, LISTEN and pipelining paths.

## [0.1.0]

The first release: a complete job queue on PostgreSQL.

### Added

- Core engine: typed workers, `Insert`/`InsertTx`/`InsertMany` (one statement,
  or `COPY` for large batches), batched `FOR UPDATE SKIP LOCKED` claims,
  batched finalize into time-partitioned history, retries with backoff,
  `Snooze` and `Cancel`, timeouts, panic recovery, and a three-step graceful
  `Run`/`Stop`.
- Reliability: per-client leases, rescue of jobs from crashed processes,
  fencing of paused processes, leader election, partition-drop retention,
  LISTEN/NOTIFY wake-ups with per-process coalescing and polling fallback,
  unique jobs with skip and replace.
- Control: cron and interval periodic jobs with time zones, in-flight
  cancellation, retry from the dead-letter queue, TTLs, queue pause and
  resume, runtime queues, middleware, `SetOutput`/`Await`, the `Jobs`
  iterator, events and `Stats`.
- `hoppermigrate`: embedded, versioned migrations under a cross-process lock.
- `hopperpgx`: the pgx v5 driver.
- `hoppertest`: test helpers for code that uses hopper.
- `hopperotel` (separate module): OpenTelemetry tracing and metrics.
- `cmd/hopper`: migrations, jobs, queues, clients and stats from the shell.
- `cmd/hopperbench`: the benchmark harness and CI performance gate.

[Unreleased]: https://github.com/parallelworks/hopper/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/parallelworks/hopper/releases/tag/v0.1.0
