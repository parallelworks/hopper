# Changelog

All notable changes to hopper are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Until v1.0.0, minor
versions may change the API.

## [Unreleased]

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
