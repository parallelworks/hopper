# External log reads, retention and following

Start from canary `8faa5a2`. Keep `LogStore.Writer` and `LogStore.Logs` compatible;
existing stores and inline database logs retain their behavior unless an application
opts into the new capabilities.

## PR 1: execution reads and managed retention

- Identify each execution by `(job ID, claim fence)`, including repeated snoozes of
  the same attempt. Add optional paged inventory, bounded-read and conditional-delete
  interfaces. Preserve `Client.JobLogs` for existing consumers.
- Add client methods to list executions and read a byte slice. A cursor carries its
  execution fence and byte offset. Following the latest execution resets the offset
  when its fence changes. Unsupported stores report `ErrNotSupported`.
- Opt into retention with `JobLogs.Retention`. Completed and failed lifetimes inherit
  the client's job-history settings unless overridden; negative lifetimes retain logs
  until their job disappears. Bound retained executions and each execution's writes.
- Reconcile from the store's durable inventory in bounded batches. Use the existing
  driver `Now` and filtered `JobList` methods to read database time and live/history
  states in batches. No schema changes or writes on insert, claim or finalize.
- Run reconciliation in its own leader-term loop, draining bounded pages before
  waiting between scans. Protect the current running claim,
  recently modified objects and objects newer than the job snapshot. Deletion is
  conditional on the inventory object's version and cannot delete a newer execution.
- Advance the scan cursor only after a successful batch. Deletions are idempotent;
  a crash or leadership change restarts reconciliation from persisted inventory.
  This covers explicit deletion, `DeleteCompleted` and dropped history partitions.
- Expose maintenance progress, observed bytes, removed bytes and failures for metrics.
  Inventory byte counts are observations, not a transactional global storage quota.
  A hard storage quota remains a backend responsibility.
- Prove retries, concurrent appends, snoozes, active claims, leader changes, storage
  failures and blocked storage shutdown. Update `docs/PLAN.md` and operations docs.

## PR 2: notifications and follow API

- Add an optional store subscription interface for cross-process change signals.
  Signals are hints: clients always read through execution-scoped cursors.
- Provide a context-bound follow API with bounded chunks and polling fallback for
  stores without subscriptions, missed signals and subscription failures.
- Subscribe before the initial read, coalesce wakes, handle run replacement and
  retention, and stop subscriptions cleanly. No new database notification writes.
- Prove immediate appends, missed/coalesced signals, execution changes, deletion,
  cancellation and slow consumers. Document a complete store integration example.

## Validation

Run focused race tests while developing, then the root, SQLite, telemetry and UI
module suites sequentially, with FIPS-only mode enabled and the shared Go caches.
Run each module's lint checks and verify tidy modules. Exercise real PostgreSQL
through the existing per-test isolated schemas and SQLite through its test suite.
PRs target canary and describe compatibility and backend contracts explicitly.
