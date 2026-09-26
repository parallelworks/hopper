# Operating hopper

## Sizing

- **`MaxWorkers`** is per client per queue. It bounds goroutines, not
  database connections: a client uses a handful of connections for claiming
  and finalizing however many workers it runs. Size it for the work, not for
  the pool.
- **Pool size.** A worker client needs about four connections for its own
  traffic (claims, finalizes, leases, leader duties) plus one dedicated
  connection for `LISTEN`, outside the pool. Give the pool what the
  application needs on top of that.
- **Replicas.** Adding replicas adds throughput. Every replica competes for
  leadership; only the leader runs periodic jobs, rescue and retention, and a
  replacement takes over within 15 seconds of the leader stopping.

## Postgres

- **Version:** 14 or later. Job IDs use `uuidv7()` on 18 and a compatible
  SQL function on 14–17.
- **Autovacuum.** `hopper_jobs` is small (only the backlog and running jobs)
  but churns fast. Let autovacuum run often on it:

  ```sql
  ALTER TABLE hopper_jobs SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_cost_delay = 0,
    autovacuum_analyze_scale_factor = 0.02
  );
  ```

  History never needs vacuuming for retention: expired data is dropped by
  partition.
- **`shared_buffers`.** Keep the live table, its indexes and the current
  history partition in shared buffers. With Postgres's 128 MB default, a
  burst of history writes evicts the live table and throughput drops by a
  third; with 1 GB the same load costs a few percent.
- **`synchronous_commit`.** hopper is correct with either setting. Turning it
  off trades durability of the last few milliseconds for throughput, as with
  any table.
- **Connection poolers.** PgBouncer in transaction mode (1.21 or later, for
  prepared statements) and RDS Proxy work for the pool. `LISTEN` needs a
  session connection: point the listener at a direct address with
  `hopperpgx.Config.ListenConnConfig`, or accept polling (the client logs
  once and polls every `PollInterval`, one second by default).
- **Isolation.** Set `search_path` on the pool to keep hopper's tables in
  their own schema. Notification channels are per database, so two hopper
  schemas in one database will wake each other up; that is harmless.

## Retention

Finalized jobs move to `hopper_job_history`, partitioned by outcome and then
by time. The leader creates partitions ahead and drops expired ones, so
retention never runs a `DELETE`.

| Outcome | Default | Config |
| --- | --- | --- |
| completed | 24 hours | `Config.CompletedRetention` |
| cancelled, discarded | 7 days | `Config.FailedRetention` |

Negative values keep history forever. Retention is applied by the leader, so
set it the same on every replica. `QueueConfig.DeleteCompleted` skips history
for a queue's completed jobs entirely, for maximum throughput.

## Failure handling

- **A process dies.** Its lease (renewed every 5 s, 15 s TTL) expires, and the
  leader moves its running jobs back to `retryable` with the error `hopper:
  client lost`. Expect them to run again within about 20 seconds. Jobs out of
  attempts are dead-lettered instead.
- **A process pauses** (long GC, VM stall, partition) longer than the TTL and
  then resumes. Its next renewal fails; it cancels every running job's context
  and re-registers under a new client ID. Results still in flight are applied
  only if the job was not rescued meanwhile, so a job never runs two attempts
  at once for long.
- **A worker hangs** ignoring its context. Timeouts (`Config.JobTimeout`, one
  minute by default, or the worker's `Timeout`) cover the common case.
  `Config.RescueStuckAfter` additionally rescues any job running longer than
  the given duration, whatever its client's state.
- **Graceful stop.** `Stop` (or `Run` when its context ends) stops claiming,
  gives up leadership, waits for running jobs, flushes results and releases
  the lease. If the stop context expires first, running jobs are cancelled;
  those that return in time are finalized as `retryable` and run again
  immediately elsewhere, and the rest are rescued after the TTL.

## Dead letters

Cancelled and discarded jobs stay in history for `FailedRetention`. Inspect
them with `client.Jobs` or `hopper jobs list -state discarded`, and re-drive
one with `client.JobRetry` or `hopper jobs retry <id>`.

## Monitoring

- `client.Stats(ctx)` (or `hopper stats`) returns depth by queue and state,
  the age of the oldest claimable job, completions in the last minute, running
  jobs per client, live clients and the leader. The oldest-job age is a good
  autoscaling signal.
- `client.Events(ctx, kinds...)` streams job, leader and lease events in
  process.
- The `hopperotel` module adds OpenTelemetry tracing (insert to work, through
  job metadata) and metrics.
- `pg_stat_activity` shows the listener connection as
  `hopper-listener:<schema>`.

## Command line

```sh
hopper migrate up|down|version
hopper jobs list [-queue Q] [-kind K] [-state S] [-limit N]
hopper jobs get|retry|cancel <id>
hopper queues list|pause|resume [name]
hopper clients list
hopper stats
```

Every command takes `-json`. The database comes from `-database-url` or
`HOPPER_DATABASE_URL`.

## Benchmarking

`cmd/hopperbench` drives the hot paths against a database and prints one
JSON line per scenario. CI runs it on pull requests that touch the insert,
claim or finalize paths and fails on a regression of more than 10% against
the base branch. Compare two runs yourself with:

```sh
hopperbench -compare base.jsonl,head.jsonl
```
