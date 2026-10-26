package hoppersqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// ClientRegister implements driver.Executor.
func (e *executor) ClientRegister(ctx context.Context, params driver.ClientRegisterParams) (int64, error) {
	info, err := jsonValue(params.Info, "{}", "client info")
	if err != nil {
		return 0, err
	}
	var id int64
	err = e.write(ctx, func(x *w) error {
		return x.QueryRowContext(ctx, `INSERT INTO hopper_clients (hostname, started_at, expires_at, info) VALUES (?, ?, ?, ?) RETURNING id`,
			params.Hostname, x.us(), x.at(params.TTL), info).Scan(&id)
	})
	if err != nil {
		return 0, fmt.Errorf("hopper: register client: %w", err)
	}
	return id, nil
}

// strings reads the text values a query selects.
func stringsOf(ctx context.Context, q querier, query string, args ...any) ([]string, error) {
	return queryAll(ctx, q, func(r scanner) (string, error) {
		var s string
		err := r.Scan(&s)
		return s, err
	}, query, args...)
}

// ClientRenew implements driver.Executor: it extends an unexpired lease and
// reads the control state the client applies.
func (e *executor) ClientRenew(ctx context.Context, params driver.ClientRenewParams) (driver.ClientRenewResult, error) {
	var res driver.ClientRenewResult
	err := e.write(ctx, func(x *w) error {
		renewed, err := x.ExecContext(ctx, `UPDATE hopper_clients SET expires_at = ? WHERE id = ? AND expires_at > ?`,
			x.at(params.TTL), params.ClientID, x.us())
		if err != nil {
			return err
		}
		n, err := renewed.RowsAffected()
		if err != nil {
			return err
		}
		res.Renewed = n > 0
		if res.CancelRequested, err = x.ids(ctx, `SELECT id FROM hopper_jobs
			WHERE state = 'running' AND attempted_by = ? AND cancel_requested_at IS NOT NULL ORDER BY id`, params.ClientID); err != nil {
			return err
		}
		if res.PausedQueues, err = stringsOf(ctx, x, `SELECT name FROM hopper_queues WHERE paused_at IS NOT NULL ORDER BY name`); err != nil {
			return err
		}
		res.LimitedQueues, err = stringsOf(ctx, x, `SELECT name FROM hopper_queues
			WHERE global_limit IS NOT NULL OR rate_per_sec IS NOT NULL OR partition_limit IS NOT NULL ORDER BY name`)
		return err
	})
	if err != nil {
		return driver.ClientRenewResult{}, fmt.Errorf("hopper: renew client lease: %w", err)
	}
	return res, nil
}

// ClientDelete implements driver.Executor.
func (e *executor) ClientDelete(ctx context.Context, clientID int64) error {
	err := e.write(ctx, func(x *w) error {
		if _, err := x.ExecContext(ctx, `DELETE FROM hopper_periodic_registrations WHERE client_id = ?`, clientID); err != nil {
			return err
		}
		_, err := x.ExecContext(ctx, `DELETE FROM hopper_clients WHERE id = ?`, clientID)
		return err
	})
	if err != nil {
		return fmt.Errorf("hopper: delete client: %w", err)
	}
	return nil
}

// ClientPruneExpired implements driver.Executor.
func (e *executor) ClientPruneExpired(ctx context.Context) (int64, error) {
	var n int64
	err := e.write(ctx, func(x *w) error {
		now := x.us()
		if _, err := x.ExecContext(ctx, `DELETE FROM hopper_periodic_registrations
			WHERE client_id IN (SELECT id FROM hopper_clients WHERE expires_at < ?)`, now); err != nil {
			return err
		}
		res, err := x.ExecContext(ctx, `DELETE FROM hopper_clients WHERE expires_at < ?`, now)
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("hopper: prune clients: %w", err)
	}
	return n, nil
}

// ClientList implements driver.Executor.
func (e *executor) ClientList(ctx context.Context) ([]*driver.ClientRow, error) {
	clients, err := queryAll(ctx, e.read(), func(r scanner) (*driver.ClientRow, error) {
		var (
			c                driver.ClientRow
			started, expires int64
			info             string
		)
		if err := r.Scan(&c.ID, &c.Hostname, &started, &expires, &info); err != nil {
			return nil, err
		}
		c.StartedAt, c.ExpiresAt, c.Info = time.UnixMicro(started).UTC(), time.UnixMicro(expires).UTC(), json.RawMessage(info)
		return &c, nil
	}, `SELECT id, hostname, started_at, expires_at, info FROM hopper_clients ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("hopper: list clients: %w", err)
	}
	return clients, nil
}

// LeaderAttempt implements driver.Executor: it takes the lease if it is
// free or expired, or renews it for the holder.
func (e *executor) LeaderAttempt(ctx context.Context, params driver.LeaderParams) (bool, error) {
	var leader bool
	err := e.write(ctx, func(x *w) error {
		var id int64
		err := x.QueryRowContext(ctx, `
			INSERT INTO hopper_leader (name, client_id, elected_at, expires_at) VALUES ('default', ?1, ?2, ?3)
			ON CONFLICT (name) DO UPDATE SET
			  client_id  = excluded.client_id,
			  elected_at = CASE WHEN hopper_leader.client_id = excluded.client_id THEN hopper_leader.elected_at ELSE excluded.elected_at END,
			  expires_at = excluded.expires_at
			WHERE hopper_leader.expires_at < ?2 OR hopper_leader.client_id = excluded.client_id
			RETURNING client_id`, params.ClientID, x.us(), x.at(params.TTL)).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		leader = err == nil && id == params.ClientID
		return err
	})
	if err != nil {
		return false, fmt.Errorf("hopper: leader attempt: %w", err)
	}
	return leader, nil
}

// LeaderResign implements driver.Executor: it gives the lease up if this
// client holds it and, only then, signals other clients to elect a
// replacement.
func (e *executor) LeaderResign(ctx context.Context, clientID int64) error {
	err := e.write(ctx, func(x *w) error {
		res, err := x.ExecContext(ctx, `DELETE FROM hopper_leader WHERE name = 'default' AND client_id = ?`, clientID)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil || n == 0 {
			return err
		}
		return x.notify(ctx, driver.ChannelLeader, "resigned")
	})
	if err != nil {
		return fmt.Errorf("hopper: leader resign: %w", err)
	}
	return nil
}

// QueueEnsure implements driver.Executor.
func (e *executor) QueueEnsure(ctx context.Context, names []string) error {
	if len(names) == 0 {
		return nil
	}
	err := e.write(ctx, func(x *w) error {
		for _, name := range names {
			if _, err := x.ExecContext(ctx, `INSERT INTO hopper_queues (name, updated_at) VALUES (?, ?) ON CONFLICT (name) DO NOTHING`, name, x.us()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("hopper: ensure queues: %w", err)
	}
	return nil
}

// QueuePause implements driver.Executor.
func (e *executor) QueuePause(ctx context.Context, name string) error {
	return e.setPaused(ctx, name, true)
}

// QueueResume implements driver.Executor.
func (e *executor) QueueResume(ctx context.Context, name string) error {
	return e.setPaused(ctx, name, false)
}

func (e *executor) setPaused(ctx context.Context, name string, paused bool) error {
	action, stmt := "resume", `INSERT INTO hopper_queues (name, updated_at) VALUES (?1, ?2)
		ON CONFLICT (name) DO UPDATE SET paused_at = NULL, updated_at = ?2`
	if paused {
		action, stmt = "pause", `INSERT INTO hopper_queues (name, paused_at, updated_at) VALUES (?1, ?2, ?2)
		ON CONFLICT (name) DO UPDATE SET paused_at = coalesce(hopper_queues.paused_at, ?2), updated_at = ?2`
	}
	err := e.write(ctx, func(x *w) error {
		if _, err := x.ExecContext(ctx, stmt, name, x.us()); err != nil {
			return err
		}
		return x.notify(ctx, driver.ChannelControl, action+":"+name)
	})
	if err != nil {
		return fmt.Errorf("hopper: %s queue: %w", action, err)
	}
	return nil
}

// QueueList implements driver.Executor.
func (e *executor) QueueList(ctx context.Context) ([]*driver.QueueRow, error) {
	queues, err := queryAll(ctx, e.read(), func(r scanner) (*driver.QueueRow, error) {
		var (
			q                               driver.QueueRow
			paused                          micros
			updated                         int64
			global, burst, partition, aging sql.NullInt64
			rate                            sql.NullFloat64
		)
		if err := r.Scan(&q.Name, &paused, &updated, &global, &rate, &burst, &partition, &aging); err != nil {
			return nil, err
		}
		q.PausedAt, q.UpdatedAt = paused.time(), time.UnixMicro(updated).UTC()
		q.Limits = driver.QueueLimits{
			Name: q.Name, GlobalLimit: int(global.Int64), RatePerSec: rate.Float64, RateBurst: int(burst.Int64),
			PartitionLimit: int(partition.Int64), Aging: time.Duration(aging.Int64) * time.Second,
		}
		return &q, nil
	}, `SELECT name, paused_at, updated_at, global_limit, rate_per_sec, rate_burst, partition_limit, aging_seconds
		FROM hopper_queues ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("hopper: list queues: %w", err)
	}
	return queues, nil
}

func nullablePositive(v int) any {
	if v <= 0 {
		return nil
	}
	return v
}

// QueueSetLimits implements driver.Executor. A new rate starts with a full
// bucket.
func (e *executor) QueueSetLimits(ctx context.Context, l driver.QueueLimits) error {
	var rate any
	if l.RatePerSec > 0 {
		rate = l.RatePerSec
	}
	err := e.write(ctx, func(x *w) error {
		if _, err := x.ExecContext(ctx, `
			INSERT INTO hopper_queues (name, global_limit, rate_per_sec, rate_burst, partition_limit, aging_seconds, tokens, refilled_at, updated_at)
			VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, ?7)
			ON CONFLICT (name) DO UPDATE SET global_limit = excluded.global_limit, rate_per_sec = excluded.rate_per_sec,
			  rate_burst = excluded.rate_burst, partition_limit = excluded.partition_limit, aging_seconds = excluded.aging_seconds,
			  tokens = CASE WHEN hopper_queues.rate_per_sec IS NOT excluded.rate_per_sec THEN NULL ELSE hopper_queues.tokens END,
			  updated_at = ?7`,
			l.Name, nullablePositive(l.GlobalLimit), rate, nullablePositive(l.RateBurst), nullablePositive(l.PartitionLimit),
			nullablePositive(int(l.Aging/time.Second)), x.us()); err != nil {
			return err
		}
		return x.notify(ctx, driver.ChannelControl, "limit:"+l.Name)
	})
	if err != nil {
		return fmt.Errorf("hopper: set queue limits: %w", err)
	}
	return nil
}

// PeriodicInsert implements driver.Executor: the job is inserted only by
// the write that advances its slot, so a slot produces at most one job.
func (e *executor) PeriodicInsert(ctx context.Context, params driver.PeriodicInsertParams) (*driver.JobRow, bool, error) {
	j, err := encodeInsert(params.Job)
	if err != nil {
		return nil, false, err
	}
	var (
		job      *driver.JobRow
		inserted bool
	)
	err = e.write(ctx, func(x *w) error {
		var name string
		err := x.QueryRowContext(ctx, `
			INSERT INTO hopper_periodic (name, last_slot) VALUES (?, ?)
			ON CONFLICT (name) DO UPDATE SET last_slot = excluded.last_slot WHERE hopper_periodic.last_slot < excluded.last_slot
			RETURNING name`, params.Name, params.Slot.UnixMicro()).Scan(&name)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		results, err := x.insertJobs(ctx, []jobInsert{j}, driver.ConflictSkip)
		if err != nil {
			return err
		}
		job, inserted = results[0].Job, !results[0].Duplicate
		return x.notify(ctx, driver.ChannelInsert, j.queue)
	})
	if err != nil {
		return nil, false, fmt.Errorf("hopper: insert periodic job %s: %w", params.Name, err)
	}
	return job, inserted, nil
}

// PeriodicLastSlots implements driver.Executor.
func (e *executor) PeriodicLastSlots(ctx context.Context) (map[string]time.Time, error) {
	type slot struct {
		name string
		at   int64
	}
	slots, err := queryAll(ctx, e.read(), func(r scanner) (slot, error) {
		var s slot
		err := r.Scan(&s.name, &s.at)
		return s, err
	}, `SELECT name, last_slot FROM hopper_periodic`)
	if err != nil {
		return nil, fmt.Errorf("hopper: periodic slots: %w", err)
	}
	out := make(map[string]time.Time, len(slots))
	for _, s := range slots {
		out[s.name] = time.UnixMicro(s.at).UTC()
	}
	return out, nil
}

// PeriodicRegister implements driver.Executor.
func (e *executor) PeriodicRegister(ctx context.Context, params driver.PeriodicRegisterParams) error {
	err := e.write(ctx, func(x *w) error {
		if _, err := x.ExecContext(ctx, `DELETE FROM hopper_periodic_registrations WHERE client_id = ?`, params.ClientID); err != nil {
			return err
		}
		for _, r := range params.Registrations {
			if _, err := x.ExecContext(ctx, `INSERT INTO hopper_periodic_registrations (client_id, name, definition)
				SELECT ?, ?, ? WHERE EXISTS (SELECT 1 FROM hopper_clients WHERE id = ? AND expires_at > ?)`,
				params.ClientID, r.Name, string(r.Definition), params.ClientID, x.us()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("hopper: register periodic jobs: %w", err)
	}
	return nil
}

// PeriodicRegistrations implements driver.Executor.
func (e *executor) PeriodicRegistrations(ctx context.Context) ([]driver.PeriodicRegistrationRow, error) {
	rows, err := queryAll(ctx, e.read(), func(r scanner) (driver.PeriodicRegistrationRow, error) {
		var (
			row        driver.PeriodicRegistrationRow
			startedAt  int64
			definition string
		)
		err := r.Scan(&row.ClientID, &startedAt, &row.Name, &definition)
		row.ClientStartedAt = time.UnixMicro(startedAt).UTC()
		row.Definition = json.RawMessage(definition)
		return row, err
	}, `SELECT c.id, c.started_at, r.name, r.definition
		FROM hopper_periodic_registrations r
		JOIN hopper_clients c ON c.id = r.client_id
		WHERE c.expires_at > ?
		ORDER BY r.name, c.started_at DESC, c.id DESC`, e.clock().UnixMicro())
	if err != nil {
		return nil, fmt.Errorf("hopper: periodic registrations: %w", err)
	}
	return rows, nil
}

// Stats implements driver.Executor, from one snapshot of the database.
func (e *executor) Stats(ctx context.Context) (*driver.Stats, error) {
	stats := &driver.Stats{Queues: map[string]*driver.QueueStats{}, RunningByClient: map[int64]int{}}
	queue := func(name string) *driver.QueueStats {
		q := stats.Queues[name]
		if q == nil {
			q = &driver.QueueStats{}
			stats.Queues[name] = q
		}
		return q
	}
	now := e.clock()
	err := e.readTx(ctx, func(q querier) error {
		rows, err := q.QueryContext(ctx, `
			SELECT queue, state, count(*),
			  min(CASE WHEN state IN ('available', 'scheduled', 'retryable') AND scheduled_at <= ?1 THEN scheduled_at END)
			FROM hopper_jobs GROUP BY queue, state`, now.UnixMicro())
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var (
				name, state string
				n           int
				oldest      micros
			)
			if err := rows.Scan(&name, &state, &n, &oldest); err != nil {
				return err
			}
			qs := queue(name)
			switch driver.JobState(state) {
			case driver.JobStateAvailable:
				qs.Available = n
			case driver.JobStateScheduled:
				qs.Scheduled = n
			case driver.JobStateRetryable:
				qs.Retryable = n
			case driver.JobStateRunning:
				qs.Running = n
			case driver.JobStatePending, driver.JobStateCompleted, driver.JobStateCanceled, driver.JobStateDiscarded:
			}
			if oldest.Valid {
				qs.OldestAvailable = max(qs.OldestAvailable, now.Sub(oldest.time()))
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows, err = q.QueryContext(ctx, `
			SELECT q.name, q.paused_at IS NOT NULL,
			  (SELECT count(*) FROM hopper_job_history h WHERE h.queue = q.name AND h.state = 'completed' AND h.finalized_at > ?1)
			FROM hopper_queues q`, now.Add(-time.Minute).UnixMicro())
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var (
				name      string
				paused    bool
				completed int
			)
			if err := rows.Scan(&name, &paused, &completed); err != nil {
				return err
			}
			qs := queue(name)
			qs.Paused, qs.CompletedLastMinute = paused, completed
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if err := q.QueryRowContext(ctx, `SELECT count(*) FROM hopper_clients WHERE expires_at > ?`, now.UnixMicro()).Scan(&stats.LiveClients); err != nil {
			return err
		}
		if err := q.QueryRowContext(ctx, `SELECT coalesce((SELECT client_id FROM hopper_leader WHERE name = 'default' AND expires_at > ?), 0)`,
			now.UnixMicro()).Scan(&stats.Leader); err != nil {
			return err
		}
		rows, err = q.QueryContext(ctx, `SELECT attempted_by, count(*) FROM hopper_jobs
			WHERE state = 'running' AND attempted_by IS NOT NULL GROUP BY attempted_by`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			var n int
			if err := rows.Scan(&id, &n); err != nil {
				return err
			}
			stats.RunningByClient[id] = n
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("hopper: stats: %w", err)
	}
	return stats, nil
}

// retentionBatch is how many rows a retention pass deletes per
// transaction, each of which holds every other write up for as long as it
// runs.
const retentionBatch = 1000

// HistoryMaintain implements driver.Executor: retention deletes finished
// jobs and stream events past their retention, a batch per transaction.
// Each batch waits at most params.LockTimeout (lockWaitBrief if zero) for
// the write lock; a group whose batch gives up is reported deferred, and a
// later pass deletes the rest. History has no storage units to create or
// drop.
func (e *executor) HistoryMaintain(ctx context.Context, params driver.HistoryMaintainParams) (driver.HistoryMaintainResult, error) {
	var res driver.HistoryMaintainResult
	if e.tx != nil {
		return res, errors.New("hopper: history maintenance must run on the pool, not in a transaction")
	}
	wait := params.LockTimeout
	if wait <= 0 {
		wait = lockWaitBrief
	}
	groups := []struct {
		name, table, where string
		keep               time.Duration
	}{
		{"hopper_job_history_completed", "hopper_job_history", "state = 'completed' AND finalized_at < ?", params.CompletedRetention},
		{"hopper_job_history_failed", "hopper_job_history", "state IN ('canceled', 'discarded') AND finalized_at < ?", params.FailedRetention},
		{"hopper_stream_events", "hopper_stream_events", "created_at < ?", params.StreamRetention},
	}
	var errs []error
	for _, g := range groups {
		if g.keep <= 0 {
			continue
		}
		for {
			var n int64
			err := e.writeLocking(ctx, wait, func(x *w) error {
				r, err := x.ExecContext(ctx, `DELETE FROM `+g.table+` WHERE rowid IN (
					SELECT rowid FROM `+g.table+` WHERE `+g.where+` LIMIT ?)`, x.at(-g.keep), retentionBatch)
				if err != nil {
					return err
				}
				n, err = r.RowsAffected()
				return err
			})
			if errors.Is(err, errLockBusy) {
				res.Deferred = append(res.Deferred, g.name)
				break
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("hopper: prune %s: %w", g.name, err))
				break
			}
			res.Pruned += n
			if n < retentionBatch {
				break
			}
		}
	}
	return res, errors.Join(errs...)
}

// HistoryIndex implements driver.Executor: history's indexes are built
// with it, so a pass finds nothing to do.
func (e *executor) HistoryIndex(context.Context, driver.HistoryIndexParams) (driver.HistoryIndexResult, error) {
	return driver.HistoryIndexResult{}, nil
}

// maintainedTables are the tables JobsMaintain keeps the planner's
// statistics of: the live table, and the live tags and dependency tables,
// which churn with it.
var maintainedTables = []string{"hopper_jobs", "hopper_job_tags", "hopper_job_deps"}

// JobsMaintain implements driver.Executor. SQLite reuses the pages of
// deleted rows and needs no vacuum, but its planner, like Postgres's,
// chooses its plans by the row counts its statistics hold: a table
// analyzed while it was empty is planned as if it still were. A table is
// analyzed when the count the statistics hold is off by an order of
// magnitude, or it has none and has grown to a thousand rows. An analyze
// reads a sample of each index (analysis_limit), and waits at most
// lockWaitBrief for the write lock; one that cannot get it waits for the
// next pass.
func (e *executor) JobsMaintain(ctx context.Context) (driver.JobsMaintainResult, error) {
	var res driver.JobsMaintainResult
	if e.tx != nil {
		return res, errors.New("hopper: live table maintenance must run on the pool, not in a transaction")
	}
	var errs []error
	for _, table := range maintainedTables {
		believed, err := e.believedRows(ctx, table)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		floor := max(believed, 100)
		// Count up to the point where the counts are an order of magnitude
		// apart, which bounds the count's cost by the table's believed size.
		var live int64
		if err := e.d.sqlDB.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT 1 FROM `+table+` LIMIT ?)`, 10*floor).Scan(&live); err != nil {
			errs = append(errs, fmt.Errorf("hopper: count %s: %w", table, err))
			continue
		}
		stale := live >= 10*floor || (believed >= 0 && believed >= 10*max(live, 100))
		if believed < 0 {
			stale = live >= 1000
		}
		if !stale {
			continue
		}
		err = e.writeBrief(ctx, func(x *w) error {
			_, err := x.ExecContext(ctx, "PRAGMA analysis_limit = 1000; ANALYZE "+table)
			return err
		})
		switch {
		case errors.Is(err, errLockBusy):
		case err != nil:
			errs = append(errs, fmt.Errorf("hopper: analyze %s: %w", table, err))
		default:
			res.Analyzed = true
		}
	}
	if err := e.truncateLog(ctx); err != nil {
		errs = append(errs, err)
	}
	return res, errors.Join(errs...)
}

// walTruncateSize is the size of write-ahead log past which JobsMaintain
// checkpoints it and truncates it: twice what SQLite's automatic
// checkpoints, every thousand pages, let it reach.
const walTruncateSize = 8 << 20

// truncateLog checkpoints the write-ahead log and truncates it to nothing
// once it is past walTruncateSize. The log grows past that while a reader
// keeps a snapshot from before the checkpoints, which then cannot start it
// over, and SQLite cuts it back only to journalSizeLimit once one can. The
// checkpoint waits for writers and readers at most busyQuantum each, and
// gives up, to be tried at the next pass, if they are still there.
func (e *executor) truncateLog(ctx context.Context) error {
	st, err := os.Stat(e.d.db.path + "-wal")
	if err != nil || st.Size() <= walTruncateSize {
		return nil //nolint:nilerr // no log, or none to speak of
	}
	var busy, logPages, checkpointed int64
	if err := e.d.sqlDB.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logPages, &checkpointed); err != nil {
		return fmt.Errorf("hopper: checkpoint the write-ahead log: %w", err)
	}
	return nil
}

// believedRows returns the row count the planner's statistics hold for a
// table, or -1 if it has none.
func (e *executor) believedRows(ctx context.Context, table string) (int64, error) {
	var hasStats bool
	if err := e.d.sqlDB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'sqlite_stat1')`).Scan(&hasStats); err != nil {
		return 0, err
	}
	if !hasStats {
		return -1, nil
	}
	// Each index's row starts with its count of rows; a partial index
	// counts only its own, and the table's other indexes all of them.
	var n sql.NullInt64
	if err := e.d.sqlDB.QueryRowContext(ctx, `SELECT max(CAST(stat AS INTEGER)) FROM sqlite_stat1 WHERE tbl = ?`, table).Scan(&n); err != nil {
		return 0, err
	}
	if !n.Valid {
		return -1, nil
	}
	return n.Int64, nil
}

// AnalyzeForTest analyzes the live table as it is, for the conformance
// suite to give the planner statistics of a known state.
func (e *executor) AnalyzeForTest(ctx context.Context) error {
	return e.write(ctx, func(x *w) error {
		_, err := x.ExecContext(ctx, "ANALYZE hopper_jobs")
		return err
	})
}
