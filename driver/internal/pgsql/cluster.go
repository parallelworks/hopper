package pgsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// ClientRegister implements driver.Executor.
func (e *Executor) ClientRegister(ctx context.Context, params driver.ClientRegisterParams) (int64, error) {
	var id int64
	err := e.Conn.QueryRow(ctx,
		`INSERT INTO hopper_clients (hostname, expires_at, info)
		 VALUES ($1, now() + make_interval(secs => $2::float8), $3::jsonb) RETURNING id`,
		params.Hostname, params.TTL.Seconds(), jsonOrEmptyObject(params.Info),
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("hopper: register client: %w", err)
	}
	return id, nil
}

// clientRenewSQL renews the lease and, in the same round trip, reads the
// control state the client must apply: cancel requests for its running jobs
// (through the running index), paused queues and limited queues.
const clientRenewSQL = `
WITH renewed AS (
  UPDATE hopper_clients SET expires_at = now() + make_interval(secs => $2::float8)
  WHERE id = $1 AND expires_at > now()
  RETURNING id
)
SELECT EXISTS (SELECT 1 FROM renewed),
       (SELECT string_agg(id::text, $3) FROM hopper_jobs
         WHERE state = 'running' AND attempted_by = $1 AND cancel_requested_at IS NOT NULL),
       (SELECT string_agg(name, $3) FROM hopper_queues WHERE paused_at IS NOT NULL),
       (SELECT string_agg(name, $3) FROM hopper_queues
         WHERE global_limit IS NOT NULL OR rate_per_sec IS NOT NULL OR partition_limit IS NOT NULL)`

// ClientRenew implements driver.Executor.
func (e *Executor) ClientRenew(ctx context.Context, params driver.ClientRenewParams) (driver.ClientRenewResult, error) {
	var (
		res                      driver.ClientRenewResult
		cancels, paused, limited joined
	)
	err := e.Conn.QueryRow(ctx, clientRenewSQL, params.ClientID, params.TTL.Seconds(), joinSep).Scan(&res.Renewed, &cancels, &paused, &limited)
	if err != nil {
		return res, fmt.Errorf("hopper: renew client lease: %w", err)
	}
	for _, s := range cancels {
		id, err := driver.ParseJobID(s)
		if err != nil {
			return res, fmt.Errorf("hopper: renew client lease: %w", err)
		}
		res.CancelRequested = append(res.CancelRequested, id)
	}
	res.PausedQueues, res.LimitedQueues = paused, limited
	return res, nil
}

// ClientDelete implements driver.Executor.
func (e *Executor) ClientDelete(ctx context.Context, clientID int64) error {
	if _, err := e.Conn.Exec(ctx, `DELETE FROM hopper_clients WHERE id = $1`, clientID); err != nil {
		return fmt.Errorf("hopper: delete client: %w", err)
	}
	return nil
}

// ClientPruneExpired implements driver.Executor.
func (e *Executor) ClientPruneExpired(ctx context.Context) (int64, error) {
	n, err := e.Conn.Exec(ctx, `DELETE FROM hopper_clients WHERE expires_at < now()`)
	if err != nil {
		return 0, fmt.Errorf("hopper: prune clients: %w", err)
	}
	return n, nil
}

// ClientList implements driver.Executor.
func (e *Executor) ClientList(ctx context.Context) ([]*driver.ClientRow, error) {
	clients, err := collect(func(r Row) (*driver.ClientRow, error) {
		var (
			c    driver.ClientRow
			info jsonText
		)
		err := r.Scan(&c.ID, &c.Hostname, &c.StartedAt, &c.ExpiresAt, &info)
		c.Info = info.raw()
		return &c, err
	})(e.Conn.Query(ctx, `SELECT id, hostname, started_at, expires_at, info FROM hopper_clients ORDER BY id`))
	if err != nil {
		return nil, fmt.Errorf("hopper: list clients: %w", err)
	}
	return clients, nil
}

// leaderSQL takes the lease if it is free or expired, or renews it for the
// holder. No row is returned when another client holds it.
const leaderSQL = `
INSERT INTO hopper_leader (name, client_id, elected_at, expires_at)
VALUES ('default', $1, now(), now() + make_interval(secs => $2::float8))
ON CONFLICT (name) DO UPDATE SET
  client_id  = EXCLUDED.client_id,
  elected_at = CASE WHEN hopper_leader.client_id = EXCLUDED.client_id THEN hopper_leader.elected_at ELSE now() END,
  expires_at = EXCLUDED.expires_at
WHERE hopper_leader.expires_at < now() OR hopper_leader.client_id = EXCLUDED.client_id
RETURNING client_id`

// LeaderAttempt implements driver.Executor.
func (e *Executor) LeaderAttempt(ctx context.Context, params driver.LeaderParams) (bool, error) {
	var id int64
	err := e.Conn.QueryRow(ctx, leaderSQL, params.ClientID, params.TTL.Seconds()).Scan(&id)
	if errors.Is(err, ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("hopper: leader attempt: %w", err)
	}
	return id == params.ClientID, nil
}

// leaderResignSQL deletes the lease if this client holds it and, only then,
// signals other clients to elect a replacement.
const leaderResignSQL = `
WITH resigned AS (
  DELETE FROM hopper_leader WHERE name = 'default' AND client_id = $1 RETURNING client_id
)
SELECT pg_notify($2, 'resigned') FROM resigned`

// LeaderResign implements driver.Executor.
func (e *Executor) LeaderResign(ctx context.Context, clientID int64) error {
	if _, err := e.Conn.Exec(ctx, leaderResignSQL, clientID, driver.ChannelLeader); err != nil {
		return fmt.Errorf("hopper: leader resign: %w", err)
	}
	return nil
}

// QueueEnsure implements driver.Executor.
func (e *Executor) QueueEnsure(ctx context.Context, names []string) error {
	if len(names) == 0 {
		return nil
	}
	_, err := e.Conn.Exec(ctx, `INSERT INTO hopper_queues (name) SELECT unnest($1::text[]) ON CONFLICT (name) DO NOTHING`, textArray(names))
	if err != nil {
		return fmt.Errorf("hopper: ensure queues: %w", err)
	}
	return nil
}

// QueuePause implements driver.Executor.
func (e *Executor) QueuePause(ctx context.Context, name string) error {
	return e.setPaused(ctx, name, true)
}

// QueueResume implements driver.Executor.
func (e *Executor) QueueResume(ctx context.Context, name string) error {
	return e.setPaused(ctx, name, false)
}

func (e *Executor) setPaused(ctx context.Context, name string, paused bool) error {
	action, stmt := "resume", `INSERT INTO hopper_queues (name) VALUES ($1)
		ON CONFLICT (name) DO UPDATE SET paused_at = NULL, updated_at = now()`
	if paused {
		action, stmt = "pause", `INSERT INTO hopper_queues (name, paused_at) VALUES ($1, now())
		ON CONFLICT (name) DO UPDATE SET paused_at = coalesce(hopper_queues.paused_at, now()), updated_at = now()`
	}
	err := e.withTx(ctx, func(tx Tx) error {
		if _, err := tx.Exec(ctx, stmt, name); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, NotifySQL, driver.ChannelControl, textArray([]string{action + ":" + name}))
		return err
	})
	if err != nil {
		return fmt.Errorf("hopper: %s queue: %w", action, err)
	}
	return nil
}

// QueueList implements driver.Executor.
func (e *Executor) QueueList(ctx context.Context) ([]*driver.QueueRow, error) {
	queues, err := collect(func(r Row) (*driver.QueueRow, error) {
		var (
			q                               driver.QueueRow
			paused                          sql.NullTime
			global, burst, partition, aging sql.NullInt64
			rate                            sql.NullFloat64
		)
		if err := r.Scan(&q.Name, &paused, &q.UpdatedAt, &global, &rate, &burst, &partition, &aging); err != nil {
			return nil, err
		}
		q.PausedAt = paused.Time
		q.Limits = driver.QueueLimits{
			Name: q.Name, GlobalLimit: int(global.Int64), RatePerSec: rate.Float64, RateBurst: int(burst.Int64),
			PartitionLimit: int(partition.Int64), Aging: time.Duration(aging.Int64) * time.Second,
		}
		return &q, nil
	})(e.Conn.Query(ctx, `SELECT name, paused_at, updated_at, global_limit, rate_per_sec, rate_burst, partition_limit, aging_seconds
		FROM hopper_queues ORDER BY name`))
	if err != nil {
		return nil, fmt.Errorf("hopper: list queues: %w", err)
	}
	return queues, nil
}

// QueueSetLimits implements driver.Executor.
func (e *Executor) QueueSetLimits(ctx context.Context, l driver.QueueLimits) error {
	err := e.withTx(ctx, func(tx Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO hopper_queues (name, global_limit, rate_per_sec, rate_burst, partition_limit, aging_seconds, tokens, refilled_at)
			VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL)
			ON CONFLICT (name) DO UPDATE SET global_limit = EXCLUDED.global_limit, rate_per_sec = EXCLUDED.rate_per_sec,
			  rate_burst = EXCLUDED.rate_burst, partition_limit = EXCLUDED.partition_limit, aging_seconds = EXCLUDED.aging_seconds,
			  tokens = CASE WHEN hopper_queues.rate_per_sec IS DISTINCT FROM EXCLUDED.rate_per_sec THEN NULL ELSE hopper_queues.tokens END,
			  updated_at = now()`,
			l.Name, nullableInt(l.GlobalLimit), nullableFloat(l.RatePerSec), nullableInt(l.RateBurst), nullableInt(l.PartitionLimit), nullableInt(int(l.Aging/time.Second)))
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, NotifySQL, driver.ChannelControl, textArray([]string{"limit:" + l.Name}))
		return err
	})
	if err != nil {
		return fmt.Errorf("hopper: set queue limits: %w", err)
	}
	return nil
}

// periodicSlotSQL advances a periodic job's last slot only if the slot is
// newer, so the job for a slot is inserted at most once, whichever leader
// gets there.
const periodicSlotSQL = `
INSERT INTO hopper_periodic (name, last_slot) VALUES ($1, $2)
ON CONFLICT (name) DO UPDATE SET last_slot = EXCLUDED.last_slot
WHERE hopper_periodic.last_slot < EXCLUDED.last_slot
RETURNING name`

// PeriodicInsert implements driver.Executor.
func (e *Executor) PeriodicInsert(ctx context.Context, params driver.PeriodicInsertParams) (*driver.JobRow, bool, error) {
	var (
		job      *driver.JobRow
		inserted bool
	)
	err := e.withTx(ctx, func(tx Tx) error {
		var name string
		err := tx.QueryRow(ctx, periodicSlotSQL, params.Name, params.Slot).Scan(&name)
		if errors.Is(err, ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		results, err := (&Executor{Conn: tx, InTx: true}).JobInsertMany(ctx, []driver.JobInsertParams{params.Job}, driver.JobInsertOpts{Notify: true})
		if err != nil {
			return err
		}
		job, inserted = results[0].Job, !results[0].Duplicate
		return nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("hopper: insert periodic job %s: %w", params.Name, err)
	}
	return job, inserted, nil
}

// PeriodicLastSlots implements driver.Executor.
func (e *Executor) PeriodicLastSlots(ctx context.Context) (map[string]time.Time, error) {
	type slot struct {
		name string
		at   time.Time
	}
	slots, err := collect(func(r Row) (slot, error) {
		var s slot
		err := r.Scan(&s.name, &s.at)
		return s, err
	})(e.Conn.Query(ctx, `SELECT name, last_slot FROM hopper_periodic`))
	if err != nil {
		return nil, fmt.Errorf("hopper: periodic slots: %w", err)
	}
	out := make(map[string]time.Time, len(slots))
	for _, s := range slots {
		out[s.name] = s.at
	}
	return out, nil
}

// statsSQL gathers everything in one round trip: depth by queue and state
// from the live table, throughput from the history index per known queue,
// and cluster state.
const statsSQL = `
SELECT
  (SELECT coalesce(jsonb_agg(jsonb_build_object('queue', queue, 'state', state, 'count', count, 'oldest', oldest)), '[]')
   FROM (
     SELECT queue, state::text AS state, count(*) AS count,
            extract(epoch FROM now() - min(scheduled_at) FILTER (WHERE state IN ('available', 'scheduled', 'retryable') AND scheduled_at <= now())) AS oldest
     FROM hopper_jobs GROUP BY queue, state
   ) d)::text,
  (SELECT coalesce(jsonb_agg(jsonb_build_object('queue', q.name, 'paused', q.paused_at IS NOT NULL, 'completed',
     (SELECT count(*) FROM hopper_job_history h WHERE h.queue = q.name AND h.state = 'completed' AND h.finalized_at > now() - interval '1 minute'))), '[]')
   FROM hopper_queues q)::text,
  (SELECT count(*) FROM hopper_clients WHERE expires_at > now()),
  (SELECT coalesce((SELECT client_id FROM hopper_leader WHERE name = 'default' AND expires_at > now()), 0)),
  (SELECT coalesce(jsonb_object_agg(attempted_by, count), '{}') FROM (
     SELECT attempted_by, count(*) AS count FROM hopper_jobs WHERE state = 'running' AND attempted_by IS NOT NULL GROUP BY attempted_by) r)::text`

// Stats implements driver.Executor.
func (e *Executor) Stats(ctx context.Context) (*driver.Stats, error) {
	var (
		depthsJSON, queuesJSON, runningJSON jsonText
		stats                               = &driver.Stats{Queues: map[string]*driver.QueueStats{}, RunningByClient: map[int64]int{}}
	)
	if err := e.Conn.QueryRow(ctx, statsSQL).Scan(&depthsJSON, &queuesJSON, &stats.LiveClients, &stats.Leader, &runningJSON); err != nil {
		return nil, fmt.Errorf("hopper: stats: %w", err)
	}
	var depths []struct {
		Queue  string   `json:"queue"`
		State  string   `json:"state"`
		Count  int      `json:"count"`
		Oldest *float64 `json:"oldest"`
	}
	var queues []struct {
		Queue     string `json:"queue"`
		Paused    bool   `json:"paused"`
		Completed int    `json:"completed"`
	}
	running := map[string]int{}
	if err := errors.Join(json.Unmarshal([]byte(depthsJSON), &depths), json.Unmarshal([]byte(queuesJSON), &queues), json.Unmarshal([]byte(runningJSON), &running)); err != nil {
		return nil, fmt.Errorf("hopper: stats: %w", err)
	}
	queue := func(name string) *driver.QueueStats {
		q := stats.Queues[name]
		if q == nil {
			q = &driver.QueueStats{}
			stats.Queues[name] = q
		}
		return q
	}
	for _, d := range depths {
		q := queue(d.Queue)
		switch driver.JobState(d.State) {
		case driver.JobStateAvailable:
			q.Available = d.Count
		case driver.JobStateScheduled:
			q.Scheduled = d.Count
		case driver.JobStateRetryable:
			q.Retryable = d.Count
		case driver.JobStateRunning:
			q.Running = d.Count
		case driver.JobStatePending, driver.JobStateCompleted, driver.JobStateCancelled, driver.JobStateDiscarded:
		}
		if d.Oldest != nil {
			if age := time.Duration(*d.Oldest * float64(time.Second)); age > q.OldestAvailable {
				q.OldestAvailable = age
			}
		}
	}
	for _, r := range queues {
		q := queue(r.Queue)
		q.Paused = r.Paused
		q.CompletedLastMinute = r.Completed
	}
	for id, n := range running {
		if clientID, err := strconv.ParseInt(id, 10, 64); err == nil {
			stats.RunningByClient[clientID] = n
		}
	}
	return stats, nil
}
