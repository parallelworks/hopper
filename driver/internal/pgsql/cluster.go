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
		`INSERT INTO {{schema}}.hopper_clients (hostname, expires_at, info)
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
  UPDATE {{schema}}.hopper_clients SET expires_at = now() + make_interval(secs => $2::float8)
  WHERE id = $1 AND expires_at > now()
  RETURNING id
)
SELECT EXISTS (SELECT 1 FROM renewed),
       (SELECT string_agg(id::text, $3) FROM {{schema}}.hopper_jobs
         WHERE state = 'running' AND attempted_by = $1 AND cancel_requested_at IS NOT NULL),
       (SELECT string_agg(name, $3) FROM {{schema}}.hopper_queues WHERE paused_at IS NOT NULL),
       (SELECT string_agg(name, $3) FROM {{schema}}.hopper_queues
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
	if _, err := e.Conn.Exec(ctx, `
		WITH gone AS (DELETE FROM {{schema}}.hopper_clients WHERE id = $1)
		DELETE FROM {{schema}}.hopper_periodic_registrations WHERE client_id = $1`, clientID); err != nil {
		return fmt.Errorf("hopper: delete client: %w", err)
	}
	return nil
}

// ClientPruneExpired implements driver.Executor.
func (e *Executor) ClientPruneExpired(ctx context.Context) (int64, error) {
	var n int64
	err := e.Conn.QueryRow(ctx, `
		WITH gone AS (DELETE FROM {{schema}}.hopper_clients WHERE expires_at < now() RETURNING id),
		registrations AS (DELETE FROM {{schema}}.hopper_periodic_registrations WHERE client_id IN (SELECT id FROM gone))
		SELECT count(*) FROM gone`).Scan(&n)
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
	})(e.Conn.Query(ctx, `SELECT id, hostname, started_at, expires_at, info FROM {{schema}}.hopper_clients ORDER BY id`))
	if err != nil {
		return nil, fmt.Errorf("hopper: list clients: %w", err)
	}
	return clients, nil
}

// leaderSQL takes the lease if it is free or expired, or renews it for the
// holder. No row is returned when another client holds it.
const leaderSQL = `
INSERT INTO {{schema}}.hopper_leader (name, client_id, elected_at, expires_at)
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
  DELETE FROM {{schema}}.hopper_leader WHERE name = 'default' AND client_id = $1 RETURNING client_id
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
	_, err := e.Conn.Exec(ctx, `INSERT INTO {{schema}}.hopper_queues (name) SELECT unnest($1::text[]) ON CONFLICT (name) DO NOTHING`, textArray(names))
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
	action, stmt := "resume", `INSERT INTO {{schema}}.hopper_queues (name) VALUES ($1)
		ON CONFLICT (name) DO UPDATE SET paused_at = NULL, updated_at = now()`
	if paused {
		action, stmt = "pause", `INSERT INTO {{schema}}.hopper_queues (name, paused_at) VALUES ($1, now())
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
		FROM {{schema}}.hopper_queues ORDER BY name`))
	if err != nil {
		return nil, fmt.Errorf("hopper: list queues: %w", err)
	}
	return queues, nil
}

// QueueSetLimits implements driver.Executor.
func (e *Executor) QueueSetLimits(ctx context.Context, l driver.QueueLimits) error {
	err := e.withTx(ctx, func(tx Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO {{schema}}.hopper_queues (name, global_limit, rate_per_sec, rate_burst, partition_limit, aging_seconds, tokens, refilled_at)
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
INSERT INTO {{schema}}.hopper_periodic (name, last_slot) VALUES ($1, $2)
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
	})(e.Conn.Query(ctx, `SELECT name, last_slot FROM {{schema}}.hopper_periodic`))
	if err != nil {
		return nil, fmt.Errorf("hopper: periodic slots: %w", err)
	}
	out := make(map[string]time.Time, len(slots))
	for _, s := range slots {
		out[s.name] = s.at
	}
	return out, nil
}

// PeriodicRegister implements driver.Executor.
func (e *Executor) PeriodicRegister(ctx context.Context, params driver.PeriodicRegisterParams) error {
	names := make([]string, len(params.Registrations))
	definitions := make([]string, len(params.Registrations))
	for i, r := range params.Registrations {
		names[i], definitions[i] = r.Name, string(r.Definition)
	}
	err := e.withTx(ctx, func(tx Tx) error {
		if _, err := tx.Exec(ctx, `
			DELETE FROM {{schema}}.hopper_periodic_registrations r
			WHERE r.client_id = $1 OR NOT EXISTS (SELECT 1 FROM {{schema}}.hopper_clients c WHERE c.id = r.client_id)`, params.ClientID); err != nil {
			return err
		}
		if len(names) == 0 {
			return nil
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO {{schema}}.hopper_periodic_registrations (client_id, name, definition)
			SELECT $1, n, d FROM unnest($2::text[], $3::text[]) AS r(n, d)
			WHERE EXISTS (SELECT 1 FROM {{schema}}.hopper_clients WHERE id = $1 AND expires_at > now())`,
			params.ClientID, textArray(names), textArray(definitions))
		return err
	})
	if err != nil {
		return fmt.Errorf("hopper: register periodic jobs: %w", err)
	}
	return nil
}

// PeriodicRegistrations implements driver.Executor.
func (e *Executor) PeriodicRegistrations(ctx context.Context) ([]driver.PeriodicRegistrationRow, error) {
	rows, err := collect(func(r Row) (driver.PeriodicRegistrationRow, error) {
		var (
			row        driver.PeriodicRegistrationRow
			definition jsonText
		)
		err := r.Scan(&row.ClientID, &row.ClientStartedAt, &row.Name, &definition)
		row.Definition = definition.raw()
		return row, err
	})(e.Conn.Query(ctx, `
		SELECT c.id, c.started_at, r.name, r.definition
		FROM {{schema}}.hopper_periodic_registrations r
		JOIN {{schema}}.hopper_clients c ON c.id = r.client_id
		WHERE c.expires_at > now()
		ORDER BY r.name, c.started_at DESC, c.id DESC`))
	if err != nil {
		return nil, fmt.Errorf("hopper: periodic registrations: %w", err)
	}
	return rows, nil
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
     FROM {{schema}}.hopper_jobs GROUP BY queue, state
   ) d)::text,
  (SELECT coalesce(jsonb_agg(jsonb_build_object('queue', q.name, 'paused', q.paused_at IS NOT NULL, 'completed',
     (SELECT count(*) FROM {{schema}}.hopper_job_history h WHERE h.queue = q.name AND h.state = 'completed' AND h.finalized_at > now() - interval '1 minute'))), '[]')
   FROM {{schema}}.hopper_queues q)::text,
  (SELECT count(*) FROM {{schema}}.hopper_clients WHERE expires_at > now()),
  (SELECT coalesce((SELECT client_id FROM {{schema}}.hopper_leader WHERE name = 'default' AND expires_at > now()), 0)),
  (SELECT coalesce(jsonb_object_agg(attempted_by, count), '{}') FROM (
     SELECT attempted_by, count(*) AS count FROM {{schema}}.hopper_jobs WHERE state = 'running' AND attempted_by IS NOT NULL GROUP BY attempted_by) r)::text`

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
		case driver.JobStatePending, driver.JobStateCompleted, driver.JobStateCanceled, driver.JobStateDiscarded:
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

// vacuumDeadRows is how many dead rows the live table may accumulate
// before the leader vacuums it.
const vacuumDeadRows = 100000

// analyzeEvery is the least time between statistics refreshes driven by
// churn alone.
const analyzeEvery = 30 * time.Second

// JobsMaintain implements driver.Executor. A vacuum of the live table
// reclaims the index entries every claim would otherwise walk past. What
// matters is their number, not their share of the table: a hundred
// thousand dead entries are about five hundred index pages, well under a
// millisecond per claim, so the vacuum runs once that many have
// accumulated, and a short burst never triggers one.
//
// ANALYZE keeps two things current that the planner needs: the row count,
// which a plan cached while the table was empty gets wrong by orders of
// magnitude, and the mix of states, from which the planner costs scans of
// the partial indexes by state. (The finalize does not depend on it: it
// compares the running state as text; see jobFinalizeHeadSQL.)
// So it runs at once when the planner's row count is ten times off, and
// otherwise once a tenth of the table has changed but no more than every
// thirty seconds, since an analyze invalidates every connection's cached
// plans and a CPU-starved server feels the replanning. The sample is kept
// small; nothing here needs fine histograms. Both skip, rather than wait,
// when autovacuum holds the lock, since that pass does the same work.
//
// The live tags table churns with tagged jobs, and a listing by tag walks
// its index, so it is kept the same way, except that its analyze keeps the
// server's statistics target. So is the dependency table, which churns
// with workflows, except that churn re-analyzes it at most every
// analyzeDepsEvery: until it has statistics, the planner takes each lookup of
// a job's edges for a two-hundredth of the table, and scans the table
// instead, in finalizes and in the sweep that promotes dependents (see
// finalizeTailSQL). The counters of all three are read in one statement,
// each table's in a LATERAL subquery that OFFSET 0 keeps from being
// flattened: joined, the view's GROUP BY kept the table's OID from reaching
// pg_class, so every pass summed the counters of every relation in the
// database. Each table is kept whether or not another's
// maintenance fails, such as a vacuum of a large hopper_jobs that runs past
// the role's statement_timeout on every pass.
func (e *Executor) JobsMaintain(ctx context.Context) (driver.JobsMaintainResult, error) {
	var res driver.JobsMaintainResult
	if e.InTx {
		return res, errors.New("hopper: live table maintenance must run on the pool, not in a transaction")
	}
	names := make([]string, len(liveTables))
	for i, t := range liveTables {
		names[i] = e.Schema.Table(t.name)
	}
	all, err := scanLiveTableStats(e.Conn.Query(ctx, `
		SELECT n, s.n_dead_tup, s.n_live_tup, s.n_mod_since_analyze, s.reltuples,
		  coalesce(extract(epoch FROM now() - greatest(s.last_analyze, s.last_autoanalyze)), 1e9)
		FROM unnest($1::text[]) n, LATERAL (
		  SELECT s.n_dead_tup, s.n_live_tup, s.n_mod_since_analyze, s.last_analyze, s.last_autoanalyze, c.reltuples
		  FROM pg_stat_user_tables s JOIN pg_class c ON c.oid = s.relid
		  WHERE s.relid = to_regclass(n) OFFSET 0) s`, textArray(names)))
	if err != nil {
		return res, fmt.Errorf("hopper: live table statistics: %w", err)
	}
	stats := make(map[string]liveTableStats, len(all))
	for _, st := range all {
		stats[st.table] = st
	}
	var errs []error
	for i, t := range liveTables {
		st, ok := stats[names[i]]
		if !ok {
			continue
		}
		vacuumed, analyzed, err := e.maintainLiveTable(ctx, t, names[i], st)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		res.Vacuumed = res.Vacuumed || vacuumed
		res.Analyzed = res.Analyzed || analyzed
	}
	return res, errors.Join(errs...)
}

// liveTableStats are a live table's activity counters, the row count the
// planner holds, and the time since its last analyze (1e9 s if never: a
// vacuum, or an index built over existing rows, also sets the row count,
// so a count that is known does not mean the table has statistics).
type liveTableStats struct {
	table                  string
	dead, live, modified   int64
	believed, sinceAnalyze float64
}

var scanLiveTableStats = collect(func(r Row) (liveTableStats, error) {
	var st liveTableStats
	err := r.Scan(&st.table, &st.dead, &st.live, &st.modified, &st.believed, &st.sinceAnalyze)
	return st, err
})

// liveTables are the tables JobsMaintain keeps, with the statistics target
// their analyze uses, or zero for the server's. The tags table keeps the
// server's target: a listing by several tags (rarestTagSQL) tells common
// tags apart by its most common values, of which a target of 10 would keep
// only ten. So does the dependency table, whose estimates rest on how many
// distinct jobs its sample holds, which a larger sample counts better.
var liveTables = []liveTable{
	{"hopper_jobs", 10, analyzeEvery},
	{"hopper_job_tags", 0, analyzeEvery},
	{"hopper_job_deps", 0, analyzeDepsEvery},
}

// analyzeDepsEvery is the least time between the dependency table's
// statistics refreshes driven by churn. What the planner reads from it,
// how many edges a job has and how many jobs depend on one, changes only
// as the shape of the workflows does, so a refresh now and then corrects a
// first analyze that sampled the table empty or holding one large fan-out,
// which has the planner scan it on every lookup. The cap bounds the cost
// of sampling the table, not replanning: analyzes of hopper_jobs make
// every connection replan the finalize statement at least as often, and
// autovacuum's own analyzes, at the table's default thresholds, keep the
// statistics fresh in between. A table never analyzed is analyzed without
// the wait, once a thousand of its rows have changed: until then it is
// small enough that a scan costs little.
const analyzeDepsEvery = 10 * time.Minute

// liveTable is a table JobsMaintain keeps, with its analyze's statistics
// target (zero for the server's) and the least time between analyzes
// driven by churn.
type liveTable struct {
	name             string
	statisticsTarget int
	analyzeEvery     time.Duration
}

func (e *Executor) maintainLiveTable(ctx context.Context, t liveTable, table string, st liveTableStats) (vacuumed, analyzed bool, err error) {
	if st.dead >= vacuumDeadRows {
		// TRUNCATE false: a vacuum otherwise ends by trying, for up to five
		// seconds, to take an exclusive lock to give back empty trailing
		// pages, and whenever it briefly gets one every claim and finalize
		// waits behind it. A churning live table reuses those pages within
		// seconds anyway.
		if _, err := e.Conn.Exec(ctx, "VACUUM (SKIP_LOCKED, TRUNCATE false) "+table); err != nil {
			return false, false, fmt.Errorf("hopper: vacuum %s: %w", t.name, err)
		}
		return true, false, nil
	}
	floor := func(v float64) float64 { return max(v, 100) }
	sizeOff := st.believed >= 0 && (float64(st.live) >= 10*floor(st.believed) || st.believed >= 10*floor(float64(st.live)))
	churned := st.modified >= 1000 && st.modified*10 >= st.live && st.sinceAnalyze >= t.analyzeEvery.Seconds()
	if !sizeOff && !churned {
		return false, false, nil
	}
	err = e.withTx(ctx, func(tx Tx) error {
		if t.statisticsTarget > 0 {
			if _, err := tx.Exec(ctx, "SET LOCAL default_statistics_target = "+strconv.Itoa(t.statisticsTarget)); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, "ANALYZE (SKIP_LOCKED) "+table)
		return err
	})
	if err != nil {
		return false, false, fmt.Errorf("hopper: analyze %s: %w", t.name, err)
	}
	return false, true, nil
}

// AnalyzeForTest analyzes the live table as it is, for the conformance
// suite to give the planner statistics of a known state.
func (e *Executor) AnalyzeForTest(ctx context.Context) error {
	_, err := e.Conn.Exec(ctx, "ANALYZE {{schema}}.hopper_jobs")
	return err
}
