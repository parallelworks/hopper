package hoppersqlite

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/internal/jobcodec"
	"github.com/parallelworks/hopper/internal/jobtags"
)

func bytesReader(s string) *strings.Reader { return strings.NewReader(s) }

// jobInsert is a job to insert, its values encoded.
type jobInsert struct {
	kind, queue               string
	priority, maxAttempts     int
	scheduledAt               time.Time
	args, metadata            string
	tags                      []string
	uniqueKey                 string
	uniqueFinished            []driver.JobState
	orderingKey, partitionKey string
	batchID                   driver.JobID
	ttl                       time.Duration
	await                     bool
	// pending starts the job waiting on workflow dependencies, and id
	// gives it its ID; a zero id has one minted.
	pending bool
	id      driver.JobID
}

func encodeInsert(p driver.JobInsertParams) (jobInsert, error) {
	priority, err := smallint(p.Priority, "priority")
	if err != nil {
		return jobInsert{}, err
	}
	maxAttempts, err := smallint(p.MaxAttempts, "max_attempts")
	if err != nil {
		return jobInsert{}, err
	}
	args, err := jsonValue(p.Args, "{}", "args")
	if err != nil {
		return jobInsert{}, err
	}
	metadata, err := jsonValue(p.Metadata, "{}", "metadata")
	if err != nil {
		return jobInsert{}, err
	}
	return jobInsert{
		kind: p.Kind, queue: p.Queue, priority: priority, maxAttempts: maxAttempts, scheduledAt: p.ScheduledAt,
		args: args, metadata: metadata, tags: p.Tags, uniqueKey: p.UniqueKey, uniqueFinished: p.UniqueFinished,
		orderingKey: p.OrderingKey, partitionKey: p.PartitionKey, batchID: p.BatchID, ttl: p.TTL, await: p.Await,
	}, nil
}

// insertJobSQL inserts a job and returns it as stored.
const insertJobSQL = `
INSERT INTO hopper_jobs (id, kind, queue, state, priority, max_attempts, scheduled_at, args, metadata, unique_key,
  expires_at, await, ordering_key, partition_key, batch_id, tags, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING ` + jobColumns

// insertJob inserts a job: pending, or scheduled if its time is ahead, or
// available.
func (x *w) insertJob(ctx context.Context, j jobInsert) (*driver.JobRow, error) {
	scheduledAt := j.scheduledAt
	if scheduledAt.IsZero() {
		scheduledAt = x.now
	}
	state := driver.JobStateAvailable
	switch {
	case j.pending:
		state = driver.JobStatePending
	case scheduledAt.After(x.now):
		state = driver.JobStateScheduled
	}
	var expires any
	if j.ttl > 0 {
		expires = x.at(j.ttl)
	}
	id := j.id
	if id.IsZero() {
		id = newID(x.now)
	}
	return scanJob(x.QueryRowContext(ctx, insertJobSQL,
		idParam(id), j.kind, j.queue, string(state), j.priority, j.maxAttempts, scheduledAt.UnixMicro(), j.args, j.metadata,
		nullable(j.uniqueKey), expires, j.await, nullable(j.orderingKey), nullable(j.partitionKey), nullableID(j.batchID),
		tagsJSON(j.tags), x.us()), false)
}

// JobInsertMany implements driver.Executor. An input with a unique key is
// first looked for among live jobs, which with ConflictReplace it replaces
// unless running, and then, with UniqueFinished, in history: a job found
// in either is the input's duplicate, and nothing is inserted for it. The
// write lock keeps both looks and the insert free of any other writer.
func (e *executor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams, opts driver.JobInsertOpts) ([]driver.JobInsertResult, error) {
	if len(params) == 0 {
		return nil, nil
	}
	jobs := make([]jobInsert, len(params))
	for i, p := range params {
		var err error
		if jobs[i], err = encodeInsert(p); err != nil {
			return nil, err
		}
	}
	var results []driver.JobInsertResult
	err := e.write(ctx, func(x *w) error {
		var err error
		if results, err = x.insertJobs(ctx, jobs, opts.OnConflict); err != nil {
			return err
		}
		if opts.Notify {
			return x.notify(ctx, driver.ChannelInsert, distinctQueues(jobs)...)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("hopper: insert jobs: %w", err)
	}
	return results, nil
}

func distinctQueues(jobs []jobInsert) []string {
	var out []string
	for _, j := range jobs {
		if !slices.Contains(out, j.queue) {
			out = append(out, j.queue)
		}
	}
	return out
}

// replaceSQL replaces a waiting job's args, metadata, tags, priority, max
// attempts and schedule, as a replacing insert does, and clears its
// progress and snooze reason.
const replaceSQL = `
UPDATE hopper_jobs SET args = ?, metadata = ?, tags = ?, priority = ?, max_attempts = ?, scheduled_at = ?, state = ?,
  progress = NULL, snooze_reason = NULL
WHERE id = ?
RETURNING ` + jobColumns

func (x *w) insertJobs(ctx context.Context, jobs []jobInsert, onConflict driver.ConflictAction) ([]driver.JobInsertResult, error) {
	results := make([]driver.JobInsertResult, len(jobs))
	for i, j := range jobs {
		if j.uniqueKey != "" {
			dup, err := scanJob(x.QueryRowContext(ctx, "SELECT "+jobColumns+" FROM hopper_jobs WHERE kind = ? AND unique_key = ?", j.kind, j.uniqueKey), false)
			switch {
			case errors.Is(err, sql.ErrNoRows):
			case err != nil:
				return nil, err
			default:
				if onConflict == driver.ConflictReplace && dup.State != driver.JobStateRunning {
					scheduledAt := cmp.Or(j.scheduledAt, x.now)
					state := driver.JobStateAvailable
					if scheduledAt.After(x.now) {
						state = driver.JobStateScheduled
					}
					if dup, err = scanJob(x.QueryRowContext(ctx, replaceSQL, j.args, j.metadata, tagsJSON(j.tags), j.priority, j.maxAttempts,
						scheduledAt.UnixMicro(), string(state), idParam(dup.ID)), false); err != nil {
						return nil, err
					}
				}
				results[i] = driver.JobInsertResult{Job: dup, Duplicate: true}
				continue
			}
			if len(j.uniqueFinished) > 0 {
				found, err := x.finishedDuplicate(ctx, j)
				if err != nil {
					return nil, err
				}
				if found != nil {
					results[i] = driver.JobInsertResult{Job: found, Duplicate: true}
					// The job never joins its batch, which counts it as
					// the outcome it was found in, as its finalize would.
					if !j.batchID.IsZero() {
						failed := 0
						if found.State != driver.JobStateCompleted {
							failed = 1
						}
						if err := x.countBatch(ctx, j.batchID, 1, failed); err != nil {
							return nil, err
						}
					}
					continue
				}
			}
		}
		job, err := x.insertJob(ctx, j)
		if err != nil {
			return nil, err
		}
		results[i] = driver.JobInsertResult{Job: job}
	}
	return results, nil
}

// finishedDuplicate returns the latest job in history with an insert's
// kind and unique key in one of its UniqueFinished states, or nil. Of jobs
// finalized at one time, as jobs of one process are while its clock holds
// the time (clock), the latest is the one inserted last, which is the one
// finalized last: a job with the key can only be inserted once the one
// before it has finished.
func (x *w) finishedDuplicate(ctx context.Context, j jobInsert) (*driver.JobRow, error) {
	args := []any{j.kind, j.uniqueKey}
	for _, s := range j.uniqueFinished {
		args = append(args, string(s))
	}
	found, err := scanJob(x.QueryRowContext(ctx, "SELECT "+historyColumns+` FROM hopper_job_history
		WHERE kind = ? AND unique_key = ? AND state IN (`+placeholders(len(j.uniqueFinished))+`)
		ORDER BY finalized_at DESC, seq DESC LIMIT 1`, args...), true)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return found, err
}

// JobInsertCopy implements driver.Executor: SQLite has no COPY.
func (e *executor) JobInsertCopy(context.Context, []driver.JobInsertParams, driver.JobInsertOpts) ([]driver.JobInsertResult, error) {
	return nil, driver.ErrNotSupported
}

// claimMax is the most jobs one claim returns, as on Postgres.
const claimMax = 512

// claimEligibleSQL is the WHERE clause of a claim, over an alias cand, with
// the time as ?1 and the queue as ?3. A job with an ordering key is
// claimable only if it is the oldest waiting job of its key and no job of
// that key is running.
const claimEligibleSQL = `
    cand.queue = ?3 AND cand.state IN ('available', 'scheduled', 'retryable')
      AND cand.scheduled_at <= ?1
      AND (cand.expires_at IS NULL OR cand.expires_at > ?1)
      AND (cand.ordering_key IS NULL OR (
        NOT EXISTS (SELECT 1 FROM hopper_jobs r
                    WHERE r.queue = cand.queue AND r.ordering_key = cand.ordering_key AND r.state = 'running')
        AND cand.seq = (SELECT min(o.seq) FROM hopper_jobs o
                        WHERE o.queue = cand.queue AND o.ordering_key = cand.ordering_key
                          AND o.state IN ('available', 'scheduled', 'retryable'))))`

// claimSetSQL moves claimed jobs to running, owned by the client ?2 at ?1,
// with their fences incremented.
const claimSetSQL = `
UPDATE hopper_jobs SET state = 'running', attempt = attempt + 1, fence = fence + 1, attempted_at = ?1, attempted_by = ?2,
  snooze_reason = NULL
WHERE seq IN (%s)
RETURNING ` + jobColumns + `, seq`

// claimSQL claims up to ?4 jobs in claim order.
var claimSQL = fmt.Sprintf(claimSetSQL, `SELECT cand.seq FROM hopper_jobs cand WHERE `+claimEligibleSQL+`
  ORDER BY cand.priority, cand.scheduled_at, cand.seq LIMIT ?4`)

// claimPartitionedSQL claims up to ?4 jobs in claim order, admitting only
// those whose rank within their partition key, counting the key's running
// jobs, fits under the partition limit ?5.
var claimPartitionedSQL = fmt.Sprintf(claimSetSQL, `SELECT seq FROM (
  SELECT cand.seq, cand.priority, cand.scheduled_at, cand.partition_key,
    row_number() OVER (PARTITION BY cand.partition_key ORDER BY cand.priority, cand.scheduled_at, cand.seq)
      + (SELECT count(*) FROM hopper_jobs pr
         WHERE pr.queue = cand.queue AND pr.partition_key = cand.partition_key AND pr.state = 'running'
           AND pr.attempted_by IS NOT NULL) AS slot
  FROM hopper_jobs cand WHERE `+claimEligibleSQL+`
) WHERE partition_key IS NULL OR slot <= ?5
ORDER BY priority, scheduled_at, seq LIMIT ?4`)

// claim claims up to limit jobs of a queue for a client, with a partition
// limit if partition is positive, and returns them in claim order.
func (x *w) claim(ctx context.Context, queue string, clientID int64, limit int, partition int64) ([]*driver.JobRow, error) {
	limit = min(limit, claimMax)
	if limit <= 0 {
		return nil, nil
	}
	type claimed struct {
		job *driver.JobRow
		seq int64
	}
	query, args := claimSQL, []any{x.us(), clientID, queue, limit}
	if partition > 0 {
		query, args = claimPartitionedSQL, append(args, partition)
	}
	rows, err := queryAll(ctx, x, func(r scanner) (claimed, error) {
		var c claimed
		var err error
		c.job, err = scanJob(r, false, &c.seq)
		return c, err
	}, query, args...)
	if err != nil {
		return nil, fmt.Errorf("hopper: claim jobs: %w", err)
	}
	slices.SortFunc(rows, func(a, b claimed) int {
		return cmp.Or(cmp.Compare(a.job.Priority, b.job.Priority), a.job.ScheduledAt.Compare(b.job.ScheduledAt), cmp.Compare(a.seq, b.seq))
	})
	jobs := make([]*driver.JobRow, len(rows))
	for i, r := range rows {
		jobs[i] = r.job
	}
	return jobs, nil
}

// JobClaim implements driver.Executor. The write lock makes the claim
// exclusive: no other transaction claims, or changes, a job between the
// statement's read of it and its update.
func (e *executor) JobClaim(ctx context.Context, params driver.JobClaimParams) (driver.JobClaimResult, error) {
	if params.Limit <= 0 {
		return driver.JobClaimResult{}, nil
	}
	if params.Limited {
		return e.claimLimited(ctx, params)
	}
	var jobs []*driver.JobRow
	err := e.write(ctx, func(x *w) error {
		var err error
		jobs, err = x.claim(ctx, params.Queue, params.ClientID, params.Limit, 0)
		return err
	})
	if err != nil {
		return driver.JobClaimResult{}, err
	}
	return driver.JobClaimResult{Jobs: jobs}, nil
}

// JobAge implements driver.Executor. Bumped jobs restart their wait, so a
// job climbs one level per period.
func (e *executor) JobAge(ctx context.Context, queue string, after time.Duration) (int64, error) {
	if after <= 0 {
		return 0, nil
	}
	var n int64
	err := e.write(ctx, func(x *w) error {
		res, err := x.ExecContext(ctx, `
			UPDATE hopper_jobs SET priority = priority - 1, scheduled_at = ?
			WHERE queue = ? AND state IN ('available', 'scheduled', 'retryable') AND priority > 1 AND scheduled_at <= ?`,
			x.us(), queue, x.at(-after))
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("hopper: age jobs: %w", err)
	}
	return n, nil
}

// JobRecordProgress implements driver.Executor. With NoWait, a write lock
// another transaction holds is waited for only briefly, after which it
// returns ErrJobLocked.
func (e *executor) JobRecordProgress(ctx context.Context, params driver.JobRecordProgressParams) (*driver.Progress, error) {
	var cursor json.RawMessage
	if len(params.Cursor) > 0 {
		c, err := jsonValue(params.Cursor, "", "the progress cursor")
		if err != nil {
			return nil, err
		}
		cursor = json.RawMessage(c)
	}
	var progress *driver.Progress
	write := e.write
	if params.NoWait {
		write = e.writeBrief
	}
	err := write(ctx, func(x *w) error {
		var stored sql.NullString
		row := func() *sql.Row {
			if params.Lease != nil {
				return x.QueryRowContext(ctx, `SELECT progress FROM hopper_jobs j WHERE j.id = ?1 AND `+leaseOwnerSQL,
					idParam(params.ID), idParam(params.Lease.Token), params.Lease.Fence)
			}
			return x.QueryRowContext(ctx, `SELECT progress FROM hopper_jobs
				WHERE id = ? AND state = 'running' AND attempted_by = ? AND fence = ? AND attempt = ?`,
				idParam(params.ID), params.AttemptedBy, params.Fence, params.Attempt)
		}
		err := row().Scan(&stored)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		step := params.Step
		if params.KeepStep {
			step = ""
			if p := jobcodec.DecodeProgress(stored.String); p != nil {
				step = p.Step
			}
		}
		b, err := json.Marshal(struct {
			Step   string          `json:"step"`
			At     time.Time       `json:"at"`
			Cursor json.RawMessage `json:"cursor,omitempty"`
		}{step, x.now, cursor})
		if err != nil {
			return err
		}
		if _, err := x.ExecContext(ctx, "UPDATE hopper_jobs SET progress = ? WHERE id = ?", string(b), idParam(params.ID)); err != nil {
			return err
		}
		progress = jobcodec.DecodeProgress(string(b))
		return nil
	})
	if errors.Is(err, errLockBusy) {
		return nil, driver.ErrJobLocked
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: record job progress: %w", err)
	}
	return progress, nil
}

// JobGet implements driver.Executor.
func (e *executor) JobGet(ctx context.Context, id driver.JobID) (*driver.JobRow, error) {
	var leaseExpiresAt micros
	job, err := scanJob(e.read().QueryRowContext(ctx, `
		SELECT `+liveAsHistory+`,
		  (SELECT l.expires_at FROM hopper_job_leases l WHERE l.id = hopper_jobs.id AND l.fence = hopper_jobs.fence
		   AND hopper_jobs.attempted_by IS NULL AND hopper_jobs.state = 'running')
		FROM hopper_jobs WHERE id = ?1
		UNION ALL
		SELECT `+historyColumns+`, NULL FROM hopper_job_history WHERE id = ?1
		LIMIT 1`, idParam(id)), true, &leaseExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, driver.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: get job: %w", err)
	}
	job.LeaseExpiresAt = leaseExpiresAt.time()
	return job, nil
}

// JobLogs implements driver.Executor.
func (e *executor) JobLogs(ctx context.Context, id driver.JobID) ([]driver.AttemptLog, error) {
	var logs sql.NullString
	err := e.read().QueryRowContext(ctx, `
		SELECT logs FROM hopper_jobs WHERE id = ?1
		UNION ALL
		SELECT logs FROM hopper_job_history WHERE id = ?1
		LIMIT 1`, idParam(id)).Scan(&logs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, driver.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: get job logs: %w", err)
	}
	return jobcodec.DecodeLogs(logs.String), nil
}

// errOrderingKeyNeedsQueue refuses a key listing across queues: a key is a
// lane only within its queue.
var errOrderingKeyNeedsQueue = fmt.Errorf("%w: an ordering key is listed within a queue; set the queue", driver.ErrInvalidFilter)

// checkFilterText refuses filter text Postgres would refuse: a NUL, or
// bytes that are not UTF-8.
func checkFilterText(texts ...string) error {
	for _, s := range texts {
		if !utf8.ValidString(s) || strings.ContainsRune(s, 0) {
			return fmt.Errorf("%w: %q is not UTF-8 text without NUL", driver.ErrInvalidFilter, s)
		}
	}
	return nil
}

// JobList implements driver.Executor. Its filters are refused where
// Postgres's are, so that a listing means the same on either engine.
func (e *executor) JobList(ctx context.Context, params driver.JobListParams) ([]*driver.JobRow, error) {
	if params.OrderingKey != "" && params.Queue == "" {
		return nil, errOrderingKeyNeedsQueue
	}
	for _, p := range params.Priorities {
		if _, err := smallint(p, "Priorities"); err != nil {
			return nil, fmt.Errorf("%w: %w", driver.ErrInvalidFilter, err)
		}
	}
	states := make([]string, len(params.States))
	for i, s := range params.States {
		states[i] = string(s)
	}
	tagsAll, tagsAny := jobtags.Distinct(params.TagsAll), jobtags.Distinct(params.TagsAny)
	for _, texts := range [][]string{{params.Queue, params.OrderingKey}, params.Kinds, states, tagsAll, tagsAny} {
		if err := checkFilterText(texts...); err != nil {
			return nil, err
		}
	}
	if len(params.Metadata) > 0 {
		if err := checkJSONFilter(string(params.Metadata)); err != nil {
			return nil, fmt.Errorf("%w: %w", driver.ErrInvalidFilter, err)
		}
	}
	live, history := len(params.States) == 0, len(params.States) == 0
	for _, s := range params.States {
		if s.Terminal() {
			history = true
		} else {
			live = true
		}
	}

	var (
		where []string
		args  []any
	)
	in := func(column string, values []any) {
		where = append(where, column+" IN ("+placeholders(len(values))+")")
		args = append(args, values...)
	}
	anyOf := func(values []string) []any {
		out := make([]any, len(values))
		for i, v := range values {
			out[i] = v
		}
		return out
	}
	if params.Queue != "" {
		where, args = append(where, "queue = ?"), append(args, params.Queue)
	}
	if len(params.Kinds) > 0 {
		in("kind", anyOf(params.Kinds))
	}
	if len(states) > 0 {
		in("state", anyOf(states))
	}
	order := "id"
	switch {
	case params.NewestFirst:
		order = "id DESC"
		if !params.After.IsZero() {
			where, args = append(where, "id < ?"), append(args, idParam(params.After))
		}
	default:
		where, args = append(where, "id > ?"), append(args, idParam(params.After))
	}
	if params.OrderingKey != "" {
		where, args = append(where, "ordering_key = ?"), append(args, params.OrderingKey)
	}
	if len(params.IDs) > 0 {
		ids := make([]any, len(params.IDs))
		for i, id := range params.IDs {
			ids[i] = idParam(id)
		}
		in("id", ids)
	}
	if len(params.Priorities) > 0 {
		ps := make([]any, len(params.Priorities))
		for i, p := range params.Priorities {
			ps[i] = p
		}
		in("priority", ps)
	}
	if len(params.Metadata) > 0 {
		where, args = append(where, "hopper_json_contains(metadata, ?)"), append(args, string(params.Metadata))
	}
	// A listing by tags walks the tags table's rows of a tag, and checks
	// the job's own tags, which those rows follow, for every tag asked for.
	tagged := func(table string) (string, []any) {
		var conds []string
		var targs []any
		walked := tagsAny
		if len(tagsAll) > 0 {
			walked = tagsAll[:1]
			for _, t := range tagsAll {
				conds = append(conds, "EXISTS (SELECT 1 FROM json_each(j.tags) WHERE type = 'text' AND value = ?)")
				targs = append(targs, t)
			}
		}
		if len(tagsAny) > 0 {
			conds = append(conds, "EXISTS (SELECT 1 FROM json_each(j.tags) WHERE type = 'text' AND value IN ("+placeholders(len(tagsAny))+"))")
			targs = append(targs, anyOf(tagsAny)...)
		}
		if len(walked) == 0 {
			return "", nil
		}
		walk := "j.id IN (SELECT t.id FROM " + table + " t WHERE t.tag IN (" + placeholders(len(walked)) + "))"
		return " AND " + strings.Join(append([]string{walk}, conds...), " AND "), append(anyOf(walked), targs...)
	}
	limit := " ORDER BY " + order + " LIMIT " + strconv.Itoa(max(params.Limit, 1))
	filter := " WHERE " + strings.Join(where, " AND ")
	if len(where) == 0 {
		filter = " WHERE true"
	}
	var (
		arms    []string
		armArgs []any
	)
	if live {
		tags, targs := tagged("hopper_job_tags")
		arms = append(arms, "SELECT * FROM (SELECT "+liveAsHistory+" FROM hopper_jobs j"+filter+tags+limit+")")
		armArgs = append(append(armArgs, args...), targs...)
	}
	if history {
		tags, targs := tagged("hopper_job_history_tags")
		arms = append(arms, "SELECT * FROM (SELECT "+historyColumns+" FROM hopper_job_history j"+filter+tags+limit+")")
		armArgs = append(append(armArgs, args...), targs...)
	}
	query := strings.Join(arms, " UNION ALL ") + limit
	jobs, err := queryAll(ctx, e.read(), scanHistoryJob, query, armArgs...)
	if err != nil {
		return nil, fmt.Errorf("hopper: list jobs: %w", err)
	}
	return jobs, nil
}

// lockState returns a live job's row, or nil if it is not live.
func (x *w) lockState(ctx context.Context, id driver.JobID) (*liveRow, error) {
	return x.liveRow(ctx, id)
}

// cancelWaiting finalizes a waiting job as canceled, archived or not, with
// everything that entails for its batch and its dependents, and returns it
// as it is afterwards.
func (x *w) cancelWaiting(ctx context.Context, row *liveRow, archive bool) (*driver.JobRow, error) {
	errs, err := hopperError(row, "hopper: canceled", x.now)
	if err != nil {
		return nil, err
	}
	f := finished{row: row, state: driver.JobStateCanceled, errors: errs, logs: row.logs, archive: archive}
	if err := x.finish(ctx, f); err != nil {
		return nil, err
	}
	if _, err := x.tail(ctx, []finished{f}); err != nil {
		return nil, err
	}
	return f.jobRow(x.now)
}

// JobCancel implements driver.Executor.
func (e *executor) JobCancel(ctx context.Context, id driver.JobID) (*driver.JobRow, error) {
	var job *driver.JobRow
	err := e.write(ctx, func(x *w) error {
		row, err := x.lockState(ctx, id)
		switch {
		case err != nil:
			return err
		case row == nil:
			// Not live: finalized already, or unknown.
			job, err = (&executor{d: e.d, tx: txOf(x)}).JobGet(ctx, id)
			return err
		case row.job.State == driver.JobStateRunning:
			if job, err = scanJob(x.QueryRowContext(ctx, `UPDATE hopper_jobs SET cancel_requested_at = coalesce(cancel_requested_at, ?)
				WHERE id = ? RETURNING `+jobColumns, x.us(), idParam(id)), false); err != nil {
				return err
			}
			return x.notify(ctx, driver.ChannelControl, "cancel:"+id.String())
		}
		job, err = x.cancelWaiting(ctx, row, true)
		return err
	})
	if err != nil {
		if errors.Is(err, driver.ErrNotFound) {
			return nil, driver.ErrNotFound
		}
		return nil, fmt.Errorf("hopper: cancel job: %w", err)
	}
	return job, nil
}

// txOf returns the transaction a write runs in, for a read within it.
func txOf(x *w) *sql.Tx {
	tx, _ := x.q.(*sql.Tx)
	return tx
}

// JobDelete implements driver.Executor.
func (e *executor) JobDelete(ctx context.Context, id driver.JobID) (*driver.JobRow, error) {
	var job *driver.JobRow
	err := e.write(ctx, func(x *w) error {
		row, err := x.lockState(ctx, id)
		switch {
		case err != nil:
			return err
		case row == nil:
			job, err = scanJob(x.QueryRowContext(ctx, `DELETE FROM hopper_job_history WHERE id = ? RETURNING `+historyColumns, idParam(id)), true)
			if errors.Is(err, sql.ErrNoRows) {
				return driver.ErrNotFound
			}
			return err
		case row.job.State == driver.JobStateRunning:
			return driver.ErrJobRunning
		}
		job, err = x.cancelWaiting(ctx, row, false)
		return err
	})
	if err != nil {
		if errors.Is(err, driver.ErrNotFound) || errors.Is(err, driver.ErrJobRunning) {
			return nil, err
		}
		return nil, fmt.Errorf("hopper: delete job: %w", err)
	}
	return job, nil
}

// runNowSQL is the SET list that makes a waiting job runnable now and drops
// the reason it was waiting. A job that was already due keeps its
// scheduled_at, so it keeps its place in claim order and its aging.
const runNowSQL = `state = 'available', scheduled_at = min(scheduled_at, ?), snooze_reason = NULL, wake_requested_at = NULL`

// retryHistorySQL moves a finalized job back to the live table with its
// history (attempts, errors, progress unless from the start, logs, fence)
// intact, a fresh seq, and one more attempt if it had run out. Its tags are
// what its tags column reads as.
const retryHistorySQL = `
INSERT INTO hopper_jobs (id, kind, queue, state, priority, attempt, max_attempts, scheduled_at, args, metadata,
  errors, unique_key, ordering_key, partition_key, batch_id, await, created_at, progress, logs, tags, fence)
SELECT id, kind, queue, 'available', priority, attempt,
  CASE WHEN attempt >= max_attempts THEN attempt + 1 ELSE max_attempts END, ?, args, metadata,
  errors, unique_key, ordering_key, partition_key, batch_id, await, created_at, CASE WHEN ? THEN NULL ELSE progress END, logs,
  ?, fence
FROM hopper_job_history WHERE rowid = ?
RETURNING ` + jobColumns

// JobRetry implements driver.Executor.
func (e *executor) JobRetry(ctx context.Context, params driver.JobRetryParams) (*driver.JobRow, error) {
	id := params.ID
	var job *driver.JobRow
	err := e.write(ctx, func(x *w) error {
		row, err := x.lockState(ctx, id)
		switch {
		case err != nil:
			return err
		case row == nil:
			var (
				rowid      int64
				kind, tags sql.NullString
				uniqueKey  sql.NullString
			)
			err := x.QueryRowContext(ctx, `SELECT rowid, kind, unique_key, tags FROM hopper_job_history WHERE id = ? LIMIT 1`, idParam(id)).
				Scan(&rowid, &kind, &uniqueKey, &tags)
			if errors.Is(err, sql.ErrNoRows) {
				return driver.ErrNotFound
			}
			if err != nil {
				return err
			}
			if uniqueKey.Valid {
				var held bool
				if err := x.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM hopper_jobs WHERE kind = ? AND unique_key = ?)`,
					kind.String, uniqueKey.String).Scan(&held); err != nil {
					return err
				}
				if held {
					return driver.ErrUniqueConflict
				}
			}
			if job, err = scanJob(x.QueryRowContext(ctx, retryHistorySQL, x.us(), params.FromStart,
				tagsJSON(jobcodec.DecodeTags(tags.String)), rowid), false); err != nil {
				return err
			}
			_, err = x.ExecContext(ctx, `DELETE FROM hopper_job_history WHERE rowid = ?`, rowid)
			return err
		case row.job.State == driver.JobStateRunning:
			return driver.ErrJobRunning
		}
		job, err = scanJob(x.QueryRowContext(ctx, `UPDATE hopper_jobs SET `+runNowSQL+`, expires_at = NULL,
			progress = CASE WHEN ? THEN NULL ELSE progress END
			WHERE id = ? RETURNING `+jobColumns, x.us(), params.FromStart, idParam(id)), false)
		return err
	})
	if err != nil {
		if errors.Is(err, driver.ErrNotFound) || errors.Is(err, driver.ErrJobRunning) || errors.Is(err, driver.ErrUniqueConflict) {
			return nil, err
		}
		return nil, fmt.Errorf("hopper: retry job: %w", err)
	}
	return job, nil
}

// JobRetryOrderingKey implements driver.Executor: it marks the key's
// running job, so that its finalize honors the wake, and wakes the key's
// waiting jobs, oldest first, leaving those past their TTL to the expiry
// sweep.
func (e *executor) JobRetryOrderingKey(ctx context.Context, params driver.JobRetryOrderingKeyParams) (int, error) {
	var n int64
	err := e.write(ctx, func(x *w) error {
		if _, err := x.ExecContext(ctx, `UPDATE hopper_jobs SET wake_requested_at = ?
			WHERE queue = ? AND ordering_key = ? AND state = 'running'`, x.us(), params.Queue, params.OrderingKey); err != nil {
			return err
		}
		res, err := x.ExecContext(ctx, `UPDATE hopper_jobs SET `+runNowSQL+`
			WHERE seq IN (
			  SELECT seq FROM hopper_jobs
			  WHERE queue = ? AND ordering_key = ? AND state IN ('scheduled', 'retryable') AND (expires_at IS NULL OR expires_at > ?)
			  ORDER BY seq LIMIT ?)`, x.us(), params.Queue, params.OrderingKey, x.us(), max(params.Limit, 1))
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("hopper: retry ordering key: %w", err)
	}
	return int(n), nil
}

// JobDiscardExpired implements driver.Executor.
func (e *executor) JobDiscardExpired(ctx context.Context, limit int) ([]*driver.JobRow, error) {
	var jobs []*driver.JobRow
	err := e.write(ctx, func(x *w) error {
		jobs = jobs[:0]
		rows, err := queryAll(ctx, x, scanLive, `SELECT `+liveRowColumns+` FROM hopper_jobs
			WHERE expires_at <= ? AND state IN ('available', 'scheduled', 'retryable')
			ORDER BY expires_at LIMIT ?`, x.us(), max(limit, 1))
		if err != nil {
			return err
		}
		done := make([]finished, 0, len(rows))
		for _, row := range rows {
			errs, err := hopperError(row, "hopper: expired", x.now)
			if err != nil {
				return err
			}
			f := finished{row: row, state: driver.JobStateDiscarded, errors: errs, logs: row.logs, archive: true}
			if err := x.finish(ctx, f); err != nil {
				return err
			}
			done = append(done, f)
			job, err := f.jobRow(x.now)
			if err != nil {
				return err
			}
			jobs = append(jobs, job)
		}
		_, err = x.tail(ctx, done)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("hopper: discard expired jobs: %w", err)
	}
	return jobs, nil
}

// JobRescueCandidates implements driver.Executor: running jobs whose
// client has no live lease, and, with StuckAfter, those claimed longer ago
// than that, oldest first.
func (e *executor) JobRescueCandidates(ctx context.Context, params driver.JobRescueParams) ([]driver.RescueCandidate, error) {
	now := e.clock()
	stuckBefore := int64(math.MinInt64)
	if params.StuckAfter > 0 {
		stuckBefore = now.Add(-params.StuckAfter).UnixMicro()
	}
	candidates, err := queryAll(ctx, e.read(), func(r scanner) (driver.RescueCandidate, error) {
		var c driver.RescueCandidate
		var err error
		c.Job, err = scanJob(r, false, &c.Stuck)
		return c, err
	}, `SELECT * FROM (
		  SELECT `+prefixed(jobColumns, "j")+`, coalesce(j.attempted_at < ?1, false) AS stuck
		  FROM hopper_jobs j WHERE j.state = 'running' AND j.attempted_by IS NOT NULL
		) j
		WHERE stuck OR NOT EXISTS (SELECT 1 FROM hopper_clients c WHERE c.id = j.attempted_by AND c.expires_at > ?2)
		ORDER BY j.attempted_at LIMIT ?3`, stuckBefore, now.UnixMicro(), params.Limit)
	if err != nil {
		return nil, fmt.Errorf("hopper: rescue candidates: %w", err)
	}
	return candidates, nil
}

// sortIDs orders IDs as they sort in the database.
func sortIDs(ids []driver.JobID) {
	slices.SortFunc(ids, func(a, b driver.JobID) int { return bytes.Compare(a[:], b[:]) })
}
