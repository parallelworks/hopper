package hoppersqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/internal/jobcodec"
	"github.com/parallelworks/hopper/internal/jobtags"
)

// Every write that finalizes jobs (the batched finalize, canceling or
// deleting a waiting job, expiring) runs in a transaction that holds the
// write lock, so it reads each job, checks it in Go and writes it back with
// no other writer in between: what Postgres's statements do in one
// statement, with row locks and a recheck of every condition, is here a
// sequence of plain statements. The tail does what finalized jobs entail
// in the same transaction: canceling the dependents of a failed job,
// archiving, counting the jobs down in their batches and completing those
// they finish, promoting dependents whose dependencies are all finished,
// and dropping satisfied edges. No other transaction can finalize a
// batch's or a dependent's last jobs at the same time, so nothing is left
// for BatchSettle or WorkflowPromoteReady to find.

// liveRow is a live job's row with the columns its finalize reads beside
// the JobRow's.
type liveRow struct {
	job             *driver.JobRow
	logs            sql.NullString
	wakeRequestedAt micros
	snoozeReason    sql.NullString
	// errors is the errors column as stored, which a finalize appends to.
	errors string
}

const liveRowColumns = jobColumns + ", logs, wake_requested_at, snooze_reason, errors"

func scanLive(row scanner) (*liveRow, error) {
	var r liveRow
	var err error
	r.job, err = scanJob(row, false, &r.logs, &r.wakeRequestedAt, &r.snoozeReason, &r.errors)
	return &r, err
}

// liveRow returns a live job's row, or nil if the job is not live.
func (x *w) liveRow(ctx context.Context, id driver.JobID) (*liveRow, error) {
	r, err := scanLive(x.QueryRowContext(ctx, "SELECT "+liveRowColumns+" FROM hopper_jobs WHERE id = ?", idParam(id)))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return r, err
}

// finished is a job leaving the live table: its final state and what its
// history row keeps.
type finished struct {
	row     *liveRow
	state   driver.JobState
	errors  string
	logs    sql.NullString
	output  any
	archive bool
}

func (f finished) failed() bool { return f.state != driver.JobStateCompleted }

// jobRow returns the job as it is once finished: in its final state, with
// its errors, logs and output, finalized at now, and waiting for nothing.
func (f finished) jobRow(now time.Time) (*driver.JobRow, error) {
	j := *f.row.job
	j.State, j.FinalizedAt, j.SnoozeReason = f.state, now, ""
	var err error
	if j.Errors, err = jobcodec.DecodeErrors(f.errors); err != nil {
		return nil, err
	}
	j.Output = nil
	if s, ok := f.output.(string); ok {
		j.Output = json.RawMessage(s)
	}
	return &j, nil
}

// archiveSQL copies a live job to history, with its final state, errors,
// logs, finalization time and output.
const archiveSQL = `
INSERT INTO hopper_job_history (id, seq, kind, queue, state, priority, attempt, max_attempts, scheduled_at, attempted_at,
  attempted_by, args, metadata, errors, unique_key, ordering_key, partition_key, batch_id, expires_at, cancel_requested_at,
  await, created_at, progress, tags, fence, logs, finalized_at, output)
SELECT id, seq, kind, queue, ?, priority, attempt, max_attempts, scheduled_at, attempted_at,
  attempted_by, args, metadata, ?, unique_key, ordering_key, partition_key, batch_id, expires_at, cancel_requested_at,
  await, created_at, progress, tags, fence, ?, ?, ?
FROM hopper_jobs WHERE id = ?`

// finish moves a job out of the live table, to history if it is archived,
// and announces it to those awaiting it.
func (x *w) finish(ctx context.Context, f finished) error {
	id := idParam(f.row.job.ID)
	if f.archive {
		if _, err := x.ExecContext(ctx, archiveSQL, string(f.state), f.errors, f.logs, x.us(), f.output, id); err != nil {
			return fmt.Errorf("archive job %s: %w", f.row.job.ID, err)
		}
	}
	if _, err := x.ExecContext(ctx, "DELETE FROM hopper_jobs WHERE id = ?", id); err != nil {
		return fmt.Errorf("delete job %s: %w", f.row.job.ID, err)
	}
	if f.row.job.Await {
		return x.notify(ctx, driver.ChannelDone, f.row.job.ID.String())
	}
	return nil
}

// appendJSON appends an element to a JSON array as stored, keeping the
// elements there as they are. A value that is not an array starts a new one.
func appendJSON(stored string, elem []byte) (string, error) {
	var elems []json.RawMessage
	_ = json.Unmarshal([]byte(stored), &elems)
	b, err := json.Marshal(append(elems, elem))
	return string(b), err
}

// withErrorAt appends an error record, timestamped at now, to stored.
func withErrorAt(stored string, e *driver.AttemptError, now time.Time) (string, error) {
	b, err := jobcodec.MarshalAttemptError(e)
	if err != nil {
		return "", err
	}
	var rec map[string]json.RawMessage
	if err := json.Unmarshal(b, &rec); err != nil {
		return "", err
	}
	rec["at"], _ = json.Marshal(now)
	b, err = json.Marshal(rec)
	if err != nil {
		return "", err
	}
	return appendJSON(stored, b)
}

// hopperError is the errors column of a job that hopper itself fails, such
// as one canceled or expired while waiting: a snoozed job's reason is kept
// in the error, since the row that held it is leaving the live table.
func hopperError(r *liveRow, message string, now time.Time) (string, error) {
	if r.job.State == driver.JobStateScheduled && r.snoozeReason.Valid {
		message += " while snoozed: " + r.snoozeReason.String
	}
	return withErrorAt(r.errors, &driver.AttemptError{Attempt: r.job.Attempt, Error: message}, now)
}

// keepLogs is how many attempt logs a finalize keeps for keep.
func keepLogs(keep int) int {
	if keep <= 0 {
		return math.MaxInt32
	}
	return keep
}

// appendLog appends an attempt's log, if it has one, to a job's logs as
// stored, keeping the latest keep. A snooze gives its attempt back, so a job
// that snoozes again and again would fill its logs with them: a snooze's
// log, or its lack of one, replaces the log of the job's previous snooze of
// the same attempt number, attempt. Logs that do not change are returned
// as stored; none at all, as NULL.
func appendLog(stored sql.NullString, attempt int, snooze bool, log []byte, keep int) (sql.NullString, error) {
	var entries []json.RawMessage
	if stored.Valid {
		_ = json.Unmarshal([]byte(stored.String), &entries)
	}
	sameSnooze := func(raw json.RawMessage) bool {
		var e map[string]json.RawMessage
		if json.Unmarshal(raw, &e) != nil {
			return false
		}
		_, snoozed := e["snoozed"]
		var a json.Number
		return snoozed && json.Unmarshal(e["attempt"], &a) == nil && a.String() == fmt.Sprint(attempt)
	}
	replaces := false
	if snooze {
		for _, e := range entries {
			replaces = replaces || sameSnooze(e)
		}
	}
	if log == nil && !replaces {
		return stored, nil
	}
	kept := entries[:0:0]
	for _, e := range entries {
		if !snooze || !sameSnooze(e) {
			kept = append(kept, e)
		}
	}
	if log != nil {
		kept = append(kept, log)
	}
	if len(kept) > keep {
		kept = kept[len(kept)-keep:]
	}
	if len(kept) == 0 {
		return sql.NullString{}, nil
	}
	b, err := json.Marshal(kept)
	return sql.NullString{String: string(b), Valid: true}, err
}

// result is a JobFinalize with its values encoded.
type result struct {
	driver.JobFinalize
	err    *driver.AttemptError
	log    []byte
	output any
	reason any
}

func encodeResults(jobs []driver.JobFinalize) ([]result, error) {
	out := make([]result, len(jobs))
	for i, f := range jobs {
		switch f.State {
		case driver.JobStateCompleted, driver.JobStateCanceled, driver.JobStateDiscarded,
			driver.JobStateRetryable, driver.JobStateScheduled:
		default:
			return nil, fmt.Errorf("hopper: finalize jobs: %s is not a finalize state", f.State)
		}
		r := result{JobFinalize: f, err: f.Error, reason: nullable(jobcodec.StorableText(f.SnoozeReason))}
		log, err := jobcodec.MarshalAttemptLog(f)
		if err != nil {
			return nil, fmt.Errorf("hopper: finalize jobs: %w", err)
		}
		r.log = log
		if len(f.Output) > 0 {
			out, err := jsonValue(f.Output, "", "output")
			if err != nil {
				return nil, fmt.Errorf("hopper: finalize jobs: %w", err)
			}
			r.output = out
		}
		out[i] = r
	}
	return out, nil
}

// JobFinalizeMany implements driver.Executor. A result applies only to a
// job running in the claim that produced it, by the same client at the same
// fence, so a stale result never overwrites a newer attempt; each applied
// result is reported with its fence. With
// SkipLocked, a write lock another transaction holds is waited for only
// briefly, after which no result is applied.
func (e *executor) JobFinalizeMany(ctx context.Context, params driver.JobFinalizeParams) ([]driver.JobFinalized, error) {
	if len(params.Jobs) == 0 {
		return nil, nil
	}
	results, err := encodeResults(params.Jobs)
	if err != nil {
		return nil, err
	}
	leased := params.Jobs[0].Lease != nil
	if leased && params.SkipLocked {
		return nil, errors.New("hopper: finalize jobs: SkipLocked with results of leased jobs")
	}
	for i, r := range results {
		if (r.Lease != nil) != leased {
			return nil, errors.New("hopper: finalize jobs: results of leased jobs and of others in one call")
		}
		if leased {
			results[i].Fence = r.Lease.Fence
		}
	}
	keep := keepLogs(params.KeepLogs)
	var applied []driver.JobFinalized
	write := e.write
	if params.SkipLocked {
		write = e.writeBrief
	}
	err = write(ctx, func(x *w) error {
		applied = applied[:0]
		var done []finished
		for _, r := range results {
			row, err := x.liveRow(ctx, r.ID)
			if err != nil {
				return err
			}
			if row == nil {
				continue
			}
			j := row.job
			if leased {
				owns, err := x.leaseOwns(ctx, row, r.Lease)
				if err != nil {
					return err
				}
				if !owns {
					continue
				}
				if err := x.releaseLease(ctx, j.ID); err != nil {
					return err
				}
			} else if j.State != driver.JobStateRunning || j.AttemptedBy == 0 || j.AttemptedBy != r.AttemptedBy || j.Fence != r.Fence {
				continue
			}
			errs := row.errors
			if r.err != nil {
				if errs, err = withErrorAt(errs, r.err, x.now); err != nil {
					return err
				}
			}
			logs, err := appendLog(row.logs, j.Attempt, r.Snooze, r.log, keep)
			if err != nil {
				return err
			}
			if r.State.Terminal() {
				f := finished{row: row, state: r.State, errors: errs, logs: logs, output: r.output, archive: r.Archived()}
				if err := x.finish(ctx, f); err != nil {
					return err
				}
				done = append(done, f)
				applied = append(applied, driver.JobFinalized{ID: r.ID, Fence: r.Fence})
				continue
			}
			// A wake since the attempt began (JobRetryOrderingKey) makes the
			// job runnable at once, and takes the place of a snooze's reason.
			woken := row.wakeRequestedAt.Valid && !j.AttemptedAt.IsZero() && !row.wakeRequestedAt.time().Before(j.AttemptedAt)
			scheduledAt := x.at(max(r.Delay, 0))
			if woken {
				scheduledAt = x.us()
			}
			attempt := j.Attempt
			var reason any
			if r.Snooze {
				attempt--
				if !woken {
					reason = r.reason
				}
			}
			if _, err := x.ExecContext(ctx, `
				UPDATE hopper_jobs SET state = ?, scheduled_at = ?, attempt = ?, errors = ?, logs = ?, attempted_by = NULL,
				  snooze_reason = ?, wake_requested_at = NULL
				WHERE id = ?`, string(r.State), scheduledAt, attempt, errs, logs, reason, idParam(r.ID)); err != nil {
				return err
			}
			applied = append(applied, driver.JobFinalized{ID: r.ID, Fence: r.Fence})
		}
		if params.NoBatches {
			return nil
		}
		_, err := x.tail(ctx, done)
		return err
	})
	if errors.Is(err, errLockBusy) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: finalize jobs: %w", err)
	}
	return applied, nil
}

// tail does what the jobs in done, which have just left the live table,
// entail, and returns done with the dependents it canceled.
func (x *w) tail(ctx context.Context, done []finished) ([]finished, error) {
	if len(done) == 0 {
		return done, nil
	}
	// Cancel the dependents of failed jobs, down the graph by cancel edges.
	cascaded := map[driver.JobID]bool{}
	var queue []driver.JobID
	for _, f := range done {
		if f.failed() {
			queue = append(queue, f.row.job.ID)
		}
	}
	seen := map[driver.JobID]bool{}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		deps, err := x.ids(ctx, "SELECT job_id FROM hopper_job_deps WHERE depends_on = ? AND on_failure = 'cancel'", idParam(id))
		if err != nil {
			return nil, err
		}
		for _, dep := range deps {
			if seen[dep] {
				continue
			}
			seen[dep] = true
			queue = append(queue, dep)
			row, err := x.liveRow(ctx, dep)
			if err != nil {
				return nil, err
			}
			if row == nil || row.job.State != driver.JobStatePending {
				continue
			}
			errs, err := hopperError(row, "hopper: dependency failed", x.now)
			if err != nil {
				return nil, err
			}
			f := finished{row: row, state: driver.JobStateCanceled, errors: errs, logs: row.logs, archive: true}
			if err := x.finish(ctx, f); err != nil {
				return nil, err
			}
			done = append(done, f)
			cascaded[dep] = true
		}
	}

	// Record the result of each job that still has a dependent, for its
	// dependents' claims. countBatch deletes a workflow's results when it
	// completes. The edges do not change until the end of the tail, so each
	// job's dependents are read once, here, for the promotion below too.
	finishing := make(map[driver.JobID]bool, len(done))
	for _, f := range done {
		finishing[f.row.job.ID] = true
	}
	dependents := make([][]driver.JobID, len(done))
	for i, f := range done {
		deps, err := x.ids(ctx, "SELECT job_id FROM hopper_job_deps WHERE depends_on = ?", idParam(f.row.job.ID))
		if err != nil {
			return nil, err
		}
		dependents[i] = deps
		if f.row.job.BatchID.IsZero() || !slices.ContainsFunc(deps, func(id driver.JobID) bool { return !finishing[id] }) {
			continue
		}
		if _, err := x.ExecContext(ctx, `
			INSERT INTO hopper_job_results (job_id, batch_id, state, output, finalized_at) VALUES (?, ?, ?, ?, ?)
			ON CONFLICT (batch_id, job_id) DO UPDATE SET state = excluded.state, output = excluded.output,
			  finalized_at = excluded.finalized_at`,
			idParam(f.row.job.ID), idParam(f.row.job.BatchID), string(f.state), f.output, x.us()); err != nil {
			return nil, fmt.Errorf("record result of job %s: %w", f.row.job.ID, err)
		}
	}

	// Count the jobs down in their batches.
	type count struct{ n, failed int }
	var batches []driver.JobID
	counts := map[driver.JobID]*count{}
	for _, f := range done {
		b := f.row.job.BatchID
		if b.IsZero() {
			continue
		}
		c := counts[b]
		if c == nil {
			c = &count{}
			counts[b] = c
			batches = append(batches, b)
		}
		c.n++
		if f.failed() {
			c.failed++
		}
	}
	for _, b := range batches {
		if err := x.countBatch(ctx, b, counts[b].n, counts[b].failed); err != nil {
			return nil, err
		}
	}

	// Promote the dependents whose dependencies have all finished, and drop
	// the edges of the jobs that finished.
	considered := map[driver.JobID]bool{}
	for i := range done {
		for _, dep := range dependents[i] {
			if considered[dep] || cascaded[dep] {
				continue
			}
			considered[dep] = true
			if _, err := x.promoteIfReady(ctx, dep); err != nil {
				return nil, err
			}
		}
	}
	for _, f := range done {
		if _, err := x.ExecContext(ctx, "DELETE FROM hopper_job_deps WHERE job_id = ?", idParam(f.row.job.ID)); err != nil {
			return nil, err
		}
	}
	return done, nil
}

// ids returns the IDs a query selects.
func (x *w) ids(ctx context.Context, query string, args ...any) ([]driver.JobID, error) {
	return queryIDs(ctx, x, query, args...)
}

func queryIDs(ctx context.Context, q querier, query string, args ...any) ([]driver.JobID, error) {
	return queryAll(ctx, q, func(r scanner) (driver.JobID, error) {
		var b []byte
		if err := r.Scan(&b); err != nil {
			return driver.JobID{}, err
		}
		var id driver.JobID
		err := id.Scan(b)
		return id, err
	}, query, args...)
}

// promoteIfReady makes a pending job claimable if none of its dependencies
// is still live, and reports whether it did.
func (x *w) promoteIfReady(ctx context.Context, id driver.JobID) (bool, error) {
	var queue string
	err := x.QueryRowContext(ctx, `
		UPDATE hopper_jobs SET state = CASE WHEN scheduled_at > ? THEN 'scheduled' ELSE 'available' END
		WHERE id = ? AND state = 'pending'
		  AND NOT EXISTS (SELECT 1 FROM hopper_job_deps d JOIN hopper_jobs x ON x.id = d.depends_on WHERE d.job_id = ?)
		RETURNING queue`, x.us(), idParam(id), idParam(id)).Scan(&queue)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, x.notify(ctx, driver.ChannelInsert, queue)
}

// countBatch counts n jobs of a batch, failed of them canceled or
// discarded, as finished, and completes the batch, inserting its
// callbacks, if nothing remains. A batch that has completed already counts
// them too, as a Postgres batch's settle does.
func (x *w) countBatch(ctx context.Context, id driver.JobID, n, failed int) error {
	var (
		pending, wasFailed               int
		completed                        micros
		onSuccess, onFailure, onComplete sql.NullString
	)
	err := x.QueryRowContext(ctx, `SELECT pending, failed, completed_at, on_success, on_failure, on_complete FROM hopper_batches WHERE id = ?`,
		idParam(id)).Scan(&pending, &wasFailed, &completed, &onSuccess, &onFailure, &onComplete)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("batch %s: %w", id, err)
	}
	pending -= n
	failed += wasFailed
	if completed.Valid || pending > 0 {
		_, err := x.ExecContext(ctx, `UPDATE hopper_batches SET pending = ?, failed = ? WHERE id = ?`, max(pending, 0), failed, idParam(id))
		return err
	}
	if _, err := x.ExecContext(ctx, `UPDATE hopper_batches SET pending = 0, failed = ?, completed_at = ? WHERE id = ?`, failed, x.us(), idParam(id)); err != nil {
		return err
	}
	if _, err := x.ExecContext(ctx, `DELETE FROM hopper_job_results WHERE batch_id = ?`, idParam(id)); err != nil {
		return err
	}
	outcome := onSuccess
	if failed > 0 {
		outcome = onFailure
	}
	for _, cb := range []sql.NullString{outcome, onComplete} {
		if cb.Valid {
			if err := x.insertCallback(ctx, cb.String, id, failed); err != nil {
				return fmt.Errorf("batch %s: callback: %w", id, err)
			}
		}
	}
	return nil
}

// insertCallback inserts a completed batch's callback. Its values are read
// as Postgres reads them: brought into range, tags as a job's tags column
// reads them, and the batch's ID and failures added to its metadata, so
// that one batch row a direct write made odd cannot fail the finalize that
// completes it.
func (x *w) insertCallback(ctx context.Context, stored string, batch driver.JobID, failed int) error {
	var cb struct {
		Kind        string          `json:"kind"`
		Queue       string          `json:"queue"`
		Priority    json.Number     `json:"priority"`
		MaxAttempts json.Number     `json:"max_attempts"`
		Args        json.RawMessage `json:"args"`
		Metadata    json.RawMessage `json:"metadata"`
		Tags        json.RawMessage `json:"tags"`
	}
	dec := json.NewDecoder(bytesReader(stored))
	dec.UseNumber()
	if err := dec.Decode(&cb); err != nil {
		return err
	}
	clamp := func(n json.Number, def, lo, hi float64) int {
		v, err := n.Float64()
		if err != nil {
			v = def
		}
		return int(math.Min(math.Max(math.Round(v), lo), hi))
	}
	queue := cb.Queue
	if queue == "" {
		queue = "default"
	}
	args := "{}"
	if len(cb.Args) > 0 && string(cb.Args) != "null" {
		args = string(cb.Args)
	}
	meta := map[string]json.RawMessage{}
	if len(cb.Metadata) > 0 {
		_ = json.Unmarshal(cb.Metadata, &meta)
	}
	meta["batch_id"], _ = json.Marshal(batch.String())
	meta["batch_failed"], _ = json.Marshal(failed)
	metadata, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	var tags []string
	for _, t := range jobcodec.DecodeTags(string(cb.Tags)) {
		if len(t) <= jobtags.MaxBytes {
			tags = append(tags, t)
		}
	}
	_, err = x.insertJob(ctx, jobInsert{
		kind: cb.Kind, queue: queue, priority: clamp(cb.Priority, 2, 1, 4), maxAttempts: clamp(cb.MaxAttempts, 25, 1, 32767),
		args: args, metadata: string(metadata), tags: tags,
	})
	if err != nil {
		return err
	}
	return x.notify(ctx, driver.ChannelInsert, queue)
}
