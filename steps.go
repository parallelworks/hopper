package hopper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/parallelworks/hopper/driver"
)

// ErrJobLocked is returned by RecordStep and RecordCursor when a transaction
// of the worker's that wrote the job's row (RecordStepTx, RecordCursorTx,
// CompleteTx) has not ended: a write on the pool would wait for the worker
// itself. End that transaction before recording on the pool, and before a
// step run by RunSteps returns.
var ErrJobLocked = driver.ErrJobLocked

// Progress is how far a job's worker got: the last step it finished and a
// cursor within the work that follows. See RecordStep.
type Progress = driver.Progress

// RecordStep records that the running job finished step, with an optional
// cursor marking a position within the work that follows: any value
// encoding/json can encode, or nil. ctx must be the context the worker was
// given. The progress is written at once and kept through retries, snoozes
// and a JobRetry from history (unless FromStart), so a later attempt finds
// it in job.Progress and can resume after step; job.Progress is updated
// too. RunSteps uses it to resume a job at its first unfinished step.
//
// The write is fenced like a finalize: if the attempt no longer owns the job
// (it was rescued, or finalized), nothing is written and ErrJobFenced is
// returned.
//
// RecordStep, RecordCursor and their Tx forms may be called from several
// goroutines: an attempt's records are written one at a time, and
// job.Progress ends as the last one stored. Reading job.Progress while
// another goroutine records is a data race.
func RecordStep[T JobArgs](ctx context.Context, job *Job[T], step string, cursor any) error {
	return record(ctx, job, nil, step, false, cursor)
}

// RecordStepTx is RecordStep inside tx, so that a step's own writes and the
// record that it finished commit together or not at all, and a later
// attempt neither repeats a step that committed nor skips one that did not.
// job.Progress is updated as if tx commits; if it rolls back, the worker
// should return an error. Until tx ends it holds the job's row: end tx
// before the step returns, since RunSteps records the next finished step on
// the pool, and a record on the pool while tx is open returns ErrJobLocked.
// A job's finalize skips a row tx still holds, and is retried once tx ends.
// On an engine that runs one writer at a time
// (driver.Capabilities.SingleWriter, SQLite), tx holds the whole database
// for writes from its first statement until it ends: every other write
// waits for it, lease renewals included, and a write of the worker's own
// on the pool fails rather than wait for tx, provided tx was begun with the
// worker's ctx.
func RecordStepTx[TTx any, T JobArgs](ctx context.Context, tx TTx, job *Job[T], step string, cursor any) error {
	return record(ctx, job, func() (driver.Executor, error) { return txExecutor(ctx, tx, job.ID) }, step, false, cursor)
}

// RecordCursor records a cursor within the step under way, keeping the last
// finished step, for a step that works through many items: a later attempt
// resumes the step at the cursor (StepCursor) instead of from its start. A
// nil cursor clears it.
func RecordCursor[T JobArgs](ctx context.Context, job *Job[T], cursor any) error {
	return record(ctx, job, nil, "", true, cursor)
}

// RecordCursorTx is RecordCursor inside tx, like RecordStepTx.
func RecordCursorTx[TTx any, T JobArgs](ctx context.Context, tx TTx, job *Job[T], cursor any) error {
	return record(ctx, job, func() (driver.Executor, error) { return txExecutor(ctx, tx, job.ID) }, "", true, cursor)
}

// StepCursor decodes the cursor of the job's progress into a C. ok is false
// when the job has no cursor.
func StepCursor[C any](job *JobRow) (cursor C, ok bool, err error) {
	if job == nil || job.Progress == nil || len(job.Progress.Cursor) == 0 {
		return cursor, false, nil
	}
	if err := json.Unmarshal(job.Progress.Cursor, &cursor); err != nil {
		return cursor, false, fmt.Errorf("hopper: decode the cursor of job %s: %w", job.ID, err)
	}
	return cursor, true, nil
}

// JobStep is one named step of a job, for RunSteps. NamedStep makes one.
type JobStep[T JobArgs] struct {
	name string
	run  func(ctx context.Context, job *Job[T]) error
}

// NamedStep names one step of a job's own work for RunSteps (unlike a
// workflow's Step, which is a job of its own). A job resumes after the step
// its progress names, so names must be unique within the job and keep their
// meaning across releases.
func NamedStep[T JobArgs](name string, run func(ctx context.Context, job *Job[T]) error) JobStep[T] {
	return JobStep[T]{name: name, run: run}
}

// RunSteps runs a job's steps in order, starting after the last one its
// progress records as finished, and records each step as finished once it
// returns nil (RecordStep), unless the step recorded that itself with
// RecordStepTx or the job was completed with CompleteTx. It returns the
// first error a step returns, which fails the attempt as any worker error
// does; the next attempt starts at that step, with the cursor it recorded,
// if any, in job.Progress.
//
//	func (w *Importer) Work(ctx context.Context, job *hopper.Job[ImportArgs]) error {
//		return hopper.RunSteps(ctx, job,
//			hopper.NamedStep("download", w.download),
//			hopper.NamedStep("load", w.load),
//			hopper.NamedStep("notify", w.notify),
//		)
//	}
//
// A job whose progress names a step that is not among steps is discarded
// (Cancel), since every attempt would fail the same way: steps were renamed
// or removed while it waited. So is one run with a step that has no name or
// is listed twice.
func RunSteps[T JobArgs](ctx context.Context, job *Job[T], steps ...JobStep[T]) error {
	for i, s := range steps {
		if s.name == "" || s.run == nil {
			return Cancel(fmt.Errorf("hopper: step %d has no name or no function", i))
		}
		if !storableStep(s.name) {
			return Cancel(fmt.Errorf("hopper: step %q holds a NUL or invalid UTF-8, which PostgreSQL cannot store", s.name))
		}
		if slices.ContainsFunc(steps[:i], func(o JobStep[T]) bool { return o.name == s.name }) {
			return Cancel(fmt.Errorf("hopper: step %q is listed twice", s.name))
		}
	}
	start := 0
	if job.Progress != nil && job.Progress.Step != "" {
		done := slices.IndexFunc(steps, func(s JobStep[T]) bool { return s.name == job.Progress.Step })
		if done < 0 {
			return Cancel(fmt.Errorf("hopper: job %s finished step %q, which is not among its steps", job.ID, job.Progress.Step))
		}
		start = done + 1
	}
	for _, s := range steps[start:] {
		if err := s.run(ctx, job); err != nil {
			return err
		}
		// A job completed with CompleteTx finishes with that transaction,
		// which holds the job's row until it ends: a record on the pool
		// would wait for it, and be fenced once it commits.
		if completedInTx(ctx) {
			continue
		}
		if job.Progress == nil || job.Progress.Step != s.name {
			if err := RecordStep(ctx, job, s.name, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

// inlineKey marks the context of Workers.Work, which runs a worker with no
// stored job: progress is kept in the job's row only.
type inlineKey struct{}

// record stores the progress of job, with the pool, or with the executor
// txExec returns. Under Workers.Work it only updates job.Progress.
func record[T JobArgs](ctx context.Context, job *Job[T], txExec func() (driver.Executor, error), step string, keepStep bool, cursor any) error {
	a, _ := ctx.Value(attemptKey{}).(*attempt)
	ok := a != nil
	if !ok && ctx.Value(inlineKey{}) == nil {
		return ErrNotInJob
	}
	if job == nil || job.JobRow == nil || (ok && job.ID != a.completion.ID) {
		return errors.New("hopper: record progress: the job is not the one ctx belongs to")
	}
	raw, err := progressCursor(job.ID, step, keepStep, cursor)
	if err != nil {
		return err
	}
	if !ok {
		p := &Progress{Step: step, Cursor: raw, RecordedAt: time.Now()}
		if keepStep {
			p.Step = ""
			if job.Progress != nil {
				p.Step = job.Progress.Step
			}
		}
		job.Progress = p
		return nil
	}
	exec := a.exec
	if txExec != nil {
		if exec, err = txExec(); err != nil {
			return err
		}
	}
	a.progressMu.Lock()
	defer a.progressMu.Unlock()
	// A transaction of the worker's that wrote the row may still hold it,
	// and a write on the pool would wait for the worker itself.
	noWait := txExec == nil && a.txHeld()
	p, err := exec.JobRecordProgress(ctx, driver.JobRecordProgressParams{
		ID: job.ID, AttemptedBy: a.completion.AttemptedBy, Fence: a.completion.Fence, Attempt: a.attempt,
		Step: step, KeepStep: keepStep, Cursor: raw, NoWait: noWait,
	})
	if errors.Is(err, driver.ErrJobLocked) {
		return fmt.Errorf("hopper: record the progress of job %s: a transaction holds the job's row, such as the worker's own from RecordStepTx, RecordCursorTx or CompleteTx, which must end first: %w", job.ID, err)
	}
	if err != nil {
		return err
	}
	if p == nil {
		return ErrJobFenced
	}
	if txExec != nil {
		a.markHeldByTx()
	}
	job.Progress = p
	return nil
}

// progressCursor checks the step of a progress record of job id, unless
// keepStep, and encodes its cursor: nil for none, which leaves the key out.
func progressCursor(id JobID, step string, keepStep bool, cursor any) (json.RawMessage, error) {
	if !keepStep && !storableStep(step) {
		return nil, fmt.Errorf("hopper: record the progress of job %s: step %q holds a NUL or invalid UTF-8, which PostgreSQL cannot store", id, step)
	}
	if cursor == nil {
		return nil, nil
	}
	b, err := json.Marshal(cursor)
	if err != nil {
		return nil, fmt.Errorf("hopper: record the progress of job %s: encode the cursor: %w", id, err)
	}
	if string(b) == "null" {
		return nil, nil
	}
	return b, nil
}

// completedInTx reports whether the attempt ctx belongs to was completed
// with CompleteTx.
func completedInTx(ctx context.Context) bool {
	a, _ := ctx.Value(attemptKey{}).(*attempt)
	return a != nil && a.completedTx() != nil
}

// storableStep reports whether PostgreSQL can store name as text. A step
// name is matched exactly on resume, so it is refused rather than replaced
// the way storableText replaces an error message.
func storableStep(name string) bool {
	return utf8.ValidString(name) && !strings.ContainsRune(name, 0)
}

// txExecutor returns the executor of tx for the client ctx carries.
func txExecutor[TTx any](ctx context.Context, tx TTx, id JobID) (driver.Executor, error) {
	c, ok := ClientFromContext[TTx](ctx)
	if !ok {
		return nil, fmt.Errorf("hopper: job %s: the transaction is not of the type its client uses", id)
	}
	return c.driver.UnwrapTx(tx), nil
}
