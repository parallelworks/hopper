// Package hoppertest has helpers for testing code that uses hopper.
//
//	job := hoppertest.RequireInserted[SendEmail](ctx, t, client, nil)
//	if job.Args.UserID != 42 { ... }
//
//	hoppertest.RequireNotInserted[SendEmail](ctx, t, client, nil)
//
//	// Run a job's worker directly, without a database round trip.
//	err := hoppertest.Work(ctx, t, workers, SendEmail{UserID: 42}, nil)
//
// The Require helpers look at live jobs (not yet finalized) of one kind, so
// they work whether or not a client is started. The Tx variants look inside
// a transaction, for code that inserts with InsertTx and tests that roll
// back.
package hoppertest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
)

// RequireOpts narrows which jobs a Require helper considers. Zero fields
// match everything.
type RequireOpts struct {
	Queue       string
	Priority    hopper.Priority
	MaxAttempts int
	State       hopper.JobState
}

func (o *RequireOpts) matches(job *hopper.JobRow) bool {
	if o == nil {
		return true
	}
	if o.Queue != "" && job.Queue != o.Queue {
		return false
	}
	if o.Priority != 0 && job.Priority != int(o.Priority) {
		return false
	}
	if o.MaxAttempts != 0 && job.MaxAttempts != o.MaxAttempts {
		return false
	}
	if o.State != "" && job.State != o.State {
		return false
	}
	return true
}

func (o *RequireOpts) String() string {
	if o == nil {
		return ""
	}
	var parts []string
	if o.Queue != "" {
		parts = append(parts, "queue="+o.Queue)
	}
	if o.Priority != 0 {
		parts = append(parts, fmt.Sprintf("priority=%d", o.Priority))
	}
	if o.MaxAttempts != 0 {
		parts = append(parts, fmt.Sprintf("max_attempts=%d", o.MaxAttempts))
	}
	if o.State != "" {
		parts = append(parts, "state="+string(o.State))
	}
	if len(parts) == 0 {
		return ""
	}
	return " (" + strings.Join(parts, ", ") + ")"
}

var liveStates = []hopper.JobState{hopper.JobStateAvailable, hopper.JobStateScheduled, hopper.JobStateRetryable, hopper.JobStateRunning, hopper.JobStatePending}

// RequireInserted fails the test unless exactly one live job of kind T
// matching opts exists, and returns it with decoded args. Use
// RequireManyInserted when several are expected.
func RequireInserted[T hopper.JobArgs, TTx any](ctx context.Context, tb testing.TB, client *hopper.Client[TTx], opts *RequireOpts) *hopper.Job[T] {
	tb.Helper()
	jobs := requireMany[T](tb, client.Jobs(ctx, filter[T]()), opts, 1)
	return jobs[0]
}

// RequireInsertedTx is RequireInserted inside the caller's transaction.
func RequireInsertedTx[T hopper.JobArgs, TTx any](ctx context.Context, tb testing.TB, client *hopper.Client[TTx], tx TTx, opts *RequireOpts) *hopper.Job[T] {
	tb.Helper()
	jobs := requireMany[T](tb, client.JobsTx(ctx, tx, filter[T]()), opts, 1)
	return jobs[0]
}

// RequireManyInserted fails the test unless exactly n live jobs of kind T
// matching opts exist, and returns them in insertion order.
func RequireManyInserted[T hopper.JobArgs, TTx any](ctx context.Context, tb testing.TB, client *hopper.Client[TTx], n int, opts *RequireOpts) []*hopper.Job[T] {
	tb.Helper()
	return requireMany[T](tb, client.Jobs(ctx, filter[T]()), opts, n)
}

// RequireManyInsertedTx is RequireManyInserted inside the caller's
// transaction.
func RequireManyInsertedTx[T hopper.JobArgs, TTx any](ctx context.Context, tb testing.TB, client *hopper.Client[TTx], tx TTx, n int, opts *RequireOpts) []*hopper.Job[T] {
	tb.Helper()
	return requireMany[T](tb, client.JobsTx(ctx, tx, filter[T]()), opts, n)
}

// RequireNotInserted fails the test if any live job of kind T matching opts
// exists.
func RequireNotInserted[T hopper.JobArgs, TTx any](ctx context.Context, tb testing.TB, client *hopper.Client[TTx], opts *RequireOpts) {
	tb.Helper()
	requireMany[T](tb, client.Jobs(ctx, filter[T]()), opts, 0)
}

// RequireNotInsertedTx is RequireNotInserted inside the caller's
// transaction.
func RequireNotInsertedTx[T hopper.JobArgs, TTx any](ctx context.Context, tb testing.TB, client *hopper.Client[TTx], tx TTx, opts *RequireOpts) {
	tb.Helper()
	requireMany[T](tb, client.JobsTx(ctx, tx, filter[T]()), opts, 0)
}

func filter[T hopper.JobArgs]() hopper.JobFilter {
	var zero T
	return hopper.JobFilter{Kinds: []string{zero.Kind()}, States: liveStates}
}

func requireMany[T hopper.JobArgs](tb testing.TB, jobs func(func(*hopper.JobRow, error) bool), opts *RequireOpts, want int) []*hopper.Job[T] {
	tb.Helper()
	var zero T
	var found []*hopper.Job[T]
	for row, err := range jobs {
		if err != nil {
			tb.Fatalf("hoppertest: list %q jobs: %v", zero.Kind(), err)
		}
		if !opts.matches(row) {
			continue
		}
		var args T
		if err := json.Unmarshal(row.Args, &args); err != nil {
			tb.Fatalf("hoppertest: decode args of job %s: %v", row.ID, err)
		}
		found = append(found, &hopper.Job[T]{JobRow: row, Args: args})
	}
	if len(found) != want {
		tb.Fatalf("hoppertest: expected %d live %q job(s)%s, found %d", want, zero.Kind(), opts, len(found))
	}
	return found
}

// WorkOpts customizes a job run by Work.
type WorkOpts struct {
	Queue    string
	Attempt  int // defaults to 1
	Priority hopper.Priority
	Metadata json.RawMessage
}

var workSeq atomic.Int64

// Work runs the worker registered for T inline, as the client would, with a
// synthetic job row. Nothing touches the database: the job is not inserted,
// claimed or finalized, and middleware does not run. The worker's Timeout
// applies. It returns the worker's error, so callers can check for
// hopper.Snooze and hopper.Cancel results with errors.As.
func Work[T hopper.JobArgs](ctx context.Context, tb testing.TB, workers *hopper.Workers, args T, opts *WorkOpts) error {
	tb.Helper()
	encoded, err := json.Marshal(args)
	if err != nil {
		tb.Fatalf("hoppertest: encode args: %v", err)
	}
	if opts == nil {
		opts = &WorkOpts{}
	}
	now := time.Now()
	row := &hopper.JobRow{
		ID:          syntheticID(),
		Kind:        args.Kind(),
		Queue:       hopper.QueueDefault,
		State:       hopper.JobStateRunning,
		Priority:    int(hopper.PriorityNormal),
		Attempt:     1,
		MaxAttempts: hopper.DefaultMaxAttempts,
		ScheduledAt: now,
		AttemptedAt: now,
		Args:        encoded,
		Metadata:    json.RawMessage(`{}`),
		Errors:      []hopper.AttemptError{},
		CreatedAt:   now,
	}
	if opts.Queue != "" {
		row.Queue = opts.Queue
	}
	if opts.Attempt > 0 {
		row.Attempt = opts.Attempt
	}
	if opts.Priority != 0 {
		row.Priority = int(opts.Priority)
	}
	if len(opts.Metadata) > 0 {
		row.Metadata = opts.Metadata
	}
	return workers.Work(ctx, row, hopper.JSONCodec{})
}

// syntheticID makes a distinct, time-ordered ID for a job that is never
// stored: the Unix time in milliseconds in the first 48 bits, as a v7 UUID
// has, and a counter in the rest.
func syntheticID() hopper.JobID {
	var id hopper.JobID
	ms := time.Now().UnixMilli()
	for i := range 6 {
		id[5-i] = byte(ms >> (8 * i)) //nolint:gosec // the low 48 bits are wanted
	}
	id[6] = 0x70 // version 7, so String() looks like a real one
	id[8] = 0x80
	n := workSeq.Add(1)
	for i := range 6 {
		id[15-i] = byte(n >> (8 * i)) //nolint:gosec // the low 48 bits are wanted
	}
	return id
}
