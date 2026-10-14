package hopper_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
)

// hookCalls records the errors Config.OnError saw, by job.
type hookCalls struct {
	mu   sync.Mutex
	errs map[hopper.JobID][]error
}

func (h *hookCalls) add(id hopper.JobID, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.errs[id] = append(h.errs[id], err)
}

func (h *hookCalls) get(id hopper.JobID) []error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.errs[id]
}

// startedWithConfig starts a client working queue "default" (and any queues
// cfg already lists) with cfg's hooks and policies.
func (h *harness) startedWithConfig(cfg *hopper.Config) *hopper.Client[pgxTx] {
	h.t.Helper()
	if cfg.Queues == nil {
		cfg.Queues = map[string]hopper.QueueConfig{}
	}
	if _, ok := cfg.Queues[hopper.QueueDefault]; !ok {
		cfg.Queues[hopper.QueueDefault] = hopper.QueueConfig{MaxWorkers: 4}
	}
	c := h.client(cfg)
	if err := c.Start(context.Background()); err != nil {
		h.t.Fatalf("Start: %v", err)
	}
	return c
}

// retryIn is how long from the database's now a job waits to run.
func (h *harness) retryIn(job *hopper.JobRow) time.Duration {
	h.t.Helper()
	var now time.Time
	if err := h.pool.QueryRow(context.Background(), "SELECT now()").Scan(&now); err != nil {
		h.t.Fatal(err)
	}
	return job.ScheduledAt.Sub(now)
}

func TestOnErrorChangesOutcome(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	calls := &hookCalls{errs: map[hopper.JobID][]error{}}
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[failing]) error {
		switch job.Args.Msg {
		case "panic":
			panic("kaboom")
		case "cancel, then retry":
			return hopper.Cancel(errors.New(job.Args.Msg))
		}
		return errors.New(job.Args.Msg)
	})
	c := h.startedWithConfig(&hopper.Config{
		Workers: workers,
		OnError: func(_ context.Context, job *hopper.JobRow, err error) hopper.ErrorOutcome {
			calls.add(job.ID, err)
			var panicked *hopper.PanicError
			switch {
			case errors.As(err, &panicked):
				return hopper.ErrorOutcome{Discard: true}
			case strings.HasSuffix(err.Error(), "discard"):
				return hopper.ErrorOutcome{Discard: true}
			case strings.HasSuffix(err.Error(), "snooze"):
				return hopper.ErrorOutcome{Snooze: time.Hour}
			case strings.HasSuffix(err.Error(), "retry"):
				return hopper.ErrorOutcome{RetryAt: time.Now().Add(time.Hour)}
			}
			return hopper.ErrorOutcome{}
		},
	})
	insert := func(msg string, maxAttempts int) hopper.JobID {
		t.Helper()
		res, err := c.Insert(ctx, failing{Msg: msg}, &hopper.InsertOpts{MaxAttempts: maxAttempts})
		if err != nil {
			t.Fatal(err)
		}
		return res.Job.ID
	}

	discard := insert("discard", 25)
	if job := waitForJob(t, c, discard, hopper.JobStateDiscarded); job.Attempt != 1 || len(job.Errors) != 1 || job.Errors[0].Error != "discard" {
		t.Errorf("discarded job = %+v", job)
	}

	panicked := insert("panic", 25)
	if job := waitForJob(t, c, panicked, hopper.JobStateDiscarded); len(job.Errors) != 1 || job.Errors[0].Trace == "" {
		t.Errorf("panicked job = %+v", job)
	}

	snooze := insert("snooze", 25)
	job := waitForJob(t, c, snooze, hopper.JobStateScheduled)
	if job.Attempt != 0 || len(job.Errors) != 0 || job.SnoozeReason != "snooze" {
		t.Errorf("snoozed job = %+v, want the attempt given back and the error as the reason", job)
	}
	if d := h.retryIn(job); d < 59*time.Minute || d > time.Hour {
		t.Errorf("snoozed for %s, want an hour", d)
	}

	for _, id := range []hopper.JobID{insert("retry", 25), insert("cancel, then retry", 25)} {
		job := waitForJob(t, c, id, hopper.JobStateRetryable)
		if d := h.retryIn(job); d < 59*time.Minute || d > time.Hour || len(job.Errors) != 1 {
			t.Errorf("job retried in %s, job = %+v, want an hour", d, job)
		}
	}

	// The last attempt is the last, whatever the hook says.
	last := insert("retry", 1)
	waitForJob(t, c, last, hopper.JobStateDiscarded)

	unchanged := insert("other", 25)
	if job := waitForJob(t, c, unchanged, hopper.JobStateRetryable); h.retryIn(job) > 7*time.Second {
		t.Errorf("default outcome retried in %s, want the default backoff", h.retryIn(job))
	}

	for _, id := range []hopper.JobID{discard, panicked, snooze, last, unchanged} {
		if n := len(calls.get(id)); n != 1 {
			t.Errorf("OnError called %d times for job %s, want 1", n, id)
		}
	}
	if errs := calls.get(panicked); len(errs) == 1 {
		var p *hopper.PanicError
		if !errors.As(errs[0], &p) || p.Value != "kaboom" {
			t.Errorf("OnError saw %v for a panic, want a *PanicError", errs[0])
		}
	}
}

func TestOnErrorNotCalled(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	calls := &hookCalls{errs: map[hopper.JobID][]error{}}
	started := make(chan struct{}, 1)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[failing]) error {
		if job.Args.Msg == "snooze" {
			return hopper.Snooze(time.Hour)
		}
		started <- struct{}{}
		<-ctx.Done()
		return context.Cause(ctx)
	})
	c := h.startedWithConfig(&hopper.Config{
		Workers: workers,
		OnError: func(_ context.Context, job *hopper.JobRow, err error) hopper.ErrorOutcome {
			calls.add(job.ID, err)
			return hopper.ErrorOutcome{Discard: true}
		},
	})
	snoozed, err := c.Insert(ctx, failing{Msg: "snooze"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	waitForJob(t, c, snoozed.Job.ID, hopper.JobStateScheduled)
	canceled, err := c.Insert(ctx, failing{Msg: "block"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := c.JobCancel(ctx, canceled.Job.ID); err != nil {
		t.Fatal(err)
	}
	waitForJob(t, c, canceled.Job.ID, hopper.JobStateCanceled)
	for _, id := range []hopper.JobID{snoozed.Job.ID, canceled.Job.ID} {
		if errs := calls.get(id); len(errs) != 0 {
			t.Errorf("OnError called for job %s with %v", id, errs)
		}
	}
}

func TestOnErrorPanicKeepsOutcome(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[failing]) error { return errors.New("x") })
	c := h.startedWithConfig(&hopper.Config{
		Workers: workers,
		OnError: func(context.Context, *hopper.JobRow, error) hopper.ErrorOutcome { panic("hook") },
	})
	res, err := c.Insert(ctx, failing{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if job := waitForJob(t, c, res.Job.ID, hopper.JobStateRetryable); h.retryIn(job) > 7*time.Second {
		t.Errorf("retried in %s, want the default backoff", h.retryIn(job))
	}
}

// fixedRetry retries after a fixed delay.
type fixedRetry time.Duration

func (r fixedRetry) NextRetry(*hopper.JobRow) time.Time { return time.Now().Add(time.Duration(r)) }

// nextRetryArgs is worked by a worker with its own NextRetry.
type nextRetryArgs struct {
	Hook bool `json:"hook"`
}

func (nextRetryArgs) Kind() string { return "next_retry" }

type nextRetryWorker struct {
	hopper.WorkerDefaults[nextRetryArgs]
}

func (nextRetryWorker) Work(context.Context, *hopper.Job[nextRetryArgs]) error {
	return errors.New("x")
}

func (nextRetryWorker) NextRetry(*hopper.Job[nextRetryArgs]) time.Time {
	return time.Now().Add(3 * time.Hour)
}

func TestRetryPolicyPrecedence(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[failing]) error { return errors.New("x") })
	hopper.AddWorker(workers, nextRetryWorker{})
	c := h.startedWithConfig(&hopper.Config{
		Workers: workers,
		Queues: map[string]hopper.QueueConfig{
			"own_policy": {MaxWorkers: 1, RetryPolicy: fixedRetry(2 * time.Hour)},
		},
		RetryPolicy: fixedRetry(time.Hour),
		OnError: func(_ context.Context, job *hopper.JobRow, _ error) hopper.ErrorOutcome {
			var args nextRetryArgs
			if json.Unmarshal(job.Args, &args) == nil && args.Hook {
				return hopper.ErrorOutcome{RetryAt: time.Now().Add(4 * time.Hour)}
			}
			return hopper.ErrorOutcome{}
		},
	})
	for _, c2 := range []struct {
		name string
		args hopper.JobArgs
		opts *hopper.InsertOpts
		want time.Duration
	}{
		{"client policy", failing{}, nil, time.Hour},
		{"queue policy", failing{}, &hopper.InsertOpts{Queue: "own_policy"}, 2 * time.Hour},
		{"worker NextRetry", nextRetryArgs{}, nil, 3 * time.Hour},
		{"OnError", nextRetryArgs{Hook: true}, nil, 4 * time.Hour},
	} {
		res, err := c.Insert(ctx, c2.args, c2.opts)
		if err != nil {
			t.Fatal(err)
		}
		job := waitForJob(t, c, res.Job.ID, hopper.JobStateRetryable)
		if d := h.retryIn(job); d < c2.want-time.Minute || d > c2.want {
			t.Errorf("%s: retried in %s, want %s", c2.name, d, c2.want)
		}
	}
}

func TestExponentialRetry(t *testing.T) {
	t.Parallel()
	r := hopper.ExponentialRetry{Base: time.Second, Max: 10 * time.Second}
	for _, c := range []struct {
		attempt  int
		min, max time.Duration
	}{
		{1, 900 * time.Millisecond, 1100 * time.Millisecond},
		{3, 3600 * time.Millisecond, 4400 * time.Millisecond},
		{20, 9 * time.Second, 10 * time.Second},
	} {
		start := time.Now()
		d := r.NextRetry(&hopper.JobRow{Attempt: c.attempt}).Sub(start)
		if d < c.min || d > c.max+10*time.Millisecond {
			t.Errorf("attempt %d: retry in %s, want %s to %s", c.attempt, d, c.min, c.max)
		}
	}
	if d := time.Until((hopper.ExponentialRetry{}).NextRetry(&hopper.JobRow{Attempt: 1})); d < 800*time.Millisecond || d > 1100*time.Millisecond {
		t.Errorf("zero ExponentialRetry retried in %s, want about a second", d)
	}
}
