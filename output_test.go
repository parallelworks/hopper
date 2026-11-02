package hopper_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/internal/outputtest"
)

func TestMaxOutputBytes(t *testing.T) {
	t.Parallel()
	outputtest.Run(t, func(t *testing.T) driver.Driver[pgxTx] { return newHarness(t).d })
}

// TestCompleteTxRolledBackAfterARefusal: a worker that completed its job
// with CompleteTx, then had an output refused and rolled its transaction
// back, returning the refusal, has its job discarded, not retried.
func TestCompleteTxRolledBackAfterARefusal(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[noop]) error {
		tx, err := h.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		if err := hopper.SetOutput(ctx, "small"); err != nil {
			return err
		}
		if err := hopper.CompleteTx(ctx, tx, job); err != nil {
			return err
		}
		return hopper.SetOutput(ctx, strings.Repeat("x", 100))
	})
	c := h.client(&hopper.Config{
		Queues:         map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers:        workers,
		MaxOutputBytes: 64,
	})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	if j := waitForJob(t, c, res.Job.ID, hopper.JobStateDiscarded, hopper.JobStateCompleted, hopper.JobStateRetryable); j.State != hopper.JobStateDiscarded || j.Attempt != 1 {
		t.Errorf("job = %s at attempt %d, want discarded at once", j.State, j.Attempt)
	}
}

// TestOnErrorGetsTheRefusal: Config.OnError is given the refusal the
// worker returned, as returned, and can choose a retry.
func TestOnErrorGetsTheRefusal(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	hooked := make(chan error, 1)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[noop]) error {
		return hopper.SetOutput(ctx, strings.Repeat("x", 100))
	})
	c := h.client(&hopper.Config{
		Queues:         map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers:        workers,
		MaxOutputBytes: 64,
		OnError: func(_ context.Context, _ *hopper.JobRow, err error) hopper.ErrorOutcome {
			select {
			case hooked <- err:
			default:
			}
			return hopper.ErrorOutcome{RetryAt: time.Now().Add(time.Hour)}
		},
	})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-hooked:
		if got := fmt.Sprintf("%T", err); got != "*hopper.OutputTooLargeError" {
			t.Errorf("OnError was given %s (%v), want the *hopper.OutputTooLargeError, unwrapped", got, err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("OnError was not called")
	}
	if j := waitForJob(t, c, res.Job.ID, hopper.JobStateRetryable, hopper.JobStateDiscarded, hopper.JobStateCompleted); j.State != hopper.JobStateRetryable {
		t.Errorf("job = %s, want retryable as OnError chose", j.State)
	}
}

// TestJobCancelWinsOverARefusal: a job canceled with JobCancel while its
// worker ran is canceled, not discarded, when the worker then returns a
// refusal.
func TestJobCancelWinsOverARefusal(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	started := make(chan struct{}, 1)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[noop]) error {
		started <- struct{}{}
		<-ctx.Done()
		return hopper.SetOutput(ctx, strings.Repeat("x", 100))
	})
	c := h.client(&hopper.Config{
		Queues:         map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers:        workers,
		MaxOutputBytes: 64,
	})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := c.Insert(ctx, noop{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("the job's worker did not run")
	}
	if _, err := c.JobCancel(ctx, res.Job.ID); err != nil {
		t.Fatal(err)
	}
	if j := waitForJob(t, c, res.Job.ID, hopper.JobStateCanceled, hopper.JobStateDiscarded); j.State != hopper.JobStateCanceled {
		t.Errorf("job = %s, want canceled", j.State)
	}
}

func TestMaxOutputBytesIsChecked(t *testing.T) {
	t.Parallel()
	if _, err := hopper.NewClient(newHarness(t).d, &hopper.Config{MaxOutputBytes: -1}); err == nil || !strings.Contains(err.Error(), "MaxOutputBytes") {
		t.Errorf("NewClient with a negative MaxOutputBytes = %v, want its refusal", err)
	}
}
