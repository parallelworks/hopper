package hopper_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/parallelworks/hopper"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/internal/outputtest"
)

func TestMaxOutputBytes(t *testing.T) {
	t.Parallel()
	outputtest.Run(t, func(t *testing.T) driver.Driver[pgxTx] { return newHarness(t).d })
}

// TestCompleteTxRefusesARefusedOutput: CompleteTx returns SetOutput's
// refusal of an output past Config.MaxOutputBytes rather than complete the
// job in the worker's transaction without it, and a worker that returns
// that error has its job discarded, not retried.
func TestCompleteTxRefusesARefusedOutput(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	refused := make(chan error, 1)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[noop]) error {
		tx, err := h.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		_ = hopper.SetOutput(ctx, strings.Repeat("x", 100))
		err = hopper.CompleteTx(ctx, tx, job)
		select {
		case refused <- err:
		default:
		}
		return err
	})
	c := h.client(&hopper.Config{
		Queues:         map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers:        workers,
		MaxOutputBytes: 64,
	})
	c.SetTuning(fastTuning)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-refused; !errors.Is(err, hopper.ErrOutputTooLarge) {
		t.Fatalf("CompleteTx after a refused output = %v, want ErrOutputTooLarge", err)
	}
	if j := waitForJob(t, c, res.Job.ID, hopper.JobStateDiscarded, hopper.JobStateCompleted, hopper.JobStateRetryable); j.State != hopper.JobStateDiscarded || j.Attempt != 1 || len(j.Output) != 0 {
		t.Errorf("job = %s at attempt %d with %d bytes of output, want discarded at once without one", j.State, j.Attempt, len(j.Output))
	}
}

func TestMaxOutputBytesIsChecked(t *testing.T) {
	t.Parallel()
	if _, err := hopper.NewClient(newHarness(t).d, &hopper.Config{MaxOutputBytes: -1}); err == nil {
		t.Error("a negative MaxOutputBytes was accepted")
	}
}
