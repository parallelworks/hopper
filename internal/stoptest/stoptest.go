// Package stoptest checks Client.StopAndCancel on a client of any driver,
// for the tests of each.
package stoptest

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/hoppertest"
)

// block is a job whose worker waits for its context, or, after the first
// client stopped, completes.
type block struct{}

func (block) Kind() string { return "stoptest_block" }

// Run checks that StopAndCancel cancels a running job at once, with
// ErrClientStopped as its context's cause, and gives the job its attempt
// back, its last one too, so another client runs it again and completes
// it.
func Run[TTx any](t *testing.T, newDriver func(t *testing.T) driver.Driver[TTx]) {
	t.Helper()
	ctx := context.Background()
	d := newDriver(t)
	start := func(t *testing.T, work func(context.Context, *hopper.Job[block]) error) *hopper.Client[TTx] {
		t.Helper()
		workers := hopper.NewWorkers()
		hopper.AddWorkFunc(workers, work)
		c, err := hopper.NewClient(d, &hopper.Config{
			Queues:       map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
			Workers:      workers,
			Logger:       slog.New(slog.DiscardHandler),
			PollInterval: 20 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		hoppertest.Start(ctx, t, c)
		return c
	}

	started := make(chan struct{}, 1)
	cause := make(chan error, 1)
	first := start(t, func(ctx context.Context, _ *hopper.Job[block]) error {
		started <- struct{}{}
		<-ctx.Done()
		cause <- context.Cause(ctx)
		return ctx.Err()
	})
	res, err := first.Insert(ctx, block{}, &hopper.InsertOpts{MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("the job did not start")
	}
	stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := first.StopAndCancel(stopCtx); err != nil {
		t.Fatalf("StopAndCancel = %v", err)
	}
	select {
	case <-first.Stopped():
	default:
		t.Error("Stopped is open after StopAndCancel returned")
	}
	if err := <-cause; !errors.Is(err, hopper.ErrClientStopped) {
		t.Errorf("the job's context was canceled with %v, want ErrClientStopped", err)
	}
	j, err := first.JobGet(ctx, res.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if j.State.Terminal() || j.Attempt != 0 || len(j.Errors) != 0 {
		t.Fatalf("job after StopAndCancel: %s at attempt %d, errors %v; want waiting with its only attempt given back", j.State, j.Attempt, j.Errors)
	}

	second := start(t, func(context.Context, *hopper.Job[block]) error { return nil })
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		j, err := second.JobGet(ctx, res.Job.ID)
		if err == nil && j.State.Terminal() {
			if j.State != hopper.JobStateCompleted || j.Attempt != 1 {
				t.Errorf("job run again: %s at attempt %d, want completed on attempt 1", j.State, j.Attempt)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("job not run again: %+v, %v", j, err)
		}
	}
}
