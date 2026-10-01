package hopper_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/hoppertest"
)

type first struct{ N int }

func (first) Kind() string { return "ctx_first" }

type second struct{ N int }

func (second) Kind() string { return "ctx_second" }

type ping struct{ N int }

func (ping) Kind() string { return "ctx_ping" }

func (ping) Topic() string { return "ctx.ping" }

// A worker and a subscriber insert follow-up jobs through the client that
// runs them, taken from the context, without holding a reference.
func TestClientFromContextInJobs(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[first]) error {
		c, ok := hopper.ClientFromContext[pgxTx](ctx)
		if !ok {
			return errors.New("no client in a worker's context")
		}
		_, err := c.Insert(ctx, second{N: job.Args.N + 1}, nil)
		return err
	})
	hopper.Subscribe(workers, hopper.Subscription{Name: "ctx-pings", Pattern: "ctx.ping"}, func(ctx context.Context, msg *hopper.Message[ping]) error {
		c, ok := hopper.ClientFromContext[pgxTx](ctx)
		if !ok {
			return errors.New("no client in a subscriber's context")
		}
		_, err := c.Insert(ctx, second{N: msg.Payload.N + 100}, nil)
		return err
	})
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[second]) error { return nil })
	c := h.client(&hopper.Config{
		Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 4}},
		Workers: workers,
	})
	hoppertest.Start(ctx, t, c)

	if _, err := c.Insert(ctx, first{N: 1}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Publish(ctx, ping{N: 1}, nil); err != nil {
		t.Fatal(err)
	}
	seen := map[int]bool{}
	waitFor(t, func() bool {
		for row := range c.Jobs(ctx, hopper.JobFilter{Kinds: []string{"ctx_second"}}) {
			var args second
			if err := json.Unmarshal(row.Args, &args); err == nil {
				seen[args.N] = true
			}
		}
		return seen[2] && seen[101]
	})
}

// Outside a job the context carries no client, and a mismatched
// transaction type is not a client either.
func TestClientFromContextOutsideJob(t *testing.T) {
	t.Parallel()
	if _, ok := hopper.ClientFromContext[pgxTx](context.Background()); ok {
		t.Fatal("a plain context yielded a client")
	}
	h := newHarness(t)
	c := h.client(&hopper.Config{})
	ctx := hopper.ContextWithClient(context.Background(), c)
	if got, ok := hopper.ClientFromContext[pgxTx](ctx); !ok || got != c {
		t.Fatal("ContextWithClient did not round-trip the client")
	}
	if _, ok := hopper.ClientFromContext[struct{}](ctx); ok {
		t.Fatal("a client of another transaction type was returned")
	}
}

// hoppertest.Work runs a worker inline; with ContextWithClient the worker
// can still reach a client.
func TestClientFromContextWithWork(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[first]) error {
		c, ok := hopper.ClientFromContext[pgxTx](ctx)
		if !ok {
			return hopper.ErrNotInJob
		}
		_, err := c.Insert(ctx, second{N: job.Args.N}, nil)
		return err
	})
	c := h.client(&hopper.Config{Workers: workers})
	if err := hoppertest.Work(ctx, t, workers, first{N: 7}, nil); !errors.Is(err, hopper.ErrNotInJob) {
		t.Fatalf("Work without a client in the context: %v, want ErrNotInJob", err)
	}
	if err := hoppertest.Work(hopper.ContextWithClient(ctx, c), t, workers, first{N: 7}, nil); err != nil {
		t.Fatal(err)
	}
	hoppertest.RequireInserted[second](ctx, t, c, nil)
}
