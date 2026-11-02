// Package outputtest checks Config.MaxOutputBytes on a client of any
// driver, for the tests of each.
package outputtest

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver"
)

// limit is the MaxOutputBytes the tests set.
const limit = 64

// sized returns a string whose JSON encoding is n bytes: its quotes and
// n-2 letters.
func sized(n int) string { return strings.Repeat("x", n-2) }

// step is a job whose worker sets each of Outputs in turn, an output of
// that many bytes encoded, and returns nil, ReturnErr's error, or with
// ReturnSetOutput what the last SetOutput returned.
type step struct {
	Name            string `json:"name"`
	Outputs         []int  `json:"outputs,omitempty"`
	ReturnErr       bool   `json:"return_err,omitempty"`
	ReturnSetOutput bool   `json:"return_set_output,omitempty"`
}

func (step) Kind() string { return "output_step" }

// remote is a job no client works, for leases.
type remote struct{}

func (remote) Kind() string { return "output_remote" }

const remoteQueue = "output_remote"

// recorder keeps what SetOutput returned and the dependencies each step's
// claim carried, by step name.
type recorder struct {
	mu   sync.Mutex
	errs map[string][]error
	deps map[string][]driver.Dependency
}

func (r *recorder) work(ctx context.Context, job *hopper.Job[step]) error {
	var errs []error
	for _, n := range job.Args.Outputs {
		errs = append(errs, hopper.SetOutput(ctx, sized(n)))
	}
	r.mu.Lock()
	r.errs[job.Args.Name] = errs
	r.deps[job.Args.Name] = job.Dependencies
	r.mu.Unlock()
	switch {
	case job.Args.ReturnErr:
		return errors.New("worker failed")
	case job.Args.ReturnSetOutput && len(errs) > 0:
		return errs[len(errs)-1]
	}
	return nil
}

func (r *recorder) get(name string) ([]error, []driver.Dependency) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.errs[name], r.deps[name]
}

// Run checks Config.MaxOutputBytes on clients of drivers newDriver makes:
// an output of exactly the limit is kept, a larger one refused and its
// attempt discarded unless a smaller one follows, no limit by default, a
// workflow step reading its dependency's output, and a lease's Finish.
func Run[TTx any](t *testing.T, newDriver func(t *testing.T) driver.Driver[TTx]) {
	t.Helper()
	ctx := context.Background()
	start := func(t *testing.T, max int) (*hopper.Client[TTx], *recorder) {
		t.Helper()
		rec := &recorder{errs: map[string][]error{}, deps: map[string][]driver.Dependency{}}
		workers := hopper.NewWorkers()
		hopper.AddWorkFunc(workers, rec.work)
		c, err := hopper.NewClient(newDriver(t), &hopper.Config{
			Queues: map[string]hopper.QueueConfig{
				hopper.QueueDefault: {MaxWorkers: 4},
			},
			Workers:        workers,
			Logger:         slog.New(slog.DiscardHandler),
			MaxOutputBytes: max,
			PollInterval:   20 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = c.Stop(sctx)
		})
		return c, rec
	}
	finished := func(t *testing.T, c *hopper.Client[TTx], id hopper.JobID) *hopper.JobRow {
		t.Helper()
		for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(20 * time.Millisecond) {
			j, err := c.JobGet(ctx, id)
			if err == nil && j.State.Terminal() {
				return j
			}
			if time.Now().After(deadline) {
				t.Fatalf("job %s not finished: %+v, %v", id, j, err)
			}
		}
	}
	// insert inserts a job of maxAttempts attempts and waits for it to
	// finish. A refused output's job is given more than one, to show that
	// it is discarded at once rather than retried.
	insert := func(t *testing.T, c *hopper.Client[TTx], args step, maxAttempts int) *hopper.JobRow {
		t.Helper()
		res, err := c.Insert(ctx, args, &hopper.InsertOpts{MaxAttempts: maxAttempts})
		if err != nil {
			t.Fatal(err)
		}
		return finished(t, c, res.Job.ID)
	}

	t.Run("limit", func(t *testing.T) {
		c, rec := start(t, limit)
		at := insert(t, c, step{Name: "at", Outputs: []int{limit}}, 1)
		if errs, _ := rec.get("at"); at.State != hopper.JobStateCompleted || len(at.Output) != limit || errs[0] != nil {
			t.Errorf("output of exactly the limit: %s, %d bytes, SetOutput = %v; want completed with it", at.State, len(at.Output), errs)
		}

		over := insert(t, c, step{Name: "over", Outputs: []int{8, limit + 1}}, 3)
		errs, _ := rec.get("over")
		var tooLarge *hopper.OutputTooLargeError
		if len(errs) != 2 || errs[0] != nil || !errors.As(errs[1], &tooLarge) || !errors.Is(errs[1], hopper.ErrOutputTooLarge) ||
			tooLarge.Size != limit+1 || tooLarge.Limit != limit {
			t.Fatalf("SetOutput of one byte past the limit = %v, want an *OutputTooLargeError of %d against %d", errs, limit+1, limit)
		}
		if over.State != hopper.JobStateDiscarded || over.Attempt != 1 || len(over.Errors) != 1 || !strings.Contains(over.Errors[0].Error, "MaxOutputBytes") {
			t.Errorf("job whose worker returned nil after the refusal: %s at attempt %d, errors %v; want discarded with it at once", over.State, over.Attempt, over.Errors)
		}
		if len(over.Output) != 8 {
			t.Errorf("discarded job's output is %d bytes, want the 8 set before the refusal", len(over.Output))
		}

		smaller := insert(t, c, step{Name: "smaller", Outputs: []int{limit + 1, limit - 1}}, 1)
		if smaller.State != hopper.JobStateCompleted || len(smaller.Output) != limit-1 {
			t.Errorf("refused, then a smaller output: %s, %d bytes; want completed with the smaller", smaller.State, len(smaller.Output))
		}

		returned := insert(t, c, step{Name: "returned", Outputs: []int{limit + 1}, ReturnSetOutput: true}, 3)
		if returned.State != hopper.JobStateDiscarded || returned.Attempt != 1 || len(returned.Errors) != 1 || !strings.Contains(returned.Errors[0].Error, "MaxOutputBytes") {
			t.Errorf("job whose worker returned SetOutput's refusal: %s at attempt %d, errors %v; want discarded with it at once", returned.State, returned.Attempt, returned.Errors)
		}

		failed := insert(t, c, step{Name: "failed", Outputs: []int{limit + 1}, ReturnErr: true}, 1)
		if len(failed.Errors) != 1 || failed.Errors[0].Error != "worker failed" {
			t.Errorf("a worker that returned its own error after the refusal failed with %v, want its error", failed.Errors)
		}
	})

	t.Run("no limit", func(t *testing.T) {
		c, _ := start(t, 0)
		big := insert(t, c, step{Name: "big", Outputs: []int{1 << 20}}, 1)
		if big.State != hopper.JobStateCompleted || len(big.Output) != 1<<20 {
			t.Errorf("1 MiB output with no limit: %s, %d bytes; want completed with it", big.State, len(big.Output))
		}
	})

	t.Run("workflow", func(t *testing.T) {
		c, rec := start(t, limit)
		wf := hopper.NewWorkflow("outputs", nil)
		ok := wf.Add(step{Name: "ok", Outputs: []int{limit}}, &hopper.StepOpts{Name: "ok"})
		over := wf.Add(step{Name: "over", Outputs: []int{limit + 1}}, &hopper.StepOpts{Name: "over"})
		wf.Add(step{Name: "reader"}, &hopper.StepOpts{After: []hopper.Step{ok}, Dependencies: true})
		wf.Add(step{Name: "after_over"}, &hopper.StepOpts{After: []hopper.Step{over}, Dependencies: true})
		res, err := c.InsertWorkflow(ctx, wf)
		if err != nil {
			t.Fatal(err)
		}
		reader := finished(t, c, res.Jobs[2].Job.ID)
		_, deps := rec.get("reader")
		if reader.State != hopper.JobStateCompleted || len(deps) != 1 || deps[0].State != hopper.JobStateCompleted || len(deps[0].Output) != limit {
			t.Errorf("step after one with an output of the limit: %s, claimed with %+v; want its output", reader.State, deps)
		}
		if j := finished(t, c, res.Jobs[1].Job.ID); j.State != hopper.JobStateDiscarded {
			t.Errorf("step with an output past the limit is %s, want discarded", j.State)
		}
		if j := finished(t, c, res.Jobs[3].Job.ID); j.State != hopper.JobStateCanceled {
			t.Errorf("step after it is %s, want canceled", j.State)
		}
		if _, deps := rec.get("after_over"); deps != nil {
			t.Error("the step after the discarded one ran")
		}
	})

	t.Run("lease", func(t *testing.T) {
		c, _ := start(t, limit)
		ins, err := c.Insert(ctx, remote{}, &hopper.InsertOpts{Queue: remoteQueue})
		if err != nil {
			t.Fatal(err)
		}
		l, err := c.Leases().Claim(ctx, ins.Job.ID, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Leases().Finish(ctx, l.Token, hopper.LeaseResult{Output: sized(limit + 1)}); !errors.Is(err, hopper.ErrOutputTooLarge) {
			t.Fatalf("Finish with an output past the limit = %v, want ErrOutputTooLarge", err)
		}
		if j, err := c.JobGet(ctx, ins.Job.ID); err != nil || j.State != hopper.JobStateRunning {
			t.Fatalf("job after the refused Finish = %+v, %v; want it still running in its lease", j, err)
		}
		if err := c.Leases().Finish(ctx, l.Token, hopper.LeaseResult{Output: sized(limit)}); err != nil {
			t.Fatal(err)
		}
		if j := finished(t, c, ins.Job.ID); j.State != hopper.JobStateCompleted || len(j.Output) != limit {
			t.Errorf("job finished with an output of the limit: %s, %d bytes", j.State, len(j.Output))
		}

		// A failure is recorded whatever its output: dropped past the limit.
		ins, err = c.Insert(ctx, remote{}, &hopper.InsertOpts{Queue: remoteQueue})
		if err != nil {
			t.Fatal(err)
		}
		if l, err = c.Leases().Claim(ctx, ins.Job.ID, 0, nil); err != nil {
			t.Fatal(err)
		}
		if err := c.Leases().Finish(ctx, l.Token, hopper.LeaseResult{Err: hopper.Cancel(errors.New("gone")), Output: sized(limit + 1)}); err != nil {
			t.Fatalf("Finish of a failure with an output past the limit = %v, want it recorded", err)
		}
		if j := finished(t, c, ins.Job.ID); j.State != hopper.JobStateDiscarded || len(j.Output) != 0 {
			t.Errorf("failed job: %s with %d bytes of output; want discarded without it", j.State, len(j.Output))
		}
	})
}
