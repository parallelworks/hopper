package hopper_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/hoppertest"
)

type stepped struct {
	N int `json:"n"`
}

func (stepped) Kind() string { return "stepped" }

// stepTuning is fastTuning with a lease that a loaded machine does not lose:
// a lost lease cancels the attempt, which these tests count.
var stepTuning = func() hopper.Tuning {
	t := fastTuning
	t.LeaseTTL = 30 * time.Second
	return t
}()

// runLog records the steps that ran, in order.
type runLog struct {
	mu  sync.Mutex
	ran []string
}

func (l *runLog) add(s string) {
	l.mu.Lock()
	l.ran = append(l.ran, s)
	l.mu.Unlock()
}

func (l *runLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.ran)
}

// A job that fails in its second step resumes there on the next attempt,
// with the cursor the step recorded, and does not run its first step again.
func TestRunStepsResumes(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var log runLog
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[stepped]) error {
		return hopper.RunSteps(ctx, job,
			hopper.NamedStep("download", func(ctx context.Context, job *hopper.Job[stepped]) error {
				log.add("download")
				return nil
			}),
			hopper.NamedStep("load", func(ctx context.Context, job *hopper.Job[stepped]) error {
				page, ok, err := hopper.StepCursor[int](job.JobRow)
				if err != nil {
					return err
				}
				if !ok {
					log.add("load from the start")
					if err := hopper.RecordCursor(ctx, job, 7); err != nil {
						return err
					}
					return errors.New("load failed at page 7")
				}
				log.add("load from page " + string(rune('0'+page)))
				return nil
			}),
			hopper.NamedStep("notify", func(ctx context.Context, job *hopper.Job[stepped]) error {
				log.add("notify")
				return nil
			}),
		)
	})
	c := h.client(&hopper.Config{
		Queues:      map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers:     workers,
		RetryPolicy: &hopper.ExponentialRetry{Base: time.Millisecond, Max: time.Millisecond},
	})
	c.SetTuning(stepTuning)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := c.Insert(ctx, stepped{N: 1}, &hopper.InsertOpts{Await: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hopper.Await[struct{}](ctx, c, res.Job.ID); err != nil {
		t.Fatal(err)
	}
	want := []string{"download", "load from the start", "load from page 7", "notify"}
	if got := log.get(); !slices.Equal(got, want) {
		t.Errorf("steps ran %q, want %q", got, want)
	}
	job, err := c.JobGet(ctx, res.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if job.Attempt != 2 || job.Progress == nil || job.Progress.Step != "notify" || job.Progress.Cursor != nil {
		t.Errorf("finished job: attempt %d, progress %+v", job.Attempt, job.Progress)
	}
}

// RecordStepTx commits with the worker's transaction: rolled back, the step
// is not recorded and runs again; committed, it is.
func TestRecordStepTx(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var log runLog
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[stepped]) error {
		return hopper.RunSteps(ctx, job,
			hopper.NamedStep("store", func(ctx context.Context, job *hopper.Job[stepped]) error {
				log.add("store")
				tx, err := h.pool.Begin(ctx)
				if err != nil {
					return err
				}
				defer tx.Rollback(ctx) //nolint:errcheck // committed on the second attempt
				if err := hopper.RecordStepTx(ctx, tx, job, "store", map[string]int{"rows": 3}); err != nil {
					return err
				}
				if job.Attempt == 1 {
					return errors.New("the store failed before its commit")
				}
				return tx.Commit(ctx)
			}),
			hopper.NamedStep("report", func(ctx context.Context, job *hopper.Job[stepped]) error {
				log.add("report")
				return nil
			}),
		)
	})
	c := h.client(&hopper.Config{
		Queues:      map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers:     workers,
		RetryPolicy: &hopper.ExponentialRetry{Base: time.Millisecond, Max: time.Millisecond},
	})
	c.SetTuning(stepTuning)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := c.Insert(ctx, stepped{N: 2}, &hopper.InsertOpts{Await: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hopper.Await[struct{}](ctx, c, res.Job.ID); err != nil {
		t.Fatal(err)
	}
	if got, want := log.get(), []string{"store", "store", "report"}; !slices.Equal(got, want) {
		t.Errorf("steps ran %q, want %q", got, want)
	}
	job, err := c.JobGet(ctx, res.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	// RunSteps then recorded "report" on the pool, which clears the cursor.
	if job.Progress == nil || job.Progress.Step != "report" {
		t.Errorf("progress = %+v", job.Progress)
	}

	// JobRetry keeps the progress: the re-driven job has nothing left to do.
	if _, err := c.JobRetry(ctx, res.Job.ID); err != nil {
		t.Fatal(err)
	}
	waitForJob(t, c, res.Job.ID, hopper.JobStateCompleted)
	if got := log.get(); len(got) != 3 {
		t.Errorf("steps ran again after JobRetry: %q", got)
	}
}

func TestRecordStepOutsideAJob(t *testing.T) {
	t.Parallel()
	job := &hopper.Job[stepped]{JobRow: &hopper.JobRow{}}
	if err := hopper.RecordStep(context.Background(), job, "x", nil); !errors.Is(err, hopper.ErrNotInJob) {
		t.Errorf("RecordStep outside a job = %v, want ErrNotInJob", err)
	}
	if _, ok, err := hopper.StepCursor[int](job.JobRow); ok || err != nil {
		t.Errorf("StepCursor without progress = %v, %v", ok, err)
	}
}

// Under hoppertest.Work progress is kept in memory, so RunSteps runs every
// step, and a test can start a job part way through.
func TestRunStepsInline(t *testing.T) {
	t.Parallel()
	var log runLog
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[stepped]) error {
		return hopper.RunSteps(ctx, job,
			hopper.NamedStep("a", func(ctx context.Context, job *hopper.Job[stepped]) error {
				log.add("a")
				return hopper.RecordCursor(ctx, job, "half")
			}),
			hopper.NamedStep("b", func(ctx context.Context, job *hopper.Job[stepped]) error {
				log.add("b")
				return nil
			}),
		)
	})
	ctx := context.Background()
	if err := hoppertest.Work(ctx, t, workers, stepped{}, nil); err != nil {
		t.Fatal(err)
	}
	if err := hoppertest.Work(ctx, t, workers, stepped{}, &hoppertest.WorkOpts{Progress: &hopper.Progress{Step: "a"}}); err != nil {
		t.Fatal(err)
	}
	if got, want := log.get(), []string{"a", "b", "b"}; !slices.Equal(got, want) {
		t.Errorf("steps ran %q, want %q", got, want)
	}
	err := hoppertest.Work(ctx, t, workers, stepped{}, &hoppertest.WorkOpts{Progress: &hopper.Progress{Step: "renamed"}})
	var cancel *hopper.CancelError
	if !errors.As(err, &cancel) {
		t.Errorf("RunSteps with progress naming an unknown step = %v, want a CancelError", err)
	}
}

// A job completed with CompleteTx in a step, in a transaction the worker
// commits after RunSteps returns, finishes in that attempt: RunSteps does
// not record the step on the pool, which would wait on the job's row until
// the job timed out.
func TestRunStepsCompleteTx(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var log runLog
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[stepped]) error {
		tx, err := h.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		if err := hopper.RunSteps(ctx, job,
			hopper.NamedStep("prepare", func(ctx context.Context, job *hopper.Job[stepped]) error {
				log.add("prepare")
				return nil
			}),
			hopper.NamedStep("complete", func(ctx context.Context, job *hopper.Job[stepped]) error {
				log.add("complete")
				return hopper.CompleteTx(ctx, tx, job)
			}),
		); err != nil {
			return err
		}
		return tx.Commit(ctx)
	})
	c := h.client(&hopper.Config{
		Queues:      map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers:     workers,
		JobTimeout:  5 * time.Second,
		RetryPolicy: &hopper.ExponentialRetry{Base: time.Millisecond, Max: time.Millisecond},
	})
	c.SetTuning(stepTuning)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := c.Insert(ctx, stepped{N: 3}, &hopper.InsertOpts{Await: true})
	if err != nil {
		t.Fatal(err)
	}
	actx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := hopper.Await[struct{}](actx, c, res.Job.ID); err != nil {
		t.Fatal(err)
	}
	if got, want := log.get(), []string{"prepare", "complete"}; !slices.Equal(got, want) {
		t.Errorf("steps ran %q, want %q", got, want)
	}
	job, err := c.JobGet(ctx, res.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != hopper.JobStateCompleted || job.Attempt != 1 || job.Progress == nil || job.Progress.Step != "prepare" {
		t.Errorf("finished job: state %s, attempt %d, progress %+v", job.State, job.Attempt, job.Progress)
	}
}

// A step name PostgreSQL cannot store is refused before any step runs, and
// by RecordStep, rather than after a step's work, when its record fails.
func TestRunStepsRefusesUnstorableNames(t *testing.T) {
	t.Parallel()
	var log runLog
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[stepped]) error {
		if job.Args.N == 1 {
			return hopper.RecordStep(ctx, job, "bad \xff", nil)
		}
		return hopper.RunSteps(ctx, job, hopper.NamedStep("bad \x00", func(ctx context.Context, job *hopper.Job[stepped]) error {
			log.add("bad")
			return nil
		}))
	})
	ctx := context.Background()
	if err := hoppertest.Work(ctx, t, workers, stepped{}, nil); err == nil {
		t.Error("RunSteps accepted a step name with a NUL")
	}
	if err := hoppertest.Work(ctx, t, workers, stepped{N: 1}, nil); err == nil {
		t.Error("RecordStep accepted a step name with invalid UTF-8")
	}
	if got := log.get(); len(got) != 0 {
		t.Errorf("steps ran %q", got)
	}
}

// A transaction of the worker's that recorded a step and stays open across
// the next one holds the job's row: RunSteps' record of the next step on
// the pool fails at once, rather than waiting for the worker itself until
// the job times out and then landing after the rollback.
func TestRunStepsTxHeldAcrossSteps(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var log runLog
	errs := make(chan error, 4)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[stepped]) error {
		tx, err := h.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		err = hopper.RunSteps(ctx, job,
			hopper.NamedStep("store", func(ctx context.Context, job *hopper.Job[stepped]) error {
				log.add("store")
				if err := hopper.RecordStepTx(ctx, tx, job, "store", nil); err != nil {
					return err
				}
				// The second attempt ends the transaction within its step.
				if job.Attempt > 1 {
					return tx.Commit(ctx)
				}
				return nil
			}),
			hopper.NamedStep("report", func(ctx context.Context, job *hopper.Job[stepped]) error {
				log.add("report")
				return nil
			}),
		)
		if err != nil {
			errs <- err
			return err
		}
		return tx.Commit(ctx)
	})
	c := h.client(&hopper.Config{
		Queues:      map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers:     workers,
		JobTimeout:  time.Minute,
		RetryPolicy: &hopper.ExponentialRetry{Base: time.Millisecond, Max: time.Millisecond},
	})
	c.SetTuning(stepTuning)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := c.Insert(ctx, stepped{N: 4}, &hopper.InsertOpts{Await: true})
	if err != nil {
		t.Fatal(err)
	}
	actx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := hopper.Await[struct{}](actx, c, res.Job.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errs:
		if !errors.Is(err, hopper.ErrJobLocked) {
			t.Errorf("first attempt failed with %v, want ErrJobLocked", err)
		}
	default:
		t.Error("the first attempt did not fail")
	}
	if got, want := log.get(), []string{"store", "report", "store", "report"}; !slices.Equal(got, want) {
		t.Errorf("steps ran %q, want %q", got, want)
	}
}

// FromStart clears the progress JobRetry would otherwise resume from.
func TestJobRetryFromStart(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var log runLog
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[stepped]) error {
		return hopper.RunSteps(ctx, job,
			hopper.NamedStep("a", func(context.Context, *hopper.Job[stepped]) error { log.add("a"); return nil }),
			hopper.NamedStep("b", func(context.Context, *hopper.Job[stepped]) error { log.add("b"); return nil }),
		)
	})
	c := h.started(workers, 1)
	res, err := c.Insert(ctx, stepped{N: 5}, &hopper.InsertOpts{Await: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hopper.Await[struct{}](ctx, c, res.Job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.JobRetry(ctx, res.Job.ID); err != nil {
		t.Fatal(err)
	}
	waitForJob(t, c, res.Job.ID, hopper.JobStateCompleted)
	job, err := c.JobRetry(ctx, res.Job.ID, hopper.FromStart())
	if err != nil {
		t.Fatal(err)
	}
	if job.Progress != nil {
		t.Errorf("progress after FromStart = %+v", job.Progress)
	}
	waitForJob(t, c, res.Job.ID, hopper.JobStateCompleted)
	if got, want := log.get(), []string{"a", "b", "a", "b"}; !slices.Equal(got, want) {
		t.Errorf("steps ran %q, want %q", got, want)
	}
}

// A worker run inline from inside a running job is not that job's attempt.
func TestWorkersWorkInsideAJob(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	inner := hopper.NewWorkers()
	hopper.AddWorkFunc(inner, func(ctx context.Context, job *hopper.Job[noop]) error {
		if err := hopper.RecordStep(ctx, job, "inner", nil); err != nil {
			return err
		}
		if job.Progress == nil || job.Progress.Step != "inner" {
			return errors.New("progress not kept in the inline job")
		}
		if err := hopper.SetOutput(ctx, 1); !errors.Is(err, hopper.ErrNotInJob) {
			return fmt.Errorf("SetOutput inline = %w, want ErrNotInJob", err)
		}
		return nil
	})
	done := make(chan error, 1)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[stepped]) error {
		err := inner.Work(ctx, &hopper.JobRow{Kind: "noop", Args: []byte(`{}`)}, nil)
		done <- err
		return err
	})
	c := h.started(workers, 1)
	if _, err := c.Insert(ctx, stepped{N: 6}, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the outer job did not run")
	}
}
