package hopper_test

import (
	"context"
	"errors"
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
	c.SetTuning(fastTuning)
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
	c.SetTuning(fastTuning)
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
	if err == nil {
		t.Error("RunSteps accepted progress naming an unknown step")
	}
}
