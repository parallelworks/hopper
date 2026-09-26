package hoppertest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver/hopperpgx"
	"github.com/parallelworks/hopper/hoppertest"
	"github.com/parallelworks/hopper/internal/testdb"
)

type sendEmail struct {
	UserID int `json:"user_id"`
}

func (sendEmail) Kind() string { return "send_email" }

type other struct{}

func (other) Kind() string { return "other" }

// fakeT captures a failure instead of ending the test.
type fakeT struct {
	testing.TB
	failed string
}

func (f *fakeT) Helper() {}
func (f *fakeT) Fatalf(format string, args ...any) {
	f.failed = format
	panic(fakeT{})
}

func expectFailure(t *testing.T, fn func(tb testing.TB)) {
	t.Helper()
	ft := &fakeT{TB: t}
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(fakeT); !ok {
				panic(r)
			}
		}
		if ft.failed == "" {
			t.Fatal("expected the helper to fail the test")
		}
	}()
	fn(ft)
}

func TestRequireInserted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.Pool(t)
	client, err := hopper.NewClient(hopperpgx.New(pool), nil)
	if err != nil {
		t.Fatal(err)
	}

	hoppertest.RequireNotInserted[sendEmail](ctx, t, client, nil)
	expectFailure(t, func(tb testing.TB) { hoppertest.RequireInserted[sendEmail](ctx, tb, client, nil) })

	if _, err := client.Insert(ctx, sendEmail{UserID: 42}, &hopper.InsertOpts{Queue: "email", Priority: hopper.PriorityHigh}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Insert(ctx, other{}, nil); err != nil {
		t.Fatal(err)
	}
	job := hoppertest.RequireInserted[sendEmail](ctx, t, client, nil)
	if job.Args.UserID != 42 || job.Queue != "email" {
		t.Errorf("job = %+v args %+v", job.JobRow, job.Args)
	}
	hoppertest.RequireInserted[sendEmail](ctx, t, client, &hoppertest.RequireOpts{Queue: "email", Priority: hopper.PriorityHigh, State: hopper.JobStateAvailable})
	expectFailure(t, func(tb testing.TB) {
		hoppertest.RequireInserted[sendEmail](ctx, tb, client, &hoppertest.RequireOpts{Queue: "default"})
	})
	expectFailure(t, func(tb testing.TB) { hoppertest.RequireNotInserted[sendEmail](ctx, tb, client, nil) })

	if _, err := client.Insert(ctx, sendEmail{UserID: 43}, nil); err != nil {
		t.Fatal(err)
	}
	expectFailure(t, func(tb testing.TB) { hoppertest.RequireInserted[sendEmail](ctx, tb, client, nil) })
	jobs := hoppertest.RequireManyInserted[sendEmail](ctx, t, client, 2, nil)
	if jobs[0].Args.UserID != 42 || jobs[1].Args.UserID != 43 {
		t.Errorf("order = %d, %d", jobs[0].Args.UserID, jobs[1].Args.UserID)
	}

	// Inside a transaction that is rolled back.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // cleanup
	if _, err := client.InsertTx(ctx, tx, other{}, nil); err != nil {
		t.Fatal(err)
	}
	hoppertest.RequireManyInsertedTx[other](ctx, t, client, tx, 2, nil)
	hoppertest.RequireInsertedTx[sendEmail](ctx, t, client, tx, &hoppertest.RequireOpts{Queue: "email"})
	hoppertest.RequireNotInsertedTx[sendEmail](ctx, t, client, tx, &hoppertest.RequireOpts{Queue: "nope"})
}

func TestWork(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	workers := hopper.NewWorkers()
	var got *hopper.Job[sendEmail]
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[sendEmail]) error {
		got = job
		if job.Args.UserID < 0 {
			return hopper.Cancel(errors.New("negative"))
		}
		if _, ok := ctx.Deadline(); !ok {
			return errors.New("no deadline")
		}
		return nil
	})
	slow := hopper.NewWorkers()
	hopper.AddWorker(slow, &slowWorker{})

	if err := hoppertest.Work(ctx, t, workers, sendEmail{UserID: 1}, &hoppertest.WorkOpts{Queue: "email", Attempt: 3}); err == nil {
		t.Fatal("expected the no-deadline error: Work applies only the worker's timeout")
	}
	if got == nil || got.Args.UserID != 1 || got.Queue != "email" || got.Attempt != 3 || got.ID.IsZero() || got.State != hopper.JobStateRunning {
		t.Errorf("job seen by worker = %+v", got.JobRow)
	}
	var cancel *hopper.CancelError
	if err := hoppertest.Work(ctx, t, workers, sendEmail{UserID: -1}, nil); !errors.As(err, &cancel) {
		t.Errorf("err = %v, want CancelError", err)
	}
	var unknown *hopper.UnknownKindError
	if err := hoppertest.Work(ctx, t, workers, other{}, nil); !errors.As(err, &unknown) {
		t.Errorf("unregistered kind: err = %v", err)
	}
	// The worker's timeout applies.
	if err := hoppertest.Work(ctx, t, slow, sendEmail{}, nil); !errors.Is(err, hopper.ErrJobTimeout) {
		t.Errorf("slow worker: err = %v, want timeout", err)
	}
}

type slowWorker struct {
	hopper.WorkerDefaults[sendEmail]
}

func (slowWorker) Work(ctx context.Context, _ *hopper.Job[sendEmail]) error {
	<-ctx.Done()
	return context.Cause(ctx)
}

func (slowWorker) Timeout(*hopper.Job[sendEmail]) time.Duration { return 20 * time.Millisecond }
