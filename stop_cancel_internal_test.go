package hopper

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/hoppermigrate"
)

// stopDriver is a driver whose executor hands out the jobs it is given,
// once, and records their results: enough for a client to start, run them
// and stop inside a synctest bubble, which fails the test if any goroutine
// the client started is left behind.
type stopDriver struct {
	driver.Driver[struct{}]
	exec *stopExec
}

func (d stopDriver) Executor() driver.Executor       { return d.exec }
func (stopDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }

type stopExec struct {
	driver.Executor
	mu      sync.Mutex
	jobs    []*driver.JobRow
	results map[driver.JobID]driver.JobFinalize
	deleted bool
}

func (e *stopExec) SchemaVersion(context.Context) (int, error) { return hoppermigrate.Latest(), nil }

func (e *stopExec) ClientRegister(context.Context, driver.ClientRegisterParams) (int64, error) {
	return 1, nil
}

func (e *stopExec) ClientRenew(context.Context, driver.ClientRenewParams) (driver.ClientRenewResult, error) {
	return driver.ClientRenewResult{Renewed: true}, nil
}

func (e *stopExec) ClientDelete(context.Context, int64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.deleted = true
	return nil
}

func (e *stopExec) QueueEnsure(context.Context, []string) error { return nil }

func (e *stopExec) QueueList(context.Context) ([]*driver.QueueRow, error) { return nil, nil }

func (e *stopExec) SubscriptionUpsert(context.Context, []driver.SubscriptionRow) error { return nil }

func (e *stopExec) StreamConsumerUpsert(context.Context, []driver.StreamConsumerRow) error {
	return nil
}

func (e *stopExec) PeriodicRegister(context.Context, driver.PeriodicRegisterParams) error {
	return nil
}
func (e *stopExec) Notify(context.Context, string, []string) error { return nil }

func (e *stopExec) JobClaim(_ context.Context, p driver.JobClaimParams) (driver.JobClaimResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := min(p.Limit, len(e.jobs))
	claimed := e.jobs[:n]
	e.jobs = e.jobs[n:]
	for _, j := range claimed {
		j.State, j.AttemptedBy = driver.JobStateRunning, p.ClientID
	}
	return driver.JobClaimResult{Jobs: claimed}, nil
}

func (e *stopExec) JobFinalizeMany(_ context.Context, p driver.JobFinalizeParams) ([]driver.JobFinalized, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	applied := make([]driver.JobFinalized, 0, len(p.Jobs))
	for _, f := range p.Jobs {
		e.results[f.ID] = f
		applied = append(applied, driver.JobFinalized{ID: f.ID, Fence: f.Fence})
	}
	return applied, nil
}

func (e *stopExec) result(id driver.JobID) (driver.JobFinalize, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	f, ok := e.results[id]
	return f, ok
}

type stopArgs struct {
	// Cancel has the worker return Cancel once its context is canceled,
	// and Fail an error of its own.
	Cancel bool `json:"cancel,omitempty"`
	Fail   bool `json:"fail,omitempty"`
}

func (stopArgs) Kind() string { return "stop_args" }

// stopClient returns an unstarted client over jobs whose worker waits for
// its context and returns its error, or Cancel, sending each context's
// cause to causes.
func stopClient(t *testing.T, causes chan<- error, jobs ...*driver.JobRow) (*Client[struct{}], *stopExec) {
	t.Helper()
	return stopClientWith(t, causes, nil, jobs...)
}

// stopClientWith is stopClient with a Config.OnError.
func stopClientWith(t *testing.T, causes chan<- error, onError func(context.Context, *JobRow, error) ErrorOutcome, jobs ...*driver.JobRow) (*Client[struct{}], *stopExec) {
	t.Helper()
	exec := &stopExec{jobs: jobs, results: map[driver.JobID]driver.JobFinalize{}}
	workers := NewWorkers()
	AddWorkFunc(workers, func(ctx context.Context, job *Job[stopArgs]) error {
		<-ctx.Done()
		causes <- context.Cause(ctx)
		switch {
		case job.Args.Cancel:
			return Cancel(errors.New("gave up"))
		case job.Args.Fail:
			return errors.New("failed")
		}
		return fmt.Errorf("work: %w", ctx.Err())
	})
	c, err := NewClient(stopDriver{exec: exec}, &Config{
		Queues:    map[string]QueueConfig{QueueDefault: {MaxWorkers: 10}},
		Workers:   workers,
		Logger:    slog.New(slog.DiscardHandler),
		NeverLead: true,
		OnError:   onError,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c, exec
}

var stopJobs atomic.Uint32

func stopJob(attempt, maxAttempts int, args string) *driver.JobRow {
	n := stopJobs.Add(1)
	return &driver.JobRow{
		ID: driver.JobID{byte(n >> 8), byte(n)}, Kind: "stop_args", Queue: QueueDefault, Args: []byte(args),
		Attempt: attempt, MaxAttempts: maxAttempts, Fence: 1, State: driver.JobStateRunning,
	}
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// TestStoppedBeforeStart: Stopped is open until Stop, before Start too, and
// a stopped client cannot be started.
func TestStoppedBeforeStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, _ := stopClient(t, make(chan error, 1))
		ctx := context.Background()
		if isClosed(c.Stopped()) {
			t.Fatal("Stopped is closed before Stop")
		}
		if err := c.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		if !isClosed(c.Stopped()) {
			t.Fatal("Stopped is open after Stop")
		}
		if err := c.StopAndCancel(ctx); err != nil {
			t.Errorf("StopAndCancel after Stop = %v", err)
		}
		if err := c.Start(ctx); !errors.Is(err, ErrClientStopped) {
			t.Errorf("Start after Stop = %v, want ErrClientStopped", err)
		}
	})
}

// TestStopAndCancelRacingStop: Stop and StopAndCancel called at once, again
// and again, all return nil once the client stopped, having canceled its
// jobs at once with ErrClientStopped. Stopped is closed once (a second
// close would panic). Each job whose worker returned an error, its own or
// the cancellation, is retried at once with the error recorded and its
// attempt given back, its last one too; the one whose worker returned
// Cancel is discarded. Nothing the client started is left running when the
// bubble ends.
func TestStopAndCancelRacingStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		causes := make(chan error, 4)
		retried, last, canceled, failed := stopJob(1, 3, `{}`), stopJob(3, 3, `{}`), stopJob(1, 3, `{"cancel":true}`), stopJob(1, 3, `{"fail":true}`)
		c, exec := stopClient(t, causes, retried, last, canceled, failed)
		ctx := context.Background()
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		synctest.Wait() // the jobs are running, waiting for their contexts
		start := time.Now()
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Go(func() {
				var err error
				if i%2 == 0 {
					err = c.Stop(ctx)
				} else {
					err = c.StopAndCancel(ctx)
				}
				if err != nil {
					t.Errorf("stop %d = %v", i, err)
				}
				if !isClosed(c.Stopped()) {
					t.Errorf("stop %d returned before Stopped was closed", i)
				}
			})
		}
		wg.Wait()
		if d := time.Since(start); d != 0 {
			t.Errorf("stopping took %s, want no wait for the jobs", d)
		}
		for range 4 {
			if cause := <-causes; !errors.Is(cause, ErrClientStopped) {
				t.Errorf("a job's context was canceled with %v, want ErrClientStopped", cause)
			}
		}
		for _, j := range []*driver.JobRow{retried, last, failed} {
			if f, ok := exec.result(j.ID); !ok || !f.Snooze || f.State != driver.JobStateRetryable || f.Delay != 0 || f.Error == nil || f.Error.Attempt != j.Attempt {
				t.Errorf("job on attempt %d of %d: %+v, %v; want retried at once with its error, its attempt given back", j.Attempt, j.MaxAttempts, f, ok)
			}
		}
		if f, _ := exec.result(failed.ID); f.Error == nil || f.Error.Error != "failed" {
			t.Errorf("failed job's error = %+v, want its own", f.Error)
		}
		if f, ok := exec.result(canceled.ID); !ok || f.State != driver.JobStateDiscarded {
			t.Errorf("job whose worker returned Cancel: %+v, %v; want discarded", f, ok)
		}
		if !exec.deleted {
			t.Error("the client's lease was not released")
		}
	})
}

// TestRunReturnsOnStop: Run returns nil once a StopAndCancel from elsewhere
// stopped the client, though its ctx has not ended.
func TestRunReturnsOnStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		causes := make(chan error, 1)
		c, _ := stopClient(t, causes, stopJob(1, 3, `{}`))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ran := make(chan error, 1)
		go func() { ran <- c.Run(ctx) }()
		synctest.Wait() // the job is running
		if err := c.StopAndCancel(ctx); err != nil {
			t.Fatal(err)
		}
		if err := <-ran; err != nil {
			t.Errorf("Run = %v, want nil once the client stopped", err)
		}
		if cause := <-causes; !errors.Is(cause, ErrClientStopped) {
			t.Errorf("the job's context was canceled with %v, want ErrClientStopped", cause)
		}
	})
}

// TestStopDeadlineEscalates: a second Stop whose ctx expires while the
// first waits returns its deadline and leaves the jobs running; a
// StopAndCancel then cancels them, and the first Stop returns nil, with the
// job given its attempt back. A single Stop's own deadline cancels the jobs.
func TestStopDeadlineEscalates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		causes := make(chan error, 1)
		job := stopJob(1, 3, `{}`)
		c, exec := stopClient(t, causes, job)
		ctx := context.Background()
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		first := make(chan error, 1)
		go func() { first <- c.Stop(ctx) }()
		synctest.Wait() // the first Stop waits for the job
		second, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := c.Stop(second); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("second Stop = %v, want its deadline", err)
		}
		synctest.Wait()
		select {
		case cause := <-causes:
			t.Fatalf("the second Stop's deadline canceled the job, with %v", cause)
		default:
		}
		if err := c.StopAndCancel(ctx); err != nil {
			t.Errorf("StopAndCancel = %v", err)
		}
		if err := <-first; err != nil {
			t.Errorf("first Stop = %v, want nil once StopAndCancel canceled the job", err)
		}
		if cause := <-causes; !errors.Is(cause, ErrClientStopped) {
			t.Errorf("the job's context was canceled with %v, want ErrClientStopped", cause)
		}
		if f, ok := exec.result(job.ID); !ok || !f.Snooze {
			t.Errorf("job = %+v, %v; want its attempt given back", f, ok)
		}

		// A single Stop's own deadline does the same, and returns it.
		job = stopJob(1, 3, `{}`)
		c, exec = stopClient(t, causes, job)
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		stopCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := c.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Stop = %v, want its deadline", err)
		}
		if cause := <-causes; !errors.Is(cause, ErrClientStopped) {
			t.Errorf("Stop's deadline canceled the job with %v, want ErrClientStopped", cause)
		}
		if f, ok := exec.result(job.ID); !ok || !f.Snooze {
			t.Errorf("job after Stop's deadline = %+v, %v; want its attempt given back", f, ok)
		}
	})
}

// TestStopAndCancelOnError: Config.OnError is called for an attempt the stop
// canceled, with a context that is not canceled, and the outcome it
// chooses applies: a discard discards the job, and a retry it schedules
// gives the attempt back, on the last attempt too.
func TestStopAndCancelOnError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		causes := make(chan error, 2)
		discard, later := stopJob(1, 3, `{"fail":true}`), stopJob(3, 3, `{}`)
		var hookCtxErr error
		var mu sync.Mutex
		c, exec := stopClientWith(t, causes, func(ctx context.Context, row *JobRow, _ error) ErrorOutcome {
			mu.Lock()
			hookCtxErr = errors.Join(hookCtxErr, ctx.Err())
			mu.Unlock()
			if row.ID == discard.ID {
				return ErrorOutcome{Discard: true}
			}
			return ErrorOutcome{RetryAt: time.Now().Add(time.Minute)}
		}, discard, later)
		ctx := context.Background()
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if err := c.StopAndCancel(ctx); err != nil {
			t.Fatal(err)
		}
		<-causes
		<-causes
		if hookCtxErr != nil {
			t.Errorf("OnError's context was canceled: %v", hookCtxErr)
		}
		if f, ok := exec.result(discard.ID); !ok || f.State != driver.JobStateDiscarded || f.Error == nil {
			t.Errorf("job OnError discarded: %+v, %v", f, ok)
		}
		if f, ok := exec.result(later.ID); !ok || f.State != driver.JobStateRetryable || !f.Snooze || f.Delay != time.Minute {
			t.Errorf("job on its last attempt OnError retried in a minute: %+v, %v; want that retry, its attempt given back", f, ok)
		}
	})
}
