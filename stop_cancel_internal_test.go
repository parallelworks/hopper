package hopper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
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
	// claimGate, if set, holds the first claim until it is closed, as a
	// claim in flight when the client stops.
	claimGate chan struct{}
	mu        sync.Mutex
	jobs      []*driver.JobRow
	results   map[driver.JobID]driver.JobFinalize
	deleted   bool
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
	if e.claimGate != nil {
		<-e.claimGate
	}
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

// TestStopAndCancelSkipsOnError: Config.OnError is not consulted for an
// attempt the stop canceled: the job is retried at once with its error
// recorded and its attempt given back, the last attempt too, whatever the
// hook would have chosen.
func TestStopAndCancelSkipsOnError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		causes := make(chan error, 2)
		failed, last := stopJob(1, 3, `{"fail":true}`), stopJob(3, 3, `{}`)
		var hooks atomic.Int32
		c, exec := stopClientWith(t, causes, func(context.Context, *JobRow, error) ErrorOutcome {
			hooks.Add(1)
			return ErrorOutcome{Discard: true}
		}, failed, last)
		ctx := context.Background()
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if err := c.StopAndCancel(ctx); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			if cause := <-causes; !errors.Is(cause, ErrClientStopped) {
				t.Errorf("a job's context was canceled with %v, want ErrClientStopped", cause)
			}
		}
		if n := hooks.Load(); n != 0 {
			t.Errorf("OnError was called %d times for attempts the stop canceled", n)
		}
		for _, j := range []*driver.JobRow{failed, last} {
			if f, ok := exec.result(j.ID); !ok || f.State != driver.JobStateRetryable || !f.Snooze || f.Delay != 0 || f.Error == nil {
				t.Errorf("job on attempt %d of %d: %+v, %v; want retried at once with its error, its attempt given back", j.Attempt, j.MaxAttempts, f, ok)
			}
		}
	})
}

type stopSlowArgs struct{}

func (stopSlowArgs) Kind() string { return "stop_args" }

// slowStopClient returns an unstarted client over jobs whose worker runs
// work, with a JobTimeout of a second and onError as Config.OnError.
func slowStopClient(t *testing.T, work func(context.Context) error, onError func(context.Context, *JobRow, error) ErrorOutcome, jobs ...*driver.JobRow) (*Client[struct{}], *stopExec) {
	t.Helper()
	exec := &stopExec{jobs: jobs, results: map[driver.JobID]driver.JobFinalize{}}
	workers := NewWorkers()
	AddWorkFunc(workers, func(ctx context.Context, _ *Job[stopSlowArgs]) error { return work(ctx) })
	c, err := NewClient(stopDriver{exec: exec}, &Config{
		Queues:     map[string]QueueConfig{QueueDefault: {MaxWorkers: 10}},
		Workers:    workers,
		Logger:     slog.New(slog.DiscardHandler),
		NeverLead:  true,
		JobTimeout: time.Second,
		OnError:    onError,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c, exec
}

// TestStopAfterTimeoutCounts: an attempt whose own timeout ended it before
// StopAndCancel came, though its worker returns only after the stop, timed
// out: its attempt counts, OnError is called for it as for any timeout,
// and on its last attempt it is discarded.
func TestStopAfterTimeoutCounts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		retried, last := stopJob(1, 3, `{}`), stopJob(3, 3, `{}`)
		var hooks atomic.Int32
		c, exec := slowStopClient(t, func(context.Context) error {
			time.Sleep(5 * time.Second) // past its timeout, ignoring its context
			return errors.New("slow")
		}, func(context.Context, *JobRow, error) ErrorOutcome {
			hooks.Add(1)
			return ErrorOutcome{}
		}, retried, last)
		ctx := context.Background()
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second) // the timeouts fired
		if err := c.StopAndCancel(ctx); err != nil {
			t.Fatal(err)
		}
		if f, ok := exec.result(retried.ID); !ok || f.Snooze || f.State != driver.JobStateRetryable || f.Error == nil || !strings.Contains(f.Error.Error, ErrJobTimeout.Error()) {
			t.Errorf("timed-out job with attempts left: %+v, %v; want a counted retry", f, ok)
		}
		if f, ok := exec.result(last.ID); !ok || f.State != driver.JobStateDiscarded {
			t.Errorf("timed-out job on its last attempt: %+v, %v; want discarded", f, ok)
		}
		if n := hooks.Load(); n != 2 {
			t.Errorf("OnError was called %d times for the two timeouts, want 2", n)
		}
	})
}

// TestStopDeadlineCancelsOnError: the hook of an attempt that failed on
// its own keeps a context that the stop's deadline cancels, so a hook that
// honors it returns within the stop's grace and the result is written.
func TestStopDeadlineCancelsOnError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		job := stopJob(1, 3, `{}`)
		c, exec := slowStopClient(t, func(context.Context) error { return errors.New("failed") }, func(ctx context.Context, _ *JobRow, _ error) ErrorOutcome {
			select { // a call that honors its context
			case <-ctx.Done():
			case <-time.After(time.Hour):
			}
			return ErrorOutcome{}
		}, job)
		ctx := context.Background()
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		synctest.Wait() // the hook is waiting
		stopCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := c.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Stop = %v, want its deadline", err)
		}
		if f, ok := exec.result(job.ID); !ok || f.State != driver.JobStateRetryable || f.Snooze {
			t.Errorf("job whose hook the deadline canceled: %+v, %v; want its counted retry written", f, ok)
		}
	})
}

// TestStopAndCancelClaimInFlight: a job from a claim in flight when
// StopAndCancel cancels the jobs never runs, and gets its attempt back with
// no error recorded.
func TestStopAndCancelClaimInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		causes := make(chan error, 1)
		job := stopJob(3, 3, `{}`)
		c, exec := stopClient(t, causes, job)
		exec.claimGate = make(chan struct{})
		ctx := context.Background()
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		synctest.Wait() // the claim is in flight
		stopped := make(chan error, 1)
		go func() { stopped <- c.StopAndCancel(ctx) }()
		synctest.Wait() // the stop waits for the claim
		close(exec.claimGate)
		if err := <-stopped; err != nil {
			t.Fatal(err)
		}
		select {
		case cause := <-causes:
			t.Errorf("the job from the claim in flight ran, canceled with %v", cause)
		default:
		}
		if f, ok := exec.result(job.ID); !ok || !f.Snooze || f.State != driver.JobStateScheduled || f.Delay != 0 || f.Error != nil {
			t.Errorf("job from the claim in flight: %+v, %v; want its attempt given back, with no error", f, ok)
		}
	})
}

// blockingStore is a LogStore whose writers' Close waits for their context.
type blockingStore struct{}

type blockingWriter struct{ ctx context.Context }

func (blockingWriter) Write(p []byte) (int, error) { return len(p), nil }

func (w blockingWriter) Close() error {
	<-w.ctx.Done()
	return w.ctx.Err()
}

func (blockingStore) Writer(ctx context.Context, _ *JobRow) (io.WriteCloser, error) {
	return blockingWriter{ctx: ctx}, nil
}

func (blockingStore) Logs(context.Context, JobID) ([]AttemptLog, error) { return nil, nil }

// TestStopDeadlineLogCloseBounded: a Stop's deadline cancels a job whose
// worker takes a moment to return and whose store's Close blocks. The log's
// close waits at most half the stop's grace, and OnError is not consulted,
// so the result is still written, its attempt given back.
func TestStopDeadlineLogCloseBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		job := stopJob(1, 3, `{}`)
		exec := &stopExec{jobs: []*driver.JobRow{job}, results: map[driver.JobID]driver.JobFinalize{}}
		workers := NewWorkers()
		AddWorkFunc(workers, func(ctx context.Context, _ *Job[stopSlowArgs]) error {
			Logger(ctx).Info("running")
			<-ctx.Done()
			time.Sleep(100 * time.Millisecond)
			return ctx.Err()
		})
		c, err := NewClient(stopDriver{exec: exec}, &Config{
			Queues:    map[string]QueueConfig{QueueDefault: {MaxWorkers: 1}},
			Workers:   workers,
			Logger:    slog.New(slog.DiscardHandler),
			NeverLead: true,
			JobLogs:   JobLogsConfig{Store: blockingStore{}},
			OnError: func(context.Context, *JobRow, error) ErrorOutcome {
				t.Error("OnError was called for an attempt the stop canceled")
				return ErrorOutcome{}
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		synctest.Wait() // the job is running
		stopCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := c.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Stop = %v, want its deadline", err)
		}
		if f, ok := exec.result(job.ID); !ok || !f.Snooze || f.State != driver.JobStateRetryable {
			t.Errorf("job whose log close blocked: %+v, %v; want its result written, its attempt given back", f, ok)
		}
	})
}
