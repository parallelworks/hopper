package hopper

import (
	"context"
	"errors"
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
	// Cancel has the worker return Cancel once its context is canceled.
	Cancel bool `json:"cancel,omitempty"`
}

func (stopArgs) Kind() string { return "stop_args" }

// stopClient returns an unstarted client over jobs whose worker waits for
// its context and returns its error, or Cancel, sending each context's
// cause to causes.
func stopClient(t *testing.T, causes chan<- error, jobs ...*driver.JobRow) (*Client[struct{}], *stopExec) {
	t.Helper()
	exec := &stopExec{jobs: jobs, results: map[driver.JobID]driver.JobFinalize{}}
	workers := NewWorkers()
	AddWorkFunc(workers, func(ctx context.Context, job *Job[stopArgs]) error {
		<-ctx.Done()
		causes <- context.Cause(ctx)
		if job.Args.Cancel {
			return Cancel(errors.New("gave up"))
		}
		return ctx.Err()
	})
	c, err := NewClient(stopDriver{exec: exec}, &Config{
		Queues:    map[string]QueueConfig{QueueDefault: {MaxWorkers: 10}},
		Workers:   workers,
		Logger:    slog.New(slog.DiscardHandler),
		NeverLead: true,
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
// close would panic). Each job gets its attempt back, its last one too,
// except the one whose worker returned Cancel. Nothing the client started
// is left running when the bubble ends.
func TestStopAndCancelRacingStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		causes := make(chan error, 3)
		retried, last, canceled := stopJob(1, 3, `{}`), stopJob(3, 3, `{}`), stopJob(1, 3, `{"cancel":true}`)
		c, exec := stopClient(t, causes, retried, last, canceled)
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
		for range 3 {
			if cause := <-causes; !errors.Is(cause, ErrClientStopped) {
				t.Errorf("a job's context was canceled with %v, want ErrClientStopped", cause)
			}
		}
		for _, j := range []*driver.JobRow{retried, last} {
			if f, ok := exec.result(j.ID); !ok || !f.Snooze || f.State != driver.JobStateScheduled || f.Delay != 0 || f.Error != nil {
				t.Errorf("job on attempt %d of %d: %+v, %v; want snoozed for no time, its attempt given back", j.Attempt, j.MaxAttempts, f, ok)
			}
		}
		if f, ok := exec.result(canceled.ID); !ok || f.State != driver.JobStateDiscarded {
			t.Errorf("job whose worker returned Cancel: %+v, %v; want discarded", f, ok)
		}
		if !exec.deleted {
			t.Error("the client's lease was not released")
		}
	})
}

// TestStopDeadlineEscalates: a Stop whose ctx expires cancels the jobs, as
// does a second Stop's that expires while the first waits; the first then
// returns nil, and the job gets its attempt back.
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
		if err := <-first; err != nil {
			t.Errorf("first Stop = %v, want nil once the second's deadline canceled the job", err)
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
		<-causes
		if f, ok := exec.result(job.ID); !ok || !f.Snooze {
			t.Errorf("job after Stop's deadline = %+v, %v; want its attempt given back", f, ok)
		}
	})
}
