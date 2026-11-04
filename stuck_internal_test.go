package hopper

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// stuckArgs has its worker wait for its context and then for After, or,
// with no After, until the test releases it.
type stuckArgs struct {
	After time.Duration `json:"after,omitempty"`
}

func (stuckArgs) Kind() string { return "stuck_args" }

// stuckExec is a stopExec that cancels a running job as a database would:
// the job stays running, and its client is told.
type stuckExec struct{ *stopExec }

func (stuckExec) JobCancel(_ context.Context, id driver.JobID) (*driver.JobRow, error) {
	return &driver.JobRow{ID: id, State: driver.JobStateRunning}, nil
}

const (
	stuckWarning  = "job has not returned since its context was canceled"
	giveUpWarning = "stop gave up on jobs still running"
)

type stuckCall struct {
	at     time.Time
	job    *JobRow
	cause  error
	client bool
	// started reports that the hook's context has the values of Start's.
	started bool
}

// stuckStartKey marks the context a test starts its client with.
type stuckStartKey struct{}

type stuckHarness struct {
	c        *Client[struct{}]
	exec     *stopExec
	release  chan struct{}
	canceled chan time.Time
	calls    chan stuckCall
	logs     *lockedBuffer
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) count(s string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Count(b.buf.String(), s)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// stuckClient returns an unstarted client over jobs of stuckArgs, with
// Config.OnStuck reporting each call. The worker sends the time its
// context was canceled to canceled.
func stuckClient(t *testing.T, jobTimeout time.Duration, onStuck func(context.Context, *JobRow, error), jobs ...*driver.JobRow) *stuckHarness {
	t.Helper()
	h := &stuckHarness{
		exec:    &stopExec{jobs: jobs, results: map[driver.JobID]driver.JobFinalize{}},
		release: make(chan struct{}), canceled: make(chan time.Time, len(jobs)), calls: make(chan stuckCall, 4),
		logs: &lockedBuffer{},
	}
	workers := NewWorkers()
	AddWorkFunc(workers, func(ctx context.Context, job *Job[stuckArgs]) error {
		<-ctx.Done()
		h.canceled <- time.Now()
		if job.Args.After > 0 {
			time.Sleep(job.Args.After)
		} else {
			<-h.release
		}
		return ctx.Err()
	})
	if onStuck == nil {
		onStuck = func(ctx context.Context, job *JobRow, cause error) {
			_, ok := ClientFromContext[struct{}](ctx)
			h.calls <- stuckCall{
				at: time.Now(), job: job, cause: cause, client: ok && ctx.Err() == nil,
				started: ctx.Value(stuckStartKey{}) != nil,
			}
		}
	}
	c, err := NewClient(managedTestDriver{exec: stuckExec{h.exec}}, &Config{
		Queues:     map[string]QueueConfig{QueueDefault: {MaxWorkers: 10}},
		Workers:    workers,
		Logger:     slog.New(slog.NewTextHandler(h.logs, nil)),
		NeverLead:  true,
		JobTimeout: jobTimeout,
		OnStuck:    onStuck,
	})
	if err != nil {
		t.Fatal(err)
	}
	h.c = c
	return h
}

func stuckJob(args string) *driver.JobRow {
	job := stopJob(1, 3, args)
	job.Kind = stuckArgs{}.Kind()
	return job
}

// noMoreCalls fails the test if OnStuck is called again within a minute.
func (h *stuckHarness) noMoreCalls(t *testing.T) {
	t.Helper()
	time.Sleep(time.Minute)
	synctest.Wait()
	select {
	case call := <-h.calls:
		t.Errorf("OnStuck called again, with %v", call.cause)
	default:
	}
}

// TestStuckAfterTimeout: a worker that ignores its timeout is reported
// StuckGrace after the timeout, once, with ErrJobTimeout, a context that
// carries the client and Start's values and is not canceled, a warning and
// an EventJobStuck; its result is written once it returns.
func TestStuckAfterTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		job := stuckJob(`{}`)
		h := stuckClient(t, time.Second, nil, job)
		ctx := context.WithValue(context.Background(), stuckStartKey{}, true)
		evCtx, evCancel := context.WithCancel(ctx)
		defer evCancel()
		events := make(chan Event, 4)
		go func() {
			for ev := range h.c.Events(evCtx, EventJobStuck) {
				events <- ev
			}
		}()
		synctest.Wait()
		if err := h.c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		canceled := <-h.canceled
		call := <-h.calls
		if d := call.at.Sub(canceled); d != DefaultStuckGrace {
			t.Errorf("OnStuck called %s after the timeout, want %s", d, DefaultStuckGrace)
		}
		if !errors.Is(call.cause, ErrJobTimeout) || call.job.ID != job.ID || !call.client || !call.started {
			t.Errorf("OnStuck(job %s, %v), client in a live context %v, Start's values %v; want job %s, ErrJobTimeout, true, true",
				call.job.ID, call.cause, call.client, call.started, job.ID)
		}
		synctest.Wait()
		select {
		case ev := <-events:
			if ev.Job == nil || ev.Job.ID != job.ID || ev.Error != ErrJobTimeout.Error() {
				t.Errorf("event %+v, want job %s with %q", ev, job.ID, ErrJobTimeout)
			}
		default:
			t.Error("no EventJobStuck")
		}
		if n := h.logs.count(stuckWarning); n != 1 {
			t.Errorf("%d warnings logged, want 1", n)
		}
		h.noMoreCalls(t)
		close(h.release)
		if err := h.c.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		if f, ok := h.exec.result(job.ID); !ok || f.State != driver.JobStateRetryable || f.Error == nil || !strings.Contains(f.Error.Error, "timed out") {
			t.Errorf("result %+v, %v; want a retry after its timeout", f, ok)
		}
	})
}

// TestStuckAfterJobCancel: a worker that ignores JobCancel is reported with
// ErrJobCanceled, StuckGrace after the cancel, and the job is canceled once
// it returns.
func TestStuckAfterJobCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		job := stuckJob(`{}`)
		h := stuckClient(t, time.Hour, nil, job)
		ctx := context.Background()
		if err := h.c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if _, err := h.c.JobCancel(ctx, job.ID); err != nil {
			t.Fatal(err)
		}
		canceled := <-h.canceled
		call := <-h.calls
		if d := call.at.Sub(canceled); d != DefaultStuckGrace || !errors.Is(call.cause, ErrJobCanceled) {
			t.Errorf("OnStuck called %s after the cancel with %v, want %s and ErrJobCanceled", d, call.cause, DefaultStuckGrace)
		}
		h.noMoreCalls(t)
		close(h.release)
		if err := h.c.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		if f, ok := h.exec.result(job.ID); !ok || f.State != driver.JobStateCanceled {
			t.Errorf("result %+v, %v; want canceled", f, ok)
		}
	})
}

// TestStuckDuringStop: a stop that waits for a canceled worker longer than
// StuckGrace has it reported at StuckGrace, with ErrClientStopped.
func TestStuckDuringStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		job := stuckJob(`{}`)
		h := stuckClient(t, time.Hour, nil, job)
		ctx := context.Background()
		if err := h.c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		stopped := make(chan error, 1)
		go func() { stopped <- h.c.StopAndCancel(ctx) }()
		canceled := <-h.canceled
		call := <-h.calls
		if d := call.at.Sub(canceled); d != DefaultStuckGrace || !errors.Is(call.cause, ErrClientStopped) {
			t.Errorf("OnStuck called %s after the stop canceled the job, with %v; want %s and ErrClientStopped", d, call.cause, DefaultStuckGrace)
		}
		close(h.release)
		if err := <-stopped; err != nil {
			t.Errorf("StopAndCancel = %v", err)
		}
		if f, ok := h.exec.result(job.ID); !ok || !f.GiveBack || f.Snooze {
			t.Errorf("result %+v, %v; want its attempt given back", f, ok)
		}
	})
}

// TestStopGiveUpListsJobs: a stop that gives up on workers at the end of its
// grace warns, before it returns, with the ID and kind of each job still
// running, and does not call OnStuck: StuckGrace after the cancellation does,
// if the process still runs, though the stop has returned.
func TestStopGiveUpListsJobs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := stuckJob(`{}`), stuckJob(`{}`)
		h := stuckClient(t, time.Hour, nil, a, b)
		ctx := context.Background()
		if err := h.c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		start := time.Now()
		stopCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if err := h.c.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Stop = %v, want its deadline", err)
		}
		logs := h.logs.String()
		if n := strings.Count(logs, giveUpWarning); n != 1 {
			t.Errorf("%d give-up warnings logged, want 1", n)
		}
		for _, j := range []*driver.JobRow{a, b} {
			if want := j.ID.String() + " (stuck_args)"; !strings.Contains(logs, want) {
				t.Errorf("the give-up warning does not list %q: %s", want, logs)
			}
		}
		select {
		case call := <-h.calls:
			t.Errorf("OnStuck called by the stop's give-up, with %v", call.cause)
		default:
		}
		for range 2 {
			call := <-h.calls
			if want := 3*time.Second + DefaultStuckGrace; call.at.Sub(start) != want || !errors.Is(call.cause, ErrClientStopped) {
				t.Errorf("OnStuck called %s into the stop with %v, want %s (its deadline and StuckGrace) and ErrClientStopped", call.at.Sub(start), call.cause, want)
			}
		}
		h.noMoreCalls(t)
		close(h.release)
		synctest.Wait()
	})
}

// TestStopGiveUpListsReplaced: an attempt that a later claim of its job on
// the same client replaced runs on until its worker returns, and a stop that
// gives up on it lists it, as replaced, beside the claim that replaced it.
func TestStopGiveUpListsReplaced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := stuckJob(`{}`)
		again := *first
		again.Fence++
		h := stuckClient(t, time.Hour, nil, first, &again)
		ctx := context.Background()
		if err := h.c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		stopCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if err := h.c.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Stop = %v, want its deadline", err)
		}
		logs := h.logs.String()
		for _, want := range []string{first.ID.String() + " (stuck_args, replaced)", first.ID.String() + " (stuck_args)"} {
			if !strings.Contains(logs, want) {
				t.Errorf("the give-up warning does not list %q: %s", want, logs)
			}
		}
		// The replaced attempt is reported StuckGrace after its replacement,
		// the other StuckGrace after the stop's deadline canceled it.
		for _, want := range []error{ErrJobFenced, ErrClientStopped} {
			if call := <-h.calls; !errors.Is(call.cause, want) {
				t.Errorf("OnStuck called with %v, want %v", call.cause, want)
			}
		}
		close(h.release)
		synctest.Wait()
	})
}

// TestStuckWithoutHook: with no OnStuck, a stuck worker is still logged and
// emitted, its own StuckGrace after its cancellation.
func TestStuckWithoutHook(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		job := stuckJob(`{}`)
		h := stuckClient(t, time.Second, nil, job)
		const grace = 3 * time.Second
		h.c.cfg.OnStuck, h.c.cfg.StuckGrace = nil, grace
		ctx := context.Background()
		evCtx, evCancel := context.WithCancel(ctx)
		defer evCancel()
		events := make(chan Event, 4)
		go func() {
			for ev := range h.c.Events(evCtx, EventJobStuck) {
				events <- ev
			}
		}()
		synctest.Wait()
		if err := h.c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		canceled := <-h.canceled
		ev := <-events
		if d := ev.At.Sub(canceled); d != grace || ev.Job == nil || ev.Job.ID != job.ID || ev.Error != ErrJobTimeout.Error() {
			t.Errorf("event %+v %s after the timeout, want job %s with %q after %s", ev, d, job.ID, ErrJobTimeout, grace)
		}
		if n := h.logs.count(stuckWarning); n != 1 {
			t.Errorf("%d warnings logged, want 1", n)
		}
		close(h.release)
		if err := h.c.Stop(ctx); err != nil {
			t.Fatal(err)
		}
	})
}

// TestRunningJobsLimit: the list of running jobs stops at its limit and
// says how many more there are.
func TestRunningJobsLimit(t *testing.T) {
	h := stuckClient(t, time.Hour, nil)
	ids := []JobID{{2}, {1}, {3}}
	for _, id := range ids {
		h.c.trackRunning(id, "k", func(error) {})
	}
	got := h.c.runningJobs(2)
	want := []string{JobID{1}.String() + " (k)", JobID{2}.String() + " (k)", "and 1 more"}
	if !slices.Equal(got, want) {
		t.Errorf("runningJobs(2) = %q, want %q", got, want)
	}
}

// TestStuckAfterFence: a worker that ignores the fence of its client's lease,
// which cancels with no cause of its own, is reported StuckGrace after it,
// with ErrJobFenced.
func TestStuckAfterFence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		job := stuckJob(`{}`)
		h := stuckClient(t, time.Hour, nil, job)
		ctx := context.Background()
		if err := h.c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		h.c.fence(ctx, h.c.clientID.Load())
		canceled := <-h.canceled
		call := <-h.calls
		if d := call.at.Sub(canceled); d != DefaultStuckGrace || !errors.Is(call.cause, ErrJobFenced) {
			t.Errorf("OnStuck called %s after the fence with %v, want %s and ErrJobFenced", d, call.cause, DefaultStuckGrace)
		}
		h.noMoreCalls(t)
		close(h.release)
		if err := h.c.Stop(ctx); err != nil {
			t.Fatal(err)
		}
	})
}

// TestStuckGraceEdges: a worker that returns just before StuckGrace is not
// reported; one that returns just after it is, once.
func TestStuckGraceEdges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		before := stuckJob(`{"after":` + jsonDuration(DefaultStuckGrace-time.Nanosecond) + `}`)
		after := stuckJob(`{"after":` + jsonDuration(DefaultStuckGrace+time.Nanosecond) + `}`)
		h := stuckClient(t, time.Second, nil, before, after)
		ctx := context.Background()
		if err := h.c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		call := <-h.calls
		if call.job.ID != after.ID || !errors.Is(call.cause, ErrJobTimeout) {
			t.Errorf("OnStuck(job %s, %v), want job %s, the one returning after the grace, and ErrJobTimeout", call.job.ID, call.cause, after.ID)
		}
		h.noMoreCalls(t)
		if err := h.c.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		for _, j := range []*driver.JobRow{before, after} {
			if f, ok := h.exec.result(j.ID); !ok || f.State != driver.JobStateRetryable {
				t.Errorf("job %s: %+v, %v; want retried after its timeout", j.ID, f, ok)
			}
		}
	})
}

func jsonDuration(d time.Duration) string { return strconv.FormatInt(int64(d), 10) }

// TestStuckHookPanics: a panic in OnStuck is logged, and the job finishes as
// usual once its worker returns.
func TestStuckHookPanics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		job := stuckJob(`{}`)
		h := stuckClient(t, time.Second, func(context.Context, *JobRow, error) { panic("boom") }, job)
		ctx := context.Background()
		if err := h.c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second + DefaultStuckGrace)
		synctest.Wait()
		if n := h.logs.count("Config.OnStuck panicked"); n != 1 {
			t.Errorf("%d panics logged, want 1", n)
		}
		close(h.release)
		if err := h.c.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		if f, ok := h.exec.result(job.ID); !ok || f.State != driver.JobStateRetryable {
			t.Errorf("result %+v, %v; want a retry after its timeout", f, ok)
		}
	})
}

// TestStuckCause: a context canceled with no cause, as a fence cancels a
// lease generation, is reported as ErrJobFenced; a cause is kept.
func TestStuckCause(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := stuckCause(ctx); !errors.Is(err, ErrJobFenced) {
		t.Errorf("fenced: %v, want ErrJobFenced", err)
	}
	ctx, cancelCause := context.WithCancelCause(context.Background())
	cancelCause(ErrJobCanceled)
	if err := stuckCause(ctx); !errors.Is(err, ErrJobCanceled) {
		t.Errorf("canceled: %v, want ErrJobCanceled", err)
	}
}

// TestStuckGraceConfig: StuckGrace defaults to DefaultStuckGrace and must
// not be negative.
func TestStuckGraceConfig(t *testing.T) {
	cfg, err := (&Config{}).withDefaults()
	if err != nil || cfg.StuckGrace != DefaultStuckGrace {
		t.Errorf("default StuckGrace = %s, %v; want %s", cfg.StuckGrace, err, DefaultStuckGrace)
	}
	if _, err := (&Config{StuckGrace: -time.Second}).withDefaults(); err == nil {
		t.Error("a negative StuckGrace was accepted")
	}
}
