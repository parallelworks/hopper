package hopper_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
)

func TestJobCancelRunning(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	w := newBlocker()
	workers := hopper.NewWorkers()
	hopper.AddWorker(workers, w)
	a := h.started(workers, 1)
	// The cancel is issued from another client, so it travels by notification.
	b := h.client(&hopper.Config{})

	res, err := a.Insert(ctx, noop{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	<-w.started
	job, err := b.JobCancel(ctx, res.Job.ID)
	if err != nil || job.State != hopper.JobStateRunning || job.CancelRequestedAt.IsZero() {
		t.Fatalf("JobCancel = %+v, %v", job, err)
	}
	job = waitForJob(t, a, res.Job.ID, hopper.JobStateCanceled)
	if len(job.Errors) != 1 || !errors.Is(context.Canceled, context.Canceled) || job.Errors[0].Error != "context canceled" {
		t.Errorf("canceled job = %+v", job)
	}
	if h.count("SELECT count(*) FROM hopper_jobs") != 0 {
		t.Error("canceled job still live")
	}

	// A waiting job is canceled outright.
	waiting, err := b.Insert(ctx, noop{}, &hopper.InsertOpts{ScheduledAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	job, err = a.JobCancel(ctx, waiting.Job.ID)
	if err != nil || job.State != hopper.JobStateCanceled {
		t.Errorf("cancel waiting = %+v, %v", job, err)
	}
	if _, err := a.JobCancel(ctx, hopper.JobID{1}); !errors.Is(err, hopper.ErrNotFound) {
		t.Errorf("cancel unknown = %v", err)
	}
}

func TestJobCancelSurvivesMissedNotification(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	w := newBlocker()
	workers := hopper.NewWorkers()
	hopper.AddWorker(workers, w)
	c := h.started(workers, 1)
	res, err := c.Insert(ctx, noop{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	<-w.started
	// Set the request directly, bypassing the notification: the lease
	// renewal delivers it.
	if _, err := h.pool.Exec(ctx, "UPDATE hopper_jobs SET cancel_requested_at = now() WHERE id = $1", [16]byte(res.Job.ID)); err != nil {
		t.Fatal(err)
	}
	waitForJob(t, c, res.Job.ID, hopper.JobStateCanceled)
}

func TestJobRetryRedrivesFromHistory(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var calls atomic.Int32
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error {
		if calls.Add(1) == 1 {
			return hopper.Cancel(errors.New("first time"))
		}
		return nil
	})
	c := h.started(workers, 1)
	res, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	waitForJob(t, c, res.Job.ID, hopper.JobStateDiscarded)

	job, err := c.JobRetry(ctx, res.Job.ID)
	if err != nil || job.State != hopper.JobStateAvailable || job.MaxAttempts != 2 {
		t.Fatalf("JobRetry = %+v, %v", job, err)
	}
	job = waitForJob(t, c, res.Job.ID, hopper.JobStateCompleted)
	if job.Attempt != 2 || len(job.Errors) != 1 {
		t.Errorf("re-driven job = %+v", job)
	}
}

func TestJobRetryOrderingKeyWakesASnoozedKey(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var calls atomic.Int32
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error {
		if calls.Add(1) == 1 {
			return &hopper.SnoozeError{Duration: time.Hour, Reason: "waiting"}
		}
		return nil
	})
	c := h.started(workers, 1)
	first, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{OrderingKey: "a"})
	if err != nil {
		t.Fatal(err)
	}
	waitForJob(t, c, first.Job.ID, hopper.JobStateScheduled)
	second, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{OrderingKey: "a"})
	if err != nil {
		t.Fatal(err)
	}

	if n, err := c.JobRetryOrderingKey(ctx, "", "a"); err != nil || n != 1 {
		t.Fatalf("JobRetryOrderingKey = %d, %v", n, err)
	}
	waitForJob(t, c, first.Job.ID, hopper.JobStateCompleted)
	waitForJob(t, c, second.Job.ID, hopper.JobStateCompleted)
	if _, err := c.JobRetryOrderingKey(ctx, "", ""); err == nil {
		t.Error("empty key: no error")
	}
}

func TestTTLDiscardsUnstartedJobs(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error { return nil })
	c := h.client(&hopper.Config{
		Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers: workers,
	})
	// Scheduled for later than its TTL: never starts.
	res, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{ScheduledAt: time.Now().Add(time.Hour), TTL: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	job := waitForJob(t, c, res.Job.ID, hopper.JobStateDiscarded)
	if len(job.Errors) != 1 || job.Errors[0].Error != "hopper: expired" {
		t.Errorf("expired job = %+v", job)
	}
}

// TestTTLDiscardsJobsWaitingToRunAgain pins that a TTL bounds every wait,
// not only the first: a job that has run and waits to run again, snoozed
// or retrying, is discarded once it passes, without running again. The
// TTL leaves a loaded machine seconds to claim each job once, and both
// waits (a snooze of an hour, a first retry about six seconds away) end
// after it.
func TestTTLDiscardsJobsWaitingToRunAgain(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var calls [2]atomic.Int32
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[noop]) error {
		calls[job.Args.N].Add(1)
		if job.Args.N == 0 {
			return &hopper.SnoozeError{Duration: time.Hour, Reason: "later"}
		}
		return errors.New("flaky")
	})
	c := h.client(&hopper.Config{
		Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 2}},
		Workers: workers,
	})
	events := make(chan hopper.Event, 100)
	ectx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		for ev := range c.Events(ectx, hopper.EventJobSnoozed, hopper.EventJobDiscarded) {
			events <- ev
		}
	}()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	var ids [2]hopper.JobID
	for n := range ids {
		res, err := c.Insert(ctx, noop{N: n}, &hopper.InsertOpts{TTL: 3 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		ids[n] = res.Job.ID
	}
	// The snoozed job's reason travels with its event and with the error
	// it is discarded with.
	want := [2]string{"hopper: expired while snoozed: later", "hopper: expired"}
	for n, id := range ids {
		job := waitForJob(t, c, id, hopper.JobStateDiscarded)
		if last := job.Errors[len(job.Errors)-1]; last.Error != want[n] || calls[n].Load() != 1 || job.SnoozeReason != "" {
			t.Errorf("job %d = %+v after %d attempts", n, job, calls[n].Load())
		}
	}
	got := map[string]bool{}
	for len(got) < 3 {
		select {
		case ev := <-events:
			got[string(ev.Kind)+": "+ev.Error] = true
		case <-time.After(10 * time.Second):
			t.Fatalf("events so far: %v", got)
		}
	}
	for _, key := range []string{"job.snoozed: later", "job.discarded: " + want[0], "job.discarded: " + want[1]} {
		if !got[key] {
			t.Errorf("no event %q in %v", key, got)
		}
	}
}

func TestPauseAndResume(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var ran atomic.Int32
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error {
		ran.Add(1)
		return nil
	})
	c := h.started(workers, 2)
	other := h.client(&hopper.Config{})
	// Every schema in the database shares the notification channels.
	foreign := newHarness(t).client(&hopper.Config{})
	waitFor(t, c.Listening)

	// The worker pauses at once. Neither a renewal in flight across the
	// pause nor another schema's resume of its own queue of the same name
	// undoes it.
	if err := c.Queues().Pause(ctx, hopper.QueueDefault); err != nil {
		t.Fatal(err)
	}
	// Renewals cross the pause, and a claim that began before it ends.
	time.Sleep(fastTuning.LeaseRenew * 2)
	if _, err := c.Insert(ctx, noop{}, nil); err != nil {
		t.Fatal(err)
	}
	if err := foreign.Queues().Resume(ctx, hopper.QueueDefault); err != nil {
		t.Fatal(err)
	}
	time.Sleep(fastTuning.LeaseRenew * 2)
	if ran.Load() != 0 {
		t.Fatal("job ran on a paused queue")
	}
	queues, err := c.Queues().List(ctx)
	if err != nil || len(queues) != 1 || queues[0].PausedAt.IsZero() {
		t.Errorf("queues = %v, %v", queues, err)
	}

	// Another client's resume reaches the worker.
	if err := other.Queues().Resume(ctx, hopper.QueueDefault); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return ran.Load() == 1 })

	// So does another client's pause, through the notification or else a
	// renewal.
	if err := other.Queues().Pause(ctx, hopper.QueueDefault); err != nil {
		t.Fatal(err)
	}
	time.Sleep(fastTuning.LeaseRenew * 2)
	if _, err := c.Insert(ctx, noop{}, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(fastTuning.LeaseRenew * 2)
	if n := ran.Load(); n != 1 {
		t.Fatalf("ran %d jobs; one ran on a queue another client paused", n)
	}
}

// TestRemoveWhileStopping removes queues while the client stops: each call
// returns, neither waits on the other, and under -race neither reads the
// client's queues while the other changes them.
func TestRemoveWhileStopping(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error { return nil })
	c := h.started(workers, 1)
	names := make([]string, 8)
	for i := range names {
		names[i] = "tenant_" + strconv.Itoa(i)
		if err := c.Queues().Add(ctx, names[i], hopper.QueueConfig{MaxWorkers: 1}); err != nil {
			t.Fatal(err)
		}
	}
	stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := c.Stop(stopCtx); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	for _, name := range names {
		wg.Go(func() {
			if err := c.Queues().Remove(stopCtx, name); err != nil {
				t.Errorf("Remove(%s): %v", name, err)
			}
		})
	}
	wg.Wait()
}

func TestRuntimeQueues(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	rec := newRecorder()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[noop]) error {
		rec.record(job.ID)
		return nil
	})
	c := h.started(workers, 1)
	if err := c.Queues().Add(ctx, "", hopper.QueueConfig{MaxWorkers: 1}); err == nil {
		t.Error("empty queue name accepted")
	}
	if err := c.Queues().Add(ctx, hopper.QueueDefault, hopper.QueueConfig{MaxWorkers: 1}); err == nil {
		t.Error("existing queue accepted")
	}

	res, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{Queue: "tenant_42"})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if rec.total.Load() != 0 {
		t.Fatal("job on an unworked queue ran")
	}
	if err := c.Queues().Add(ctx, "tenant_42", hopper.QueueConfig{MaxWorkers: 2}); err != nil {
		t.Fatal(err)
	}
	waitForJob(t, c, res.Job.ID, hopper.JobStateCompleted)

	if err := c.Queues().Remove(ctx, "tenant_42"); err != nil {
		t.Fatal(err)
	}
	if err := c.Queues().Remove(ctx, "tenant_42"); err == nil {
		t.Error("removing twice succeeded")
	}
	if _, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{Queue: "tenant_42"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if rec.total.Load() != 1 {
		t.Error("job ran on a removed queue")
	}
	queues, err := c.Queues().List(ctx)
	if err != nil || len(queues) != 2 {
		t.Errorf("queues = %v, %v", queues, err)
	}
}

func TestMiddleware(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var trace []string
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[noop]) error {
		trace = append(trace, "work:"+string(job.Metadata))
		return errors.New("boom")
	})
	logging := hopper.WorkMiddleware(func(ctx context.Context, job *hopper.JobRow, next func(context.Context) error) error {
		trace = append(trace, "outer>")
		err := next(ctx)
		trace = append(trace, "<outer:"+err.Error())
		return err
	})
	inner := hopper.WorkMiddleware(func(ctx context.Context, job *hopper.JobRow, next func(context.Context) error) error {
		trace = append(trace, "inner>")
		defer func() { trace = append(trace, "<inner") }()
		return next(ctx)
	})
	// Insert middleware injects metadata, as a tracing integration would.
	tagging := hopper.InsertMiddleware(func(ctx context.Context, params []hopper.InsertParams, next func(context.Context, []hopper.InsertParams) ([]*hopper.InsertResult, error)) ([]*hopper.InsertResult, error) {
		tagged := make([]hopper.InsertParams, len(params))
		for i, p := range params {
			var opts hopper.InsertOpts
			if p.Opts != nil {
				opts = *p.Opts
			}
			opts.Metadata = []byte(`{"trace":"t1"}`)
			tagged[i] = hopper.InsertParams{Args: p.Args, Opts: &opts}
		}
		return next(ctx, tagged)
	})
	c := h.client(&hopper.Config{
		Queues:     map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers:    workers,
		Middleware: []hopper.Middleware{logging, tagging, inner},
	})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	if string(res.Job.Metadata) != `{"trace": "t1"}` {
		t.Errorf("metadata = %s", res.Job.Metadata)
	}
	waitForJob(t, c, res.Job.ID, hopper.JobStateDiscarded)
	want := []string{"outer>", "inner>", `work:{"trace": "t1"}`, "<inner", "<outer:boom"}
	if !slices.Equal(trace, want) {
		t.Errorf("trace = %v, want %v", trace, want)
	}
}

type report struct{ URL string }

func TestSetOutputAndAwait(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[noop]) error {
		if job.Args.N < 0 {
			return hopper.Cancel(errors.New("negative"))
		}
		return hopper.SetOutput(ctx, report{URL: "https://example/" + job.ID.String()})
	})
	worker := h.client(&hopper.Config{
		Queues:       map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 2}},
		Workers:      workers,
		PollInterval: 5 * time.Second,
	})
	if err := worker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, worker.Listening)
	// The requester is a separate, insert-only client: it polls slowly, so
	// only the done notification can make Await fast.
	requester := h.client(&hopper.Config{PollInterval: 5 * time.Second})

	res, err := requester.Insert(ctx, noop{N: 1}, &hopper.InsertOpts{Await: true})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	actx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := hopper.Await[report](actx, worker, res.Job.ID) // the worker's listener carries the done notification
	if err != nil || out.URL != "https://example/"+res.Job.ID.String() {
		t.Fatalf("Await = %+v, %v", out, err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("Await took %s; the done notification should have woken it", took)
	}
	// The output is also on the job record.
	if job, err := requester.JobGet(ctx, res.Job.ID); err != nil || len(job.Output) == 0 {
		t.Errorf("job output = %s, %v", job.Output, err)
	}

	// A failed job surfaces as JobFailedError.
	res, err = requester.Insert(ctx, noop{N: -1}, &hopper.InsertOpts{Await: true, MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	var failed *hopper.JobFailedError
	if _, err := hopper.Await[report](actx, worker, res.Job.ID); !errors.As(err, &failed) || failed.Job.State != hopper.JobStateDiscarded {
		t.Errorf("Await of failed job = %v", err)
	}

	// Outside a job, SetOutput is refused.
	if err := hopper.SetOutput(ctx, 1); !errors.Is(err, hopper.ErrNotInJob) {
		t.Errorf("SetOutput outside a job = %v", err)
	}
}

func TestJobsIteratorPages(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	c := h.client(&hopper.Config{})
	params := make([]hopper.InsertParams, 250)
	for i := range params {
		params[i] = hopper.InsertParams{Args: noop{N: i}}
		if i%2 == 0 {
			params[i].Opts = &hopper.InsertOpts{Queue: "even"}
		}
	}
	if _, err := c.InsertMany(ctx, params); err != nil {
		t.Fatal(err)
	}
	var n int
	var last string
	for job, err := range c.Jobs(ctx, hopper.JobFilter{Queue: "even"}) {
		if err != nil {
			t.Fatal(err)
		}
		if job.Queue != "even" || job.ID.String() <= last {
			t.Fatalf("unexpected job %+v after %s", job, last)
		}
		last = job.ID.String()
		n++
	}
	if n != 125 {
		t.Errorf("iterated %d jobs, want 125", n)
	}
	n, last = 0, ""
	for job, err := range c.Jobs(ctx, hopper.JobFilter{Queue: "even", NewestFirst: true}) {
		if err != nil {
			t.Fatal(err)
		}
		if job.Queue != "even" || (last != "" && job.ID.String() >= last) {
			t.Fatalf("unexpected job %+v after %s, newest first", job, last)
		}
		last = job.ID.String()
		n++
	}
	if n != 125 {
		t.Errorf("iterated %d jobs newest first, want 125", n)
	}
	// Breaking out early stops fetching.
	n = 0
	for _, err := range c.Jobs(ctx, hopper.JobFilter{}) {
		if err != nil {
			t.Fatal(err)
		}
		if n++; n == 3 {
			break
		}
	}
	if n != 3 {
		t.Errorf("break did not stop the iterator")
	}
}

func TestEventsAndStats(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[noop]) error {
		if job.Args.N == 1 {
			return errors.New("fail once")
		}
		return nil
	})
	c := h.client(&hopper.Config{
		Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 2}},
		Workers: workers,
	})
	events := make(chan hopper.Event, 100)
	ectx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		for ev := range c.Events(ectx, hopper.EventJobCompleted, hopper.EventJobDiscarded, hopper.EventLeaderElected) {
			events <- ev
		}
	}()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Insert(ctx, noop{N: 0}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Insert(ctx, noop{N: 1}, &hopper.InsertOpts{MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	got := map[hopper.EventKind]int{}
	deadline := time.After(10 * time.Second)
	for len(got) < 3 {
		select {
		case ev := <-events:
			if ev.ClientID != c.ClientID() || ev.At.IsZero() {
				t.Errorf("event = %+v", ev)
			}
			if ev.Kind == hopper.EventJobDiscarded && (ev.Job == nil || ev.Error != "fail once") {
				t.Errorf("discarded event = %+v", ev)
			}
			got[ev.Kind]++
		case <-deadline:
			t.Fatalf("events so far: %v", got)
		}
	}

	waitFor(t, func() bool { return h.count("SELECT count(*) FROM hopper_jobs") == 0 })
	stats, err := c.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Leader != c.ClientID() || stats.LiveClients != 1 || stats.Queues[hopper.QueueDefault] == nil || stats.Queues[hopper.QueueDefault].CompletedLastMinute != 1 {
		t.Errorf("stats = %+v", stats)
	}
}

// noSlots is a schedule that never comes due.
type noSlots struct{}

func (noSlots) Next(time.Time) time.Time { return time.Time{} }

func TestPeriodicJobsRunOnceAcrossLeaders(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	rec := newRecorder()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[noop]) error {
		rec.record(job.ID)
		return nil
	})
	cfg := func() *hopper.Config {
		return &hopper.Config{
			Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 2}},
			Workers: workers,
			Periodic: []hopper.PeriodicJob{
				hopper.Every(time.Second, noop{N: 1}, nil),
				hopper.Cron("* * * * * *", noop{N: 2}, &hopper.PeriodicOpts{Name: "cron-seconds"}),
				// No regular slots, so only RunOnStart inserts this one: an
				// hourly schedule adds a job when the test spans the hour.
				{Name: "on-start", Schedule: noSlots{}, Args: noop{N: 3}, Opts: &hopper.PeriodicOpts{RunOnStart: true}},
			},
		}
	}
	a := h.client(cfg())
	b := h.client(cfg())
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3500 * time.Millisecond)

	// RunOnStart fired once, and each second produced exactly one job per
	// schedule, whichever client was leader.
	if got := h.count("SELECT count(*) FROM hopper_job_history WHERE args->>'n' = '3'"); got != 1 {
		t.Errorf("RunOnStart jobs = %d, want 1", got)
	}
	for _, n := range []string{"1", "2"} {
		got := h.count("SELECT count(*) FROM hopper_job_history WHERE args->>'n' = $1", n) + h.count("SELECT count(*) FROM hopper_jobs WHERE args->>'n' = $1", n)
		if got < 2 || got > 4 {
			t.Errorf("schedule %s produced %d jobs in ~3.5s, want 3±1", n, got)
		}
	}
	var dup int
	if err := h.pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT scheduled_at, args, count(*) FROM hopper_job_history GROUP BY 1, 2 HAVING count(*) > 1) d`).Scan(&dup); err != nil {
		t.Fatal(err)
	}
	if dup != 0 {
		t.Errorf("%d slots produced more than one job", dup)
	}

	// Leader failover keeps the schedule going without duplicates.
	leader, follower := a, b
	if b.IsLeader() {
		leader, follower = b, a
	}
	before := h.count("SELECT count(*) FROM hopper_job_history WHERE args->>'n' = '1'")
	if err := leader.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, follower.IsLeader)
	time.Sleep(2500 * time.Millisecond)
	after := h.count("SELECT count(*) FROM hopper_job_history WHERE args->>'n' = '1'")
	if after-before < 1 {
		t.Errorf("no periodic jobs after failover (%d -> %d)", before, after)
	}
	if err := h.pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT scheduled_at, args, count(*) FROM hopper_job_history GROUP BY 1, 2 HAVING count(*) > 1) d`).Scan(&dup); err != nil {
		t.Fatal(err)
	}
	if dup != 0 {
		t.Errorf("%d slots produced more than one job after failover", dup)
	}
}

func TestJobDeleteEndsAwait(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	c := h.client(&hopper.Config{})
	res, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{Await: true, ScheduledAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	awaited := make(chan error, 1)
	go func() {
		_, err := hopper.Await[struct{}](ctx, c, res.Job.ID)
		awaited <- err
	}()
	job, err := c.JobDelete(ctx, res.Job.ID)
	if err != nil || job.State != hopper.JobStateCanceled {
		t.Fatalf("JobDelete = %+v, %v", job, err)
	}
	select {
	case err := <-awaited:
		if !errors.Is(err, hopper.ErrNotFound) {
			t.Errorf("Await on a deleted job = %v, want ErrNotFound", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Await still waiting on a deleted job")
	}
	if _, err := c.JobDelete(ctx, res.Job.ID); !errors.Is(err, hopper.ErrNotFound) {
		t.Errorf("second delete = %v, want ErrNotFound", err)
	}
}

// TestControlTx runs each control call in a transaction that rolls back,
// which must leave the job as it was and tell no process anything, and
// then in one that commits.
func TestControlTx(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	w := newBlocker()
	workers := hopper.NewWorkers()
	hopper.AddWorker(workers, w)
	// The producer polls only once a minute, so a committed retry starts its
	// job only if the transaction woke the queue.
	c := h.client(&hopper.Config{
		Queues:       map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 2}},
		Workers:      workers,
		PollInterval: time.Minute,
	})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	started := func(want hopper.JobID) {
		t.Helper()
		select {
		case id := <-w.started:
			if id != want {
				t.Fatalf("started %s, want %s", id, want)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("job %s did not start", want)
		}
	}
	inTx := func(commit bool, call func(tx pgxTx) error) {
		t.Helper()
		tx, err := h.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := call(tx); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if commit {
			err = tx.Commit(ctx)
		} else {
			err = tx.Rollback(ctx)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	state := func(id hopper.JobID) hopper.JobState {
		t.Helper()
		job, err := c.JobGet(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return job.State
	}

	// JobCancelTx on a job running on this very client.
	running, err := c.Insert(ctx, noop{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	started(running.Job.ID)
	inTx(false, func(tx pgxTx) error { _, err := c.JobCancelTx(ctx, tx, running.Job.ID); return err })
	time.Sleep(3 * fastTuning.LeaseRenew)
	if s := state(running.Job.ID); s != hopper.JobStateRunning {
		t.Fatalf("job after a rolled-back cancel = %s, want running", s)
	}
	inTx(true, func(tx pgxTx) error { _, err := c.JobCancelTx(ctx, tx, running.Job.ID); return err })
	waitForJob(t, c, running.Job.ID, hopper.JobStateCanceled)

	// JobRetryTx brings a scheduled job forward.
	later, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{ScheduledAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	inTx(false, func(tx pgxTx) error { _, err := c.JobRetryTx(ctx, tx, later.Job.ID); return err })
	if s := state(later.Job.ID); s != hopper.JobStateScheduled {
		t.Fatalf("job after a rolled-back retry = %s, want scheduled", s)
	}
	// The insert woke the queue too; let that claim pass first.
	time.Sleep(time.Second)
	inTx(true, func(tx pgxTx) error { _, err := c.JobRetryTx(ctx, tx, later.Job.ID); return err })
	started(later.Job.ID)
	w.release <- struct{}{}
	waitForJob(t, c, later.Job.ID, hopper.JobStateCompleted)

	// JobRetryOrderingKeyTx wakes a key's scheduled job.
	keyed, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{OrderingKey: "k", ScheduledAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	inTx(false, func(tx pgxTx) error { _, err := c.JobRetryOrderingKeyTx(ctx, tx, "", "k"); return err })
	if s := state(keyed.Job.ID); s != hopper.JobStateScheduled {
		t.Fatalf("job after a rolled-back key wake = %s, want scheduled", s)
	}
	time.Sleep(time.Second)
	inTx(true, func(tx pgxTx) error {
		n, err := c.JobRetryOrderingKeyTx(ctx, tx, "", "k")
		if err == nil && n != 1 {
			t.Errorf("JobRetryOrderingKeyTx woke %d, want 1", n)
		}
		return err
	})
	started(keyed.Job.ID)
	w.release <- struct{}{}
	waitForJob(t, c, keyed.Job.ID, hopper.JobStateCompleted)

	// JobDeleteTx.
	doomed, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{ScheduledAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	inTx(false, func(tx pgxTx) error { _, err := c.JobDeleteTx(ctx, tx, doomed.Job.ID); return err })
	if s := state(doomed.Job.ID); s != hopper.JobStateScheduled {
		t.Fatalf("job after a rolled-back delete = %s, want scheduled", s)
	}
	inTx(true, func(tx pgxTx) error { _, err := c.JobDeleteTx(ctx, tx, doomed.Job.ID); return err })
	if _, err := c.JobGet(ctx, doomed.Job.ID); !errors.Is(err, hopper.ErrNotFound) {
		t.Errorf("job after a committed delete: %v, want ErrNotFound", err)
	}
}

// TestHeldRowDoesNotStallFinalizer holds a running job's row in a caller's
// transaction (JobCancelTx, not committed) while the job finishes: its
// result waits for the transaction, and other jobs' results do not.
func TestHeldRowDoesNotStallFinalizer(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	w := newBlocker()
	workers := hopper.NewWorkers()
	hopper.AddWorker(workers, w)
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[failing]) error { return nil })
	c := h.started(workers, 2)

	held, err := c.Insert(ctx, noop{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	<-w.started
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := c.JobCancelTx(ctx, tx, held.Job.ID); err != nil {
		t.Fatal(err)
	}
	w.release <- struct{}{}

	other, err := c.Insert(ctx, failing{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		job, err := c.JobGet(ctx, other.Job.ID)
		if err == nil && job.State == hopper.JobStateCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("another job's result waited for the held row")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if job, err := c.JobGet(ctx, held.Job.ID); err != nil || job.State != hopper.JobStateRunning {
		t.Fatalf("held job = %+v, %v, want running until the transaction ends", job, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	waitForJob(t, c, held.Job.ID, hopper.JobStateCompleted, hopper.JobStateCanceled)
}

// TestJobRetryTxSchedulesAtStatementTime retries a job in a transaction
// that began a while before: the job is due from the retry, not from the
// transaction's start, so it does not jump ahead of jobs scheduled since.
func TestJobRetryTxSchedulesAtStatementTime(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	c := h.client(&hopper.Config{})
	res, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{ScheduledAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var began time.Time
	if err := tx.QueryRow(ctx, "SELECT now()").Scan(&began); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	job, err := c.JobRetryTx(ctx, tx, res.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d := job.ScheduledAt.Sub(began); d < 400*time.Millisecond {
		t.Errorf("retried job due %s after the transaction began, want the retry's time", d)
	}
}

// TestJobDeleteWaitingWritesNoHistory deletes a waiting job while another
// transaction keeps every insert out of the history its cancel would go
// to: the delete does not wait, as it writes no history row.
func TestJobDeleteWaitingWritesNoHistory(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	c := h.client(&hopper.Config{})
	res, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{ScheduledAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "LOCK TABLE hopper_job_history_failed IN SHARE MODE"); err != nil {
		t.Fatal(err)
	}
	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if job, err := c.JobDelete(dctx, res.Job.ID); err != nil || job.State != hopper.JobStateCanceled {
		t.Fatalf("JobDelete = %+v, %v", job, err)
	}
	if _, err := c.JobGet(ctx, res.Job.ID); !errors.Is(err, hopper.ErrNotFound) {
		t.Errorf("deleted job: %v, want ErrNotFound", err)
	}
}
