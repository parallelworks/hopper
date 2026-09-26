package hopper_test

import (
	"context"
	"errors"
	"slices"
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
	job = waitForJob(t, a, res.Job.ID, hopper.JobStateCancelled)
	if len(job.Errors) != 1 || !errors.Is(context.Canceled, context.Canceled) || job.Errors[0].Error != "context canceled" {
		t.Errorf("cancelled job = %+v", job)
	}
	if h.count("SELECT count(*) FROM hopper_jobs") != 0 {
		t.Error("cancelled job still live")
	}

	// A waiting job is cancelled outright.
	waiting, err := b.Insert(ctx, noop{}, &hopper.InsertOpts{ScheduledAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	job, err = a.JobCancel(ctx, waiting.Job.ID)
	if err != nil || job.State != hopper.JobStateCancelled {
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
	waitForJob(t, c, res.Job.ID, hopper.JobStateCancelled)
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

	if err := other.Queues().Pause(ctx, hopper.QueueDefault); err != nil {
		t.Fatal(err)
	}
	// Wait until the worker has learned of the pause (through the
	// notification or a renewal), then insert.
	waitFor(t, func() bool {
		s, err := c.Stats(ctx)
		return err == nil && s.Queues[hopper.QueueDefault] != nil && s.Queues[hopper.QueueDefault].Paused
	})
	time.Sleep(fastTuning.LeaseRenew * 2)
	if _, err := c.Insert(ctx, noop{}, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if ran.Load() != 0 {
		t.Fatal("job ran on a paused queue")
	}
	queues, err := c.Queues().List(ctx)
	if err != nil || len(queues) != 1 || queues[0].PausedAt.IsZero() {
		t.Errorf("queues = %v, %v", queues, err)
	}

	if err := other.Queues().Resume(ctx, hopper.QueueDefault); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return ran.Load() == 1 })
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
				hopper.Every(time.Hour, noop{N: 3}, &hopper.PeriodicOpts{Name: "hourly", RunOnStart: true}),
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
