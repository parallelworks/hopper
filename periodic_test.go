package hopper_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
)

func noopWorkers() *hopper.Workers {
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error { return nil })
	return workers
}

func periodicNames(jobs []hopper.PeriodicJob) []string {
	names := make([]string, len(jobs))
	for i, j := range jobs {
		names[i] = j.Name
	}
	return names
}

func TestPeriodicJobsRegistry(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	c := h.client(&hopper.Config{Periodic: []hopper.PeriodicJob{hopper.Every(time.Minute, noop{}, nil)}})
	jobs := c.PeriodicJobs()

	for name, job := range map[string]hopper.PeriodicJob{
		"bad cron":     hopper.Cron("bad", noop{}, &hopper.PeriodicOpts{Name: "a"}),
		"bad interval": hopper.Every(0, noop{}, &hopper.PeriodicOpts{Name: "b"}),
		"nil args":     hopper.Every(time.Second, nil, nil),
		"taken name":   hopper.Every(time.Second, noop{}, nil),
	} {
		if err := jobs.Add(job); err == nil {
			t.Errorf("%s: Add succeeded", name)
		}
	}
	err := jobs.AddMany([]hopper.PeriodicJob{
		hopper.Every(time.Second, noop{}, &hopper.PeriodicOpts{Name: "x"}),
		hopper.Every(time.Minute, noop{}, &hopper.PeriodicOpts{Name: "x"}),
	})
	if err == nil {
		t.Error("AddMany accepted a name twice")
	}
	if got := periodicNames(jobs.All()); !slices.Equal(got, []string{"noop"}) {
		t.Errorf("after failed adds, All = %v", got)
	}

	opts := &hopper.PeriodicOpts{Name: "later", InsertOpts: &hopper.InsertOpts{Queue: "q", Tags: []string{"t"}, Metadata: []byte(`{"a":1}`)}}
	if err := jobs.AddMany([]hopper.PeriodicJob{hopper.Every(time.Second, noop{}, opts), hopper.Cron("@hourly", noop{}, &hopper.PeriodicOpts{Name: "hourly"})}); err != nil {
		t.Fatal(err)
	}
	opts.Name, opts.InsertOpts.Queue = "changed", "changed"
	opts.InsertOpts.Tags[0], opts.InsertOpts.Metadata[2] = "changed", 'X'
	all := jobs.All()
	if got := periodicNames(all); !slices.Equal(got, []string{"noop", "later", "hourly"}) {
		t.Errorf("All = %v", got)
	}
	if io := all[1].Opts.InsertOpts; io.Queue != "q" || io.Tags[0] != "t" || string(io.Metadata) != `{"a":1}` {
		t.Errorf("the caller's options changed a registered job's options to %+v", io)
	}
	all[1].Opts.InsertOpts.Tags[0] = "changed"
	if tag := jobs.All()[1].Opts.InsertOpts.Tags[0]; tag != "t" {
		t.Errorf("a listed job's options changed a registered job's tag to %q", tag)
	}

	if err := jobs.Remove("missing"); !errors.Is(err, hopper.ErrNotFound) {
		t.Errorf("Remove of an unknown name = %v, want ErrNotFound", err)
	}
	if err := jobs.Remove("noop"); err != nil {
		t.Fatal(err)
	}
	if err := jobs.Add(hopper.Every(time.Hour, noop{}, nil)); err != nil {
		t.Errorf("Add after Remove: %v", err)
	}
	if got := periodicNames(jobs.All()); !slices.Equal(got, []string{"later", "hourly", "noop"}) {
		t.Errorf("All = %v", got)
	}
	jobs.Clear()
	if got := jobs.All(); len(got) != 0 {
		t.Errorf("after Clear, All = %v", periodicNames(got))
	}
}

func TestPeriodicJobsAddOnClientThatNeverLeads(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	var out bytes.Buffer
	c := h.client(&hopper.Config{Logger: slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))})
	if err := c.PeriodicJobs().Add(hopper.Every(time.Second, noop{}, nil)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "never leads") {
		t.Errorf("no record of a periodic job added on a client that never leads: %q", out.String())
	}
}

// Jobs added and removed on running clients: only the leader schedules, a
// RunOnStart added on every client inserts one job, a removed job stops and
// keeps its last slot, and a job added only on a follower is scheduled once
// it leads.
func TestPeriodicJobsAtRuntime(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := noopWorkers()
	cfg := func() *hopper.Config {
		return &hopper.Config{Queues: map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 2}}, Workers: workers}
	}
	a, b := h.client(cfg()), h.client(cfg())
	for _, c := range []*hopper.Client[pgxTx]{a, b} {
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return a.IsLeader() || b.IsLeader() })
	jobs := func(n string) int {
		return h.count("SELECT count(*) FROM hopper_jobs WHERE args->>'n' = $1", n) +
			h.count("SELECT count(*) FROM hopper_job_history WHERE args->>'n' = $1", n)
	}

	for _, c := range []*hopper.Client[pgxTx]{a, b} {
		err := c.PeriodicJobs().AddMany([]hopper.PeriodicJob{
			hopper.Every(time.Second, noop{N: 1}, &hopper.PeriodicOpts{Name: "every"}),
			{Name: "on-start", Schedule: noSlots{}, Args: noop{N: 2}, Opts: &hopper.PeriodicOpts{RunOnStart: true}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return jobs("1") >= 2 })
	if n := jobs("2"); n != 1 {
		t.Errorf("RunOnStart added on both clients inserted %d jobs, want 1", n)
	}

	for _, c := range []*hopper.Client[pgxTx]{a, b} {
		if err := c.PeriodicJobs().Remove("every"); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(200 * time.Millisecond)
	before := jobs("1")
	time.Sleep(2 * time.Second)
	if after := jobs("1"); after != before {
		t.Errorf("removed job inserted %d more jobs", after-before)
	}
	if n := h.count("SELECT count(*) FROM hopper_periodic WHERE name = 'every'"); n != 1 {
		t.Errorf("removed job's last slot rows = %d, want 1", n)
	}

	leader, follower := a, b
	if b.IsLeader() {
		leader, follower = b, a
	}
	if err := follower.PeriodicJobs().Add(hopper.Every(time.Second, noop{N: 3}, &hopper.PeriodicOpts{Name: "follower"})); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)
	if n := jobs("3"); n != 0 {
		t.Errorf("a job added only on the follower was inserted %d times while it followed", n)
	}
	if err := leader.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, follower.IsLeader)
	waitFor(t, func() bool { return jobs("3") >= 1 })
}

// minutesUntil has a slot at every minute up to end, and none after it, so
// that a test of missed slots does not depend on the minute it spans.
type minutesUntil struct{ end time.Time }

func (s minutesUntil) Next(t time.Time) time.Time {
	next := t.Truncate(time.Minute).Add(time.Minute)
	if next.After(s.end) {
		return time.Time{}
	}
	return next
}

// A job added mid-term whose last slot is old back-fills CatchUp slots, as
// at the start of a term, and RunOnStart adds its job to them.
func TestPeriodicJobsAddCatchesUp(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	c := h.started(noopWorkers(), 2)
	waitFor(t, c.IsLeader)
	var end time.Time
	if err := h.pool.QueryRow(ctx, "SELECT now()").Scan(&end); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"minutely", "minutely-start"} {
		if _, err := h.pool.Exec(ctx, "INSERT INTO hopper_periodic (name, last_slot) VALUES ($1, date_trunc('minute', now()) - interval '1 hour')", name); err != nil {
			t.Fatal(err)
		}
	}
	err := c.PeriodicJobs().AddMany([]hopper.PeriodicJob{
		{Name: "minutely", Schedule: minutesUntil{end}, Args: noop{N: 5}, Opts: &hopper.PeriodicOpts{CatchUp: 3}},
		{Name: "minutely-start", Schedule: minutesUntil{end}, Args: noop{N: 6}, Opts: &hopper.PeriodicOpts{CatchUp: 3, RunOnStart: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	count := func(n string) int {
		return h.count("SELECT count(*) FROM hopper_jobs WHERE args->>'n' = $1", n) +
			h.count("SELECT count(*) FROM hopper_job_history WHERE args->>'n' = $1", n)
	}
	waitFor(t, func() bool { return count("5") >= 3 && count("6") >= 4 })
	time.Sleep(300 * time.Millisecond)
	if n := count("5"); n != 3 {
		t.Errorf("caught up %d slots, want 3", n)
	}
	if n := count("6"); n != 4 {
		t.Errorf("with RunOnStart, inserted %d jobs, want 3 caught up and 1 on start", n)
	}
}

// Jobs added, removed and cleared from many goroutines on two clients that
// compete for leadership insert each slot at most once: no job has more jobs
// than it had slots.
func TestPeriodicJobsConcurrentChanges(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	a, b := h.started(noopWorkers(), 4), h.started(noopWorkers(), 4)
	waitFor(t, func() bool { return a.IsLeader() || b.IsLeader() })

	const interval = 100 * time.Millisecond
	start := time.Now()
	stop := start.Add(3 * time.Second)
	var wg sync.WaitGroup
	for _, c := range []*hopper.Client[pgxTx]{a, b} {
		for g := range 4 {
			wg.Go(func() {
				jobs := c.PeriodicJobs()
				for i := 0; time.Now().Before(stop); i++ {
					n := (g + i) % 3
					name := fmt.Sprintf("p%d", n)
					_ = jobs.Add(hopper.Every(interval, noop{N: 10 + n}, &hopper.PeriodicOpts{Name: name}))
					_ = jobs.All()
					switch i % 7 {
					case 0:
						jobs.Clear()
					case 3:
						_ = jobs.Remove(name)
					}
					time.Sleep(time.Duration(i%5) * 10 * time.Millisecond)
				}
			})
		}
	}
	wg.Wait()
	a.PeriodicJobs().Clear()
	b.PeriodicJobs().Clear()
	time.Sleep(200 * time.Millisecond)
	slots := int(time.Since(start)/interval) + 1

	var total int
	for n := 10; n < 13; n++ {
		got := h.count("SELECT count(*) FROM hopper_jobs WHERE args->>'n' = $1", strconv.Itoa(n)) +
			h.count("SELECT count(*) FROM hopper_job_history WHERE args->>'n' = $1", strconv.Itoa(n))
		if got > slots {
			t.Errorf("p%d inserted %d jobs in %d slots", n-10, got, slots)
		}
		total += got
	}
	if total == 0 {
		t.Error("no periodic job was inserted")
	}
}
