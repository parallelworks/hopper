package hopper_test

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/parallelworks/hopper/driver"
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

// countN counts the jobs, live and finished, whose args' N is n.
func (h *harness) countN(n string) int {
	// One statement, so a job finishing between two counts is not counted in both tables.
	return h.count(`SELECT count(*) FROM (SELECT id FROM hopper_jobs WHERE args->>'n' = $1
		UNION ALL SELECT id FROM hopper_job_history WHERE args->>'n' = $1) j`, n)
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
		"bad metadata": hopper.Every(time.Second, noop{}, &hopper.PeriodicOpts{Name: "c", InsertOpts: &hopper.InsertOpts{Metadata: []byte("{")}}),
		"bad priority": hopper.Every(time.Second, noop{}, &hopper.PeriodicOpts{Name: "d", InsertOpts: &hopper.InsertOpts{Priority: 9}}),
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

func TestPeriodicJobsAddOnClientWithNoQueues(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	var out bytes.Buffer
	c := h.client(&hopper.Config{Logger: slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))})
	if err := c.PeriodicJobs().Add(hopper.Every(time.Second, noop{}, nil)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "no queues") {
		t.Errorf("no record of a periodic job added on a client with no queues: %q", out.String())
	}
}

// Jobs added and removed on running clients: only the leader schedules, so
// a RunOnStart added on every client inserts one job, a removed job stops
// and keeps its last slot, and a job added only on a follower is scheduled
// by the leader while the follower runs.
func TestPeriodicJobsAtRuntime(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := noopWorkers()
	cfg := func() *hopper.Config {
		return &hopper.Config{Queues: map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 2}}, Workers: workers}
	}
	// Leases long enough that leadership moves only when the test stops the
	// leader: a new term would run RunOnStart again.
	tun := fastTuning
	tun.LeaseTTL, tun.LeaseRenew, tun.LeaderTTL = 30*time.Second, time.Second, 30*time.Second
	a, b := h.client(cfg()), h.client(cfg())
	for _, c := range []*hopper.Client[pgxTx]{a, b} {
		c.SetTuning(tun)
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return a.IsLeader() || b.IsLeader() })
	jobs := h.countN

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
		t.Errorf("RunOnStart added on both clients inserted %d jobs, want 1, from the leader only", n)
	}

	for _, c := range []*hopper.Client[pgxTx]{a, b} {
		if err := c.PeriodicJobs().Remove("every"); err != nil {
			t.Fatal(err)
		}
	}
	// Each client records the removal, and the leader reads it within a
	// leader interval.
	time.Sleep(600 * time.Millisecond)
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
	waitFor(t, func() bool { return jobs("3") >= 1 })
	if !leader.IsLeader() || follower.IsLeader() {
		t.Fatal("leadership moved, so the follower may have scheduled its own job")
	}
	if err := follower.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if n := h.count("SELECT count(*) FROM hopper_periodic_registrations"); n != 0 {
		t.Errorf("%d registrations left after the only client with any stopped", n)
	}
	time.Sleep(600 * time.Millisecond)
	before = jobs("3")
	time.Sleep(2 * time.Second)
	if after := jobs("3"); after != before {
		t.Errorf("a job whose only client stopped inserted %d more jobs", after-before)
	}
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
	count := h.countN
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
// compete for leadership stay free of races and insert no more jobs than
// there were slots. Jobs do not record their slot, so this cannot see a
// slot inserted twice; the slot guard is tested by drivertest's periodic
// test and TestPeriodicJobsRunOnceAcrossLeaders.
func TestPeriodicJobsConcurrentChanges(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	a, b := h.started(noopWorkers(), 4), h.started(noopWorkers(), 4)
	waitFor(t, func() bool { return a.IsLeader() || b.IsLeader() })

	// The leader computes the slots on the database clock, so the slots
	// elapsed are measured on it too.
	dbNow := func() time.Time {
		var now time.Time
		if err := h.pool.QueryRow(context.Background(), "SELECT now()").Scan(&now); err != nil {
			t.Fatal(err)
		}
		return now
	}
	const interval = 100 * time.Millisecond
	start := dbNow()
	stop := time.Now().Add(3 * time.Second)
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

	// The leader may insert slots until another client's Clear reaches it,
	// so the slots are counted up to after the jobs are.
	var got [3]int
	for i := range got {
		got[i] = h.countN(strconv.Itoa(10 + i))
	}
	slots := int(dbNow().Sub(start)/interval) + 1
	var total int
	for i, n := range got {
		if n > slots {
			t.Errorf("p%d inserted %d jobs in %d slots", i, n, slots)
		}
		total += n
	}
	if total == 0 {
		t.Error("no periodic job was inserted")
	}
}

// Two programs share an install, each registering only its own periodic job
// and working only its own queue: whichever leads schedules both, through a
// change of leader, and each slot is inserted once.
func TestPeriodicJobsAcrossPrograms(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	start := func(queue string, n int) *hopper.Client[pgxTx] {
		c := h.client(&hopper.Config{
			Queues:  map[string]hopper.QueueConfig{queue: {MaxWorkers: 2}},
			Workers: noopWorkers(),
			Periodic: []hopper.PeriodicJob{hopper.Every(time.Second, noop{N: n}, &hopper.PeriodicOpts{
				Name:       queue,
				InsertOpts: &hopper.InsertOpts{Queue: queue, Unique: &hopper.UniqueOpts{ByQueue: true, ByPeriod: time.Second}},
			})},
		})
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return c
	}
	jobs := h.countN
	a, b := start("a", 1), start("b", 2)
	waitFor(t, func() bool { return a.IsLeader() || b.IsLeader() })
	waitFor(t, func() bool { return jobs("1") >= 2 && jobs("2") >= 2 })

	leader, program, n := a, "a", 1
	if b.IsLeader() {
		leader, program, n = b, "b", 2
	}
	if err := leader.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	start(program, n)
	other := map[*hopper.Client[pgxTx]]*hopper.Client[pgxTx]{a: b, b: a}[leader]
	waitFor(t, other.IsLeader)
	before1, before2 := jobs("1"), jobs("2")
	waitFor(t, func() bool { return jobs("1") >= before1+2 && jobs("2") >= before2+2 })

	for _, n := range []string{"1", "2"} {
		var total, distinct int
		// One statement, so a job finishing between two counts is not counted in both tables.
		if err := h.pool.QueryRow(ctx, `SELECT count(*), count(DISTINCT unique_key) FROM (
			SELECT unique_key FROM hopper_jobs WHERE args->>'n' = $1
			UNION ALL SELECT unique_key FROM hopper_job_history WHERE args->>'n' = $1) j`, n).Scan(&total, &distinct); err != nil {
			t.Fatal(err)
		}
		if total != distinct {
			t.Errorf("job %s: %d jobs for %d slots", n, total, distinct)
		}
	}
}

// A client's registrations go with its row, whether it stops or the leader
// prunes its expired lease, and none are recorded for a client already gone.
func TestPeriodicRegistrationsGoWithTheirClient(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	exec := h.d.Executor()
	register := func() int64 {
		id, err := exec.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "h", TTL: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		if err := exec.PeriodicRegister(ctx, driver.PeriodicRegisterParams{ClientID: id, Registrations: []driver.PeriodicRegistration{
			{Name: "j", Definition: json.RawMessage(`{}`), Digest: "d"},
		}}); err != nil {
			t.Fatal(err)
		}
		return id
	}
	rows := func(id int64) int {
		return h.count("SELECT count(*) FROM hopper_periodic_registrations WHERE client_id = $1", id)
	}

	expired, deleted := register(), register()
	if _, err := h.pool.Exec(ctx, "UPDATE hopper_clients SET expires_at = now() - interval '1 second' WHERE id = $1", expired); err != nil {
		t.Fatal(err)
	}
	if _, err := exec.ClientPruneExpired(ctx); err != nil {
		t.Fatal(err)
	}
	if n := rows(expired); n != 0 {
		t.Errorf("%d registrations left after their client was pruned", n)
	}
	if err := exec.ClientDelete(ctx, deleted); err != nil {
		t.Fatal(err)
	}
	if n := rows(deleted); n != 0 {
		t.Errorf("%d registrations left after their client was deleted", n)
	}
	if err := exec.PeriodicRegister(ctx, driver.PeriodicRegisterParams{ClientID: deleted, Registrations: []driver.PeriodicRegistration{
		{Name: "j", Definition: json.RawMessage(`{}`), Digest: "d"},
	}}); err != nil {
		t.Fatal(err)
	}
	if n := rows(deleted); n != 0 {
		t.Errorf("%d registrations recorded for a deleted client", n)
	}

	if _, err := h.pool.Exec(ctx, `INSERT INTO hopper_periodic_registrations (client_id, name, definition, digest) VALUES (-1, 'left', '{}', 'd')`); err != nil {
		t.Fatal(err)
	}
	register()
	if n := rows(-1); n != 0 {
		t.Errorf("%d registrations of a client with no row survived another client's write", n)
	}
}

// Registration writes racing the prune of their clients leave no row that a
// read returns, and a later write by a live client deletes any they left.
func TestPeriodicRegistrationsRacePrune(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	exec := h.d.Executor()
	regs := []driver.PeriodicRegistration{{Name: "j", Definition: json.RawMessage(`{}`), Digest: "d"}}
	var wg sync.WaitGroup
	for range 50 {
		id, err := exec.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "h", TTL: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() {
			if err := exec.PeriodicRegister(ctx, driver.PeriodicRegisterParams{ClientID: id, Registrations: regs}); err != nil {
				t.Error(err)
			}
		})
		wg.Go(func() {
			if _, err := exec.ClientPruneExpired(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	time.Sleep(10 * time.Millisecond)
	if rows, err := exec.PeriodicRegistrations(ctx); err != nil || len(rows) != 0 {
		t.Errorf("registrations of expired clients read: %v, %v", rows, err)
	}
	if _, err := exec.ClientPruneExpired(ctx); err != nil {
		t.Fatal(err)
	}
	live, err := exec.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "h", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.PeriodicRegister(ctx, driver.PeriodicRegisterParams{ClientID: live, Registrations: regs}); err != nil {
		t.Fatal(err)
	}
	if n := h.count("SELECT count(*) FROM hopper_periodic_registrations WHERE client_id <> $1", live); n != 0 {
		t.Errorf("%d registrations of pruned clients left after a live client's write", n)
	}
}

type strictOther struct{}

func (strictOther) Kind() string { return "strict-other" }

// A StrictKinds client records only the periodic jobs of kinds it has a
// worker for, and records a job once its worker is registered.
func TestPeriodicStrictKindsRecordsOnlyWorkedKinds(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	leader := h.client(&hopper.Config{Queues: map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}}, Workers: noopWorkers()})
	if err := leader.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, leader.IsLeader)

	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[strictOther]) error { return nil })
	strict := h.client(&hopper.Config{
		Queues:      map[string]hopper.QueueConfig{"strict": {MaxWorkers: 1}},
		Workers:     workers,
		StrictKinds: true,
		NeverLead:   true,
		Periodic:    []hopper.PeriodicJob{hopper.Every(time.Hour, noop{N: 8}, &hopper.PeriodicOpts{RunOnStart: true})},
	})
	if err := strict.Start(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	if n := h.count("SELECT count(*) FROM hopper_periodic_registrations"); n != 0 {
		t.Errorf("%d registrations of a kind the StrictKinds client has no worker for", n)
	}
	if n := h.countN("8"); n != 0 {
		t.Errorf("%d jobs of a kind the StrictKinds client has no worker for", n)
	}

	hopper.AddWorkFunc(workers, func(context.Context, *hopper.Job[noop]) error { return nil })
	waitFor(t, func() bool { return h.countN("8") == 1 })
}

// Replicas registering the same job do not start it again as clients come
// and go: the leader's own definition and another client's stored one are
// the same definition.
func TestPeriodicRunOnStartOnceAcrossReplicas(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	tun := fastTuning
	tun.LeaseTTL, tun.LeaseRenew, tun.LeaderTTL = 30*time.Second, time.Second, 30*time.Second
	start := func() *hopper.Client[pgxTx] {
		c := h.client(&hopper.Config{
			Queues:   map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 2}},
			Workers:  noopWorkers(),
			Periodic: []hopper.PeriodicJob{hopper.Every(time.Hour, noop{N: 7}, &hopper.PeriodicOpts{RunOnStart: true})},
		})
		c.SetTuning(tun)
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		return c
	}
	jobs := h.countN
	leader := start()
	waitFor(t, leader.IsLeader)
	waitFor(t, func() bool { return jobs("7") == 1 })
	follower := start()
	time.Sleep(time.Second)
	if err := follower.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	if !leader.IsLeader() {
		t.Fatal("leadership moved, which runs RunOnStart again")
	}
	if n := jobs("7"); n != 1 {
		t.Errorf("RunOnStart inserted %d jobs as a replica started and stopped, want 1", n)
	}
}

// A client that leads from its start schedules its own definition of a job
// from the term's first pass, not first an older client's.
func TestPeriodicLeaderStartsWithItsOwnDefinition(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	start := func(n int, neverLead bool) *hopper.Client[pgxTx] {
		c := h.client(&hopper.Config{
			Queues:    map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 2}},
			Workers:   noopWorkers(),
			NeverLead: neverLead,
			Periodic:  []hopper.PeriodicJob{hopper.Every(time.Hour, noop{N: n}, &hopper.PeriodicOpts{Name: "x", RunOnStart: true})},
		})
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Stop(context.Background()) })
		return c
	}
	start(8, true)
	leader := start(9, false)
	waitFor(t, leader.IsLeader)
	waitFor(t, func() bool { return h.countN("9") == 1 })
	time.Sleep(500 * time.Millisecond)
	if n := h.countN("8"); n != 0 {
		t.Errorf("the older client's definition was inserted %d times before the leader's own", n)
	}
}
