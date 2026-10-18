package hopper

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

type tickArgs struct{}

func (tickArgs) Kind() string { return "tick" }

// periodicExec answers the periodic loop's reads and records its inserts.
// Its first slotsFailures reads of the last slots fail, its next
// insertFailures inserts fail, every insert fails while failInserts is set
// and every clock read while failNow is, and its clock is skew from the
// local one.
type periodicExec struct {
	driver.Executor
	mu             sync.Mutex
	slotsFailures  int
	insertFailures int
	failInserts    bool
	failNow        bool
	skew           time.Duration
	last           map[string]time.Time
	nowReads       int
	inserts        []driver.PeriodicInsertParams
}

func (e *periodicExec) Now(context.Context) (time.Time, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.nowReads++
	if e.failNow {
		return time.Time{}, errors.New("connection reset")
	}
	return time.Now().Add(e.skew), nil
}

func (e *periodicExec) reads() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.nowReads
}

func (e *periodicExec) PeriodicLastSlots(context.Context) (map[string]time.Time, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.slotsFailures > 0 {
		e.slotsFailures--
		return nil, errors.New("connection reset")
	}
	return maps.Clone(e.last), nil
}

func (e *periodicExec) PeriodicInsert(_ context.Context, p driver.PeriodicInsertParams) (*driver.JobRow, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.failInserts || e.insertFailures > 0 {
		e.insertFailures = max(0, e.insertFailures-1)
		return nil, false, errors.New("connection reset")
	}
	e.inserts = append(e.inserts, p)
	return &driver.JobRow{Queue: p.Job.Queue}, true, nil
}

func (e *periodicExec) set(f func(*periodicExec)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	f(e)
}

func (e *periodicExec) inserted(name string) int {
	return len(e.slots(name))
}

// slots returns the slots inserted for the job, in insert order.
func (e *periodicExec) slots(name string) []time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []time.Time
	for _, p := range e.inserts {
		if p.Name == name {
			out = append(out, p.Slot)
		}
	}
	return out
}

func newPeriodicTestClient(t *testing.T, exec driver.Executor, jobs ...PeriodicJob) *Client[struct{}] {
	t.Helper()
	cfg, err := (&Config{Periodic: jobs, Logger: slog.New(slog.DiscardHandler)}).withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	c := &Client[struct{}]{exec: exec, cfg: cfg, logger: cfg.Logger, tuning: defaultTuning, workers: NewWorkers()}
	c.tuning.leaderInterval = 10 * time.Millisecond
	c.periodic = newPeriodicJobs(cfg.Periodic, cfg.Logger, false)
	return c
}

func runPeriodicLoop(t *testing.T, c *Client[struct{}]) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.periodicLoop(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A failed read of the last slots when a term starts is retried, rather than
// leaving the term without periodic jobs.
func TestPeriodicLoopRetriesFailedRead(t *testing.T) {
	exec := &periodicExec{slotsFailures: 2}
	c := newPeriodicTestClient(t, exec, Every(time.Hour, tickArgs{}, &PeriodicOpts{RunOnStart: true}))
	runPeriodicLoop(t, c)
	eventually(t, func() bool { return exec.inserted("tick") == 1 })
}

// A job added while the loop runs is scheduled at once, RunOnStart
// included, and a removed one stops being inserted.
func TestPeriodicLoopAddAndRemove(t *testing.T) {
	exec := &periodicExec{}
	c := newPeriodicTestClient(t, exec)
	runPeriodicLoop(t, c)
	if err := c.PeriodicJobs().Add(Every(time.Hour, tickArgs{}, &PeriodicOpts{Name: "hourly", RunOnStart: true})); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return exec.inserted("hourly") == 1 })

	if err := c.PeriodicJobs().Add(Every(10*time.Millisecond, tickArgs{}, nil)); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return exec.inserted("tick") >= 2 })
	if err := c.PeriodicJobs().Remove("tick"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	n := exec.inserted("tick")
	time.Sleep(100 * time.Millisecond)
	if got := exec.inserted("tick"); got != n {
		t.Errorf("removed job inserted %d more slots", got-n)
	}
	if got := exec.inserted("hourly"); got != 1 {
		t.Errorf("hourly job inserted %d times, want 1", got)
	}
}

// A job removed and added again with another schedule is scheduled anew,
// and RunOnStart fires again.
func TestPeriodicLoopReplace(t *testing.T) {
	exec := &periodicExec{}
	c := newPeriodicTestClient(t, exec, Every(time.Hour, tickArgs{}, &PeriodicOpts{RunOnStart: true}))
	runPeriodicLoop(t, c)
	eventually(t, func() bool { return exec.inserted("tick") == 1 })
	jobs := c.PeriodicJobs()
	if err := jobs.Remove("tick"); err != nil {
		t.Fatal(err)
	}
	if err := jobs.Add(Every(10*time.Millisecond, tickArgs{}, &PeriodicOpts{RunOnStart: true})); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return exec.inserted("tick") >= 4 })
}

// A registration removed before its slot is inserted is not inserted.
func TestInsertPeriodicSkipsRemoved(t *testing.T) {
	exec := &periodicExec{}
	c := newPeriodicTestClient(t, exec, Every(time.Hour, tickArgs{}, nil))
	r := c.periodic.snapshot()[0]
	c.PeriodicJobs().Clear()
	c.insertPeriodic(context.Background(), r, time.Now())
	if n := exec.inserted("tick"); n != 0 {
		t.Errorf("removed job inserted %d times", n)
	}
}

// A database clock behind the local one neither makes the loop spin on
// clock reads nor delays the slots.
func TestPeriodicLoopDatabaseClockBehind(t *testing.T) {
	exec := &periodicExec{skew: -time.Minute}
	c := newPeriodicTestClient(t, exec, Every(50*time.Millisecond, tickArgs{}, nil))
	runPeriodicLoop(t, c)
	time.Sleep(500 * time.Millisecond)
	if n := exec.inserted("tick"); n < 5 {
		t.Errorf("inserted %d slots in 500ms of 50ms slots", n)
	}
	if n := exec.reads(); n > 30 {
		t.Errorf("read the clock %d times for about 10 slots", n)
	}
}

// A job already scheduled keeps inserting its slots while the last slots
// for a job added later cannot be read.
func TestPeriodicLoopFailedReadKeepsOthers(t *testing.T) {
	exec := &periodicExec{}
	c := newPeriodicTestClient(t, exec, Every(20*time.Millisecond, tickArgs{}, nil))
	c.tuning.leaderInterval = time.Hour
	runPeriodicLoop(t, c)
	eventually(t, func() bool { return exec.inserted("tick") >= 1 })
	exec.mu.Lock()
	exec.slotsFailures = 1 << 30
	exec.mu.Unlock()
	if err := c.PeriodicJobs().Add(Every(time.Hour, tickArgs{}, &PeriodicOpts{Name: "added", RunOnStart: true})); err != nil {
		t.Fatal(err)
	}
	n := exec.inserted("tick")
	eventually(t, func() bool { return exec.inserted("tick") >= n+3 })
	if got := exec.inserted("added"); got != 0 {
		t.Errorf("a job whose slots could not be read inserted %d jobs", got)
	}
}

// A failed insert of RunOnStart's job is retried, rather than lost.
func TestPeriodicLoopRetriesFailedStart(t *testing.T) {
	exec := &periodicExec{insertFailures: 2}
	c := newPeriodicTestClient(t, exec, Every(time.Hour, tickArgs{}, &PeriodicOpts{RunOnStart: true}))
	runPeriodicLoop(t, c)
	eventually(t, func() bool { return exec.inserted("tick") == 1 })
}

// A slot whose insert fails is retried, so that no slot is skipped.
func TestPeriodicLoopRetriesFailedSlot(t *testing.T) {
	const interval = 50 * time.Millisecond
	exec := &periodicExec{}
	c := newPeriodicTestClient(t, exec, Every(interval, tickArgs{}, nil))
	runPeriodicLoop(t, c)
	eventually(t, func() bool { return exec.inserted("tick") >= 1 })
	exec.set(func(e *periodicExec) { e.insertFailures = 1 })
	eventually(t, func() bool { return exec.inserted("tick") >= 4 })
	slots := exec.slots("tick")
	for i := 1; i < len(slots); i++ {
		if d := slots[i].Sub(slots[i-1]); d != interval {
			t.Errorf("slot %d is %s after the one before, want %s", i, d, interval)
		}
	}
}

// Slots that fall more than a lease TTL behind while the database fails,
// on inserts or on reads of the clock, are skipped when it recovers, rather
// than inserted all at once.
func TestPeriodicLoopSkipsSlotsPastTTL(t *testing.T) {
	for name, fail := range map[string]func(e *periodicExec, on bool){
		"insert": func(e *periodicExec, on bool) { e.failInserts = on },
		"clock":  func(e *periodicExec, on bool) { e.failNow = on },
	} {
		t.Run(name, func(t *testing.T) {
			const interval = 10 * time.Millisecond
			exec := &periodicExec{}
			c := newPeriodicTestClient(t, exec, Every(interval, tickArgs{}, nil))
			c.tuning.leaderTTL = 100 * time.Millisecond
			runPeriodicLoop(t, c)
			eventually(t, func() bool { return exec.inserted("tick") >= 1 })
			exec.set(func(e *periodicExec) { fail(e, true) })
			n := exec.inserted("tick")
			time.Sleep(500 * time.Millisecond)
			recovered := time.Now()
			exec.set(func(e *periodicExec) { fail(e, false) })
			eventually(t, func() bool { return exec.inserted("tick") > n })
			// The leader interval is 10ms, so the pass that recovers runs
			// within it, and inserts nothing older than a lease TTL before it.
			if first := exec.slots("tick")[n]; first.Before(recovered.Add(-c.tuning.leaderTTL - 5*interval)) {
				t.Errorf("after recovering, inserted slot %s, %s before the recovery", first, recovered.Sub(first))
			}
		})
	}
}

func TestRecentSlots(t *testing.T) {
	s := IntervalSchedule{Interval: time.Second}
	now := time.Date(2026, 1, 1, 12, 0, 0, 500_000_000, time.UTC)
	at := func(sec int) time.Time {
		return time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC).Add(time.Duration(sec) * time.Second)
	}
	for _, tc := range []struct {
		name string
		last time.Time
		n    int
		want []time.Time
	}{
		{"long outage", at(-30 * 24 * 3600), 3, []time.Time{at(-2), at(-1), at(0)}},
		{"fewer than n", at(-2), 5, []time.Time{at(-1), at(0)}},
		{"exactly n", at(-3), 3, []time.Time{at(-2), at(-1), at(0)}},
		{"none", at(0), 3, nil},
		{"last after now", at(10), 3, nil},
		{"distant past", time.Time{}.Add(time.Second), 2, []time.Time{at(-1), at(0)}},
	} {
		got := recentSlots(s, tc.last, now, tc.n)
		if !slices.EqualFunc(got, tc.want, time.Time.Equal) {
			t.Errorf("%s: recentSlots = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// CatchUp back-fills at least the slots the failover window covers, so a
// small CatchUp does not cover a failover worse than none.
func TestPeriodicCatchUpKeepsFailoverWindow(t *testing.T) {
	const interval = 10 * time.Millisecond
	start := time.Now()
	exec := &periodicExec{last: map[string]time.Time{"tick": start.Add(-60 * time.Millisecond).Truncate(interval)}}
	c := newPeriodicTestClient(t, exec, Every(interval, tickArgs{}, &PeriodicOpts{CatchUp: 1}))
	c.tuning.leaderTTL = time.Second
	runPeriodicLoop(t, c)
	eventually(t, func() bool {
		missed := 0
		for _, slot := range exec.slots("tick") {
			if !slot.After(start) {
				missed++
			}
		}
		return missed >= 5
	})
}
