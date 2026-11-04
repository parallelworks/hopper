package hopper_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver"
)

// dutyDriver is a driver whose executor counts the leader duties its
// client runs after it gave the leader lease up.
type dutyDriver struct {
	driver.Driver[pgxTx]
	exec *dutyExec
}

func (d dutyDriver) Executor() driver.Executor { return d.exec }

type dutyExec struct {
	driver.Executor
	resigned atomic.Bool
	after    atomic.Int32
	// running counts the term's live-table passes in progress, and
	// overlapped the resigns made while one was.
	running    atomic.Int32
	overlapped atomic.Int32
}

func (e *dutyExec) duty() {
	if e.resigned.Load() {
		e.after.Add(1)
	}
}

func (e *dutyExec) LeaderResign(ctx context.Context, clientID int64) (bool, error) {
	if e.running.Load() > 0 {
		e.overlapped.Add(1)
	}
	deleted, err := e.Executor.LeaderResign(ctx, clientID)
	e.resigned.Store(true)
	return deleted, err
}

func (e *dutyExec) JobRescueCandidates(ctx context.Context, p driver.JobRescueParams) ([]driver.RescueCandidate, error) {
	e.duty()
	return e.Executor.JobRescueCandidates(ctx, p)
}

func (e *dutyExec) ClientPruneExpired(ctx context.Context) (int64, error) {
	e.duty()
	return e.Executor.ClientPruneExpired(ctx)
}

// JobsMaintain stands for a duty of the term's own loops (the live loop's)
// that lasts until the term ends, so that a resign that does not wait for
// the term's loops gives the lease up while it runs.
func (e *dutyExec) JobsMaintain(ctx context.Context) (driver.JobsMaintainResult, error) {
	e.duty()
	e.running.Add(1)
	defer e.running.Add(-1)
	<-ctx.Done()
	return driver.JobsMaintainResult{}, ctx.Err()
}

// TestLeaderResign asks the leader to resign, from another client and
// from the leader itself: another client takes over well within a lease's
// TTL, the resigned leader emits EventLeaderResigned, runs no duty after it
// gave the lease up, and sits out the election while the other leads.
func TestLeaderResign(t *testing.T) {
	t.Parallel()
	for _, from := range []string{"other", "leader"} {
		t.Run(from, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			ctx := context.Background()
			exec := &dutyExec{Executor: h.d.Executor()}
			a, err := hopper.NewClient[pgxTx](dutyDriver{Driver: h.d, exec: exec}, &hopper.Config{
				Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
				Workers: hopper.NewWorkers(),
				Logger:  testLogger(),
			})
			if err != nil {
				t.Fatal(err)
			}
			a.SetTuning(fastTuning)
			t.Cleanup(func() {
				sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = a.Stop(sctx)
			})
			events, stopEvents := context.WithCancel(ctx)
			defer stopEvents()
			// The term ends on request once: the count catches a second
			// event, which a send on a full channel would hide.
			var resignedN atomic.Int32
			resigned := make(chan struct{}, 1)
			go func() {
				for range a.Events(events, hopper.EventLeaderResigned) {
					resignedN.Add(1)
					select {
					case resigned <- struct{}{}:
					default:
					}
				}
			}()
			if err := a.Start(ctx); err != nil {
				t.Fatal(err)
			}
			waitFor(t, a.IsLeader)
			b := h.started(hopper.NewWorkers(), 1)

			// Another client names the leader it means; the leader asks
			// whichever client leads.
			asker, of := b, a.ClientID()
			if from == "leader" {
				asker, of = a, 0
			}
			// A request that names a client that does not lead asks no one.
			if _, err := asker.LeaderResign(ctx, b.ClientID()); !errors.Is(err, hopper.ErrNotLeader) {
				t.Fatalf("LeaderResign of a client that does not lead = %v, want ErrNotLeader", err)
			}
			start := time.Now()
			id, err := asker.LeaderResign(ctx, of)
			if err != nil {
				t.Fatal(err)
			}
			if id != a.ClientID() {
				t.Fatalf("LeaderResign asked client %d, want the leader %d", id, a.ClientID())
			}
			waitFor(t, b.IsLeader)
			if d := time.Since(start); d >= fastTuning.LeaderTTL {
				t.Errorf("the other client took over after %s, not before the lease's TTL (%s)", d, fastTuning.LeaderTTL)
			}
			if a.IsLeader() {
				t.Error("the resigned leader still leads")
			}
			select {
			case <-resigned:
			case <-time.After(5 * time.Second):
				t.Error("no EventLeaderResigned")
			}
			// Past the hold-off, the resigned client stays out while the
			// other leads, and runs no duty.
			time.Sleep(fastTuning.LeaderTTL + 2*fastTuning.LeaderInterval)
			if a.IsLeader() || !b.IsLeader() {
				t.Errorf("after the hold-off: resigned client leads %v, other client leads %v; want the other only", a.IsLeader(), b.IsLeader())
			}
			if n := exec.after.Load(); n != 0 {
				t.Errorf("the resigned client ran %d leader duties after it gave the lease up", n)
			}
			if n := exec.overlapped.Load(); n != 0 {
				t.Errorf("the resigned client gave the lease up while a duty of its term ran (%d times)", n)
			}
			if n := resignedN.Load(); n != 1 {
				t.Errorf("%d EventLeaderResigned, want 1", n)
			}
		})
	}

	// A lone leader that resigned on request sits out the election for a
	// lease's TTL, so the cluster is leaderless that long, and then it is
	// elected again.
	t.Run("alone", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t)
		a := h.started(hopper.NewWorkers(), 1)
		waitFor(t, a.IsLeader)
		if id, err := a.LeaderResign(context.Background(), a.ClientID()); err != nil || id != a.ClientID() {
			t.Fatalf("LeaderResign = %d, %v; want %d", id, err, a.ClientID())
		}
		waitFor(t, func() bool { return !a.IsLeader() })
		resigned := time.Now()
		time.Sleep(fastTuning.LeaderTTL / 2)
		if a.IsLeader() {
			t.Fatal("the resigned client was elected again within the hold-off")
		}
		waitFor(t, a.IsLeader)
		if d := time.Since(resigned); d < fastTuning.LeaderTTL-fastTuning.LeaderInterval {
			t.Errorf("elected again %s after it resigned, before the hold-off (%s) ended", d, fastTuning.LeaderTTL)
		}
	})

	// With no leader, a request asks no one.
	h := newHarness(t)
	c := h.client(&hopper.Config{})
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if id, err := c.LeaderResign(context.Background(), 0); err != nil || id != 0 {
		t.Errorf("LeaderResign with no leader = %d, %v; want 0", id, err)
	}
	if id, err := c.LeaderResign(context.Background(), 5); !errors.Is(err, hopper.ErrNotLeader) || id != 0 {
		t.Errorf("LeaderResign of client 5 with no leader = %d, %v; want 0, ErrNotLeader", id, err)
	}
}

// TestLeaderLapseStartsTerm lets the leader's lease lapse while the client
// still runs: the client takes it back as a new term, so it emits
// EventLeaderLost and then EventLeaderElected, and the new term inserts the
// RunOnStart job again.
func TestLeaderLapseStartsTerm(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	c := h.client(&hopper.Config{
		Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers: noopWorkers(),
		Periodic: []hopper.PeriodicJob{
			{Name: "on-start", Schedule: noSlots{}, Args: noop{N: 41}, Opts: &hopper.PeriodicOpts{RunOnStart: true}},
		},
	})
	events, stopEvents := context.WithCancel(ctx)
	defer stopEvents()
	var (
		mu    sync.Mutex
		kinds []hopper.EventKind
	)
	go func() {
		for ev := range c.Events(events, hopper.EventLeaderElected, hopper.EventLeaderLost) {
			mu.Lock()
			kinds = append(kinds, ev.Kind)
			mu.Unlock()
		}
	}()
	seen := func() []hopper.EventKind {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(kinds)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return h.countN("41") == 1 && len(seen()) == 1 })
	if _, err := h.pool.Exec(ctx, `UPDATE hopper_leader SET expires_at = now() - interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	want := []hopper.EventKind{hopper.EventLeaderElected, hopper.EventLeaderLost, hopper.EventLeaderElected}
	waitFor(t, func() bool { return len(seen()) >= len(want) })
	if got := seen(); !slices.Equal(got, want) {
		t.Errorf("leader events = %v, want %v", got, want)
	}
	waitFor(t, func() bool { return h.countN("41") == 2 })
	if !c.IsLeader() {
		t.Error("the client does not lead after taking its lapsed lease back")
	}
}

// TestLeaderStopAfterFence stops a leader that a fence has moved to
// another client ID before its leader loop noticed: the stop deletes the
// lease under the ID the lease was held under, so another client need not
// wait for it to expire.
func TestLeaderStopAfterFence(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	tun := fastTuning
	tun.LeaseTTL, tun.LeaseRenew, tun.LeaderInterval = time.Minute, 30*time.Second, time.Hour
	c := h.startedWith(hopper.NewWorkers(), 1, tun)
	waitFor(t, c.IsLeader)
	c.FenceTo(c.ClientID() + 1_000_000)
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Stop(sctx); err != nil {
		t.Fatal(err)
	}
	if n := h.count(`SELECT count(*) FROM hopper_leader`); n != 0 {
		t.Errorf("leader leases after the stop = %d, want 0", n)
	}
}

// lossyDriver is a driver whose executor, once armed, loses the response
// of the first leadership attempt that started a term: the takeover
// commits, but the client sees an error.
type lossyDriver struct {
	driver.Driver[pgxTx]
	exec *lossyExec
}

func (d lossyDriver) Executor() driver.Executor { return d.exec }

type lossyExec struct {
	driver.Executor
	mu    sync.Mutex
	armed bool
	last  time.Time // the elected_at of the last term an attempt returned
	lost  int
}

func (e *lossyExec) LeaderAttempt(ctx context.Context, p driver.LeaderParams) (driver.LeaderResult, error) {
	res, err := e.Executor.LeaderAttempt(ctx, p)
	if err != nil || !res.Leader {
		return res, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.armed && !res.ElectedAt.Equal(e.last) {
		e.armed = false
		e.lost++
		return driver.LeaderResult{}, errors.New("connection reset after the takeover committed")
	}
	e.last = res.ElectedAt
	return res, nil
}

// TestLeaderLostTakeoverResponseStartsTerm loses the response of the
// attempt that took the client's lapsed lease back: the renewal after it
// reports the new term's elected_at, so the client still ends the old
// term and starts the new one (EventLeaderLost, then EventLeaderElected,
// and RunOnStart again) rather than carry the old term over the lapse.
func TestLeaderLostTakeoverResponseStartsTerm(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	exec := &lossyExec{Executor: h.d.Executor()}
	c, err := hopper.NewClient[pgxTx](lossyDriver{Driver: h.d, exec: exec}, &hopper.Config{
		Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers: noopWorkers(),
		Logger:  testLogger(),
		Periodic: []hopper.PeriodicJob{
			{Name: "on-start", Schedule: noSlots{}, Args: noop{N: 43}, Opts: &hopper.PeriodicOpts{RunOnStart: true}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	c.SetTuning(fastTuning)
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = c.Stop(sctx)
	})
	events, stopEvents := context.WithCancel(ctx)
	defer stopEvents()
	var (
		mu    sync.Mutex
		kinds []hopper.EventKind
	)
	go func() {
		for ev := range c.Events(events, hopper.EventLeaderElected, hopper.EventLeaderLost) {
			mu.Lock()
			kinds = append(kinds, ev.Kind)
			mu.Unlock()
		}
	}()
	seen := func() []hopper.EventKind {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(kinds)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return h.countN("43") == 1 && len(seen()) == 1 })
	exec.mu.Lock()
	exec.armed = true
	exec.mu.Unlock()
	if _, err := h.pool.Exec(ctx, `UPDATE hopper_leader SET expires_at = now() - interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	want := []hopper.EventKind{hopper.EventLeaderElected, hopper.EventLeaderLost, hopper.EventLeaderElected}
	waitFor(t, func() bool { return len(seen()) >= len(want) })
	if got := seen(); !slices.Equal(got, want) {
		t.Errorf("leader events = %v, want %v", got, want)
	}
	exec.mu.Lock()
	lost := exec.lost
	exec.mu.Unlock()
	if lost != 1 {
		t.Errorf("lost takeover responses = %d, want 1", lost)
	}
	waitFor(t, func() bool { return h.countN("43") == 2 })
}
