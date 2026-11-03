package hopper_test

import (
	"context"
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
}

func (e *dutyExec) duty() {
	if e.resigned.Load() {
		e.after.Add(1)
	}
}

func (e *dutyExec) LeaderResign(ctx context.Context, clientID int64) error {
	err := e.Executor.LeaderResign(ctx, clientID)
	e.resigned.Store(true)
	return err
}

func (e *dutyExec) JobRescueCandidates(ctx context.Context, p driver.JobRescueParams) ([]driver.RescueCandidate, error) {
	e.duty()
	return e.Executor.JobRescueCandidates(ctx, p)
}

func (e *dutyExec) ClientPruneExpired(ctx context.Context) (int64, error) {
	e.duty()
	return e.Executor.ClientPruneExpired(ctx)
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
			resigned := make(chan struct{}, 1)
			go func() {
				for range a.Events(events, hopper.EventLeaderResigned) {
					resigned <- struct{}{}
				}
			}()
			if err := a.Start(ctx); err != nil {
				t.Fatal(err)
			}
			waitFor(t, a.IsLeader)
			b := h.started(hopper.NewWorkers(), 1)

			asker := b
			if from == "leader" {
				asker = a
			}
			start := time.Now()
			id, err := asker.LeaderResign(ctx)
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
		if id, err := a.LeaderResign(context.Background()); err != nil || id != a.ClientID() {
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
	if id, err := c.LeaderResign(context.Background()); err != nil || id != 0 {
		t.Errorf("LeaderResign with no leader = %d, %v; want 0", id, err)
	}
}
