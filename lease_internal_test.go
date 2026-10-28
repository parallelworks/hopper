package hopper

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// leaseExec registers clients and answers renewals: it renews at once
// while renew is set, and otherwise holds each renewal until its context
// ends, as a renewal behind a write lock another transaction holds waits,
// recording how long each was given.
type leaseExec struct {
	driver.Executor
	mu    sync.Mutex
	renew bool
	waits []time.Duration
}

func (e *leaseExec) ClientRegister(context.Context, driver.ClientRegisterParams) (int64, error) {
	return 1, nil
}

func (e *leaseExec) ClientRenew(ctx context.Context, _ driver.ClientRenewParams) (driver.ClientRenewResult, error) {
	deadline, _ := ctx.Deadline()
	e.mu.Lock()
	e.waits = append(e.waits, time.Until(deadline))
	renew := e.renew
	e.mu.Unlock()
	if renew {
		return driver.ClientRenewResult{Renewed: true}, nil
	}
	<-ctx.Done()
	return driver.ClientRenewResult{}, ctx.Err()
}

func (e *leaseExec) setRenew(renew bool) {
	e.mu.Lock()
	e.renew = renew
	e.mu.Unlock()
}

// TestRenewalWaitsUntilTheLeaseExpires checks the lease renewal's bound: a
// renewal held up waits until the lease it renews would expire, counted
// from when it was last taken or renewed (by registering again too), and
// at least one renewal interval once it has.
func TestRenewalWaitsUntilTheLeaseExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		exec := &leaseExec{renew: true}
		tun := defaultTuning.withLease(15 * time.Second) // renewals every 5s
		c := &Client[struct{}]{exec: exec, logger: slog.New(slog.DiscardHandler), tuning: tun}
		if _, err := c.register(ctx); err != nil { // the lease from 0s
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() { defer close(done); c.leaseLoop(ctx) }()

		time.Sleep(6 * time.Second) // renewed at 5s
		exec.setRenew(false)
		time.Sleep(2 * time.Second)
		if _, err := c.register(ctx); err != nil { // registered again at 8s
			t.Fatal(err)
		}
		// Renewals at 10s, held until 23s, the new lease's expiry; then
		// at 23s, the expired lease's, held one interval, to 28s.
		time.Sleep(25 * time.Second)
		cancel()
		<-done
		exec.mu.Lock()
		defer exec.mu.Unlock()
		if want := []time.Duration{10 * time.Second, 13 * time.Second, 5 * time.Second}; !slices.Equal(exec.waits[:min(len(exec.waits), 3)], want) {
			t.Errorf("renewals were given %v, want %v first", exec.waits, want)
		}
	})
}

// TestRenewalBoundMovesWithRenewals checks the common case: each renewal
// that succeeds moves the bound, so a renewal held up after two that
// succeeded is given until the lease the last of them took expires.
func TestRenewalBoundMovesWithRenewals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		exec := &leaseExec{renew: true}
		tun := defaultTuning.withLease(15 * time.Second) // renewals every 5s
		c := &Client[struct{}]{exec: exec, logger: slog.New(slog.DiscardHandler), tuning: tun}
		if _, err := c.register(ctx); err != nil { // the lease from 0s
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() { defer close(done); c.leaseLoop(ctx) }()

		time.Sleep(12 * time.Second) // renewed at 5s and 10s
		exec.setRenew(false)
		// The renewal at 15s is held, and given until 25s, when the lease
		// renewed at 10s expires.
		time.Sleep(5 * time.Second)
		cancel()
		<-done
		exec.mu.Lock()
		defer exec.mu.Unlock()
		if want := []time.Duration{10 * time.Second, 10 * time.Second, 10 * time.Second}; !slices.Equal(exec.waits, want) {
			t.Errorf("renewals were given %v, want %v", exec.waits, want)
		}
	})
}

// lostLeaseExec loses a client's first lease at its first renewal, and
// records each write of a client's periodic jobs: the client and when.
type lostLeaseExec struct {
	driver.Executor
	mu     sync.Mutex
	lastID int64
	writes []registrationWrite
}

type registrationWrite struct {
	client int64
	at     time.Time
}

func (e *lostLeaseExec) ClientRegister(context.Context, driver.ClientRegisterParams) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lastID++
	return e.lastID, nil
}

func (e *lostLeaseExec) ClientRenew(_ context.Context, p driver.ClientRenewParams) (driver.ClientRenewResult, error) {
	return driver.ClientRenewResult{Renewed: p.ClientID != 1}, nil
}

func (e *lostLeaseExec) PeriodicRegister(_ context.Context, p driver.PeriodicRegisterParams) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.writes = append(e.writes, registrationWrite{p.ClientID, time.Now()})
	return nil
}

// A client that registers a new lease after losing one records its
// periodic jobs under it at once, rather than at its next registration
// tick, while the leader sees none of them.
func TestNewLeaseRecordsPeriodicJobsAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		exec := &lostLeaseExec{}
		logger := slog.New(slog.DiscardHandler)
		c := &Client[struct{}]{exec: exec, logger: logger, tuning: defaultTuning.withLease(15 * time.Second), events: newEventBus(), periodic: newPeriodicJobs(nil, logger, false)}
		c.workCtx, c.workCancel = context.WithCancel(ctx)
		c.newGeneration()
		id, err := c.register(ctx)
		if err != nil {
			t.Fatal(err)
		}
		c.clientID.Store(id)
		start := time.Now()
		var wg sync.WaitGroup
		wg.Go(func() { c.leaseLoop(ctx) }) // renews at 5s and loses the lease
		time.Sleep(time.Second)
		wg.Go(func() { c.registrationLoop(ctx) }) // ticks at 6s, 11s
		time.Sleep(5*time.Second + time.Millisecond)
		cancel()
		wg.Wait()

		exec.mu.Lock()
		defer exec.mu.Unlock()
		for _, w := range exec.writes {
			if w.client == 2 {
				if at := w.at.Sub(start); at != 5*time.Second {
					t.Errorf("the new lease was recorded at %s, want 5s, when it was registered", at)
				}
				return
			}
		}
		t.Errorf("the new lease was never recorded: %v", exec.writes)
	})
}
