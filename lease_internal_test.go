package hopper

import (
	"context"
	"log/slog"
	"maps"
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

// staleRenewExec holds each renewal until the test lets it go, and answers
// it with the paused and limited queues as they were when it began, as a
// renewal whose statement began before a change committed does. A queue
// listing reads them as they are, then runs listed, if set, once: a change
// applied while the listing is in flight.
type staleRenewExec struct {
	driver.Executor
	mu              sync.Mutex
	paused, limited []string
	listed          func()
	began, release  chan struct{}
}

func (e *staleRenewExec) ClientRenew(ctx context.Context, _ driver.ClientRenewParams) (driver.ClientRenewResult, error) {
	e.mu.Lock()
	res := driver.ClientRenewResult{Renewed: true, PausedQueues: e.paused, LimitedQueues: e.limited}
	e.mu.Unlock()
	select {
	case e.began <- struct{}{}:
	case <-ctx.Done():
		return driver.ClientRenewResult{}, ctx.Err()
	}
	select {
	case <-e.release:
		return res, nil
	case <-ctx.Done():
		return driver.ClientRenewResult{}, ctx.Err()
	}
}

func (e *staleRenewExec) QueueList(context.Context) ([]*driver.QueueRow, error) {
	e.mu.Lock()
	rows := map[string]*driver.QueueRow{}
	row := func(name string) *driver.QueueRow {
		if rows[name] == nil {
			rows[name] = &driver.QueueRow{Name: name}
		}
		return rows[name]
	}
	for _, name := range e.paused {
		row(name).PausedAt = time.Now()
	}
	for _, name := range e.limited {
		row(name).Limits = driver.QueueLimits{Name: name, GlobalLimit: 1}
	}
	listed := e.listed
	e.listed = nil
	e.mu.Unlock()
	if listed != nil {
		listed()
	}
	return slices.Collect(maps.Values(rows)), nil
}

func (e *staleRenewExec) set(paused, limited []string) {
	e.mu.Lock()
	e.paused, e.limited = paused, limited
	e.mu.Unlock()
}

// TestRenewalDoesNotUndoANewerControlChange checks that a renewal that read
// the queues before a pause, resume or limit change committed does not undo
// the change a notification applied while it was in flight, that the next
// renewal applies the queues' state again, that a notification is checked
// against the queues, as one from another schema in the database names a
// queue of the same name, and that a notification's re-read that a local
// change overtook reads again rather than undo it.
func TestRenewalDoesNotUndoANewerControlChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		exec := &staleRenewExec{began: make(chan struct{}), release: make(chan struct{})}
		logger := slog.New(slog.DiscardHandler)
		p := &producer{queue: QueueDefault, logger: logger, wake: make(chan struct{}, 1)}
		c := &Client[struct{}]{exec: exec, logger: logger, tuning: defaultTuning, producers: map[string]*producer{QueueDefault: p}}
		done := make(chan struct{})
		go func() { defer close(done); c.leaseLoop(ctx) }()

		renew := func(change func()) {
			t.Helper()
			<-exec.began
			if change != nil {
				change()
			}
			exec.release <- struct{}{}
			synctest.Wait()
		}
		check := func(when string, paused, limited bool) {
			t.Helper()
			if p.paused.Load() != paused || p.limited.Load() != limited {
				t.Errorf("%s: paused %v, limited %v; want %v, %v", when, p.paused.Load(), p.limited.Load(), paused, limited)
			}
		}
		def := []string{QueueDefault}

		renew(func() {
			exec.set(def, nil)
			c.control(ctx, "pause:"+QueueDefault)
		})
		check("after a renewal that began before the pause", true, false)
		renew(nil)
		check("after the next renewal", true, false)

		renew(func() {
			exec.set(nil, nil)
			c.control(ctx, "resume:"+QueueDefault)
		})
		check("after a renewal that began before the resume", false, false)

		renew(func() {
			exec.set(nil, def)
			c.control(ctx, "limit:"+QueueDefault)
		})
		check("after a renewal that began before the limit", false, true)
		renew(nil)
		check("after the next renewal", false, true)

		c.control(ctx, "pause:"+QueueDefault)
		check("after another schema's pause", false, true)
		exec.set(def, def)
		c.control(ctx, "pause:"+QueueDefault)
		c.control(ctx, "resume:"+QueueDefault)
		check("after another schema's resume", true, true)

		exec.set(nil, nil)
		c.control(ctx, "resume:"+QueueDefault)
		check("after a resume", false, false)
		exec.mu.Lock()
		exec.listed = func() {
			exec.set(def, nil)
			c.setPaused(QueueDefault, true) // this client's own Pause
		}
		exec.mu.Unlock()
		c.control(ctx, "limit:"+QueueDefault)
		check("after a re-read in flight across this client's pause", true, false)

		cancel()
		<-done
	})
}
