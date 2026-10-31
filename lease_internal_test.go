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
// applied while the listing is in flight. A pause or resume writes the
// queue's state, then runs committed, if set, once: a change committed and
// applied after it, before its caller goes on.
type staleRenewExec struct {
	driver.Executor
	mu                sync.Mutex
	paused, limited   []string
	listed, committed func()
	began, release    chan struct{}
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

func (e *staleRenewExec) QueuePause(_ context.Context, name string) error {
	return e.setQueuePaused(name, true)
}

func (e *staleRenewExec) QueueResume(_ context.Context, name string) error {
	return e.setQueuePaused(name, false)
}

func (e *staleRenewExec) setQueuePaused(name string, paused bool) error {
	e.mu.Lock()
	e.paused = slices.DeleteFunc(slices.Clone(e.paused), func(q string) bool { return q == name })
	if paused {
		e.paused = append(e.paused, name)
	}
	committed := e.committed
	e.committed = nil
	e.mu.Unlock()
	if committed != nil {
		committed()
	}
	return nil
}

// TestRenewalDoesNotUndoANewerControlChange checks that a renewal that read
// the queues before a pause, resume or limit change committed does not undo
// the change a notification applied while it was in flight, that the next
// renewal applies the queues' state again, that a notification is checked
// against the queues, as one from another schema in the database names a
// queue of the same name, that a notification's re-read that a local
// change or a renewal overtook reads again rather than undo it, that this
// client's own resume does not undo a pause that committed after it, that a
// re-read overtaken on every reading still reads only twice, that a re-read
// overtaken by a renewal that may have begun before its change reads again
// rather than stop on it, and that this client's own pause and resume are
// in effect when they return.
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
			if err := c.Queues().Pause(ctx, QueueDefault); err != nil {
				t.Error(err)
			}
		}
		exec.mu.Unlock()
		c.control(ctx, "limit:"+QueueDefault)
		check("after a re-read in flight across this client's pause", true, false)

		// This client's resume commits, then another client's pause
		// commits and its notification is applied before the resume goes
		// on: the resume does not set the queue going again.
		exec.mu.Lock()
		exec.committed = func() {
			exec.set(def, nil)
			c.control(ctx, "pause:"+QueueDefault)
		}
		exec.mu.Unlock()
		if err := c.Queues().Resume(ctx, QueueDefault); err != nil {
			t.Fatal(err)
		}
		check("after this client's resume, overtaken by another client's pause", true, false)

		// A re-read that read the queues before another client's pause
		// committed, overtaken by a renewal that read the pause and applied
		// it, reads them again rather than set the queue going again.
		exec.set(nil, nil)
		c.control(ctx, "resume:"+QueueDefault)
		check("after a resume", false, false)
		exec.mu.Lock()
		exec.listed = func() {
			exec.set(def, nil) // whose notification this client missed
			renew(nil)
		}
		exec.mu.Unlock()
		c.control(ctx, "limit:"+QueueDefault)
		check("after a re-read overtaken by a renewal", true, false)

		// A re-read overtaken on every reading stops once a renewal's
		// reading has been applied that began after the queues' state was
		// applied since the re-read began, which has the change the re-read
		// is for: the one during its second reading. The one during its
		// first may, as far as the client can tell, have begun before it.
		exec.set(nil, def)
		reads := 0
		var overtake func()
		overtake = func() {
			reads++
			// A renewal that began during this reading, and was applied.
			c.applyRenewed(c.controlSeq.Load(), nil, def)
			if reads < 5 {
				exec.mu.Lock()
				exec.listed = overtake
				exec.mu.Unlock()
			}
		}
		exec.mu.Lock()
		exec.listed = overtake
		exec.mu.Unlock()
		c.control(ctx, "limit:"+QueueDefault)
		check("after a re-read overtaken on every reading", false, true)
		if reads != 2 {
			t.Errorf("a re-read overtaken on every reading read %d times; want 2", reads)
		}
		exec.mu.Lock()
		exec.listed = nil
		exec.mu.Unlock()

		// A renewal that began before another client's pause committed,
		// and so read the queue going, is applied while the pause's re-read
		// is in flight. Nothing was applied between the renewal's start and
		// the re-read's, so for all the client can tell the renewal began
		// before the pause, as it did: the re-read reads again rather than
		// stop on it and leave the pause undone.
		<-exec.began // the renewal begins, and reads the queue going
		exec.set(def, def)
		exec.mu.Lock()
		exec.listed = func() {
			exec.release <- struct{}{}
			synctest.Wait()
		}
		exec.mu.Unlock()
		c.control(ctx, "pause:"+QueueDefault)
		check("after a re-read overtaken by a renewal that began before the pause", true, true)

		// This client's own resume and pause are in effect when they
		// return, before any notification of them arrives.
		if err := c.Queues().Resume(ctx, QueueDefault); err != nil {
			t.Fatal(err)
		}
		check("after this client's resume", false, true)
		if err := c.Queues().Pause(ctx, QueueDefault); err != nil {
			t.Fatal(err)
		}
		check("after this client's pause", true, true)

		cancel()
		<-done
	})
}
