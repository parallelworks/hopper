package hopper

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// notifyExec sends notifications for a notifier's tests: each takes a
// little while, and while block is set it waits until release is closed,
// saying on entered that it began. It counts those in flight, and those
// that began once stopped was set.
type notifyExec struct {
	driver.Executor
	block    bool
	entered  chan struct{}
	release  chan struct{}
	inFlight atomic.Int64
	stopped  atomic.Bool
	late     atomic.Int64
}

func newNotifyExec(block bool) *notifyExec {
	return &notifyExec{block: block, entered: make(chan struct{}, 1), release: make(chan struct{})}
}

func (e *notifyExec) Notify(ctx context.Context, _ string, _ []string) error {
	e.inFlight.Add(1)
	defer e.inFlight.Add(-1)
	if e.stopped.Load() {
		e.late.Add(1)
	}
	if e.block {
		e.entered <- struct{}{}
		<-e.release
	}
	select {
	case <-time.After(200 * time.Microsecond):
	case <-ctx.Done():
	}
	return nil
}

// TestFlushNowWaitsForFiringTimer checks that flushNow, which Stop calls,
// returns only once a flush its timer began has finished.
func TestFlushNowWaitsForFiringTimer(t *testing.T) {
	exec := newNotifyExec(true)
	n := newNotifier(exec, slog.New(slog.DiscardHandler), time.Millisecond)
	n.mark("q")
	<-exec.entered // the timer fired, and its flush is notifying
	returned := make(chan struct{})
	go func() {
		n.flushNow(context.Background())
		close(returned)
	}()
	select {
	case <-returned:
		t.Fatal("flushNow returned while the flush its timer began was still notifying")
	case <-time.After(50 * time.Millisecond):
	}
	close(exec.release)
	<-returned
	if exec.inFlight.Load() != 0 {
		t.Error("a notification was in flight once flushNow returned")
	}
}

// TestStopReturnsWithItsContext checks that Stop, given a context that has
// ended, returns while a flush the notifier's timer began is still
// notifying, through a driver that doesn't heed its context.
func TestStopReturnsWithItsContext(t *testing.T) {
	exec := newNotifyExec(true)
	defer close(exec.release)
	c := &Client[struct{}]{notifier: newNotifier(exec, slog.New(slog.DiscardHandler), time.Millisecond)}
	c.notifier.mark("q")
	<-exec.entered // the timer fired, and its flush is notifying
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	returned := make(chan error, 1)
	go func() { returned <- c.Stop(ctx) }()
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop, given a context that had ended, waited for a notification")
	}
}

// TestStopAsTheNotifierFires stops a client as the timer that its inserts
// armed fires, again and again, at times around the moment it does: no
// notification is in flight once Stop returns, nor begins after.
func TestStopAsTheNotifierFires(t *testing.T) {
	ctx := context.Background()
	const interval = time.Millisecond
	execs := make([]*notifyExec, 200)
	for i := range execs {
		exec := newNotifyExec(false)
		execs[i] = exec
		c := &Client[struct{}]{notifier: newNotifier(exec, slog.New(slog.DiscardHandler), interval)}
		c.notifier.mark("q")
		c.notifier.markStream("topic")
		time.Sleep(time.Duration(i%20) * interval / 10)
		if err := c.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		if n := exec.inFlight.Load(); n != 0 {
			t.Fatalf("stop %d: %d notifications in flight once Stop returned", i, n)
		}
		exec.stopped.Store(true)
	}
	time.Sleep(10 * interval)
	for i, exec := range execs {
		if n := exec.late.Load(); n != 0 {
			t.Fatalf("stop %d: %d notifications began after Stop returned", i, n)
		}
	}
}
