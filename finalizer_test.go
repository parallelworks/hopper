package hopper

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// stubExec records the size of every finalize batch. Each flush waits for
// release when it is set, and failUntil makes batches larger than it fail
// with a deadline error, as a statement that hit its timeout does.
type stubExec struct {
	mu        sync.Mutex
	sizes     []int
	release   chan struct{}
	started   chan int
	failAbove int
}

func (e *stubExec) JobFinalizeMany(ctx context.Context, params driver.JobFinalizeParams) ([]driver.JobID, error) {
	n := len(params.Jobs)
	e.mu.Lock()
	e.sizes = append(e.sizes, n)
	e.mu.Unlock()
	if e.started != nil {
		e.started <- n
	}
	if e.failAbove > 0 && n > e.failAbove {
		return nil, context.DeadlineExceeded
	}
	if e.release != nil {
		select {
		case <-e.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	ids := make([]driver.JobID, n)
	for i, j := range params.Jobs {
		ids[i] = j.ID
	}
	return ids, nil
}

func (e *stubExec) batches() []int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]int(nil), e.sizes...)
}

func newTestFinalizer(exec finalizeExecutor, t tuning, applied func()) (*finalizer, func()) {
	f := newFinalizer(exec, slog.New(slog.DiscardHandler), t, func(string) {}, func(*driver.JobRow, driver.JobFinalize) {
		if applied != nil {
			applied()
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	go f.run(ctx)
	return f, func() { cancel(); <-f.done }
}

func submitN(f *finalizer, n int) {
	for i := range n {
		f.submit(&driver.JobRow{Queue: "default"}, driver.JobFinalize{ID: driver.JobID{byte(i), byte(i >> 8)}, State: driver.JobStateCompleted})
	}
}

// Results behind a slow flush wait in the buffer without blocking submit,
// and submit blocks only once the buffer is full.
func TestFinalizerBufferAbsorbsSlowFlush(t *testing.T) {
	exec := &stubExec{release: make(chan struct{}), started: make(chan int, 64)}
	// The interval is short so the last, lone result flushes on its own.
	tun := tuning{finalizeInterval: 20 * time.Millisecond, finalizeBatch: 10, finalizeBuffer: 100}
	f, stop := newTestFinalizer(exec, tun, nil)
	defer stop()

	// The first batch flushes at finalizeBatch and then blocks in the executor.
	submitN(f, 10)
	if n := <-exec.started; n != 10 {
		t.Fatalf("first flush took %d results, want 10", n)
	}
	// 100 more results fill the buffer without blocking, although no flush
	// can complete.
	done := make(chan struct{})
	go func() { submitN(f, 100); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("submit blocked with a buffer of 100 free slots")
	}
	// The next one blocks: the buffer is full and the flush has not returned.
	blocked := make(chan struct{})
	go func() { submitN(f, 1); close(blocked) }()
	select {
	case <-blocked:
		t.Fatal("submit did not block on a full buffer")
	case <-time.After(100 * time.Millisecond):
	}
	// Once flushes complete, everything drains in batches of finalizeBatch.
	close(exec.release)
	<-blocked
	deadline := time.After(5 * time.Second)
	for total := 10; total < 111; {
		select {
		case n := <-exec.started:
			total += n
			if n > tun.finalizeBatch {
				t.Fatalf("a flush took %d results, more than finalizeBatch %d", n, tun.finalizeBatch)
			}
		case <-deadline:
			t.Fatalf("backlog not drained: flushed %d of 111", total)
		}
	}
}

// A flush that times out is retried as halves, down to single results,
// and every result is still applied exactly once.
func TestFinalizerSplitsTimedOutFlush(t *testing.T) {
	exec := &stubExec{failAbove: 3}
	var mu sync.Mutex
	applied := 0
	// The interval is long so the 16 results flush as one batch, on size: a
	// short one can fire while they are still being submitted and split them.
	tun := tuning{finalizeInterval: time.Minute, finalizeBatch: 16, finalizeBuffer: 32}
	f, stop := newTestFinalizer(exec, tun, func() { mu.Lock(); applied++; mu.Unlock() })
	submitN(f, 16)
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := applied
		mu.Unlock()
		if n == 16 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("applied %d of 16 results", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	sizes := exec.batches()
	// 16 fails, then 8+8 fail, then 4×4 fail, then 8×2 succeed: 15 statements.
	want := map[int]int{16: 1, 8: 2, 4: 4, 2: 8}
	got := map[int]int{}
	for _, n := range sizes {
		got[n]++
	}
	for n, c := range want {
		if got[n] != c {
			t.Fatalf("flushes by size %v, want %v", got, want)
		}
	}
	if len(sizes) != 15 {
		t.Fatalf("%d flushes %v, want 15", len(sizes), sizes)
	}
}

// Errors other than a timeout are retried as they are.
func TestFinalizerRetriesFailedFlushWhole(t *testing.T) {
	var calls int
	exec := &failNExec{fail: 2, err: errors.New("connection reset"), calls: &calls}
	tun := tuning{finalizeInterval: time.Millisecond, finalizeBatch: 8, finalizeBuffer: 16}
	done := make(chan struct{}, 8)
	f, stop := newTestFinalizer(exec, tun, func() { done <- struct{}{} })
	submitN(f, 8)
	for range 8 {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("results not applied after retries")
		}
	}
	stop()
	if calls != 3 {
		t.Fatalf("flush attempts = %d, want 3 (two failures, then success)", calls)
	}
}

type failNExec struct {
	fail  int
	err   error
	calls *int
}

func (e *failNExec) JobFinalizeMany(_ context.Context, params driver.JobFinalizeParams) ([]driver.JobID, error) {
	*e.calls++
	if *e.calls <= e.fail {
		return nil, e.err
	}
	if len(params.Jobs) != 8 {
		return nil, errors.New("batch was split")
	}
	ids := make([]driver.JobID, len(params.Jobs))
	for i, j := range params.Jobs {
		ids[i] = j.ID
	}
	return ids, nil
}
