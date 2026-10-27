package hopper

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
	skipped   []bool
	release   chan struct{}
	started   chan int
	failAbove int
}

func (e *stubExec) BatchSettle(context.Context, driver.BatchSettleParams) (driver.BatchSettleResult, error) {
	return driver.BatchSettleResult{}, nil
}

func (e *stubExec) WorkflowPromoteReady(context.Context, driver.WorkflowPromoteParams) (driver.WorkflowPromoteResult, error) {
	return driver.WorkflowPromoteResult{}, nil
}

func (e *stubExec) JobGet(context.Context, driver.JobID) (*driver.JobRow, error) {
	return nil, driver.ErrNotFound
}

func (e *stubExec) JobFinalizeMany(ctx context.Context, params driver.JobFinalizeParams) ([]driver.JobFinalized, error) {
	n := len(params.Jobs)
	e.mu.Lock()
	e.sizes = append(e.sizes, n)
	e.skipped = append(e.skipped, params.SkipLocked)
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
	ids := make([]driver.JobFinalized, n)
	for i, j := range params.Jobs {
		ids[i] = driver.JobFinalized{ID: j.ID, Fence: j.Fence}
	}
	return ids, nil
}

func (e *stubExec) batches() []int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]int(nil), e.sizes...)
}

func newTestFinalizer(exec finalizeExecutor, t tuning, applied func()) (*finalizer, func()) {
	f := newFinalizer(exec, slog.New(slog.DiscardHandler), t, 0, func(string) {}, func(*driver.JobRow, driver.JobFinalize) {
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
		f.submit(pending{job: &driver.JobRow{Queue: "default"}, result: driver.JobFinalize{ID: driver.JobID{byte(i), byte(i >> 8)}, State: driver.JobStateCompleted}})
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
	// 16 fails, as if waiting on a held row, and is written again skipping
	// held rows; that fails too, then 8+8 fail, then 4×4 fail, then 8×2
	// succeed: 16 statements, all but the first skipping held rows.
	want := map[int]int{16: 2, 8: 2, 4: 4, 2: 8}
	got := map[int]int{}
	for _, n := range sizes {
		got[n]++
	}
	for n, c := range want {
		if got[n] != c {
			t.Fatalf("flushes by size %v, want %v", got, want)
		}
	}
	if len(sizes) != 16 {
		t.Fatalf("%d flushes %v, want 16", len(sizes), sizes)
	}
	exec.mu.Lock()
	skipped := exec.skipped
	exec.mu.Unlock()
	if skipped[0] || slices.Contains(skipped[1:], false) {
		t.Errorf("statements skipping held rows = %v, want all but the first", skipped)
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

func (e *failNExec) BatchSettle(context.Context, driver.BatchSettleParams) (driver.BatchSettleResult, error) {
	return driver.BatchSettleResult{}, nil
}

func (e *failNExec) WorkflowPromoteReady(context.Context, driver.WorkflowPromoteParams) (driver.WorkflowPromoteResult, error) {
	return driver.WorkflowPromoteResult{}, nil
}

func (e *failNExec) JobGet(context.Context, driver.JobID) (*driver.JobRow, error) {
	return nil, driver.ErrNotFound
}

func (e *failNExec) JobFinalizeMany(_ context.Context, params driver.JobFinalizeParams) ([]driver.JobFinalized, error) {
	*e.calls++
	if *e.calls <= e.fail {
		return nil, e.err
	}
	if len(params.Jobs) != 8 {
		return nil, errors.New("batch was split")
	}
	ids := make([]driver.JobFinalized, len(params.Jobs))
	for i, j := range params.Jobs {
		ids[i] = driver.JobFinalized{ID: j.ID, Fence: j.Fence}
	}
	return ids, nil
}

// A result whose row a worker's transaction wrote (RecordStepTx) goes to the
// statement that skips held rows, while the others do not.
func TestFinalizerSkipsRowsHeldByTx(t *testing.T) {
	exec := &stubExec{started: make(chan int, 4)}
	tun := tuning{finalizeInterval: time.Minute, finalizeBatch: 2, finalizeBuffer: 4}
	f, stop := newTestFinalizer(exec, tun, nil)
	defer stop()
	f.submit(pending{job: &driver.JobRow{Queue: "default"}, result: driver.JobFinalize{ID: driver.JobID{1}, State: driver.JobStateCompleted}, heldByTx: true})
	f.submit(pending{job: &driver.JobRow{Queue: "default"}, result: driver.JobFinalize{ID: driver.JobID{2}, State: driver.JobStateCompleted}})
	for range 2 {
		select {
		case <-exec.started:
		case <-time.After(5 * time.Second):
			t.Fatal("batch not flushed")
		}
	}
	exec.mu.Lock()
	skipped, sizes := slices.Clone(exec.skipped), slices.Clone(exec.sizes)
	exec.mu.Unlock()
	if len(sizes) != 2 || sizes[0] != 1 || sizes[1] != 1 || skipped[0] || !skipped[1] {
		t.Errorf("statements of sizes %v skipping held rows %v, want a plain one and one that skips", sizes, skipped)
	}
}

// loggedResult is a result whose attempt kept n bytes of logs.
func loggedResult(i, n int) pending {
	return pending{job: &driver.JobRow{Queue: "default"}, result: driver.JobFinalize{
		ID: driver.JobID{byte(i), byte(i >> 8)}, State: driver.JobStateCompleted,
		Log: &driver.AttemptLog{Attempt: 1, Log: strings.Repeat("x", n)},
	}}
}

// A batch is flushed once its results hold flushLogBytes of logs, without
// waiting for the interval or a full batch, so that one statement's
// parameters stay small.
func TestFinalizerFlushesAtLogBytes(t *testing.T) {
	exec := &stubExec{started: make(chan int, 64)}
	f := newFinalizer(exec, slog.New(slog.DiscardHandler), tuning{finalizeInterval: time.Hour, finalizeBatch: 500, finalizeBuffer: 100}, 0, func(string) {}, func(*driver.JobRow, driver.JobFinalize) {})
	f.flushLogBytes = 1000
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); <-f.done }()
	go f.run(ctx)
	f.submit(loggedResult(0, 10))
	f.submit(loggedResult(1, 600))
	f.submit(loggedResult(2, 400))
	select {
	case n := <-exec.started:
		if n != 3 {
			t.Errorf("flush took %d results, want 3", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no flush at flushLogBytes")
	}
	f.submit(loggedResult(3, 10))
	cancel()
	<-f.done
	if got := exec.batches(); !slices.Equal(got, []int{3, 1}) {
		t.Errorf("batches %v, want [3 1]", got)
	}
}

// Results holding the whole log budget make submit block until a flush
// frees it, as a full buffer does; a result over the budget alone goes
// through when nothing else holds it.
func TestFinalizerBoundsBufferedLogs(t *testing.T) {
	exec := &stubExec{release: make(chan struct{}), started: make(chan int, 64)}
	f := newFinalizer(exec, slog.New(slog.DiscardHandler), tuning{finalizeInterval: time.Millisecond, finalizeBatch: 500, finalizeBuffer: 100}, 0, func(string) {}, func(*driver.JobRow, driver.JobFinalize) {})
	f.flushLogBytes = 100
	f.logs.max = 300
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); <-f.done }()

	f.submit(loggedResult(0, 1000))
	go f.run(ctx)
	<-exec.started
	submitted := make(chan int, 4)
	go func() {
		for i := 1; i <= 4; i++ {
			f.submit(loggedResult(i, 100))
			submitted <- i
		}
	}()
	select {
	case i := <-submitted:
		t.Fatalf("result %d submitted while a flush held the budget", i)
	case <-time.After(100 * time.Millisecond):
	}
	close(exec.release)
	for want := 1; want <= 4; want++ {
		select {
		case i := <-submitted:
			if i != want {
				t.Fatalf("submitted %d, want %d", i, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("result %d still blocked after the flush", want)
		}
	}
}

// A stop drains the results whose submits wait on the log budget, as it
// drains those that wait on a full buffer, instead of leaving their jobs to
// be rescued and run again.
func TestFinalizerDrainTakesResultsWaitingOnLogs(t *testing.T) {
	exec := &stubExec{release: make(chan struct{}), started: make(chan int, 64)}
	f := newFinalizer(exec, slog.New(slog.DiscardHandler), tuning{finalizeInterval: time.Hour, finalizeBatch: 500, finalizeBuffer: 100}, 0, func(string) {}, func(*driver.JobRow, driver.JobFinalize) {})
	f.logs.max = 300
	ctx, cancel := context.WithCancel(context.Background())
	go f.run(ctx)
	f.submit(loggedResult(0, 200))
	submitted := make(chan struct{})
	go func() {
		f.submit(loggedResult(1, 200))
		close(submitted)
	}()
	cancel()
	<-exec.started
	select {
	case <-submitted:
	case <-time.After(5 * time.Second):
		close(exec.release)
		<-f.done
		t.Fatal("the drain left a result waiting on the log budget")
	}
	close(exec.release)
	<-f.done
	total := 0
	for _, n := range exec.batches() {
		total += n
	}
	if total != 2 {
		t.Errorf("drain wrote %d results in batches %v, want 2", total, exec.batches())
	}
}

// However a submit races with the stop, its result is either written by the
// final drain or dropped with a warning.
func TestFinalizerDrainRacingSubmits(t *testing.T) {
	for round := range 50 {
		exec := &stubExec{}
		var dropped atomic.Int64
		f := newFinalizer(exec, slog.New(dropCounter{&dropped}), tuning{finalizeInterval: time.Hour, finalizeBatch: 4, finalizeBuffer: 8}, 0, func(string) {}, func(*driver.JobRow, driver.JobFinalize) {})
		f.logs.max = 300
		ctx, cancel := context.WithCancel(context.Background())
		go f.run(ctx)
		const n = 40
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() { f.submit(loggedResult(i, 100)) })
		}
		cancel()
		wg.Wait()
		<-f.done
		written := 0
		for _, b := range exec.batches() {
			written += b
		}
		if written+int(dropped.Load()) != n {
			t.Fatalf("round %d: %d written and %d dropped of %d", round, written, dropped.Load(), n)
		}
	}
}

// A stop's final drain takes results for at most drainTimeout: a job that
// finishes later has its result dropped with a warning instead of holding
// the stop, unless its CompleteTx committed, which is applied. Once the
// drain has ended, a committed result is dropped without a warning.
func TestFinalizerDrainTimeout(t *testing.T) {
	exec := &stubExec{}
	var dropped, applied atomic.Int64
	f := newFinalizer(exec, slog.New(dropCounter{&dropped}), tuning{finalizeInterval: time.Hour, finalizeBatch: 4, finalizeBuffer: 8}, 0, func(string) {},
		func(*driver.JobRow, driver.JobFinalize) { applied.Add(1) })
	f.drainTimeout = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	go f.run(ctx)
	f.submit(loggedResult(0, 10))
	f.submits.Add(2) // a submit in flight that has not reached the buffer
	cancel()
	// Past drainTimeout the drain stops taking submits (the low bit of
	// submits) but waits for the one in flight.
	for stopped := time.After(5 * time.Second); f.submits.Load()&1 == 0; {
		select {
		case <-f.done:
			t.Fatal("the drain ended with a submit in flight")
		case <-stopped:
			t.Fatal("the drain did not stop taking submits past drainTimeout")
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case <-f.done:
		t.Fatal("the drain ended with a submit in flight")
	case <-time.After(20 * time.Millisecond):
	}
	late := loggedResult(1, 10)
	late.handedIn = time.Now() // as deliver stamps it during the drain
	f.in <- late
	done := driver.JobFinalize{ID: driver.JobID{3}, State: driver.JobStateCompleted}
	f.in <- pending{job: &driver.JobRow{Queue: "default"}, result: done, completedTx: &done, committed: true, handedIn: time.Now()}
	f.submitDone()
	select {
	case <-f.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the drain did not end past drainTimeout")
	}
	f.submit(loggedResult(2, 10))
	if got := exec.batches(); !slices.Equal(got, []int{1}) || dropped.Load() != 2 {
		t.Errorf("batches %v, %d dropped; want the first result written and the later two dropped", got, dropped.Load())
	}
	if applied.Load() != 2 {
		t.Errorf("applied %d results, want the first and the late committed one", applied.Load())
	}
	f.submit(pending{job: &driver.JobRow{Queue: "default"}, result: done, completedTx: &done, committed: true})
	if applied.Load() != 2 || dropped.Load() != 2 {
		t.Errorf("a committed CompleteTx result after the drain: applied %d, %d dropped; want neither", applied.Load()-2, dropped.Load()-2)
	}
}

// A result handed in before drainTimeout passed is written, even when the
// write before it ran past drainTimeout.
func TestFinalizerDrainWritesResultsHandedInBeforeDeadline(t *testing.T) {
	exec := &stubExec{release: make(chan struct{}), started: make(chan int, 8)}
	var dropped atomic.Int64
	f := newFinalizer(exec, slog.New(dropCounter{&dropped}), tuning{finalizeInterval: time.Hour, finalizeBatch: 4, finalizeBuffer: 8}, 0, func(string) {}, func(*driver.JobRow, driver.JobFinalize) {})
	f.drainTimeout = 200 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	f.submits.Add(2) // a job still finishing keeps the drain open
	go f.run(ctx)
	cancel()
	<-f.draining
	time.Sleep(10 * time.Millisecond) // past the first round, which starts drainTimeout
	f.submit(loggedResult(0, 10))
	<-exec.started // its write is held past drainTimeout
	f.submit(loggedResult(1, 10))
	time.Sleep(2 * f.drainTimeout)
	close(exec.release)
	f.submitDone()
	select {
	case <-f.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the drain did not end")
	}
	if got := exec.batches(); !slices.Equal(got, []int{1, 1}) || dropped.Load() != 0 {
		t.Errorf("batches %v, %d dropped; want both results written", got, dropped.Load())
	}
}

// heldOnceExec finds every job's row held by a transaction the first time
// it is finalized, and the job still running in its attempt.
type heldOnceExec struct {
	stubExec
	held atomic.Bool
}

func (e *heldOnceExec) JobGet(_ context.Context, id driver.JobID) (*driver.JobRow, error) {
	return &driver.JobRow{ID: id, State: driver.JobStateRunning}, nil
}

func (e *heldOnceExec) JobFinalizeMany(ctx context.Context, params driver.JobFinalizeParams) ([]driver.JobFinalized, error) {
	if e.held.CompareAndSwap(false, true) {
		return nil, nil
	}
	return e.stubExec.JobFinalizeMany(ctx, params)
}

// A stop's final drain waits for the retry of a result whose row a
// transaction held, rather than dropping it.
func TestFinalizerDrainWaitsForHeldRetry(t *testing.T) {
	exec := &heldOnceExec{}
	var dropped atomic.Int64
	f := newFinalizer(exec, slog.New(dropCounter{&dropped}), tuning{finalizeInterval: time.Hour, finalizeBatch: 4, finalizeBuffer: 8}, 0, func(string) {}, func(*driver.JobRow, driver.JobFinalize) {})
	ctx, cancel := context.WithCancel(context.Background())
	go f.run(ctx)
	f.submit(pending{job: &driver.JobRow{Queue: "default"}, result: driver.JobFinalize{ID: driver.JobID{1}, State: driver.JobStateCompleted}, heldByTx: true})
	cancel()
	select {
	case <-f.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the drain did not end")
	}
	if got := exec.batches(); !slices.Equal(got, []int{1}) || dropped.Load() != 0 {
		t.Errorf("batches %v after the held write, %d dropped; want the retry written", got, dropped.Load())
	}
}

// dropCounter counts the finalizer's warnings of dropped results.
type dropCounter struct{ n *atomic.Int64 }

func (h dropCounter) Enabled(context.Context, slog.Level) bool { return true }

func (h dropCounter) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "result dropped") {
		h.n.Add(1)
	}
	return nil
}
func (h dropCounter) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h dropCounter) WithGroup(string) slog.Handler      { return h }

// claimExec has one job running in a claim, and applies only the results
// of that claim, as the finalize statement does.
type claimExec struct {
	stubExec
	job        driver.JobRow
	statements int
}

func (e *claimExec) JobGet(context.Context, driver.JobID) (*driver.JobRow, error) {
	j := e.job
	return &j, nil
}

func (e *claimExec) JobFinalizeMany(_ context.Context, params driver.JobFinalizeParams) ([]driver.JobFinalized, error) {
	e.statements++
	var applied []driver.JobFinalized
	for _, r := range params.Jobs {
		if r.ID == e.job.ID && r.AttemptedBy == e.job.AttemptedBy && r.Fence == e.job.Fence {
			applied = append(applied, driver.JobFinalized{ID: r.ID, Fence: r.Fence})
		}
	}
	return applied, nil
}

// A batch can hold a rescued attempt's late result and the result of the
// claim after it, on the same client. The statement returns the fence of
// the result that applied, so that one is credited, whichever came first,
// and whichever has the greater fence: a JobRetry from history by a client
// of the previous release resets it.
func TestFinalizerCreditsTheResultThatApplied(t *testing.T) {
	id := driver.JobID{1}
	for _, fences := range [][2]int64{{1, 2}, {5, 1}} {
		late, current := fences[0], fences[1]
		job := driver.JobRow{ID: id, Queue: "default", State: driver.JobStateRunning, AttemptedBy: 7, Attempt: 2, Fence: current}
		lateRow := job
		lateRow.Fence, lateRow.Attempt = late, 1
		results := []pending{
			{job: &lateRow, result: driver.JobFinalize{ID: id, AttemptedBy: 7, Fence: late, State: driver.JobStateCompleted, Archive: true}},
			{job: &job, result: driver.JobFinalize{ID: id, AttemptedBy: 7, Fence: current, State: driver.JobStateRetryable, Delay: time.Hour}},
		}
		for _, batch := range [][]pending{results, {results[1], results[0]}} {
			exec := &claimExec{job: job}
			var got []driver.JobFinalize
			f := newFinalizer(exec, slog.New(slog.DiscardHandler), tuning{finalizeBatch: 10, finalizeBuffer: 10}, 0, func(string) {},
				func(_ *driver.JobRow, r driver.JobFinalize) { got = append(got, r) })
			f.write(context.Background(), batch, false)
			if exec.statements != 1 {
				t.Errorf("fences %d, %d: %d statements; want the batch in one", batch[0].result.Fence, batch[1].result.Fence, exec.statements)
			}
			if len(got) != 1 || got[0].Fence != current || got[0].State != driver.JobStateRetryable {
				t.Errorf("fences %d, %d: applied %+v; want the current claim's result alone", batch[0].result.Fence, batch[1].result.Fence, got)
			}
		}
	}
}

// fixedJob is a JobGet that reads one job, as it is.
type fixedJob driver.JobRow

func (j fixedJob) JobGet(context.Context, driver.JobID) (*driver.JobRow, error) {
	row := driver.JobRow(j)
	return &row, nil
}

// readTx matches a job to the attempt by its client, fence and attempt
// number: a JobRetry from history by a client of the previous release drops
// the fence, so a later claim by the same client can repeat it.
func TestReadTxChecksTheAttemptWithTheFence(t *testing.T) {
	claimed := driver.JobRow{ID: driver.JobID{1}, State: driver.JobStateRunning, AttemptedBy: 7, Attempt: 1, Fence: 1}
	for _, c := range []struct {
		name string
		edit func(*driver.JobRow)
		want txState
	}{
		{"running in this attempt", func(*driver.JobRow) {}, txRunning},
		{"completed in this attempt", func(j *driver.JobRow) { j.State = driver.JobStateCompleted }, txCommitted},
		{"another client", func(j *driver.JobRow) { j.AttemptedBy = 8 }, txLost},
		{"another claim", func(j *driver.JobRow) { j.Fence = 2 }, txLost},
		{"another attempt at the same fence", func(j *driver.JobRow) { j.Attempt = 3 }, txLost},
	} {
		job := claimed
		c.edit(&job)
		if got := readTx(context.Background(), fixedJob(job), &claimed); got != c.want {
			t.Errorf("%s: readTx = %d, want %d", c.name, got, c.want)
		}
	}
}
