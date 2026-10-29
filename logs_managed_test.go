package hopper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

type managedRunKey struct {
	id    JobID
	fence int64
}

type managedRun struct {
	LogRun
	text string
}

type managedTestStore struct {
	mu            sync.Mutex
	runs          map[managedRunKey]managedRun
	version       int
	lastRead      LogReadParams
	failDelete    int
	beforeDelete  func()
	listOverride  *LogRunPage
	chunkOverride *LogChunk
}

func (s *managedTestStore) put(id JobID, fence int64, attempt int, text string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.putLocked(id, fence, attempt, text, at)
}

func (s *managedTestStore) putLocked(id JobID, fence int64, attempt int, text string, at time.Time) {
	if s.runs == nil {
		s.runs = map[managedRunKey]managedRun{}
	}
	s.version++
	s.runs[managedRunKey{id, fence}] = managedRun{
		LogRun: LogRun{JobID: id, Fence: fence, Attempt: attempt, Size: int64(len(text)), UpdatedAt: at, Version: strconv.Itoa(s.version)},
		text:   text,
	}
}

func (s *managedTestStore) Writer(_ context.Context, row *JobRow) (io.WriteCloser, error) {
	s.put(row.ID, row.Fence, row.Attempt, "", time.Now())
	return &managedTestWriter{store: s, row: *row}, nil
}

type managedTestWriter struct {
	store *managedTestStore
	row   JobRow
}

func (w *managedTestWriter) Write(p []byte) (int, error) {
	s := w.store
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := s.runs[managedRunKey{w.row.ID, w.row.Fence}]
	s.putLocked(w.row.ID, w.row.Fence, w.row.Attempt, previous.text+string(p), time.Now())
	return len(p), nil
}

func (*managedTestWriter) Close() error { return nil }

func (*managedTestStore) Logs(context.Context, JobID) ([]AttemptLog, error) {
	return nil, errors.New("whole-log reads must not be used by the managed API")
}

func (s *managedTestStore) List(_ context.Context, p LogListParams) (LogRunPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listOverride != nil {
		return *s.listOverride, nil
	}
	var after LogRun
	if p.After != "" {
		id, fence, _ := strings.Cut(p.After, "/")
		var err error
		if after.JobID, err = ParseJobID(id); err != nil {
			return LogRunPage{}, err
		}
		if after.Fence, err = strconv.ParseInt(fence, 10, 64); err != nil {
			return LogRunPage{}, err
		}
	}
	page := LogRunPage{}
	for _, run := range s.runs {
		if !p.JobID.IsZero() && p.JobID != run.JobID || p.Fence != 0 && p.Fence != run.Fence ||
			p.After != "" && compareLogRuns(after, run.LogRun) >= 0 {
			continue
		}
		page.Runs = append(page.Runs, run.LogRun)
	}
	slices.SortFunc(page.Runs, compareLogRuns)
	if len(page.Runs) > p.Limit {
		last := page.Runs[p.Limit-1]
		page.Next = last.JobID.String() + "/" + strconv.FormatInt(last.Fence, 10)
		page.Runs = page.Runs[:p.Limit]
	}
	return page, nil
}

func (s *managedTestStore) Read(_ context.Context, p LogReadParams) (LogChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastRead = p
	if s.chunkOverride != nil {
		return *s.chunkOverride, nil
	}
	run, ok := s.runs[managedRunKey{p.JobID, p.Fence}]
	if !ok {
		return LogChunk{}, ErrNotFound
	}
	size := int64(len(run.text))
	start := p.Offset
	if start == -1 {
		start = max(0, size-int64(p.Limit))
	}
	if start > size {
		start = 0
	}
	end := min(size, start+int64(p.Limit))
	return LogChunk{Log: run.text[start:end], Offset: start, NextOffset: end, Size: size}, nil
}

func (s *managedTestStore) Delete(_ context.Context, run LogRun) (bool, error) {
	s.mu.Lock()
	hook := s.beforeDelete
	s.beforeDelete = nil
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failDelete > 0 {
		s.failDelete--
		return false, errors.New("storage unavailable")
	}
	key := managedRunKey{run.JobID, run.Fence}
	if current, ok := s.runs[key]; !ok || current.Version != run.Version {
		return false, nil
	}
	delete(s.runs, key)
	return true, nil
}

type managedTestExecutor struct {
	driver.Executor
	now     time.Time
	jobs    map[JobID]*JobRow
	batches [][]JobID
	err     error
}

func (e *managedTestExecutor) Now(context.Context) (time.Time, error) { return e.now, e.err }

func (e *managedTestExecutor) JobList(_ context.Context, p driver.JobListParams) ([]*JobRow, error) {
	e.batches = append(e.batches, slices.Clone(p.IDs))
	if len(p.IDs) == 0 || p.Limit != len(p.IDs) {
		return nil, errors.New("expected a bounded batch of job IDs")
	}
	var rows []*JobRow
	for _, id := range p.IDs {
		if row := e.jobs[id]; row != nil {
			copy := *row
			rows = append(rows, &copy)
		}
	}
	return rows, e.err
}

type managedTestDriver struct {
	driver.Driver[struct{}]
	exec driver.Executor
}

func (d managedTestDriver) Executor() driver.Executor       { return d.exec }
func (managedTestDriver) Capabilities() driver.Capabilities { return driver.Capabilities{} }

func managedClient(t *testing.T, store LogStore, policy *LogRetentionConfig) (*Client[struct{}], *managedTestExecutor) {
	t.Helper()
	e := &managedTestExecutor{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), jobs: map[JobID]*JobRow{}}
	c, err := NewClient(managedTestDriver{exec: e}, &Config{
		Logger:  slog.New(slog.DiscardHandler),
		JobLogs: JobLogsConfig{Store: store, Retention: policy},
	})
	if err != nil {
		t.Fatal(err)
	}
	return c, e
}

func TestJobLogReadExecutionCursor(t *testing.T) {
	store := &managedTestStore{}
	c, e := managedClient(t, store, nil)
	id := JobID{15: 1}
	store.put(id, 1, 1, "first", e.now)
	first, err := c.JobLogRead(t.Context(), id, &LogReadOpts{Limit: 3})
	if err != nil || first.Log != "fir" || first.Cursor != (LogCursor{Fence: 1, Offset: 3}) {
		t.Fatalf("first read = %+v, %v", first, err)
	}
	store.put(id, 2, 1, "second", e.now)
	next, err := c.JobLogRead(t.Context(), id, &LogReadOpts{Cursor: first.Cursor, Limit: 3})
	if err != nil || next.Log != "ond" || next.Cursor.Fence != 2 || store.lastRead.Offset != -1 {
		t.Fatalf("new snooze's execution = %+v, %v; read %+v", next, err, store.lastRead)
	}
	old, err := c.JobLogRead(t.Context(), id, &LogReadOpts{Fence: 1, Cursor: first.Cursor, Limit: 3})
	if err != nil || old.Log != "st" || old.Run.Fence != 1 {
		t.Fatalf("selected earlier execution = %+v, %v", old, err)
	}
	store.put(id, 2, 1, "second appended", e.now)
	appended, err := c.JobLogRead(t.Context(), id, &LogReadOpts{Cursor: next.Cursor})
	if err != nil || appended.Log != " appended" || appended.Offset != 6 {
		t.Fatalf("appended bytes = %+v, %v", appended, err)
	}
}

func TestJobLogReadContracts(t *testing.T) {
	store := &managedTestStore{}
	c, e := managedClient(t, store, nil)
	id := JobID{15: 1}
	store.put(id, 1, 1, "text", e.now)
	for _, p := range []LogReadOpts{
		{Fence: -1}, {Cursor: LogCursor{Offset: -2}}, {Limit: -1}, {Limit: MaxLogReadBytes + 1},
	} {
		if _, err := c.JobLogRead(t.Context(), id, &p); err == nil {
			t.Errorf("accepted %+v", p)
		}
	}
	for _, chunk := range []LogChunk{
		{Log: "too long", NextOffset: 8, Size: 8},
		{Log: "x", Offset: 0, NextOffset: 2, Size: 2},
		{Log: "x", Offset: 0, NextOffset: 1, Size: 0},
		{Offset: -1},
	} {
		store.chunkOverride = &chunk
		if _, err := c.JobLogRead(t.Context(), id, &LogReadOpts{Limit: 3}); err == nil {
			t.Errorf("accepted invalid chunk %+v", chunk)
		}
	}
	store.chunkOverride = nil
	if _, err := c.JobLogRead(t.Context(), id, &LogReadOpts{Fence: 99}); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown execution = %v", err)
	}
	legacy, _ := managedClient(t, &memOnlyStore{}, nil)
	if _, err := legacy.JobLogRead(t.Context(), id, nil); !errors.Is(err, ErrNotSupported) {
		t.Errorf("legacy store = %v", err)
	}
}

type memOnlyStore struct{}

func (*memOnlyStore) Writer(context.Context, *JobRow) (io.WriteCloser, error) { return nil, nil }
func (*memOnlyStore) Logs(context.Context, JobID) ([]AttemptLog, error)       { return nil, nil }

func TestLogRetentionOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		state                    JobState
		age                      time.Duration
		gone, recent, wantDelete bool
	}{
		{name: "completed retained", state: JobStateCompleted, age: 12 * time.Hour},
		{name: "completed expired", state: JobStateCompleted, age: 25 * time.Hour, wantDelete: true},
		{name: "canceled retained", state: JobStateCanceled, age: 2 * 24 * time.Hour},
		{name: "discarded expired", state: JobStateDiscarded, age: 8 * 24 * time.Hour, wantDelete: true},
		{name: "explicitly deleted", gone: true, wantDelete: true},
		{name: "recent orphan", gone: true, recent: true},
		{name: "running", state: JobStateRunning, age: 8 * 24 * time.Hour},
		{name: "waiting after snooze", state: JobStateScheduled, age: 8 * 24 * time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &managedTestStore{}
			c, e := managedClient(t, store, &LogRetentionConfig{})
			id := JobID{15: 1}
			at := e.now.Add(-2 * time.Hour)
			if tc.recent {
				at = e.now
			}
			store.put(id, 1, 1, "some output", at)
			if !tc.gone {
				e.jobs[id] = &JobRow{ID: id, Fence: 1, State: tc.state, FinalizedAt: e.now.Add(-tc.age)}
			}
			res, err := c.MaintainJobLogs(t.Context())
			if err != nil || (res.Deleted == 1) != tc.wantDelete {
				t.Fatalf("retention = %+v, %v", res, err)
			}
			if len(e.batches) != 1 || len(e.batches[0]) != 1 {
				t.Fatalf("lookup batches = %v", e.batches)
			}
		})
	}
}

func TestLogRetentionCountAcrossPages(t *testing.T) {
	store := &managedTestStore{}
	c, e := managedClient(t, store, &LogRetentionConfig{MaxRuns: 2, BatchSize: 3})
	id := JobID{15: 1}
	e.jobs[id] = &JobRow{ID: id, Fence: 12, State: JobStateRunning}
	for fence := int64(1); fence <= 12; fence++ {
		store.put(id, fence, 1, "x", e.now.Add(-2*time.Hour))
	}
	for pass := 0; ; pass++ {
		res, err := c.MaintainJobLogs(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if res.Scanned > 3 {
			t.Fatal("unbounded inventory scan")
		}
		if !res.More {
			break
		}
		if pass > 10 {
			t.Fatal("scan did not finish")
		}
	}
	page, err := c.JobLogRuns(t.Context(), id, "", 10)
	if err != nil || len(page.Runs) != 2 || page.Runs[0].Fence != 12 || page.Runs[1].Fence != 11 {
		t.Fatalf("retained = %+v, %v", page, err)
	}
	stats := c.LogRetentionStats()
	if stats.Deleted != 10 || stats.DeletedBytes != 10 || stats.Passes != 4 || stats.InventoryRuns != 2 || stats.InventoryBytes != 2 || stats.InventoryAt != e.now {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestLogRetentionChangedVersionAndNewClaim(t *testing.T) {
	store := &managedTestStore{}
	c, e := managedClient(t, store, &LogRetentionConfig{})
	id := JobID{15: 1}
	store.put(id, 1, 1, "old", e.now.Add(-2*time.Hour))
	store.beforeDelete = func() {
		store.put(id, 1, 1, "appended", e.now)
		store.put(id, 2, 1, "new claim", e.now)
	}
	res, err := c.MaintainJobLogs(t.Context())
	if err != nil || res.Deleted != 0 {
		t.Fatalf("concurrent append = %+v, %v", res, err)
	}
	page, err := c.JobLogRuns(t.Context(), id, "", 10)
	if err != nil || len(page.Runs) != 2 {
		t.Fatalf("runs = %+v, %v", page, err)
	}
}

func TestLogRetentionRetriesFailedBatch(t *testing.T) {
	store := &managedTestStore{}
	c, e := managedClient(t, store, &LogRetentionConfig{BatchSize: 1})
	for i := byte(1); i <= 3; i++ {
		store.put(JobID{15: i}, 1, 1, "x", e.now.Add(-2*time.Hour))
	}
	store.failDelete = 1
	if _, err := c.MaintainJobLogs(t.Context()); err == nil {
		t.Fatal("expected storage failure")
	}
	if c.logRetention.after != "" {
		t.Fatal("advanced after a failed delete")
	}
	for range 3 {
		if _, err := c.MaintainJobLogs(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	stats := c.LogRetentionStats()
	if len(store.runs) != 0 || stats.Failures != 1 || stats.Deleted != 3 || stats.LastError != "" {
		t.Fatalf("retry stats = %+v, runs %v", stats, store.runs)
	}
}

func TestLogRetentionDatabaseFailurePreservesObjects(t *testing.T) {
	store := &managedTestStore{}
	c, e := managedClient(t, store, &LogRetentionConfig{})
	store.put(JobID{15: 1}, 1, 1, "x", e.now.Add(-2*time.Hour))
	e.err = errors.New("database unavailable")
	if _, err := c.MaintainJobLogs(t.Context()); !errors.Is(err, e.err) {
		t.Fatal(err)
	}
	if len(store.runs) != 1 {
		t.Fatal("deleted while database unavailable")
	}
}

func TestLogRetentionLifetimeOverridesAndFutureFence(t *testing.T) {
	for _, keep := range []time.Duration{2 * time.Hour, -1} {
		store := &managedTestStore{}
		c, e := managedClient(t, store, &LogRetentionConfig{CompletedRetention: keep})
		id := JobID{15: 1}
		e.jobs[id] = &JobRow{ID: id, Fence: 1, State: JobStateCompleted, FinalizedAt: e.now.Add(-3 * time.Hour)}
		store.put(id, 1, 1, "old", e.now.Add(-2*time.Hour))
		store.put(id, 2, 1, "future claim", e.now.Add(-2*time.Hour))
		res, err := c.MaintainJobLogs(t.Context())
		want := 0
		if keep > 0 {
			want = 1
		}
		if err != nil || res.Deleted != want {
			t.Fatalf("keep %v = %+v, %v", keep, res, err)
		}
	}
}

func TestLogRetentionWriteLimit(t *testing.T) {
	store := &managedTestStore{}
	c, _ := managedClient(t, store, &LogRetentionConfig{MaxRunBytes: 4})
	row := &JobRow{ID: JobID{15: 1}, Fence: 1, Attempt: 1}
	w := newStoreWriter(store, t.Context(), row, slog.New(slog.DiscardHandler))
	w.maxBytes = c.cfg.JobLogs.Retention.MaxRunBytes
	for _, line := range []string{"abc\n", "def\n", "ghi\n"} {
		if n, err := w.Write([]byte(line)); err != nil || n != len(line) {
			t.Fatalf("write = %d, %v", n, err)
		}
	}
	w.close(time.Second)
	store.mu.Lock()
	text := store.runs[managedRunKey{row.ID, 1}].text
	store.mu.Unlock()
	if text != "abc\n"+logStoreLimitMessage {
		t.Fatalf("stored %q", text)
	}
}

func TestLogRetentionConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		store  LogStore
		policy LogRetentionConfig
	}{
		{store: &memOnlyStore{}},
		{store: &managedTestStore{}, policy: LogRetentionConfig{GracePeriod: -1}},
		{store: &managedTestStore{}, policy: LogRetentionConfig{Interval: -1}},
		{store: &managedTestStore{}, policy: LogRetentionConfig{Timeout: -1}},
		{store: &managedTestStore{}, policy: LogRetentionConfig{BatchSize: MaxLogListSize + 1}},
	} {
		_, err := NewClient(managedTestDriver{}, &Config{JobLogs: JobLogsConfig{Store: tc.store, Retention: &tc.policy}})
		if err == nil {
			t.Errorf("accepted %+v", tc.policy)
		}
	}
	policy := &LogRetentionConfig{}
	c, _ := managedClient(t, &managedTestStore{}, policy)
	policy.MaxRuns = 99
	if c.cfg.JobLogs.Retention.MaxRuns != 10 || policy.MaxRunBytes != 0 {
		t.Fatal("mutated or retained caller-owned policy")
	}
}

func TestLogRunInventoryRejectsInvalidOrder(t *testing.T) {
	store := &managedTestStore{}
	c, e := managedClient(t, store, &LogRetentionConfig{})
	id := JobID{15: 1}
	store.put(id, 1, 1, "x", e.now.Add(-2*time.Hour))
	store.put(id, 2, 1, "x", e.now.Add(-2*time.Hour))
	page, _ := store.List(t.Context(), LogListParams{Limit: 10})
	slices.Reverse(page.Runs)
	store.listOverride = &page
	if _, err := c.MaintainJobLogs(t.Context()); err == nil {
		t.Fatal("accepted unsorted inventory")
	}
	if len(store.runs) != 2 {
		t.Fatal("deleted with malformed inventory")
	}
}

type blockedManagedStore struct {
	*managedTestStore
	started chan struct{}
	release chan struct{}
}

func (s *blockedManagedStore) List(ctx context.Context, p LogListParams) (LogRunPage, error) {
	close(s.started)
	<-s.release
	return s.managedTestStore.List(ctx, p)
}

func TestLogRetentionBlockedStoreDoesNotBlockTerm(t *testing.T) {
	store := &blockedManagedStore{managedTestStore: &managedTestStore{}, started: make(chan struct{}), release: make(chan struct{})}
	c, _ := managedClient(t, store, &LogRetentionConfig{Interval: time.Millisecond})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { c.logRetentionLoop(ctx); close(done) }()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("maintenance did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("storage blocked leader shutdown")
	}
	_ = c.LogRetentionStats()
	close(store.release)
}

func ExampleLogRetentionConfig() {
	policy := LogRetentionConfig{MaxRuns: 10, MaxRunBytes: 1 << 20, GracePeriod: time.Hour}
	fmt.Println(policy.MaxRuns, policy.MaxRunBytes)
	// Output: 10 1048576
}

type deadlineManagedStore struct {
	*managedTestStore
	deadlines chan time.Time
}

func (s *deadlineManagedStore) List(ctx context.Context, p LogListParams) (LogRunPage, error) {
	deadline, _ := ctx.Deadline()
	select {
	case s.deadlines <- deadline:
	default:
	}
	return s.managedTestStore.List(ctx, p)
}

func TestLogRetentionBatchTimeoutIsIndependent(t *testing.T) {
	store := &deadlineManagedStore{managedTestStore: &managedTestStore{}, deadlines: make(chan time.Time, 1)}
	c, _ := managedClient(t, store, &LogRetentionConfig{Interval: time.Millisecond})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { c.logRetentionLoop(ctx); close(done) }()
	select {
	case deadline := <-store.deadlines:
		if !deadline.After(time.Now().Add(time.Second)) {
			t.Fatal("batch deadline was coupled to its polling interval")
		}
	case <-time.After(time.Second):
		t.Fatal("maintenance did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("maintenance did not stop")
	}
}
