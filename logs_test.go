package hopper_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
)

type logged struct {
	N int `json:"n"`
}

func (logged) Kind() string { return "logged" }

// syncBuffer is a bytes.Buffer safe for concurrent writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// runLogged runs one job of a worker that fails the attempts before last
// and returns the finished job.
func runLogged(t *testing.T, cfg *hopper.Config, last int, work func(ctx context.Context, job *hopper.Job[logged]) error) (*hopper.JobRow, []hopper.AttemptLog) {
	t.Helper()
	return runLoggedTuned(t, cfg, stepTuning, last, work)
}

// runLoggedTuned is runLogged with the client's tuning.
func runLoggedTuned(t *testing.T, cfg *hopper.Config, tun hopper.Tuning, last int, work func(ctx context.Context, job *hopper.Job[logged]) error) (*hopper.JobRow, []hopper.AttemptLog) {
	t.Helper()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[logged]) error {
		if err := work(ctx, job); err != nil {
			return err
		}
		if job.Attempt < last {
			return fmt.Errorf("attempt %d failed", job.Attempt)
		}
		return nil
	})
	cfg.Queues = map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}}
	cfg.Workers = workers
	cfg.RetryPolicy = &hopper.ExponentialRetry{Base: time.Millisecond, Max: time.Millisecond}
	c := h.client(cfg)
	c.SetTuning(tun)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := c.Insert(ctx, logged{N: 1}, &hopper.InsertOpts{Await: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hopper.Await[struct{}](ctx, c, res.Job.ID); err != nil {
		t.Fatal(err)
	}
	job, err := c.JobGet(ctx, res.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	logs, err := c.JobLogs(ctx, res.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	return job, logs
}

// Logger's lines go to Config.Logger with the job's attributes, and each
// attempt's lines at the kept level are stored with the job, from goroutines
// of the worker's too.
func TestLoggerKeepsAttemptLogs(t *testing.T) {
	t.Parallel()
	var out syncBuffer
	cfg := &hopper.Config{Logger: slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))}
	job, logs := runLogged(t, cfg, 2, func(ctx context.Context, job *hopper.Job[logged]) error {
		log := hopper.Logger(ctx)
		log.Debug("not kept")
		log.Info("starting", "n", job.Attempt)
		var wg sync.WaitGroup
		for i := range 4 {
			wg.Go(func() { log.With("worker", i).Warn("part") })
		}
		wg.Wait()
		return nil
	})
	if len(logs) != 2 {
		t.Fatalf("logs = %+v, want two attempts", logs)
	}
	for i, l := range logs {
		if l.Attempt != i+1 {
			t.Errorf("log %d is of attempt %d", i, l.Attempt)
		}
		if !strings.Contains(l.Log, fmt.Sprintf("level=INFO msg=starting n=%d\n", i+1)) {
			t.Errorf("attempt %d log %q has no start line", l.Attempt, l.Log)
		}
		if n := strings.Count(l.Log, "level=WARN msg=part worker="); n != 4 {
			t.Errorf("attempt %d log has %d lines from goroutines, want 4", l.Attempt, n)
		}
		if strings.Contains(l.Log, "not kept") || strings.Contains(l.Log, "job_id=") {
			t.Errorf("attempt %d log %q has a debug line or the job's attributes", l.Attempt, l.Log)
		}
	}
	got := out.String()
	for _, want := range []string{
		fmt.Sprintf("level=DEBUG msg=\"not kept\" job_id=%s kind=logged attempt=1\n", job.ID),
		fmt.Sprintf("level=INFO msg=starting job_id=%s kind=logged attempt=2 n=2\n", job.ID),
	} {
		if !strings.Contains(got, want) {
			t.Errorf("Config.Logger got %q, want a line ending %q", got, want)
		}
	}
}

// An attempt's log is cut at MaxBytes, between lines, with a count of the
// lines dropped, and a job keeps the logs of its latest MaxAttempts.
func TestLoggerBoundsJobLogs(t *testing.T) {
	t.Parallel()
	const maxBytes = 300
	cfg := &hopper.Config{JobLogs: hopper.JobLogsConfig{MaxBytes: maxBytes, MaxAttempts: 2}}
	_, logs := runLogged(t, cfg, 4, func(ctx context.Context, job *hopper.Job[logged]) error {
		for i := range 20 {
			hopper.Logger(ctx).Info("line", "i", i)
		}
		return nil
	})
	if len(logs) != 2 || logs[0].Attempt != 3 || logs[1].Attempt != 4 {
		t.Fatalf("logs = %+v, want attempts 3 and 4", logs)
	}
	for _, l := range logs {
		lines := strings.Split(strings.TrimSuffix(l.Log, "\n"), "\n")
		marker := lines[len(lines)-1]
		kept := strings.Join(lines[:len(lines)-1], "\n") + "\n"
		if len(kept) > maxBytes || len(lines) < 2 {
			t.Errorf("attempt %d kept %d bytes, over %d", l.Attempt, len(kept), maxBytes)
		}
		want := fmt.Sprintf("hopper: %d more lines dropped: the attempt's log is limited to %d bytes", 20-(len(lines)-1), maxBytes)
		if marker != want {
			t.Errorf("attempt %d log ends %q, want %q", l.Attempt, marker, want)
		}
		for i, line := range lines[:len(lines)-1] {
			if !strings.HasSuffix(line, fmt.Sprintf("level=INFO msg=line i=%d", i)) {
				t.Errorf("attempt %d line %d = %q", l.Attempt, i, line)
			}
		}
	}
}

// Once an attempt's log has dropped a line, its later lines are counted
// without being formatted.
func TestLoggerSkipsFormattingDroppedLines(t *testing.T) {
	t.Parallel()
	var formatted atomic.Int32
	cfg := &hopper.Config{JobLogs: hopper.JobLogsConfig{MaxBytes: 200, ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == "i" {
			formatted.Add(1)
		}
		return a
	}}}
	const lines = 50
	_, logs := runLogged(t, cfg, 1, func(ctx context.Context, job *hopper.Job[logged]) error {
		for i := range lines {
			hopper.Logger(ctx).Info("line", "i", i)
		}
		return nil
	})
	if len(logs) != 1 {
		t.Fatalf("logs = %+v, want one attempt", logs)
	}
	kept := strings.Count(logs[0].Log, "msg=line")
	if want := fmt.Sprintf("hopper: %d more lines dropped", lines-kept); !strings.Contains(logs[0].Log, want) {
		t.Errorf("log %q does not count the dropped lines (%q)", logs[0].Log, want)
	}
	// The kept lines, and the one that did not fit.
	if got := formatted.Load(); got != int32(kept+1) {
		t.Errorf("formatted %d lines, want %d", got, kept+1)
	}
}

// JobLogs.ReplaceAttr redacts the kept lines, which Config.Logger's
// handler options do not reach.
func TestLoggerRedactsKeptLines(t *testing.T) {
	t.Parallel()
	cfg := &hopper.Config{JobLogs: hopper.JobLogsConfig{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == "password" {
			a.Value = slog.StringValue("REDACTED")
		}
		return a
	}}}
	_, logs := runLogged(t, cfg, 1, func(ctx context.Context, job *hopper.Job[logged]) error {
		hopper.Logger(ctx).Info("login", "password", "hunter2")
		return nil
	})
	if len(logs) != 1 || !strings.Contains(logs[0].Log, "password=REDACTED") || strings.Contains(logs[0].Log, "hunter2") {
		t.Errorf("logs = %+v, want the password redacted", logs)
	}
}

// With a negative MaxBytes no lines are kept, and they still go to
// Config.Logger.
func TestLoggerKeepsNoLogs(t *testing.T) {
	t.Parallel()
	var out syncBuffer
	cfg := &hopper.Config{
		Logger:  slog.New(slog.NewTextHandler(&out, nil)),
		JobLogs: hopper.JobLogsConfig{MaxBytes: -1},
	}
	_, logs := runLogged(t, cfg, 1, func(ctx context.Context, job *hopper.Job[logged]) error {
		hopper.Logger(ctx).Info("hello")
		return nil
	})
	if len(logs) != 0 {
		t.Errorf("logs = %+v, want none", logs)
	}
	if !strings.Contains(out.String(), "msg=hello job_id=") {
		t.Errorf("Config.Logger got %q", out.String())
	}
}

// A job completed with CompleteTx keeps the lines logged before the call.
func TestLoggerCompleteTx(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[logged]) error {
		tx, err := h.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx) //nolint:errcheck
		hopper.Logger(ctx).Info("before")
		if err := hopper.CompleteTx(ctx, tx, job); err != nil {
			return err
		}
		hopper.Logger(ctx).Info("after")
		return tx.Commit(ctx)
	})
	c := h.startedWith(workers, 1, stepTuning)
	res, err := c.Insert(ctx, logged{}, &hopper.InsertOpts{Await: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hopper.Await[struct{}](ctx, c, res.Job.ID); err != nil {
		t.Fatal(err)
	}
	logs, err := c.JobLogs(ctx, res.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 || !strings.Contains(logs[0].Log, "msg=before\n") || strings.Contains(logs[0].Log, "after") {
		t.Errorf("logs = %+v, want the line before CompleteTx", logs)
	}
}

func TestLoggerOutsideJob(t *testing.T) {
	t.Parallel()
	if hopper.Logger(context.Background()) != slog.Default() {
		t.Error("Logger outside a job is not slog.Default()")
	}
}

func TestJobLogsConfigValidated(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	for _, tc := range []struct {
		cfg  hopper.JobLogsConfig
		want string
	}{
		{hopper.JobLogsConfig{MaxAttempts: -1}, "JobLogs.MaxAttempts"},
		{hopper.JobLogsConfig{MaxAttempts: hopper.MaxJobLogAttempts + 1}, "JobLogs.MaxAttempts"},
		{hopper.JobLogsConfig{MaxBytes: hopper.MaxJobLogBytes + 1}, "JobLogs.MaxBytes"},
		{hopper.JobLogsConfig{MaxBytes: hopper.MaxJobLogBytes}, "JobLogs.MaxBytes times MaxAttempts"},
		{hopper.JobLogsConfig{MaxBytes: hopper.MaxJobLogTotalBytes/hopper.MaxJobLogAttempts + 1, MaxAttempts: hopper.MaxJobLogAttempts}, "JobLogs.MaxBytes times MaxAttempts"},
	} {
		if _, err := hopper.NewClient(h.d, &hopper.Config{JobLogs: tc.cfg}); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("NewClient with %+v = %v", tc.cfg, err)
		}
	}
	for _, cfg := range []hopper.JobLogsConfig{
		{MaxBytes: hopper.MaxJobLogBytes, MaxAttempts: hopper.MaxJobLogTotalBytes / hopper.MaxJobLogBytes},
		{MaxBytes: hopper.MaxJobLogTotalBytes / hopper.MaxJobLogAttempts, MaxAttempts: hopper.MaxJobLogAttempts},
		{MaxBytes: hopper.MaxJobLogBytes, MaxAttempts: hopper.MaxJobLogAttempts, Store: &memStore{}},
	} {
		if _, err := hopper.NewClient(h.d, &hopper.Config{JobLogs: cfg}); err != nil {
			t.Errorf("NewClient at the limits with %+v = %v", cfg, err)
		}
	}
}

// A snooze gives its attempt back, so a job that snoozes again and again
// keeps the log of only its latest snooze of each attempt number, marked
// as a snooze, and the log of the attempt that follows.
func TestLoggerSnoozes(t *testing.T) {
	t.Parallel()
	var runs atomic.Int32
	_, logs := runLogged(t, &hopper.Config{}, 1, func(ctx context.Context, job *hopper.Job[logged]) error {
		n := runs.Add(1)
		hopper.Logger(ctx).Info("run", "n", n)
		if n <= 3 {
			return hopper.Snooze(time.Millisecond)
		}
		return nil
	})
	if len(logs) != 2 || !logs[0].Snoozed || logs[1].Snoozed || logs[0].Attempt != 1 || logs[1].Attempt != 1 ||
		!strings.Contains(logs[0].Log, "msg=run n=3\n") || !strings.Contains(logs[1].Log, "msg=run n=4\n") {
		t.Errorf("logs = %+v, want the third snooze's and the last attempt's", logs)
	}
}

type inner struct{}

func (inner) Kind() string { return "inner" }

// A worker run with Workers.Work inside a job's attempt logs as that
// attempt.
func TestLoggerInlineWorker(t *testing.T) {
	t.Parallel()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[inner]) error {
		hopper.Logger(ctx).Info("inner")
		return nil
	})
	_, logs := runLogged(t, &hopper.Config{}, 1, func(ctx context.Context, job *hopper.Job[logged]) error {
		return workers.Work(ctx, &hopper.JobRow{Kind: "inner", Args: []byte("{}")}, nil)
	})
	if len(logs) != 1 || !strings.Contains(logs[0].Log, "msg=inner\n") {
		t.Errorf("logs = %+v, want the inline worker's line", logs)
	}
}

// A worker run with Workers.Work logs to the attempt's store, and the
// store's context leads back to neither the attempt nor the inline worker.
func TestLogStoreInlineWorker(t *testing.T) {
	t.Parallel()
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[inner]) error {
		hopper.Logger(ctx).Info("inner")
		return nil
	})
	store := &memStore{}
	_, logs := runLogged(t, &hopper.Config{JobLogs: hopper.JobLogsConfig{Store: store}}, 1,
		func(ctx context.Context, job *hopper.Job[logged]) error {
			return workers.Work(ctx, &hopper.JobRow{Kind: "inner", Args: []byte("{}")}, nil)
		})
	if len(logs) != 1 || !strings.Contains(logs[0].Log, "msg=inner\n") {
		t.Errorf("logs = %+v, want the inline worker's line", logs)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, ctx := range store.ctxs {
		if hopper.Logger(ctx) != slog.Default() {
			t.Error("the store's context leads back to the attempt's logger")
		}
	}
	if len(store.ctxs) != 1 {
		t.Errorf("store opened %d writers, want 1", len(store.ctxs))
	}
}

// memStore is a LogStore in memory.
type memStore struct {
	mu       sync.Mutex
	logs     map[hopper.JobID][]*memLog
	openErr  error
	writeErr error
	// reenter logs with Logger from Writer; nilWriter opens no writer;
	// closePanic panics in Close.
	reenter, nilWriter, closePanic bool
	// closeBlock and writeBlock, if set, hold Close and Write until they
	// are closed. logsErr fails Logs.
	closeBlock chan struct{}
	writeBlock chan struct{}
	logsErr    error
	// ctxs are the contexts writers were opened with.
	ctxs []context.Context
}

type memLog struct {
	store   *memStore
	attempt int
	buf     strings.Builder
	closes  int
}

func (s *memStore) Writer(ctx context.Context, job *hopper.JobRow) (io.WriteCloser, error) {
	if s.reenter {
		hopper.Logger(ctx).Debug("opening the job's log")
	}
	if s.openErr != nil {
		return nil, s.openErr
	}
	if s.nilWriter {
		return nil, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.logs == nil {
		s.logs = map[hopper.JobID][]*memLog{}
	}
	l := &memLog{store: s, attempt: job.Attempt}
	s.logs[job.ID] = append(s.logs[job.ID], l)
	s.ctxs = append(s.ctxs, ctx)
	return l, nil
}

func (s *memStore) Logs(_ context.Context, id hopper.JobID) ([]hopper.AttemptLog, error) {
	if s.logsErr != nil {
		return nil, s.logsErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []hopper.AttemptLog
	for _, l := range s.logs[id] {
		out = append(out, hopper.AttemptLog{Attempt: l.attempt, Log: l.buf.String()})
	}
	return out, nil
}

// get returns the text and the number of closes of each of a job's logs.
func (s *memStore) get(id hopper.JobID) (texts []string, closes []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range s.logs[id] {
		texts = append(texts, l.buf.String())
		closes = append(closes, l.closes)
	}
	return texts, closes
}

func (l *memLog) Write(p []byte) (int, error) {
	if l.store.writeBlock != nil {
		<-l.store.writeBlock
	}
	l.store.mu.Lock()
	defer l.store.mu.Unlock()
	if l.store.writeErr != nil {
		return 0, l.store.writeErr
	}
	if l.closes > 0 {
		return 0, errors.New("write after close")
	}
	return l.buf.Write(p)
}

func (l *memLog) Close() error {
	if l.store.closeBlock != nil {
		<-l.store.closeBlock
	}
	l.store.mu.Lock()
	defer l.store.mu.Unlock()
	l.closes++
	if l.store.closePanic {
		panic("close panics")
	}
	return nil
}

// With a LogStore, an attempt's lines reach the store as they are logged,
// each attempt's writer is closed when its worker returns, Client.JobLogs
// reads the store, and nothing is written to the job's row. The store logs
// with Logger as it opens a writer, which must not re-enter the attempt's.
func TestLogStore(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	store := &memStore{reenter: true}
	started, release := make(chan hopper.JobID), make(chan struct{})
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[logged]) error {
		hopper.Logger(ctx).Info("attempt", "n", job.Attempt)
		if job.Attempt == 1 {
			started <- job.ID
			<-release
			return errors.New("first attempt fails")
		}
		return nil
	})
	var out syncBuffer
	c := h.client(&hopper.Config{
		Queues:      map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers:     workers,
		Logger:      slog.New(slog.NewTextHandler(&out, nil)),
		RetryPolicy: &hopper.ExponentialRetry{Base: time.Millisecond, Max: time.Millisecond},
		JobLogs:     hopper.JobLogsConfig{Store: store},
	})
	c.SetTuning(stepTuning)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := c.Insert(ctx, logged{}, &hopper.InsertOpts{Await: true})
	if err != nil {
		t.Fatal(err)
	}
	id := <-started
	waitFor(t, func() bool {
		texts, closes := store.get(id)
		return len(texts) == 1 && strings.Contains(texts[0], "msg=attempt n=1\n") && closes[0] == 0
	})
	close(release)
	if _, err := hopper.Await[struct{}](ctx, c, res.Job.ID); err != nil {
		t.Fatal(err)
	}
	texts, closes := store.get(id)
	if len(texts) != 2 || !strings.Contains(texts[1], "msg=attempt n=2\n") || !slices.Equal(closes, []int{1, 1}) {
		t.Errorf("store has %q, closed %v", texts, closes)
	}
	logs, err := c.JobLogs(ctx, id)
	if err != nil || len(logs) != 2 || logs[0].Attempt != 1 || logs[1].Attempt != 2 {
		t.Errorf("JobLogs = %+v, %v", logs, err)
	}
	if n := h.count("SELECT count(*) FROM hopper_job_history WHERE id = $1 AND logs IS NULL", [16]byte(id)); n != 1 {
		t.Errorf("the job's row has logs")
	}
	store.mu.Lock()
	for i, ctx := range store.ctxs {
		if ctx.Err() == nil {
			t.Errorf("writer %d's context is live after its close", i)
		}
	}
	store.mu.Unlock()
	if !strings.Contains(out.String(), "msg=attempt job_id=") {
		t.Errorf("Config.Logger got %q", out.String())
	}
}

// A store that fails to open an attempt's writer, opens none, fails to
// write to it or panics closing it costs that attempt its lines in the
// store, and nothing else.
func TestLogStoreFailures(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		store  *memStore
		warn   string
		closes []int
	}{
		{"open", &memStore{openErr: errors.New("store down")}, "open the job's log in the store", nil},
		{"nil writer", &memStore{nilWriter: true}, "open the job's log in the store", nil},
		{"write", &memStore{writeErr: errors.New("disk full")}, "write the job's log to the store", []int{1}},
		{"close panics", &memStore{closePanic: true}, "close the job's log in the store", []int{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var out syncBuffer
			cfg := &hopper.Config{Logger: slog.New(slog.NewTextHandler(&out, nil)), JobLogs: hopper.JobLogsConfig{Store: tc.store}}
			job, _ := runLogged(t, cfg, 1, func(ctx context.Context, job *hopper.Job[logged]) error {
				hopper.Logger(ctx).Info("one")
				hopper.Logger(ctx).Info("two")
				return nil
			})
			if job.State != hopper.JobStateCompleted {
				t.Errorf("job %s", job.State)
			}
			if got := out.String(); strings.Count(got, tc.warn) != 1 || !strings.Contains(got, "msg=two job_id=") {
				t.Errorf("Config.Logger got %q, want one warning %q and both lines", got, tc.warn)
			}
			if _, closes := tc.store.get(job.ID); !slices.Equal(closes, tc.closes) {
				t.Errorf("writers closed %v times, want %v", closes, tc.closes)
			}
		})
	}
}

// A goroutine of the worker that first calls Logger once the attempt has
// ended opens no writer that would never be closed.
func TestLogStoreAfterAttempt(t *testing.T) {
	t.Parallel()
	store := &memStore{}
	ended, done := make(chan struct{}), make(chan struct{})
	job, _ := runLogged(t, &hopper.Config{JobLogs: hopper.JobLogsConfig{Store: store}}, 1, func(ctx context.Context, job *hopper.Job[logged]) error {
		go func() {
			defer close(done)
			<-ended
			hopper.Logger(ctx).Info("late")
		}()
		return nil
	})
	close(ended)
	<-done
	if texts, closes := store.get(job.ID); len(texts) != 0 {
		t.Errorf("store has %q, closed %v; want no writer", texts, closes)
	}
}

// An attempt that calls Logger but writes no line at the kept level opens
// no writer.
func TestLogStoreOpensOnFirstLine(t *testing.T) {
	t.Parallel()
	store := &memStore{}
	job, _ := runLogged(t, &hopper.Config{JobLogs: hopper.JobLogsConfig{Store: store}}, 1, func(ctx context.Context, job *hopper.Job[logged]) error {
		hopper.Logger(ctx).Debug("below the kept level")
		return nil
	})
	if texts, _ := store.get(job.ID); len(texts) != 0 {
		t.Errorf("store has %q, want no writer", texts)
	}
}

// A store whose Close does not return holds the attempt's result for the
// close timeout only.
func TestLogStoreCloseTimeout(t *testing.T) {
	t.Parallel()
	store := &memStore{closeBlock: make(chan struct{})}
	defer close(store.closeBlock)
	var out syncBuffer
	tun := stepTuning
	tun.LogStoreClose = 50 * time.Millisecond
	cfg := &hopper.Config{Logger: slog.New(slog.NewTextHandler(&out, nil)), JobLogs: hopper.JobLogsConfig{Store: store}}
	job, _ := runLoggedTuned(t, cfg, tun, 1, func(ctx context.Context, job *hopper.Job[logged]) error {
		hopper.Logger(ctx).Info("one")
		return nil
	})
	if job.State != hopper.JobStateCompleted || !strings.Contains(out.String(), "the store has not closed the job's log") {
		t.Errorf("job %s; Config.Logger got %q", job.State, out.String())
	}
}

// With a store, JobLogs returns the logs the job's row holds, then the
// store's, in attempt order; a store log followed by another of the same
// attempt number reads as a snooze; and a store that fails returns the
// row's logs with the error.
func TestJobLogsWithStore(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	store := &memStore{}
	var runs atomic.Int32
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[logged]) error {
		n := runs.Add(1)
		hopper.Logger(ctx).Info("run", "n", n)
		if n == 1 {
			return hopper.Snooze(time.Millisecond)
		}
		return nil
	})
	c := h.client(&hopper.Config{
		Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers: workers,
		JobLogs: hopper.JobLogsConfig{Store: store, MaxBytes: 4 * hopper.MaxJobLogBytes},
	})
	c.SetTuning(stepTuning)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := c.Insert(ctx, logged{}, &hopper.InsertOpts{Await: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hopper.Await[struct{}](ctx, c, res.Job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.pool.Exec(ctx, `UPDATE hopper_job_history SET logs = '[{"attempt": 1, "log": "from the row\n"}]' WHERE id = $1`, [16]byte(res.Job.ID)); err != nil {
		t.Fatal(err)
	}
	logs, err := c.JobLogs(ctx, res.Job.ID)
	if err != nil || len(logs) != 3 || logs[0].Log != "from the row\n" ||
		!logs[1].Snoozed || !strings.Contains(logs[1].Log, "msg=run n=1\n") ||
		logs[2].Snoozed || !strings.Contains(logs[2].Log, "msg=run n=2\n") {
		t.Errorf("JobLogs = %+v, %v", logs, err)
	}
	if _, err := c.JobLogs(ctx, hopper.JobID{9}); !errors.Is(err, hopper.ErrNotFound) {
		t.Errorf("JobLogs of an unknown job = %v", err)
	}
	store.logsErr = errors.New("store down")
	logs, err = c.JobLogs(ctx, res.Job.ID)
	if err == nil || !strings.Contains(err.Error(), "store down") || len(logs) != 1 {
		t.Errorf("JobLogs with the store down = %+v, %v", logs, err)
	}
	if _, err := c.JobLogs(ctx, hopper.JobID{9}); !errors.Is(err, hopper.ErrNotFound) || !strings.Contains(err.Error(), "store down") {
		t.Errorf("JobLogs of an unknown job with the store down = %v", err)
	}
	// A store that answers ErrNotFound for a job it has no logs of.
	store.logsErr = fmt.Errorf("no such object: %w", hopper.ErrNotFound)
	if logs, err = c.JobLogs(ctx, res.Job.ID); err != nil || len(logs) != 1 {
		t.Errorf("JobLogs with a store that has none = %+v, %v", logs, err)
	}
}

// A client that stops hard waits for a store's Close for only part of its
// stop grace, so the result of the job it canceled is still written.
func TestLogStoreHardStop(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	store := &memStore{closeBlock: make(chan struct{})}
	defer close(store.closeBlock)
	started := make(chan struct{}, 1)
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, _ *hopper.Job[logged]) error {
		hopper.Logger(ctx).Info("running")
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})
	c := h.client(&hopper.Config{
		Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 1}},
		Workers: workers,
		JobLogs: hopper.JobLogsConfig{Store: store},
	})
	tun := stepTuning
	tun.LogStoreClose = time.Minute
	c.SetTuning(tun)
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := c.Insert(ctx, logged{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	stopCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := c.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop = %v, want DeadlineExceeded", err)
	}
	if job, err := c.JobGet(ctx, res.Job.ID); err != nil || job.State != hopper.JobStateRetryable {
		t.Errorf("job after a hard stop with the store's Close blocked = %+v, %v", job, err)
	}
}

// add stores a log of id's attempt directly, as another process would.
func (s *memStore) add(id hopper.JobID, attempt int, text string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.logs == nil {
		s.logs = map[hopper.JobID][]*memLog{}
	}
	l := &memLog{store: s, attempt: attempt, closes: 1}
	l.buf.WriteString(text)
	s.logs[id] = append(s.logs[id], l)
}

// A store whose Write blocks holds up neither the worker nor, past the close
// timeout, the attempt's result, and its writer's context is canceled.
func TestLogStoreDoesNotBlockWorker(t *testing.T) {
	t.Parallel()
	store := &memStore{writeBlock: make(chan struct{})}
	defer close(store.writeBlock)
	var out syncBuffer
	tun := stepTuning
	tun.LogStoreClose = 50 * time.Millisecond
	cfg := &hopper.Config{Logger: slog.New(slog.NewTextHandler(&out, nil)), JobLogs: hopper.JobLogsConfig{Store: store}}
	var took time.Duration
	job, _ := runLoggedTuned(t, cfg, tun, 1, func(ctx context.Context, job *hopper.Job[logged]) error {
		start := time.Now()
		for i := range 100 {
			hopper.Logger(ctx).Info("line", "i", i)
		}
		took = time.Since(start)
		return nil
	})
	if job.State != hopper.JobStateCompleted || took > time.Second {
		t.Errorf("job %s; logging took %s", job.State, took)
	}
	if !strings.Contains(out.String(), "the store has not closed the job's log") {
		t.Errorf("Config.Logger got %q", out.String())
	}
	waitFor(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return len(store.ctxs) == 1 && store.ctxs[0].Err() != nil
	})
}

// Lines past what may wait for a store that falls behind are dropped and
// counted, and the rest reach it in order.
func TestLogStoreDropsWhenBehind(t *testing.T) {
	t.Parallel()
	store := &memStore{writeBlock: make(chan struct{})}
	line := strings.Repeat("x", 1000)
	job, _ := runLogged(t, &hopper.Config{JobLogs: hopper.JobLogsConfig{Store: store}}, 1, func(ctx context.Context, job *hopper.Job[logged]) error {
		for i := range 1000 {
			hopper.Logger(ctx).Info(line, "i", i)
		}
		close(store.writeBlock)
		return nil
	})
	texts, closes := store.get(job.ID)
	if len(texts) != 1 || !slices.Equal(closes, []int{1}) {
		t.Fatalf("store has %d logs, closed %v", len(texts), closes)
	}
	lines := strings.Split(strings.TrimSuffix(texts[0], "\n"), "\n")
	last := lines[len(lines)-1]
	if !strings.HasPrefix(last, "hopper: ") || !strings.HasSuffix(last, " lines dropped: the store fell behind") || len(lines) > 300 {
		t.Errorf("%d lines, ending %q", len(lines), last)
	}
	for i, l := range lines[:len(lines)-1] {
		if !strings.HasSuffix(l, fmt.Sprintf(" i=%d", i)) {
			t.Fatalf("line %d = %q", i, l)
		}
	}
}

// JobLogs marks a store's snoozed runs, the latest one by the job's attempt
// number, and keeps the latest snooze of each number and the latest
// MaxAttempts logs.
func TestJobLogsBoundsStore(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	store := &memStore{}
	c := h.client(&hopper.Config{JobLogs: hopper.JobLogsConfig{Store: store, MaxAttempts: 3}})

	// A job that is gone: its runs read by number alone.
	gone := hopper.JobID{7}
	for i, attempt := range []int{1, 1, 1, 2, 2, 3} {
		store.add(gone, attempt, fmt.Sprintf("run %d\n", i))
	}
	logs, err := c.JobLogs(ctx, gone)
	want := []hopper.AttemptLog{{Attempt: 2, Snoozed: true, Log: "run 3\n"}, {Attempt: 2, Log: "run 4\n"}, {Attempt: 3, Log: "run 5\n"}}
	if err != nil || !slices.Equal(logs, want) {
		t.Errorf("JobLogs = %+v, %v; want %+v", logs, err, want)
	}

	// A job waiting out a snooze has given its attempt back: the store's
	// run past its attempt number was the snooze.
	res, err := c.Insert(ctx, logged{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	store.add(res.Job.ID, 1, "snoozed run\n")
	logs, err = c.JobLogs(ctx, res.Job.ID)
	if err != nil || len(logs) != 1 || !logs[0].Snoozed {
		t.Errorf("JobLogs of a snoozed job = %+v, %v", logs, err)
	}
}
