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
	c.SetTuning(stepTuning)
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
	} {
		if _, err := hopper.NewClient(h.d, &hopper.Config{JobLogs: tc.cfg}); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("NewClient with %+v = %v", tc.cfg, err)
		}
	}
	if _, err := hopper.NewClient(h.d, &hopper.Config{JobLogs: hopper.JobLogsConfig{MaxBytes: hopper.MaxJobLogBytes, MaxAttempts: hopper.MaxJobLogAttempts}}); err != nil {
		t.Errorf("NewClient at the limits = %v", err)
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

// memStore is a LogStore in memory.
type memStore struct {
	mu       sync.Mutex
	logs     map[hopper.JobID][]*memLog
	openErr  error
	writeErr error
}

type memLog struct {
	store   *memStore
	attempt int
	buf     strings.Builder
	closes  int
}

func (s *memStore) Writer(ctx context.Context, job *hopper.JobRow) (io.WriteCloser, error) {
	if s.openErr != nil {
		return nil, s.openErr
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
	return l, nil
}

func (s *memStore) Logs(_ context.Context, id hopper.JobID) ([]hopper.AttemptLog, error) {
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
	l.store.mu.Lock()
	defer l.store.mu.Unlock()
	l.closes++
	return nil
}

// With a LogStore, an attempt's lines reach the store as they are logged,
// each attempt's writer is closed when its worker returns, Client.JobLogs
// reads the store, and nothing is written to the job's row.
func TestLogStore(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	store := &memStore{}
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
	if texts, closes := store.get(id); len(texts) != 1 || !strings.Contains(texts[0], "msg=attempt n=1\n") || closes[0] != 0 {
		t.Errorf("while the attempt runs: store has %q, closed %v", texts, closes)
	}
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
	if !strings.Contains(out.String(), "msg=attempt job_id=") {
		t.Errorf("Config.Logger got %q", out.String())
	}
}

// A store that fails to open an attempt's writer, or to write to it, costs
// that attempt its lines in the store, and nothing else.
func TestLogStoreFailures(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		store *memStore
		warn  string
	}{
		{"open", &memStore{openErr: errors.New("store down")}, "open the job's log in the store"},
		{"write", &memStore{writeErr: errors.New("disk full")}, "write the job's log to the store"},
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
			if _, closes := tc.store.get(job.ID); tc.name == "write" && !slices.Equal(closes, []int{1}) {
				t.Errorf("writer closed %v times, want once", closes)
			}
		})
	}
}
