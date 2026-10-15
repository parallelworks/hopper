package hopper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// AttemptLog is the log output of one attempt of a job. See Logger.
type AttemptLog = driver.AttemptLog

// LogStore keeps jobs' log lines outside hopper's tables, for logs that are
// large, must be read while the attempt runs, or are written by a process
// that runs a job's work elsewhere. Logs are keyed by job ID and attempt:
// hopper writes nothing of them to the database, so they survive the
// client's crash, and a remote process writes an attempt's lines under the
// ID and attempt its job was claimed with. The store keeps them for as long
// as it chooses; hopper does not remove them with the job.
//
// Its methods are called from many goroutines at once.
type LogStore interface {
	// Writer returns where the lines of the attempt of job that is running
	// go, as Logger logs them: one line per Write, in slog's text format.
	// It is called with the attempt's first line, so an attempt that logs
	// nothing opens no writer. Its context carries the job's values but not
	// its cancellation, deadline or attempt (Logger called with it logs as
	// outside a job), and is canceled once the writer is closed.
	//
	// Hopper closes the writer once the worker returns and waits for Close
	// for up to 5 seconds before it hands in the attempt's result, so a
	// store that saves on Close has the log by the time the job is finished.
	// A job completed with CompleteTx is finished when the worker's
	// transaction commits, which may be before. Lines logged after the
	// close go to Config.Logger only.
	//
	// A snooze gives the attempt back, so the run after it calls Writer
	// again with the same job ID and attempt: keep both, in order. A failed
	// or panicking Writer, Write or Close, or a nil writer, is logged to
	// Config.Logger and drops the attempt's remaining lines; the job's
	// outcome does not depend on it.
	Writer(ctx context.Context, job *JobRow) (io.WriteCloser, error)
	// Logs returns the logs of a job's attempts, oldest first, for
	// Client.JobLogs and hopperui, with no entry for a job it has none of.
	Logs(ctx context.Context, id JobID) ([]AttemptLog, error)
}

// JobLogsConfig bounds the log lines Logger keeps with each job, or sends
// them to a LogStore.
type JobLogsConfig struct {
	// Store, if set, receives each attempt's lines as they are logged
	// instead of the job's row in the database, and Client.JobLogs reads
	// them from it, after any the job's row holds. MaxBytes and
	// MaxAttempts then do not apply, nor are they checked: what the store
	// keeps is its own choice.
	Store LogStore
	// MaxBytes is how much of an attempt's log is kept. Once a line would
	// pass it, that line and the rest are dropped, and the log ends with a
	// line that counts them. Defaults to 32 KiB, and is at most 1 MiB;
	// negative keeps no lines. A finished attempt's lines wait in memory
	// with its result until it is written, so a client holds at most
	// 64 MiB of them; past that, finished jobs wait to hand in their
	// results, as they do when the finalizer's buffer is full.
	MaxBytes int
	// MaxAttempts is how many attempts' logs a job keeps, the latest. Each
	// snooze keeps its own until the next snooze of the same attempt
	// number. Defaults to 10, and is at most 100.
	MaxAttempts int
	// Level is the lowest level kept. Defaults to slog.LevelInfo. Lines
	// go to Config.Logger at whatever level its handler takes.
	Level slog.Leveler
	// ReplaceAttr, if set, rewrites or drops each attribute of a kept line,
	// as slog.HandlerOptions.ReplaceAttr does. The options of
	// Config.Logger's handler do not apply to the kept lines, so whatever
	// that handler redacts is redacted here too, or it is stored with the
	// job and shown by hopper jobs get and hopperui.
	ReplaceAttr func(groups []string, a slog.Attr) slog.Attr
}

// Defaults for JobLogsConfig fields.
const (
	DefaultJobLogBytes    = 32 << 10
	DefaultJobLogAttempts = 10
)

// Limits of JobLogsConfig fields, which keep a job's logs and a finalize
// statement's well within what Postgres takes.
const (
	MaxJobLogBytes    = 1 << 20
	MaxJobLogAttempts = 100
)

// Logger returns the logger of the job whose context ctx is. Its lines go
// to Config.Logger, with the job's ID, kind and attempt, and are kept with
// the job, in text form, as the logs of the attempt: Client.JobLogs returns
// them, and hopper jobs get and hopperui show them. They are written
// with the attempt's result, so they cost no writes of their own; a job
// keeps those of its latest attempts, bounded by Config.JobLogs, through
// retries and into history, and they leave with the job. With
// Config.JobLogs.Store, the lines go to the store as they are logged
// instead, and nothing of them is written to the database.
//
//	func (w *Importer) Work(ctx context.Context, job *hopper.Job[ImportArgs]) error {
//		log := hopper.Logger(ctx)
//		log.Info("downloading", "url", job.Args.URL)
//		...
//	}
//
// Without a store, the lines of an attempt whose result is not written are
// lost with it: one whose client crashes or loses its lease, or that is
// rescued for running past Config.RescueStuckAfter. The rescue's error says
// which. A job completed with CompleteTx keeps the lines logged before that
// call. A
// worker run with Workers.Work logs as the attempt that ran it, if any.
// Outside a job, Logger returns slog.Default().
func Logger(ctx context.Context) *slog.Logger {
	a, _ := ctx.Value(attemptKey{}).(*attempt)
	if a == nil {
		if l, ok := ctx.Value(inlineLoggerKey{}).(*slog.Logger); ok {
			return l
		}
		return slog.Default()
	}
	a.loggerOnce.Do(func() { a.newLogger(ctx) })
	return a.logger
}

// inlineLoggerKey carries the logger of the attempt that runs a worker
// with Workers.Work.
type inlineLoggerKey struct{}

// newLogger builds the attempt's logger: lines go to the client's logger and
// to the attempt's log, in the store or, unless the client keeps none, in
// the attempt's buffer.
func (a *attempt) newLogger(ctx context.Context) {
	base := a.baseLogger.With("job_id", a.completion.ID, "kind", a.row.Kind, "attempt", a.attempt)
	a.logger = base
	var out io.Writer
	switch {
	case a.logs.Store != nil:
		// The store's context does not carry the attempt: a store that logs
		// with Logger would otherwise re-enter the attempt's logger.
		sctx, cancel := context.WithCancel(context.WithValue(context.WithoutCancel(ctx), attemptKey{}, (*attempt)(nil)))
		sw := &storeWriter{store: a.logs.Store, ctx: sctx, cancel: cancel, row: a.row, logger: base}
		a.mu.Lock()
		ended := a.logEnded
		if !ended {
			a.stored = sw
		}
		a.mu.Unlock()
		if ended {
			cancel()
			return
		}
		out = sw
	case a.logs.MaxBytes > 0:
		kept := &logBuffer{max: a.logs.MaxBytes}
		a.mu.Lock()
		a.kept = kept
		a.mu.Unlock()
		out = kept
	default:
		return
	}
	text := slog.NewTextHandler(out, &slog.HandlerOptions{Level: a.logs.Level, ReplaceAttr: a.logs.ReplaceAttr})
	a.logger = slog.New(slog.NewMultiHandler(base.Handler(), text))
}

// closeLog ends the attempt's log in the store, if it has one, and keeps
// one from starting later: lines logged after it go to Config.Logger only.
// It waits for the store's Close for at most timeout.
func (a *attempt) closeLog(timeout time.Duration) {
	if a.logs.Store == nil {
		return
	}
	a.mu.Lock()
	a.logEnded = true
	sw := a.stored
	a.mu.Unlock()
	if sw != nil {
		sw.close(timeout)
	}
}

// storeWriter is an attempt's log in a LogStore. It opens the store's writer
// with the first line, so an attempt that writes none opens none, and stops
// using it once the attempt ends or the store fails. Store calls run outside
// the worker's panic guard once the worker has returned, so a panic in one
// is logged as a failure.
type storeWriter struct {
	store  LogStore
	ctx    context.Context
	cancel context.CancelFunc
	row    *driver.JobRow
	logger *slog.Logger

	mu   sync.Mutex
	w    io.WriteCloser
	done bool
}

func (s *storeWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return len(p), nil
	}
	if s.w == nil {
		w, err := s.open()
		if err != nil {
			s.logger.Warn("hopper: open the job's log in the store; the attempt's lines go to Config.Logger only", "error", err)
			s.done = true
			s.cancel()
			return len(p), nil
		}
		s.w = w
	}
	if err := storeCall(func() error { _, err := s.w.Write(p); return err }); err != nil {
		s.logger.Warn("hopper: write the job's log to the store; the attempt's later lines go to Config.Logger only", "error", err)
		s.closeLocked()
	}
	return len(p), nil
}

func (s *storeWriter) open() (w io.WriteCloser, err error) {
	err = storeCall(func() error {
		w, err = s.store.Writer(s.ctx, s.row)
		return err
	})
	if err == nil && w == nil {
		err = errors.New("the store returned no writer")
	}
	return w, err
}

// close closes the store's writer, waiting at most timeout for it, and for
// a write or an open under way: past it, the attempt's result goes ahead
// while the close finishes on its own.
func (s *storeWriter) close(timeout time.Duration) {
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.done {
			s.closeLocked()
		}
	}()
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-closed:
	case <-t.C:
		s.logger.Warn("hopper: the store has not closed the job's log; the attempt's result goes ahead without it", "waited", timeout)
	}
}

// closeLocked closes the store's writer, if it opened one, and cancels the
// writer's context.
func (s *storeWriter) closeLocked() {
	s.done = true
	defer s.cancel()
	if s.w == nil {
		return
	}
	if err := storeCall(s.w.Close); err != nil {
		s.logger.Warn("hopper: close the job's log in the store", "error", err)
	}
}

// storeCall calls a LogStore method, turning a panic into an error.
func storeCall(f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return f()
}

// keptLog returns the attempt's log, or nil if it logged nothing that is
// kept.
func (a *attempt) keptLog() *driver.AttemptLog {
	a.mu.Lock()
	kept := a.kept
	a.mu.Unlock()
	if kept == nil {
		return nil
	}
	return kept.log(a.attempt)
}

// logBuffer holds an attempt's lines up to max bytes. The text handler
// writes each line in one call.
type logBuffer struct {
	mu      sync.Mutex
	max     int
	buf     []byte
	dropped int
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.dropped > 0 || len(b.buf)+len(p) > b.max {
		b.dropped++
	} else {
		b.buf = append(b.buf, p...)
	}
	return len(p), nil
}

func (b *logBuffer) log(attempt int) *driver.AttemptLog {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.buf) == 0 && b.dropped == 0 {
		return nil
	}
	l := &driver.AttemptLog{Attempt: attempt, Log: string(b.buf)}
	if b.dropped > 0 {
		l.Log += fmt.Sprintf("hopper: %d more lines dropped: the attempt's log is limited to %d bytes\n", b.dropped, b.max)
	}
	return l
}

// withJobLogDefaults fills in the defaults of cfg.
func withJobLogDefaults(cfg JobLogsConfig) (JobLogsConfig, error) {
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = DefaultJobLogBytes
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = DefaultJobLogAttempts
	}
	if cfg.Store != nil {
		if cfg.Level == nil {
			cfg.Level = slog.LevelInfo
		}
		return cfg, nil
	}
	if cfg.MaxAttempts < 0 || cfg.MaxAttempts > MaxJobLogAttempts {
		return cfg, fmt.Errorf("hopper: Config.JobLogs.MaxAttempts must be between 1 and %d", MaxJobLogAttempts)
	}
	if cfg.MaxBytes > MaxJobLogBytes {
		return cfg, fmt.Errorf("hopper: Config.JobLogs.MaxBytes must be at most %d", MaxJobLogBytes)
	}
	if cfg.Level == nil {
		cfg.Level = slog.LevelInfo
	}
	return cfg, nil
}
