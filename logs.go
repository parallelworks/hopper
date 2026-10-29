package hopper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
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
// ID and attempt its job was claimed with. The store chooses retention unless
// JobLogs.Retention opts its versioned inventory into managed cleanup.
//
// Its methods are called from many goroutines at once.
type LogStore interface {
	// Writer returns where the lines of the attempt of job that is running
	// go, as Logger logs them: one line per Write, in slog's text format,
	// in the order they were logged. job is the row as it was claimed.
	//
	// Logging never waits for the store: lines are queued, and one
	// goroutine per attempt opens the writer with the first line and hands
	// the lines to it. Up to 256 KiB of lines wait for a store that falls
	// behind; lines past that are dropped, and a line counting them follows.
	// Writer's context carries the values of the context the worker first
	// called Logger with, such as a trace span its middleware started, but
	// not its cancellation, deadline or attempt (Logger called with it logs
	// as outside a job). It is canceled once the writer is closed, or once
	// hopper stops waiting for it.
	//
	// Hopper closes the writer once the worker returns and waits for the
	// queued lines and Close for up to 5 seconds before it hands in the
	// attempt's result, so a store that saves on Close has the log by the
	// time the job is finished; past that, the context is canceled and the
	// result goes ahead. A client that stops hard waits for at most half its
	// stop grace, so the result of the job it canceled is still written. A
	// job completed with CompleteTx is finished when the worker's
	// transaction commits, which may be before. Lines logged after the
	// close go to Config.Logger only.
	//
	// A snooze gives the attempt back, so the run after it calls Writer
	// again with the same job ID and attempt: keep both, in order. A failed
	// or panicking Writer, Write or Close, or a nil writer, is logged to
	// Config.Logger once and drops the attempt's remaining lines; the job's
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
	// them from it, after any the job's row holds. MaxBytes then neither
	// applies nor is checked, and MaxAttempts bounds what Client.JobLogs
	// returns. Retention optionally manages the store's execution count, age and writes.
	Store LogStore
	// Retention opts a versioned inventory store into managed cleanup and write limits.
	Retention *LogRetentionConfig
	// MaxBytes is how much of an attempt's log is kept. Once a line would
	// pass it, that line and the rest are dropped, and the log ends with a
	// line that counts them. Defaults to 32 KiB, and is at most 1 MiB,
	// and MaxBytes times MaxAttempts at most 4 MiB; negative keeps no
	// lines. For larger logs, use a Store. A finished attempt's lines
	// wait in memory with its result until it is written, so a client
	// holds at most 64 MiB of them; past that, finished jobs wait to hand
	// in their results, as they do when the finalizer's buffer is full.
	MaxBytes int
	// MaxAttempts is how many attempts' logs a job keeps, the latest. Each
	// snooze keeps its own until the next snooze of the same attempt
	// number, which replaces it, or removes it if that snooze logged
	// nothing. A store keeps what it is given: a snooze that logs nothing
	// writes nothing to it, so it removes only a snooze's log that the
	// job's row holds. Defaults to 10, and is at most 100.
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

// Limits of JobLogsConfig fields. A finalize that writes a log rewrites
// all of the job's, so MaxJobLogTotalBytes bounds MaxBytes times
// MaxAttempts, and with it what one write costs: about that much, as each
// entry may pass MaxBytes by its dropped-lines line and JSON escaping.
const (
	MaxJobLogBytes      = 1 << 20
	MaxJobLogAttempts   = 100
	MaxJobLogTotalBytes = 4 << 20
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
		a, _ = ctx.Value(inlineAttemptKey{}).(*attempt)
	}
	if a == nil {
		return slog.Default()
	}
	a.loggerOnce.Do(func() { a.newLogger(ctx) })
	return a.logger
}

// inlineAttemptKey carries the attempt that runs a worker with
// Workers.Work, whose logger the worker logs with.
type inlineAttemptKey struct{}

// newLogger builds the attempt's logger: lines go to the client's logger and
// to the attempt's log, in the store or, unless the client keeps none, in
// the attempt's buffer.
func (a *attempt) newLogger(ctx context.Context) {
	base := a.baseLogger.With("job_id", a.completion.ID, "kind", a.row.Kind, "attempt", a.attempt)
	a.logger = base
	var (
		out    io.Writer
		kept   *logBuffer
		stored *storeWriter
	)
	switch {
	case a.logs.Store != nil:
		// The store's context does not carry the attempt: a store that logs
		// with Logger would otherwise re-enter the attempt's logger.
		storeCtx := context.WithValue(context.WithoutCancel(ctx), attemptKey{}, (*attempt)(nil))
		storeCtx = context.WithValue(storeCtx, inlineAttemptKey{}, (*attempt)(nil))
		storeCtx = context.WithValue(storeCtx, inlineKey{}, nil)
		sw := newStoreWriter(a.logs.Store, storeCtx, a.row, base)
		stored = sw
		if a.logs.Retention != nil {
			sw.maxBytes = a.logs.Retention.MaxRunBytes
		}
		a.mu.Lock()
		ended := a.logEnded
		if !ended {
			a.stored = sw
		}
		a.mu.Unlock()
		if ended {
			sw.cancel()
			return
		}
		out = sw
	case a.logs.MaxBytes > 0:
		kept = &logBuffer{max: a.logs.MaxBytes}
		a.mu.Lock()
		a.kept = kept
		a.mu.Unlock()
		out = kept
	default:
		return
	}
	text := slog.NewTextHandler(out, &slog.HandlerOptions{Level: a.logs.Level, ReplaceAttr: a.logs.ReplaceAttr})
	a.logger = slog.New(attemptHandler{base: base.Handler(), text: text, kept: kept, stored: stored})
}

// attemptHandler sends an attempt's lines to Config.Logger's handler and
// formats them into its log. Once its buffer (kept, nil with a store) has
// dropped a line it only counts the rest, so a worker that logs past
// MaxBytes does not pay to format lines that are dropped. A record is not
// cloned: the text handler keeps nothing of it, and a handler that does
// clones it itself.
type attemptHandler struct {
	base, text slog.Handler
	kept       *logBuffer
	stored     *storeWriter
}

func (h attemptHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.base.Enabled(ctx, level) || !h.storeFull() && h.text.Enabled(ctx, level)
}

func (h attemptHandler) storeFull() bool {
	return h.stored != nil && h.stored.full.Load()
}

func (h attemptHandler) Handle(ctx context.Context, r slog.Record) error {
	var baseErr, textErr error
	if h.base.Enabled(ctx, r.Level) {
		baseErr = h.base.Handle(ctx, r)
	}
	if !h.storeFull() && h.text.Enabled(ctx, r.Level) && (h.kept == nil || !h.kept.dropIfFull()) {
		textErr = h.text.Handle(ctx, r)
	}
	return errors.Join(baseErr, textErr)
}

func (h attemptHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	text := h.text
	// A full log formats no more lines, so it need not format their
	// attributes either.
	if !h.storeFull() && (h.kept == nil || h.kept.dropped.Load() == 0) {
		text = text.WithAttrs(attrs)
	}
	return attemptHandler{base: h.base.WithAttrs(attrs), text: text, kept: h.kept, stored: h.stored}
}

func (h attemptHandler) WithGroup(name string) slog.Handler {
	return attemptHandler{base: h.base.WithGroup(name), text: h.text.WithGroup(name), kept: h.kept, stored: h.stored}
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

// logCloseTimeout is how long an attempt that ran with ctx waits for its log
// in the LogStore to close. A hard stop, which cancels ctx, waits for the
// canceled jobs only for the stop's grace, so their attempts wait for at
// most half of it and their results still reach the finalizer; so does a
// fenced attempt's, which is ctx's other cancellation.
func (c *Client[TTx]) logCloseTimeout(ctx context.Context) time.Duration {
	if ctx.Err() != nil {
		return min(c.tuning.logCloseTimeout, c.tuning.stopGrace/2)
	}
	return c.tuning.logCloseTimeout
}

// storePending bounds the bytes of an attempt's lines that wait for the
// store: a store that falls behind costs the attempt the lines past it, not
// its worker's time.
const storePending = 256 << 10

// storeWriter is an attempt's log in a LogStore. Lines are queued, and one
// goroutine, started with the first line, opens the store's writer and
// hands them to it in order, so logging never waits for the store and an
// attempt that logs nothing opens no writer. Lines that arrive while the
// queue is full are dropped and counted. After a failure, the store gets
// no more of the attempt's lines.
type storeWriter struct {
	store    LogStore
	ctx      context.Context
	cancel   context.CancelFunc
	row      *driver.JobRow
	logger   *slog.Logger
	maxBytes int64
	full     atomic.Bool

	mu      sync.Mutex
	queue   [][]byte
	queued  int
	dropped int
	ended   bool
	started bool
	wake    chan struct{} // signals the goroutine; buffered by one
	done    chan struct{} // closed when the goroutine returns
}

func newStoreWriter(store LogStore, ctx context.Context, row *driver.JobRow, logger *slog.Logger) *storeWriter {
	ctx, cancel := context.WithCancel(ctx)
	return &storeWriter{
		store: store, ctx: ctx, cancel: cancel, row: row, logger: logger,
		wake: make(chan struct{}, 1), done: make(chan struct{}),
	}
}

func (s *storeWriter) Write(p []byte) (int, error) {
	if s.full.Load() {
		return len(p), nil
	}
	s.mu.Lock()
	switch {
	case s.ended:
	case s.queued+len(p) > storePending:
		s.dropped++
	default:
		s.queue = append(s.queue, slices.Clone(p))
		s.queued += len(p)
	}
	start := !s.ended && !s.started
	s.started = s.started || start
	s.mu.Unlock()
	if start {
		go s.run()
	}
	s.signal()
	return len(p), nil
}

func (s *storeWriter) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// run hands the queued lines to the store until the attempt ends.
func (s *storeWriter) run() {
	defer close(s.done)
	defer s.cancel()
	var (
		w      io.WriteCloser
		failed bool
	)
	fail := func(what string, err error) {
		failed = true
		s.logger.Warn("hopper: "+what+"; the attempt's later lines go to Config.Logger only", "error", err)
	}
	for {
		s.mu.Lock()
		lines, dropped, ended := s.queue, s.dropped, s.ended
		s.queue, s.queued, s.dropped = nil, 0, 0
		s.mu.Unlock()
		if dropped > 0 {
			lines = append(lines, fmt.Appendf(nil, "hopper: %d lines dropped: the store fell behind\n", dropped))
		}
		for _, line := range lines {
			if failed || s.full.Load() {
				break
			}
			if w == nil {
				var err error
				if w, err = s.open(); err != nil {
					fail("open the job's log in the store", err)
					break
				}
			}
			if err := storeCall(func() error { _, err := w.Write(line); return err }); err != nil {
				fail("write the job's log to the store", err)
				_ = storeCall(w.Close) // the failure is logged once
			}
		}
		if ended {
			if w != nil && !failed {
				if err := storeCall(w.Close); err != nil {
					s.logger.Warn("hopper: close the job's log in the store", "error", err)
				}
			}
			return
		}
		select {
		case <-s.wake:
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *storeWriter) open() (w io.WriteCloser, err error) {
	err = storeCall(func() error {
		w, err = s.store.Writer(s.ctx, s.row)
		return err
	})
	if err == nil && w == nil {
		err = errors.New("the store returned no writer")
	}
	if err == nil && s.maxBytes > 0 {
		w = &limitedLogWriter{WriteCloser: w, max: s.maxBytes, onLimit: func() { s.full.Store(true) }}
	}
	return w, err
}

// close ends the attempt's log and waits at most timeout for the store to
// take its last lines and close: past it, the writer's context is canceled
// and the attempt's result goes ahead without it.
func (s *storeWriter) close(timeout time.Duration) {
	s.mu.Lock()
	s.ended = true
	started := s.started
	s.mu.Unlock()
	if !started {
		s.cancel()
		return
	}
	s.signal()
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-s.done:
	case <-t.C:
		s.logger.Warn("hopper: the store has not closed the job's log; its context is canceled, and the attempt's result goes ahead without it", "waited", timeout)
		s.cancel()
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
	mu  sync.Mutex
	max int
	buf []byte
	// dropped counts the lines dropped once one did not fit, after which
	// every line is.
	dropped atomic.Int64
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.dropped.Load() > 0 || len(b.buf)+len(p) > b.max {
		b.dropped.Add(1)
	} else {
		b.buf = append(b.buf, p...)
	}
	return len(p), nil
}

// dropIfFull counts a line as dropped, and reports so, once the buffer has
// dropped one: every later line is dropped too.
func (b *logBuffer) dropIfFull() bool {
	if b.dropped.Load() == 0 {
		return false
	}
	b.dropped.Add(1)
	return true
}

func (b *logBuffer) log(attempt int) *driver.AttemptLog {
	b.mu.Lock()
	defer b.mu.Unlock()
	dropped := b.dropped.Load()
	if len(b.buf) == 0 && dropped == 0 {
		return nil
	}
	l := &driver.AttemptLog{Attempt: attempt, Log: string(b.buf)}
	if dropped > 0 {
		l.Log += fmt.Sprintf("hopper: %d more lines dropped: the attempt's log is limited to %d bytes\n", dropped, b.max)
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
	if cfg.Level == nil {
		cfg.Level = slog.LevelInfo
	}
	if cfg.MaxAttempts < 0 || cfg.MaxAttempts > MaxJobLogAttempts {
		return cfg, fmt.Errorf("hopper: Config.JobLogs.MaxAttempts must be between 1 and %d", MaxJobLogAttempts)
	}
	if cfg.Store == nil && cfg.MaxBytes > MaxJobLogBytes {
		return cfg, fmt.Errorf("hopper: Config.JobLogs.MaxBytes must be at most %d", MaxJobLogBytes)
	}
	if cfg.Store == nil && cfg.MaxBytes > 0 && cfg.MaxBytes*cfg.MaxAttempts > MaxJobLogTotalBytes {
		return cfg, fmt.Errorf("hopper: Config.JobLogs.MaxBytes times MaxAttempts must be at most %d", MaxJobLogTotalBytes)
	}
	var err error
	if cfg.Retention, err = withLogRetentionDefaults(cfg.Retention, cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

const logStoreLimitMessage = "hopper: log byte limit reached; subsequent output omitted\n"

type limitedLogWriter struct {
	io.WriteCloser
	max, written int64
	truncated    bool
	onLimit      func()
}

func (w *limitedLogWriter) Write(p []byte) (int, error) {
	if w.truncated {
		return len(p), nil
	}
	if int64(len(p)) > w.max-w.written {
		w.truncated = true
		if w.onLimit != nil {
			w.onLimit()
		}
		_, err := w.WriteCloser.Write([]byte(logStoreLimitMessage))
		return len(p), err
	}
	n, err := w.WriteCloser.Write(p)
	w.written += int64(n)
	return n, err
}
