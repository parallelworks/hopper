package hopper

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/parallelworks/hopper/driver"
)

// AttemptLog is the log output of one attempt of a job. See Logger.
type AttemptLog = driver.AttemptLog

// JobLogsConfig bounds the log lines Logger keeps with each job.
type JobLogsConfig struct {
	// MaxBytes is how much of an attempt's log is kept. Once a line would
	// pass it, that line and the rest are dropped, and the log ends with a
	// line that counts them. Defaults to 32 KiB; negative keeps no lines.
	MaxBytes int
	// MaxAttempts is how many attempts' logs a job keeps, the latest.
	// Defaults to 10.
	MaxAttempts int
	// Level is the lowest level kept. Defaults to slog.LevelInfo. Lines
	// go to Config.Logger at whatever level its handler takes.
	Level slog.Leveler
}

// Defaults for JobLogsConfig fields.
const (
	DefaultJobLogBytes    = 32 << 10
	DefaultJobLogAttempts = 10
)

// Logger returns the logger of the job whose context ctx is. Its lines go
// to Config.Logger, with the job's ID, kind and attempt, and are kept with
// the job, in text form, as the logs of the attempt: JobGet returns them in
// JobRow.Logs, and hopper jobs get and hopperui show them. They are written
// with the attempt's result, so they cost no writes of their own; a job
// keeps those of its latest attempts, bounded by Config.JobLogs, through
// retries and into history, and they leave with the job.
//
//	func (w *Importer) Work(ctx context.Context, job *hopper.Job[ImportArgs]) error {
//		log := hopper.Logger(ctx)
//		log.Info("downloading", "url", job.Args.URL)
//		...
//	}
//
// The lines of an attempt whose client crashes are lost with it; its error
// says the client was lost. A job completed with CompleteTx keeps the lines
// logged before that call. Outside a job, Logger returns slog.Default().
func Logger(ctx context.Context) *slog.Logger {
	a, _ := ctx.Value(attemptKey{}).(*attempt)
	if a == nil {
		return slog.Default()
	}
	a.loggerOnce.Do(a.newLogger)
	return a.logger
}

// newLogger builds the attempt's logger: lines go to the client's logger and,
// unless the client keeps none, to the attempt's log.
func (a *attempt) newLogger() {
	base := a.baseLogger.With("job_id", a.completion.ID, "kind", a.kind, "attempt", a.attempt)
	if a.logs.MaxBytes <= 0 {
		a.logger = base
		return
	}
	kept := &logBuffer{max: a.logs.MaxBytes}
	a.mu.Lock()
	a.kept = kept
	a.mu.Unlock()
	text := slog.NewTextHandler(kept, &slog.HandlerOptions{Level: a.logs.Level})
	a.logger = slog.New(slog.NewMultiHandler(base.Handler(), text))
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
	if cfg.MaxAttempts < 0 {
		return cfg, errors.New("hopper: Config.JobLogs.MaxAttempts must be positive")
	}
	if cfg.Level == nil {
		cfg.Level = slog.LevelInfo
	}
	return cfg, nil
}
