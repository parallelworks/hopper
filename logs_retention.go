package hopper

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// LogStorePruner optionally removes only the execution snapshot passed to Delete.
type LogStorePruner interface {
	// Delete atomically compares Version; a changed or missing run returns false.
	Delete(ctx context.Context, run LogRun) (bool, error)
}

// LogRetentionConfig opts an external store into leader-managed retention.
type LogRetentionConfig struct {
	// Zero inherits history retention; negative keeps logs until the job disappears.
	CompletedRetention time.Duration
	FailedRetention    time.Duration
	// MaxRuns defaults to JobLogs.MaxAttempts; negative keeps every execution.
	MaxRuns int
	// MaxRunBytes defaults to 1 MiB, plus a truncation marker; negative is unlimited.
	MaxRunBytes int64
	// GracePeriod protects recent writes, including logs of jobs already removed.
	GracePeriod time.Duration
	// Interval delays between complete inventory scans; default one minute.
	Interval time.Duration
	// Timeout bounds one batch independently of its scheduling interval; default 30s.
	Timeout   time.Duration
	BatchSize int
}

// LogRetentionResult reports one bounded inventory pass, measured at database time.
type LogRetentionResult struct {
	At           time.Time
	Scanned      int
	ScannedBytes int64
	Deleted      int
	DeletedBytes int64
	// More reports that this scan has more pages; a completed scan restarts next pass.
	More bool
}

// LogRetentionStats reports the latest pass and cumulative successful deletion counts.
type LogRetentionStats struct {
	Last         LogRetentionResult
	Passes       int64
	Failures     int64
	Deleted      int64
	DeletedBytes int64
	LastError    string
	// Inventory is the observed retained size at the last completed scan, not a quota.
	InventoryRuns  int64
	InventoryBytes int64
	InventoryAt    time.Time
}

type logRetentionState struct {
	mu                  sync.Mutex
	busy                atomic.Bool
	stats               atomic.Pointer[LogRetentionStats]
	after               string
	last                LogRun
	rank                int
	scanRuns, scanBytes int64
}

func withLogRetentionDefaults(cfg *LogRetentionConfig, logs JobLogsConfig) (*LogRetentionConfig, error) {
	if cfg == nil {
		return nil, nil
	}
	out := *cfg
	if _, ok := logs.Store.(LogStoreLister); !ok {
		return nil, errors.New("hopper: log retention requires a LogStoreLister")
	}
	if _, ok := logs.Store.(LogStorePruner); !ok {
		return nil, errors.New("hopper: log retention requires a LogStorePruner")
	}
	if out.MaxRuns == 0 {
		out.MaxRuns = logs.MaxAttempts
	}
	if out.MaxRunBytes == 0 {
		out.MaxRunBytes = 1 << 20
	}
	if out.GracePeriod == 0 {
		out.GracePeriod = time.Hour
	}
	if out.Interval == 0 {
		out.Interval = time.Minute
	}
	if out.Timeout == 0 {
		out.Timeout = 30 * time.Second
	}
	if out.BatchSize == 0 {
		out.BatchSize = DefaultLogListSize
	}
	if out.GracePeriod < 0 || out.Interval < 0 || out.Timeout < 0 || out.BatchSize < 1 || out.BatchSize > MaxLogListSize {
		return nil, errors.New("hopper: invalid log retention grace, interval, timeout or batch size")
	}
	return &out, nil
}

// LogRetentionStats returns a snapshot without waiting for storage maintenance.
func (c *Client[TTx]) LogRetentionStats() LogRetentionStats {
	if s := c.logRetention.stats.Load(); s != nil {
		return *s
	}
	return LogRetentionStats{}
}

// MaintainJobLogs reconciles one inventory page; callers need not start the client.
func (c *Client[TTx]) MaintainJobLogs(ctx context.Context) (result LogRetentionResult, err error) {
	policy := c.cfg.JobLogs.Retention
	if policy == nil {
		return result, ErrNotSupported
	}
	state := &c.logRetention
	state.mu.Lock()
	defer state.mu.Unlock()
	defer func() {
		stats := c.LogRetentionStats()
		stats.Last = result
		stats.Deleted += int64(result.Deleted)
		stats.DeletedBytes += result.DeletedBytes
		if err != nil {
			stats.Failures++
			stats.LastError = err.Error()
		} else {
			stats.Passes++
			stats.LastError = ""
		}
		state.stats.Store(&stats)
	}()
	if err = ctx.Err(); err != nil {
		return result, err
	}
	lister := c.cfg.JobLogs.Store.(LogStoreLister)
	pruner := c.cfg.JobLogs.Store.(LogStorePruner)
	page, err := listLogRuns(ctx, lister, LogListParams{After: state.after, Limit: policy.BatchSize})
	if err != nil {
		return result, err
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	result.More = page.Next != ""
	if result.At, err = c.exec.Now(ctx); err != nil {
		return result, err
	}
	ids := make([]JobID, 0, len(page.Runs))
	for i, run := range page.Runs {
		if run.UpdatedAt.IsZero() || run.Version == "" || i == 0 && !state.last.JobID.IsZero() && compareLogRuns(state.last, run) >= 0 {
			return result, errors.New("hopper: retention requires versioned, timestamped log runs in seek order")
		}
		if len(ids) == 0 || ids[len(ids)-1] != run.JobID {
			ids = append(ids, run.JobID)
		}
	}
	jobs := map[JobID]*JobRow{}
	if len(ids) != 0 {
		rows, err := c.exec.JobList(ctx, driver.JobListParams{IDs: ids, Limit: len(ids)})
		if err != nil {
			return result, err
		}
		for _, row := range rows {
			jobs[row.ID] = row
		}
	}
	last, rank := state.last, state.rank
	for _, run := range page.Runs {
		if err = ctx.Err(); err != nil {
			return result, err
		}
		if run.JobID != last.JobID {
			rank = 0
		}
		rank++
		last = run
		result.Scanned++
		result.ScannedBytes += run.Size
		if !c.expiredLogRun(policy, result.At, jobs[run.JobID], run, rank) {
			continue
		}
		var deleted bool
		if err = storeCall(func() error {
			var err error
			deleted, err = pruner.Delete(ctx, run)
			return err
		}); err != nil {
			return result, fmt.Errorf("hopper: delete log execution: %w", err)
		}
		if deleted {
			result.Deleted++
			result.DeletedBytes += run.Size
		}
	}
	state.after, state.last, state.rank = page.Next, last, rank
	state.scanRuns += int64(result.Scanned - result.Deleted)
	state.scanBytes += result.ScannedBytes - result.DeletedBytes
	if page.Next == "" {
		state.last, state.rank = LogRun{}, 0
		stats := c.LogRetentionStats()
		stats.InventoryRuns, stats.InventoryBytes, stats.InventoryAt = state.scanRuns, state.scanBytes, result.At
		state.stats.Store(&stats)
		state.scanRuns, state.scanBytes = 0, 0
	}
	return result, nil
}

func (c *Client[TTx]) expiredLogRun(p *LogRetentionConfig, now time.Time, job *JobRow, run LogRun, rank int) bool {
	if run.UpdatedAt.After(now.Add(-p.GracePeriod)) {
		return false
	}
	if job == nil {
		return true
	}
	if job.Fence == 0 || run.Fence > job.Fence || job.State == JobStateRunning && run.Fence == job.Fence {
		return false
	}
	if p.MaxRuns > 0 && rank > p.MaxRuns {
		return true
	}
	keep := p.CompletedRetention
	fallback := c.cfg.CompletedRetention
	switch job.State {
	case JobStateCompleted:
	case JobStateCanceled, JobStateDiscarded:
		keep, fallback = p.FailedRetention, c.cfg.FailedRetention
	default:
		return false
	}
	if keep == 0 {
		keep = fallback
	}
	return keep > 0 && !job.FinalizedAt.IsZero() && !job.FinalizedAt.Add(keep).After(now)
}

func (c *Client[TTx]) logRetentionLoop(ctx context.Context) {
	policy := c.cfg.JobLogs.Retention
	if policy == nil {
		return
	}
	timer := time.NewTimer(policy.Interval)
	defer timer.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if c.logRetention.busy.CompareAndSwap(false, true) {
			done := make(chan bool, 1)
			// A store that ignores cancellation can strand one call, never leader renewal or shutdown.
			go func() {
				passCtx, cancel := context.WithTimeout(ctx, policy.Timeout)
				defer cancel()
				result, err := c.MaintainJobLogs(passCtx)
				if err != nil && ctx.Err() == nil {
					c.logger.WarnContext(ctx, "hopper: maintain external logs", "error", err)
				} else if result.Deleted > 0 {
					c.logger.InfoContext(ctx, "hopper: maintained external logs", "deleted", result.Deleted, "bytes", result.DeletedBytes)
				}
				c.logRetention.busy.Store(false)
				done <- err == nil && result.More
			}()
			select {
			case <-ctx.Done():
				return
			case more := <-done:
				if more {
					continue
				}
			}
		}
		timer.Reset(policy.Interval)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}
