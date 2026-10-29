package hopper

import (
	"context"
	"errors"
	"iter"
	"time"
)

// LogStoreWatcher optionally signals visible changes to a job's stored executions.
type LogStoreWatcher interface {
	// Watch subscribes before returning and releases its resources when ctx ends.
	// Signals are coalesced hints for appends, new runs or deletion, never log data.
	Watch(ctx context.Context, id JobID) (<-chan struct{}, error)
}

// LogFollowOpts starts following at LogReadOpts and polls when no hint arrives.
type LogFollowOpts struct {
	LogReadOpts
	// PollInterval defaults to one second and remains active with a subscription.
	PollInterval time.Duration
}

// LogUpdate replaces the view on Reset, appends otherwise, and clears it on Missing.
type LogUpdate struct {
	LogReadResult
	Reset   bool
	Missing bool
}

// JobLogFollow streams bounded reads until cancellation or the consumer stops.
func (c *Client[TTx]) JobLogFollow(ctx context.Context, id JobID, opts *LogFollowOpts) iter.Seq2[LogUpdate, error] {
	var config LogFollowOpts
	if opts != nil {
		config = *opts
	}
	return func(yield func(LogUpdate, error) bool) {
		lister, lists := c.cfg.JobLogs.Store.(LogStoreLister)
		reader, reads := c.cfg.JobLogs.Store.(LogStoreReader)
		if !lists || !reads {
			yield(LogUpdate{}, ErrNotSupported)
			return
		}
		var readOpts *LogReadOpts
		if opts != nil {
			readOpts = &config.LogReadOpts
		}
		p, err := withLogReadOpts(id, readOpts)
		if err != nil || config.PollInterval < 0 {
			if err == nil {
				err = errors.New("hopper: log follow poll interval must be positive")
			}
			yield(LogUpdate{}, err)
			return
		}
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		if ctx.Err() != nil {
			return
		}
		wake, stopWatching := c.watchJobLogs(ctx, id)
		defer stopWatching()
		interval := config.PollInterval
		if interval == 0 {
			interval = time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		var previous LogRun
		seen, missing := false, false
		for {
			chunk, err := readJobLog(ctx, lister, reader, id, p)
			if ctx.Err() != nil {
				return
			}
			switch {
			case errors.Is(err, ErrNotFound):
				if !seen || !missing {
					if previous.JobID.IsZero() {
						previous = LogRun{JobID: id, Fence: p.Fence}
					}
					if !yield(LogUpdate{LogReadResult: LogReadResult{Run: previous}, Missing: true, Reset: true}, nil) {
						return
					}
				}
				seen, missing = true, true
			case err != nil:
				yield(LogUpdate{}, err)
				return
			default:
				reset := missing || chunk.Run.Fence != p.Cursor.Fence || chunk.Offset != p.Cursor.Offset
				previous, seen, missing = chunk.Run, true, false
				p.Cursor = chunk.Cursor
				if reset || chunk.Log != "" {
					if !yield(LogUpdate{LogReadResult: chunk, Reset: reset}, nil) {
						return
					}
				}
				if chunk.Log != "" && chunk.NextOffset < chunk.Size {
					continue
				}
			}
			select {
			case <-ctx.Done():
				return
			case _, open := <-wake:
				if !open {
					wake = nil
					stopWatching()
				}
			case <-ticker.C:
			}
		}
	}
}

func (c *Client[TTx]) watchJobLogs(ctx context.Context, id JobID) (<-chan struct{}, context.CancelFunc) {
	watcher, ok := c.cfg.JobLogs.Store.(LogStoreWatcher)
	if !ok {
		return nil, func() {}
	}
	watchCtx, cancel := context.WithCancel(ctx)
	var wake <-chan struct{}
	if err := storeCall(func() error {
		var err error
		wake, err = watcher.Watch(watchCtx, id)
		return err
	}); err != nil {
		cancel()
		if ctx.Err() == nil {
			c.logger.WarnContext(ctx, "hopper: watch external logs; falling back to polling", "error", err)
		}
		return nil, cancel
	}
	if wake == nil {
		cancel()
	}
	return wake, cancel
}
