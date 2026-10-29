package hopper

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"
)

// LogRun identifies one claimed execution, including one snooze's execution.
type LogRun struct {
	JobID     JobID
	Fence     int64
	Attempt   int
	Size      int64
	UpdatedAt time.Time
	// Version identifies this snapshot; Delete must compare it atomically.
	Version string
}

// LogListParams selects a bounded page, globally when JobID is zero.
type LogListParams struct {
	JobID JobID
	// Fence, when positive, selects one execution and requires JobID.
	Fence int64
	After string
	Limit int
}

// LogRunPage is ordered by job ID ascending, then claim fence descending.
type LogRunPage struct {
	Runs []LogRun
	// Next is a stable seek cursor, empty at the end; deletions do not shift it.
	Next string
}

// LogStoreLister optionally exposes a store's durable execution inventory.
type LogStoreLister interface {
	List(ctx context.Context, params LogListParams) (LogRunPage, error)
}

// LogReadParams reads a particular execution; Offset -1 tails its last Limit bytes.
type LogReadParams struct {
	JobID  JobID
	Fence  int64
	Offset int64
	Limit  int
}

// LogChunk is a byte slice; a replaced or shortened log can reset Offset to zero.
type LogChunk struct {
	Log        string
	Offset     int64
	NextOffset int64
	Size       int64
}

// LogStoreReader optionally reads an execution without loading its whole log.
type LogStoreReader interface {
	Read(ctx context.Context, params LogReadParams) (LogChunk, error)
}

// LogCursor scopes a byte offset to one execution.
type LogCursor struct {
	Fence  int64
	Offset int64
}

// LogReadOpts selects an execution, or the latest when Fence is zero.
type LogReadOpts struct {
	Fence  int64
	Cursor LogCursor
	Limit  int
}

// LogReadResult is a chunk and the execution-scoped cursor to read after it.
type LogReadResult struct {
	LogChunk
	Run    LogRun
	Cursor LogCursor
}

// Log read and inventory bounds.
const (
	DefaultLogReadBytes = 64 << 10
	MaxLogReadBytes     = 1 << 20
	DefaultLogListSize  = 100
	MaxLogListSize      = 1000
)

// JobLogRuns lists stored executions; stores without inventory return ErrNotSupported.
func (c *Client[TTx]) JobLogRuns(ctx context.Context, id JobID, after string, limit int) (LogRunPage, error) {
	if id.IsZero() {
		return LogRunPage{}, errors.New("hopper: a log listing requires a job ID")
	}
	s, ok := c.cfg.JobLogs.Store.(LogStoreLister)
	if !ok {
		return LogRunPage{}, ErrNotSupported
	}
	return listLogRuns(ctx, s, LogListParams{JobID: id, After: after, Limit: limit})
}

func listLogRuns(ctx context.Context, s LogStoreLister, params LogListParams) (LogRunPage, error) {
	if params.Limit == 0 {
		params.Limit = DefaultLogListSize
	}
	if params.Limit < 1 || params.Limit > MaxLogListSize {
		return LogRunPage{}, fmt.Errorf("hopper: log listing limit must be between 1 and %d", MaxLogListSize)
	}
	if params.Fence < 0 || params.Fence > 0 && params.JobID.IsZero() {
		return LogRunPage{}, errors.New("hopper: an execution filter requires a job ID and positive fence")
	}
	if err := ctx.Err(); err != nil {
		return LogRunPage{}, err
	}
	var page LogRunPage
	err := storeCall(func() error {
		var err error
		page, err = s.List(ctx, params)
		return err
	})
	if err != nil {
		return LogRunPage{}, fmt.Errorf("hopper: list log executions: %w", err)
	}
	if len(page.Runs) > params.Limit || page.Next != "" && (page.Next == params.After || len(page.Runs) == 0) {
		return LogRunPage{}, errors.New("hopper: store returned an invalid log page")
	}
	for i, run := range page.Runs {
		if run.JobID.IsZero() || run.Fence < 1 || run.Attempt < 1 || run.Size < 0 ||
			!params.JobID.IsZero() && run.JobID != params.JobID ||
			params.Fence > 0 && run.Fence != params.Fence ||
			i > 0 && compareLogRuns(page.Runs[i-1], run) >= 0 {
			return LogRunPage{}, errors.New("hopper: store returned an invalid log execution")
		}
	}
	return page, nil
}

func compareLogRuns(a, b LogRun) int {
	return cmp.Or(bytes.Compare(a.JobID[:], b.JobID[:]), cmp.Compare(b.Fence, a.Fence))
}

// JobLogRead reads bounded output, resetting to the tail when the latest execution changes.
func (c *Client[TTx]) JobLogRead(ctx context.Context, id JobID, opts *LogReadOpts) (LogReadResult, error) {
	lister, lists := c.cfg.JobLogs.Store.(LogStoreLister)
	reader, reads := c.cfg.JobLogs.Store.(LogStoreReader)
	if !lists || !reads {
		return LogReadResult{}, ErrNotSupported
	}
	p := LogReadOpts{Cursor: LogCursor{Offset: -1}}
	if opts != nil {
		p = *opts
	}
	if id.IsZero() || p.Fence < 0 || p.Cursor.Fence < 0 || p.Cursor.Offset < -1 {
		return LogReadResult{}, errors.New("hopper: invalid log read cursor")
	}
	if p.Limit == 0 {
		p.Limit = DefaultLogReadBytes
	}
	if p.Limit < 1 || p.Limit > MaxLogReadBytes {
		return LogReadResult{}, fmt.Errorf("hopper: log read limit must be between 1 and %d", MaxLogReadBytes)
	}
	page, err := listLogRuns(ctx, lister, LogListParams{JobID: id, Fence: p.Fence, Limit: 1})
	if err != nil {
		return LogReadResult{}, err
	}
	if len(page.Runs) == 0 {
		return LogReadResult{}, ErrNotFound
	}
	run := page.Runs[0]
	offset := p.Cursor.Offset
	if p.Cursor.Fence != 0 && p.Cursor.Fence != run.Fence {
		offset = -1
	}
	var chunk LogChunk
	err = storeCall(func() error {
		var err error
		chunk, err = reader.Read(ctx, LogReadParams{JobID: id, Fence: run.Fence, Offset: offset, Limit: p.Limit})
		return err
	})
	if err != nil {
		return LogReadResult{}, fmt.Errorf("hopper: read log execution: %w", err)
	}
	if chunk.Offset < 0 || chunk.NextOffset < chunk.Offset || chunk.Size < chunk.NextOffset ||
		int64(len(chunk.Log)) != chunk.NextOffset-chunk.Offset || len(chunk.Log) > p.Limit {
		return LogReadResult{}, errors.New("hopper: store returned an invalid log chunk")
	}
	run.Size = chunk.Size
	return LogReadResult{LogChunk: chunk, Run: run, Cursor: LogCursor{Fence: run.Fence, Offset: chunk.NextOffset}}, nil
}
