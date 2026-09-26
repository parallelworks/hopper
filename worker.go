package hopper

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Worker performs one kind of job. Embed WorkerDefaults to get default
// Timeout and NextRetry implementations.
type Worker[T JobArgs] interface {
	// Work runs the job. Returning nil completes it. Returning an error
	// schedules a retry with backoff, or discards the job when it is out of
	// attempts. Return Snooze(d) to run again later without using an attempt
	// and Cancel(err) to discard now.
	Work(ctx context.Context, job *Job[T]) error
	// Timeout returns how long the job may run before its context is
	// cancelled: 0 for the client's default, or a negative duration for no
	// timeout.
	Timeout(job *Job[T]) time.Duration
	// NextRetry returns when a failed job runs again, or the zero time for
	// the default backoff.
	NextRetry(job *Job[T]) time.Time
}

// WorkerDefaults provides default Timeout and NextRetry methods. Embed it in
// a worker to implement only Work.
type WorkerDefaults[T JobArgs] struct{}

// Timeout returns 0, which selects the client's default.
func (WorkerDefaults[T]) Timeout(*Job[T]) time.Duration { return 0 }

// NextRetry returns the zero time, which selects the default backoff.
func (WorkerDefaults[T]) NextRetry(*Job[T]) time.Time { return time.Time{} }

// Workers is a registry of workers by kind. Register at startup with
// AddWorker and AddWorkFunc, then pass it to NewClient.
type Workers struct {
	mu    sync.RWMutex
	kinds map[string]*workerInfo
}

// NewWorkers returns an empty registry.
func NewWorkers() *Workers {
	return &Workers{kinds: map[string]*workerInfo{}}
}

// workUnit is one claimed job bound to its worker, with the generic type
// erased so the client can run it.
type workUnit interface {
	Work(ctx context.Context) error
	Timeout() time.Duration
	NextRetry() time.Time
}

type workerInfo struct {
	kind    string
	newUnit func(row *JobRow, codec Codec) (workUnit, error)
}

// AddWorker registers worker for the kind of T. It panics if the kind is
// already registered, like http.Handle.
func AddWorker[T JobArgs](workers *Workers, worker Worker[T]) {
	if worker == nil {
		panic("hopper: AddWorker called with a nil worker")
	}
	var zero T
	kind := zero.Kind()
	if kind == "" {
		panic(fmt.Sprintf("hopper: %T has an empty kind", zero))
	}
	workers.add(&workerInfo{
		kind: kind,
		newUnit: func(row *JobRow, codec Codec) (workUnit, error) {
			var args T
			if err := codec.Unmarshal(row.Args, &args); err != nil {
				return nil, err
			}
			return &typedUnit[T]{worker: worker, job: &Job[T]{JobRow: row, Args: args}}, nil
		},
	})
}

// AddWorkFunc registers a function as the worker for the kind of T, with
// default timeout and retry behavior.
func AddWorkFunc[T JobArgs](workers *Workers, fn func(ctx context.Context, job *Job[T]) error) {
	if fn == nil {
		panic("hopper: AddWorkFunc called with a nil function")
	}
	AddWorker(workers, workFunc[T](fn))
}

func (w *Workers) add(info *workerInfo) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, dup := w.kinds[info.kind]; dup {
		panic(fmt.Sprintf("hopper: worker for kind %q registered twice", info.kind))
	}
	w.kinds[info.kind] = info
}

// Work runs the registered worker for job.Kind inline, decoding job.Args
// with codec and honoring the worker's Timeout. It exists for tests (see the
// hoppertest package): nothing is claimed or finalized, and middleware does
// not run. It returns an UnknownKindError for an unregistered kind.
func (w *Workers) Work(ctx context.Context, job *JobRow, codec Codec) error {
	info, ok := w.lookup(job.Kind)
	if !ok {
		return &UnknownKindError{Kind: job.Kind}
	}
	if codec == nil {
		codec = JSONCodec{}
	}
	unit, err := info.newUnit(job, codec)
	if err != nil {
		return fmt.Errorf("hopper: decode args for %q: %w", job.Kind, err)
	}
	if timeout := unit.Timeout(); timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(ctx, timeout, ErrJobTimeout)
		defer cancel()
	}
	return unit.Work(ctx)
}

func (w *Workers) lookup(kind string) (*workerInfo, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	info, ok := w.kinds[kind]
	return info, ok
}

type typedUnit[T JobArgs] struct {
	worker Worker[T]
	job    *Job[T]
}

func (u *typedUnit[T]) Work(ctx context.Context) error { return u.worker.Work(ctx, u.job) }
func (u *typedUnit[T]) Timeout() time.Duration         { return u.worker.Timeout(u.job) }
func (u *typedUnit[T]) NextRetry() time.Time           { return u.worker.NextRetry(u.job) }

type workFunc[T JobArgs] func(ctx context.Context, job *Job[T]) error

func (f workFunc[T]) Work(ctx context.Context, job *Job[T]) error { return f(ctx, job) }
func (workFunc[T]) Timeout(*Job[T]) time.Duration                 { return 0 }
func (workFunc[T]) NextRetry(*Job[T]) time.Time                   { return time.Time{} }
