package hopper

import "context"

// Middleware wraps job execution and inserts, for logging, metrics, tracing
// and custom policy. Embed MiddlewareDefaults to implement only one side, or
// use WorkMiddleware and InsertMiddleware for a function.
//
// Middleware runs in Config.Middleware order: the first wraps the second,
// and so on down to the worker or the insert.
type Middleware interface {
	// Work wraps one attempt of a job. next runs the worker (and any inner
	// middleware); its error is the worker's return value.
	Work(ctx context.Context, job *JobRow, next func(ctx context.Context) error) error
	// Insert wraps an insert batch. next performs the insert; middleware may
	// pass it a modified copy of params, for example with trace context in
	// Metadata.
	Insert(ctx context.Context, params []InsertParams, next func(ctx context.Context, params []InsertParams) ([]*InsertResult, error)) ([]*InsertResult, error)
}

// MiddlewareDefaults implements Middleware as pass-through.
type MiddlewareDefaults struct{}

// Work implements Middleware.
func (MiddlewareDefaults) Work(ctx context.Context, _ *JobRow, next func(ctx context.Context) error) error {
	return next(ctx)
}

// Insert implements Middleware.
func (MiddlewareDefaults) Insert(ctx context.Context, params []InsertParams, next func(ctx context.Context, params []InsertParams) ([]*InsertResult, error)) ([]*InsertResult, error) {
	return next(ctx, params)
}

// WorkMiddleware makes a Middleware from a function that wraps execution.
func WorkMiddleware(fn func(ctx context.Context, job *JobRow, next func(ctx context.Context) error) error) Middleware {
	return workMiddleware{fn: fn}
}

type workMiddleware struct {
	MiddlewareDefaults
	fn func(ctx context.Context, job *JobRow, next func(ctx context.Context) error) error
}

func (m workMiddleware) Work(ctx context.Context, job *JobRow, next func(ctx context.Context) error) error {
	return m.fn(ctx, job, next)
}

// InsertMiddleware makes a Middleware from a function that wraps inserts.
func InsertMiddleware(fn func(ctx context.Context, params []InsertParams, next func(ctx context.Context, params []InsertParams) ([]*InsertResult, error)) ([]*InsertResult, error)) Middleware {
	return insertMiddleware{fn: fn}
}

type insertMiddleware struct {
	MiddlewareDefaults
	fn func(ctx context.Context, params []InsertParams, next func(ctx context.Context, params []InsertParams) ([]*InsertResult, error)) ([]*InsertResult, error)
}

func (m insertMiddleware) Insert(ctx context.Context, params []InsertParams, next func(ctx context.Context, params []InsertParams) ([]*InsertResult, error)) ([]*InsertResult, error) {
	return m.fn(ctx, params, next)
}
