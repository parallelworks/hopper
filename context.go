package hopper

import "context"

type clientKey struct{}

// ClientFromContext returns the client that is running the job whose
// context ctx is, so a worker or subscriber can insert follow-up jobs,
// publish, or query without holding a client reference. That matters for
// code that is wired before the client exists: the workers are registered
// first and the client is built from them. The transaction type must match
// the client's; a mismatch, or a context from outside a job, returns false.
//
//	func (w *Worker) Work(ctx context.Context, job *hopper.Job[Args]) error {
//		client, ok := hopper.ClientFromContext[pgx.Tx](ctx)
//		if !ok {
//			return hopper.ErrNotInJob
//		}
//		_, err := client.Insert(ctx, Next{ID: job.Args.ID}, nil)
//		return err
//	}
func ClientFromContext[TTx any](ctx context.Context) (*Client[TTx], bool) {
	c, ok := ctx.Value(clientKey{}).(*Client[TTx])
	return c, ok
}

// ContextWithClient returns a context from which ClientFromContext returns
// client. Jobs get this from the client that runs them; it is for tests
// that call a worker directly, as with hoppertest.Work.
func ContextWithClient[TTx any](ctx context.Context, client *Client[TTx]) context.Context {
	return context.WithValue(ctx, clientKey{}, client)
}
