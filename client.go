package hopper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// Client is the per-process runtime. It inserts, claims, works and finalizes
// jobs, and holds a liveness lease while running. TTx is the engine's
// transaction type, fixed by the driver.
type Client[TTx any] struct {
	driver   driver.Driver[TTx]
	exec     driver.Executor
	caps     driver.Capabilities
	cfg      Config
	tuning   tuning
	logger   *slog.Logger
	workers  *Workers
	notifier *notifier

	// clientID is the lease row ID while running. It changes if the lease is
	// lost and re-registered.
	clientID atomic.Int64

	mu    sync.Mutex
	state clientState

	// Contexts for the running phases. All derive from Start's context with
	// its cancellation removed, so values (loggers, tracers) propagate.
	claimCtx, workCtx, finCtx, bgCtx             context.Context
	claimCancel, workCancel, finCancel, bgCancel context.CancelFunc

	// gen is the lease generation: jobs run under a context that is
	// cancelled when the lease is lost, so a fenced client stops its work.
	genMu     sync.Mutex
	genCtx    context.Context
	genCancel context.CancelFunc

	producers map[string]*producer
	finalizer *finalizer
	// Loops by the phase that stops them: producers and the leader stop
	// with claiming; the listener and the lease loop run until the end.
	producerWG sync.WaitGroup
	leaderWG   sync.WaitGroup
	listenWG   sync.WaitGroup
	leaseWG    sync.WaitGroup

	listening  atomic.Bool
	isLeader   atomic.Bool
	leaderPoke chan struct{}

	events *eventBus

	// running maps jobs running on this client to their cancel functions.
	runningMu sync.Mutex
	running   map[JobID]context.CancelCauseFunc

	// doneWaiters are Await callers by job.
	doneMu      sync.Mutex
	doneWaiters map[JobID][]chan struct{}
}

type clientState int

const (
	clientIdle clientState = iota
	clientStarted
	clientStopping
	clientStopped
)

// NewClient validates cfg and returns a client. Nothing runs until Start.
func NewClient[TTx any](d driver.Driver[TTx], cfg *Config) (*Client[TTx], error) {
	if d == nil {
		return nil, errors.New("hopper: driver must not be nil")
	}
	resolved, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	c := &Client[TTx]{
		driver:      d,
		exec:        d.Executor(),
		caps:        d.Capabilities(),
		cfg:         resolved,
		tuning:      defaultTuning,
		logger:      resolved.Logger,
		workers:     resolved.Workers,
		leaderPoke:  make(chan struct{}, 1),
		events:      newEventBus(),
		running:     map[JobID]context.CancelCauseFunc{},
		doneWaiters: map[JobID][]chan struct{}{},
	}
	if c.workers == nil {
		c.workers = NewWorkers()
	}
	c.notifier = newNotifier(c.exec, c.logger, c.tuning.notifyInterval)
	return c, nil
}

// Start registers the client's lease and begins claiming and working jobs.
// ctx bounds the startup itself; the client keeps running after it ends.
// Use Run for a lifecycle tied to a context.
func (c *Client[TTx]) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.state {
	case clientStarted, clientStopping:
		return ErrClientStarted
	case clientStopped:
		return ErrClientStopped
	case clientIdle:
	}
	if len(c.cfg.Queues) == 0 {
		// Insert-only clients hold no lease and run nothing.
		c.state = clientStarted
		return nil
	}

	id, err := c.register(ctx)
	if err != nil {
		return err
	}
	c.clientID.Store(id)
	if err := c.declare(ctx); err != nil {
		// Leave nothing behind: the lease row would otherwise wait for the
		// leader to prune it.
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if derr := c.exec.ClientDelete(releaseCtx, id); derr != nil {
			c.logger.WarnContext(ctx, "hopper: release client lease after failed start", "error", derr)
		}
		return err
	}

	// Each phase has its own context, cancelled in order by Stop.
	base := context.WithoutCancel(ctx)
	c.claimCtx, c.claimCancel = context.WithCancel(base) //nolint:gosec // cancelled in Stop
	c.workCtx, c.workCancel = context.WithCancel(base)   //nolint:gosec // cancelled in Stop
	c.finCtx, c.finCancel = context.WithCancel(base)     //nolint:gosec // cancelled in Stop
	c.bgCtx, c.bgCancel = context.WithCancel(base)       //nolint:gosec // cancelled in Stop
	c.newGeneration()

	c.finalizer = newFinalizer(c.exec, c.logger, c.tuning, c.wakeQueue, c.finalized)
	go c.finalizer.run(c.finCtx)

	// Producers start with the cluster's current pause and limit state, so
	// a paused or limited queue is never claimed freely in the window
	// before the first renewal.
	state, err := c.exec.QueueList(ctx)
	if err != nil {
		// Older schemas have no limit columns; the renewal path applies
		// the state once the schema is upgraded.
		c.logger.WarnContext(ctx, "hopper: read queue state", "error", err)
	}
	c.producers = make(map[string]*producer, len(c.cfg.Queues))
	for name, qcfg := range c.cfg.Queues {
		paused, limited := queueState(state, name)
		c.startProducer(name, qcfg, paused, limited)
	}
	if c.caps.Listen {
		c.listenWG.Go(func() { c.listenLoop(c.bgCtx) })
	}
	c.leaderWG.Go(func() { c.leaderLoop(c.claimCtx) })
	c.leaseWG.Go(func() { c.leaseLoop(c.bgCtx) })

	c.state = clientStarted
	c.logger.InfoContext(ctx, "hopper: client started", "client_id", id, "queues", slices.Sorted(maps.Keys(c.cfg.Queues)))
	return nil
}

// queueState returns a queue's pause and limit flags from a listing.
func queueState(queues []*driver.QueueRow, name string) (paused, limited bool) {
	for _, q := range queues {
		if q.Name == name {
			return !q.PausedAt.IsZero(), q.Limits.Limited()
		}
	}
	return false, false
}

// startProducer creates and starts the producer for a queue with its
// initial pause and limit state. The caller holds c.mu.
func (c *Client[TTx]) startProducer(name string, qcfg QueueConfig, paused, limited bool) {
	ctx, cancel := context.WithCancel(c.claimCtx)
	p := &producer{
		queue:        name,
		cfg:          qcfg,
		logger:       c.logger.With("queue", name),
		pollInterval: c.cfg.PollInterval,
		cooldown:     c.tuning.claimCooldown,
		claim: func(ctx context.Context, limit int, limited bool) (driver.JobClaimResult, error) {
			return c.exec.JobClaim(ctx, driver.JobClaimParams{Queue: name, ClientID: c.clientID.Load(), Limit: limit, Limited: limited})
		},
		work:   func(row *driver.JobRow) driver.JobFinalize { return c.execute(c.generation(), row, qcfg) },
		submit: c.finalizer.submit,
		wake:   make(chan struct{}, 1),
		freed:  make(chan struct{}, 1),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	p.paused.Store(paused)
	p.limited.Store(limited)
	c.producers[name] = p
	c.producerWG.Go(func() { p.run(ctx) })
}

// finalized is called by the finalizer for each applied result: it emits
// the job event and wakes local Await callers.
func (c *Client[TTx]) finalized(job *driver.JobRow, result driver.JobFinalize) {
	var (
		kind    EventKind
		errText string
	)
	if result.Error != nil {
		errText = result.Error.Error
	}
	switch result.State {
	case driver.JobStateCompleted:
		kind = EventJobCompleted
	case driver.JobStateRetryable:
		kind = EventJobFailed
	case driver.JobStateScheduled:
		kind = EventJobSnoozed
	case driver.JobStateCancelled:
		kind = EventJobCancelled
	case driver.JobStateDiscarded:
		kind = EventJobDiscarded
	case driver.JobStatePending, driver.JobStateAvailable, driver.JobStateRunning:
		return
	}
	c.emit(kind, job, errText)
	if result.State.Terminal() {
		c.signalDone(job.ID)
	}
}

// declare records what this client works: its queues, subscriptions and
// declared limits, which take effect cluster-wide.
func (c *Client[TTx]) declare(ctx context.Context) error {
	if err := c.exec.QueueEnsure(ctx, slices.Sorted(maps.Keys(c.cfg.Queues))); err != nil {
		c.logger.WarnContext(ctx, "hopper: record queues", "error", err)
	}
	// Subscriptions and limits are declared in code and take effect
	// cluster-wide once recorded, so a failure here is a startup error.
	if err := c.exec.SubscriptionUpsert(ctx, c.workers.subscriptionRows()); err != nil {
		return err
	}
	for name, q := range c.cfg.Queues {
		if l := q.limits(name); l.Limited() || l.Aging > 0 {
			if err := c.exec.QueueSetLimits(ctx, l); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Client[TTx]) register(ctx context.Context) (int64, error) {
	info, _ := json.Marshal(map[string]any{
		"pid":    os.Getpid(),
		"queues": slices.Sorted(maps.Keys(c.cfg.Queues)),
	})
	id, err := c.exec.ClientRegister(ctx, driver.ClientRegisterParams{
		Hostname: c.cfg.Hostname,
		TTL:      c.tuning.leaseTTL,
		Info:     info,
	})
	if err != nil {
		return 0, fmt.Errorf("hopper: register client: %w", err)
	}
	return id, nil
}

// newGeneration starts a lease generation under workCtx.
func (c *Client[TTx]) newGeneration() {
	c.genMu.Lock()
	defer c.genMu.Unlock()
	c.genCtx, c.genCancel = context.WithCancel(c.workCtx) //nolint:gosec // cancelled by fence or Stop
}

// generation returns the context jobs run under.
func (c *Client[TTx]) generation() context.Context {
	c.genMu.Lock()
	defer c.genMu.Unlock()
	return c.genCtx
}

// Stop shuts the client down in three steps: stop claiming and give up
// leadership; wait for running jobs to finish and flush their results;
// release the lease. If ctx expires during the wait, every job's context is
// cancelled, whatever finishes within a short grace period is flushed, and
// ctx's error is returned. Jobs still running then are rescued by another
// client once this one's lease expires.
func (c *Client[TTx]) Stop(ctx context.Context) error {
	c.mu.Lock()
	switch c.state {
	case clientIdle, clientStopped:
		c.state = clientStopped
		c.mu.Unlock()
		c.notifier.flushNow()
		return nil
	case clientStopping:
		c.mu.Unlock()
		return ErrClientStopped
	case clientStarted:
	}
	c.state = clientStopping
	c.mu.Unlock()

	if c.producers == nil {
		// Insert-only client.
		c.notifier.flushNow()
		c.mu.Lock()
		c.state = clientStopped
		c.mu.Unlock()
		return nil
	}

	// 1. Stop claiming and give up leadership. Producer loops exit once any
	// in-flight claim returns, and the jobs from that claim are started, so
	// nothing claimed is lost.
	c.claimCancel()
	c.producerWG.Wait()
	c.leaderWG.Wait()

	// 2. Wait for running jobs. The lease keeps being renewed meanwhile, so
	// a long drain cannot let it lapse.
	var stopErr error
	if !c.waitForJobs(ctx) {
		stopErr = ctx.Err()
		c.logger.WarnContext(ctx, "hopper: stop deadline reached; cancelling running jobs")
		c.workCancel()
		graceCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.tuning.stopGrace)
		c.waitForJobs(graceCtx)
		cancel()
	}
	c.finCancel()
	<-c.finalizer.done
	c.notifier.flushNow()

	// 3. Release the lease.
	c.workCancel()
	c.bgCancel()
	c.listenWG.Wait()
	c.leaseWG.Wait()
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := c.exec.ClientDelete(releaseCtx, c.clientID.Load()); err != nil {
		c.logger.WarnContext(ctx, "hopper: release client lease", "error", err)
	}

	c.mu.Lock()
	c.state = clientStopped
	c.mu.Unlock()
	c.logger.InfoContext(ctx, "hopper: client stopped", "client_id", c.clientID.Load())
	return stopErr
}

// waitForJobs blocks until every producer's jobs have returned or ctx is
// done, and reports which.
func (c *Client[TTx]) waitForJobs(ctx context.Context) bool {
	done := make(chan struct{})
	go func() {
		for _, p := range c.producers {
			p.jobs.Wait()
		}
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

// Run starts the client and blocks until ctx is cancelled, then stops it,
// waiting up to Config.StopTimeout for running jobs. It returns nil after a
// clean drain, so it fits errgroup.
func (c *Client[TTx]) Run(ctx context.Context) error {
	if err := c.Start(ctx); err != nil {
		if ctx.Err() != nil {
			// Cancelled during startup: a clean stop, nothing to drain.
			return nil
		}
		return err
	}
	<-ctx.Done()
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.StopTimeout)
	defer cancel()
	return c.Stop(stopCtx)
}

// wakeQueue nudges the local producer for queue, if this client works it.
func (c *Client[TTx]) wakeQueue(queue string) {
	c.mu.Lock()
	p := c.producers[queue]
	c.mu.Unlock()
	if p != nil {
		p.wakeUp()
	}
}

// leaseLoop renews the client's lease. A renewal that matches no row means
// the lease expired while this process was not renewing it (a long pause or
// a partition), so the leader may already have rescued its jobs. The client
// fences itself: it cancels every running job and re-registers under a new
// ID. Results still in flight are submitted anyway; the finalize statement
// rejects any whose job was rescued meanwhile and applies the rest, which is
// equivalent to a rescue.
func (c *Client[TTx]) leaseLoop(ctx context.Context) {
	ticker := time.NewTicker(c.tuning.leaseRenew)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		id := c.clientID.Load()
		res, err := c.exec.ClientRenew(ctx, driver.ClientRenewParams{ClientID: id, TTL: c.tuning.leaseTTL})
		if err != nil {
			if ctx.Err() == nil {
				c.logger.WarnContext(ctx, "hopper: renew client lease", "client_id", id, "error", err)
			}
			continue
		}
		if !res.Renewed {
			c.fence(ctx, id)
			continue
		}
		// Control state rides along with every renewal, in case a
		// notification was missed.
		for _, jobID := range res.CancelRequested {
			c.cancelLocal(jobID)
		}
		c.applyPaused(res.PausedQueues)
		c.applyLimited(res.LimitedQueues)
	}
}

// limitedQueues reads which queues have limits.
func (c *Client[TTx]) limitedQueues(ctx context.Context) ([]string, error) {
	queues, err := c.exec.QueueList(ctx)
	if err != nil {
		return nil, err
	}
	var limited []string
	for _, q := range queues {
		if q.Limits.Limited() {
			limited = append(limited, q.Name)
		}
	}
	return limited, nil
}

// applyLimited sets every local producer's claim path from the list of
// limited queues.
func (c *Client[TTx]) applyLimited(limited []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setLimited(limited)
}

// setLimited is applyLimited for a caller holding c.mu.
func (c *Client[TTx]) setLimited(limited []string) {
	set := make(map[string]struct{}, len(limited))
	for _, q := range limited {
		set[q] = struct{}{}
	}
	for name, p := range c.producers {
		_, isLimited := set[name]
		p.limited.Store(isLimited)
	}
}

// fence handles the loss of lease id: it cancels every job running under
// it and re-registers under a new ID. It is a no-op if the client has
// already moved on from id, so the lease loop and the rescuer can both call
// it.
func (c *Client[TTx]) fence(ctx context.Context, id int64) {
	c.genMu.Lock()
	defer c.genMu.Unlock()
	if c.clientID.Load() != id {
		return
	}
	c.logger.ErrorContext(ctx, "hopper: client lease expired; fencing running jobs and re-registering", "client_id", id)
	c.emit(EventLeaseLost, nil, "")
	c.genCancel()
	newID, err := c.register(ctx)
	if err != nil {
		// Keep the cancelled generation: nothing runs until the next
		// renewal attempt succeeds in registering.
		c.logger.ErrorContext(ctx, "hopper: re-register client", "error", err)
		return
	}
	c.clientID.Store(newID)
	c.genCtx, c.genCancel = context.WithCancel(c.workCtx) //nolint:gosec // cancelled by fence or Stop
	c.pokeLeader()
}
