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
	driver  driver.Driver[TTx]
	exec    driver.Executor
	caps    driver.Capabilities
	cfg     Config
	tuning  tuning
	logger  *slog.Logger
	workers *Workers

	// clientID is the lease row ID while running. It changes if the lease is
	// lost and re-registered.
	clientID atomic.Int64

	mu    sync.Mutex
	state clientState

	// Contexts for the running phases. All derive from Start's context with
	// its cancellation removed, so values (loggers, tracers) propagate.
	claimCtx, workCtx, finCtx, bgCtx             context.Context
	claimCancel, workCancel, finCancel, bgCancel context.CancelFunc

	producers map[string]*producer
	finalizer *finalizer
	loops     sync.WaitGroup // producer loops
	lease     sync.WaitGroup // the lease loop
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
		driver:  d,
		exec:    d.Executor(),
		caps:    d.Capabilities(),
		cfg:     resolved,
		tuning:  defaultTuning,
		logger:  resolved.Logger,
		workers: resolved.Workers,
	}
	if c.workers == nil {
		c.workers = NewWorkers()
	}
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

	// Each phase has its own context, cancelled in order by Stop.
	base := context.WithoutCancel(ctx)
	c.claimCtx, c.claimCancel = context.WithCancel(base) //nolint:gosec // cancelled in Stop
	c.workCtx, c.workCancel = context.WithCancel(base)   //nolint:gosec // cancelled in Stop
	c.finCtx, c.finCancel = context.WithCancel(base)     //nolint:gosec // cancelled in Stop
	c.bgCtx, c.bgCancel = context.WithCancel(base)       //nolint:gosec // cancelled in Stop

	c.finalizer = newFinalizer(c.exec, c.logger, c.tuning, c.wakeQueue)
	go c.finalizer.run(c.finCtx)

	c.producers = make(map[string]*producer, len(c.cfg.Queues))
	for name, qcfg := range c.cfg.Queues {
		p := &producer{
			queue:        name,
			cfg:          qcfg,
			logger:       c.logger.With("queue", name),
			pollInterval: c.cfg.PollInterval,
			cooldown:     c.tuning.claimCooldown,
			claim: func(ctx context.Context, limit int) ([]*driver.JobRow, error) {
				return c.exec.JobClaim(ctx, driver.JobClaimParams{Queue: name, ClientID: c.clientID.Load(), Limit: limit})
			},
			work:   func(row *driver.JobRow) driver.JobFinalize { return c.execute(c.workCtx, row, qcfg) },
			submit: c.finalizer.submit,
			wake:   make(chan struct{}, 1),
			freed:  make(chan struct{}, 1),
		}
		c.producers[name] = p
		c.loops.Go(func() { p.run(c.claimCtx) })
	}
	c.lease.Go(func() { c.leaseLoop(c.bgCtx) })

	c.state = clientStarted
	c.logger.InfoContext(ctx, "hopper: client started", "client_id", id, "queues", slices.Sorted(maps.Keys(c.cfg.Queues)))
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

// Stop shuts the client down in three steps: stop claiming; wait for running
// jobs to finish and flush their results; release the lease. If ctx expires
// during the wait, every job's context is cancelled, whatever finishes within
// a short grace period is flushed, and ctx's error is returned. Jobs still
// running then are rescued by another client once this one's lease expires.
func (c *Client[TTx]) Stop(ctx context.Context) error {
	c.mu.Lock()
	switch c.state {
	case clientIdle, clientStopped:
		c.state = clientStopped
		c.mu.Unlock()
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
		c.mu.Lock()
		c.state = clientStopped
		c.mu.Unlock()
		return nil
	}

	// 1. Stop claiming. Producer loops exit once any in-flight claim returns,
	// and the jobs from that claim are started, so nothing claimed is lost.
	c.claimCancel()
	c.loops.Wait()

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

	// 3. Release the lease.
	c.workCancel()
	c.bgCancel()
	c.lease.Wait()
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
// the lease expired, so the client re-registers under a new ID.
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
		ok, err := c.exec.ClientRenew(ctx, driver.ClientRenewParams{ClientID: id, TTL: c.tuning.leaseTTL})
		if err != nil {
			if ctx.Err() == nil {
				c.logger.WarnContext(ctx, "hopper: renew client lease", "client_id", id, "error", err)
			}
			continue
		}
		if ok {
			continue
		}
		c.logger.ErrorContext(ctx, "hopper: client lease expired; re-registering", "client_id", id)
		newID, err := c.register(ctx)
		if err != nil {
			c.logger.ErrorContext(ctx, "hopper: re-register client", "error", err)
			continue
		}
		c.clientID.Store(newID)
	}
}
