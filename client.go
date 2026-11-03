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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/hoppermigrate"
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
	periodic *PeriodicJobs
	// recorded is what the client last wrote of its periodic jobs.
	recorded periodicRecord

	// clientID is the lease row ID while running. It changes if the lease is
	// lost and re-registered.
	clientID atomic.Int64
	// leaseFrom is when the current lease was last taken or renewed, as
	// register and the lease loop saw it: its expiry bounds a renewal's
	// wait.
	leaseFrom atomic.Pointer[time.Time]

	mu    sync.Mutex
	state clientState
	// stopped is closed once the client has stopped (Stopped).
	stopped chan struct{}

	// Contexts for the running phases. All derive from Start's context with
	// its cancellation removed, so values (loggers, tracers) propagate.
	claimCtx, workCtx, finCtx, bgCtx context.Context
	claimCancel, finCancel, bgCancel context.CancelFunc
	// workCancel cancels every job's context with ErrClientStopped as the
	// cause. It is set by Start, under mu.
	workCancel context.CancelCauseFunc

	// gen is the lease generation: jobs run under a context that is
	// canceled when the lease is lost, so a fenced client stops its work.
	genMu     sync.Mutex
	genCtx    context.Context
	genCancel context.CancelFunc

	producers map[string]*producer
	// controlSeq counts applications of the queues' pause and limit state
	// to the producers; it changes only under mu. A renewal or a re-read
	// that another application overtook does not apply its reading, which
	// may be older. appliedFrom, under mu, is the controlSeq that the
	// renewal or re-read last applied read before its statement.
	controlSeq  atomic.Uint64
	appliedFrom uint64
	finalizer   *finalizer
	// Loops by the phase that stops them: producers and the leader stop
	// with claiming; the listener and the lease loop run until the end.
	producerWG sync.WaitGroup
	leaderWG   sync.WaitGroup
	listenWG   sync.WaitGroup
	leaseWG    sync.WaitGroup

	listening  atomic.Bool
	isLeader   atomic.Bool
	leaderPoke chan struct{}
	// settleAfter and promoteAfter are where the leader's next batch and
	// workflow sweeps resume, ddlDeferrals counts its consecutive passes
	// with deferred partition work, and keptLogged is when it last logged
	// partitions kept past retention for their index.
	settleAfter, promoteAfter driver.JobID
	// resultsSweptAt is when the leader last deleted orphaned dependency
	// results.
	resultsSweptAt time.Time
	ddlDeferrals   int
	keptLogged     time.Time
	streamPoke     chan struct{}

	events       *eventBus
	logRetention logRetentionState

	// running maps jobs running on this client to their latest claim here.
	runningMu sync.Mutex
	running   map[JobID]*runningJob

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
		driver:       d,
		exec:         d.Executor(),
		caps:         d.Capabilities(),
		cfg:          resolved,
		tuning:       defaultTuning.withLease(resolved.LeaseTTL),
		logger:       resolved.Logger,
		workers:      resolved.Workers,
		leaderPoke:   make(chan struct{}, 1),
		streamPoke:   make(chan struct{}, 1),
		periodic:     newPeriodicJobs(resolved.Periodic, resolved.Logger, len(resolved.Queues) == 0),
		events:       newEventBus(),
		running:      map[JobID]*runningJob{},
		doneWaiters:  map[JobID][]chan struct{}{},
		stopped:      make(chan struct{}),
		logRetention: logRetentionState{gate: make(chan struct{}, 1)},
	}
	if c.workers == nil {
		c.workers = NewWorkers()
	}
	c.periodic.validate = func(p *PeriodicJob) error {
		_, err := c.periodicParams(p, time.Now())
		return err
	}
	// Workers may be added after NewClient, so StrictKinds is left to Add
	// and to each insert. c is returned only once StrictKinds is restored,
	// so an error or a panic here cannot leave a client without it.
	strict := c.cfg.StrictKinds
	c.cfg.StrictKinds = false
	c.periodic.define = c.periodicDefinition
	for _, r := range c.periodic.jobs {
		if _, err := c.periodicParams(&r.job, time.Now()); err != nil {
			return nil, fmt.Errorf("hopper: periodic job %q: %w", r.job.Name, err)
		}
		if err := c.periodic.setDefinition(r); err != nil {
			return nil, err
		}
	}
	c.cfg.StrictKinds = strict
	c.periodic.recordable = func(r *registeredPeriodic) bool {
		if !c.cfg.StrictKinds {
			return true
		}
		_, ok := c.workers.lookup(r.job.Args.Kind())
		return ok
	}
	c.notifier = newNotifier(c.exec, c.logger, c.tuning.notifyInterval)
	return c, nil
}

// Start registers the client's lease and begins claiming and working jobs.
// ctx bounds the startup itself; the client keeps running after it ends.
// Use Run for a lifecycle tied to a context. It returns ErrSchemaOutdated,
// naming the missing migrations, if the database's schema is older than
// this release needs.
func (c *Client[TTx]) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.state {
	case clientStarted:
		return ErrClientStarted
	case clientStopping, clientStopped:
		return ErrClientStopped
	case clientIdle:
	}
	if err := checkSchema(ctx, c.exec); err != nil {
		return err
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

	// Each phase has its own context, canceled in order by Stop.
	base := context.WithoutCancel(ctx)
	c.claimCtx, c.claimCancel = context.WithCancel(base)    //nolint:gosec // canceled in Stop
	c.workCtx, c.workCancel = context.WithCancelCause(base) //nolint:gosec // canceled in Stop
	c.finCtx, c.finCancel = context.WithCancel(base)        //nolint:gosec // canceled in Stop
	c.bgCtx, c.bgCancel = context.WithCancel(base)          //nolint:gosec // canceled in Stop
	c.newGeneration()

	c.finalizer = newFinalizer(c.exec, c.logger, c.tuning, c.cfg.JobLogs.MaxAttempts, c.wakeQueue, c.finalized)
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
	// Recorded before leading, so the term's first read has this client's
	// own jobs rather than only those of older clients.
	c.recordPeriodic(ctx)
	if c.cfg.leads() {
		c.leaderWG.Go(func() { c.leaderLoop(c.claimCtx) })
	}
	c.leaseWG.Go(func() { c.leaseLoop(c.bgCtx) })
	c.leaseWG.Go(func() { c.registrationLoop(c.bgCtx) })

	c.state = clientStarted
	c.logger.InfoContext(ctx, "hopper: client started", "client_id", id, "queues", slices.Sorted(maps.Keys(c.cfg.Queues)))
	return nil
}

// checkSchema fails unless the database has every migration this release's
// statements rely on. Without it, a client deployed before its migrations
// would fail every statement with a missing column or function.
func checkSchema(ctx context.Context, exec driver.Executor) error {
	have, err := exec.SchemaVersion(ctx)
	if err != nil {
		return err
	}
	need := hoppermigrate.Latest()
	if have >= need {
		return nil
	}
	missing := make([]string, 0, need-have)
	for _, m := range hoppermigrate.Migrations()[have:] {
		missing = append(missing, fmt.Sprintf("%03d_%s", m.Version, m.Name))
	}
	return fmt.Errorf("%w: it is at version %d and this release needs %d; apply %s with hoppermigrate.Up or `hopper migrate up` before starting clients",
		ErrSchemaOutdated, have, need, strings.Join(missing, ", "))
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
		work:   func(row *driver.JobRow) pending { return c.execute(c.jobContext(row), row, qcfg) },
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

// finalized is called by the finalizer for each applied result, and for
// each job a worker completed with CompleteTx: it emits the job event and
// wakes local Await callers.
func (c *Client[TTx]) finalized(job *driver.JobRow, result driver.JobFinalize) {
	var (
		kind    EventKind
		errText string
	)
	if result.Error != nil {
		errText = result.Error.Error
	}
	if result.Snooze && result.Error == nil {
		errText = result.SnoozeReason
	}
	switch result.State {
	case driver.JobStateCompleted:
		kind = EventJobCompleted
	case driver.JobStateRetryable:
		kind = EventJobFailed
	case driver.JobStateScheduled:
		kind = EventJobSnoozed
	case driver.JobStateCanceled:
		kind = EventJobCanceled
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

// declare records what this client works: its queues, subscriptions,
// stream consumers and declared limits, which take effect cluster-wide.
func (c *Client[TTx]) declare(ctx context.Context) error {
	if err := c.exec.QueueEnsure(ctx, slices.Sorted(maps.Keys(c.cfg.Queues))); err != nil {
		c.logger.WarnContext(ctx, "hopper: record queues", "error", err)
	}
	// Subscriptions and limits are declared in code and take effect
	// cluster-wide once recorded, so a failure here is a startup error.
	if err := c.exec.SubscriptionUpsert(ctx, c.workers.subscriptionRows()); err != nil {
		return err
	}
	if err := c.exec.StreamConsumerUpsert(ctx, c.workers.consumerRows()); err != nil {
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
	start := time.Now()
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
	c.leaseFrom.Store(&start)
	return id, nil
}

// newGeneration starts a lease generation under workCtx.
func (c *Client[TTx]) newGeneration() {
	c.genMu.Lock()
	defer c.genMu.Unlock()
	c.genCtx, c.genCancel = context.WithCancel(c.workCtx) //nolint:gosec // canceled by fence or Stop
}

// jobContext returns the context a claimed job runs under. A job with
// attempts left runs under the lease generation, so that a client which
// loses its lease stops it before another client runs it again. A job on its
// last attempt will not run again, so canceling it would only discard its
// work: it runs under the work context, which only a stop cancels.
func (c *Client[TTx]) jobContext(row *driver.JobRow) context.Context {
	if row.Attempt >= row.MaxAttempts {
		return c.workCtx
	}
	return c.generation()
}

// generation returns the context of the current lease generation.
func (c *Client[TTx]) generation() context.Context {
	c.genMu.Lock()
	defer c.genMu.Unlock()
	return c.genCtx
}

// Stop shuts the client down in three steps: stop claiming and give up
// leadership; wait for running jobs to finish and flush their results;
// release the lease. If ctx expires during the wait, every job's context is
// canceled, as StopAndCancel does, whatever finishes within a short grace
// period is flushed, and ctx's error is returned. Jobs still running then
// are rescued by another client once this one's lease expires, and that
// rescue counts the attempt, as any does. A job the deadline canceled whose
// worker returns within the grace gets its attempt back, as StopAndCancel
// says, so a job with MaxAttempts 1 runs again.
//
// Stop and StopAndCancel may be called more than once and from several
// goroutines: one call stops the client, and the others wait for it to
// stop, until their ctx expires, and return nil once it has, or ctx's
// error. Only the call that stops the client cancels the running jobs when
// its ctx expires; a StopAndCancel cancels them at once, whichever call
// came first. Stop before Start stops the client, which can then not be
// started.
func (c *Client[TTx]) Stop(ctx context.Context) error {
	return c.stop(ctx, false)
}

// StopAndCancel stops the client as Stop does, but cancels every running
// job's context at once, with ErrClientStopped as its cause, rather than
// waiting for the jobs to finish. It then waits, as Stop does, for the
// workers to return and their results to be flushed, until ctx expires.
//
// An attempt canceled this way does not count against MaxAttempts: a job
// whose worker then returns an error, whatever it is, is retried at once,
// its last attempt too, with the attempt given back. The error, with a
// panic's trace, is recorded in the job's Errors and logged, but
// Config.OnError is not consulted. The rerun has the same attempt number,
// so Errors holds an entry for each run of it that fails. A worker that
// returns nil, Snooze, Cancel or an output refusal (ErrOutputTooLarge)
// completes, snoozes or discards the job as usual. The job's timeout still
// bounds each run, and an attempt its timeout ended before the stop came
// is a timeout: it counts, and OnError is called, as for any timeout. The
// same holds for the jobs a Stop cancels when its ctx expires. A job from a
// claim in flight as the stop begins never runs, and gets its attempt back
// with no error. A worker still running when the stop's grace ends is
// rescued once the client is gone, and that rescue counts the attempt.
//
// MaxAttempts does not bound a job that every stop interrupts, and each
// interruption adds an entry to its Errors: only a timeout shorter than
// the time between stops ends such a job, by ending a run before a stop
// does; one with no timeout, or a longer one, runs again after every stop.
// Since a stop uses up no attempt whose worker returns within its grace, a
// job with MaxAttempts 1 that a stop interrupts runs again: delivery is
// at-least-once anyway, as a rescue reruns a crashed client's jobs, so a
// job that must not repeat should be idempotent. The close of an
// interrupted attempt's log in Config.JobLogs.Store waits at most half the
// stop's short grace, within which its result can still be written. A
// worker must not call Stop or StopAndCancel itself: the stop waits for the
// worker, which then loses its result.
func (c *Client[TTx]) StopAndCancel(ctx context.Context) error {
	return c.stop(ctx, true)
}

// Stopped returns a channel that is closed once the client has stopped,
// through Stop, StopAndCancel or Run, when they return. It can be called at
// any time, before Start too. A Run whose Start fails leaves the client
// unstarted, as Start does, and the channel open.
func (c *Client[TTx]) Stopped() <-chan struct{} {
	return c.stopped
}

func (c *Client[TTx]) stop(ctx context.Context, cancelJobs bool) error {
	c.mu.Lock()
	switch c.state {
	case clientStopping, clientStopped:
		c.mu.Unlock()
		return c.awaitStop(ctx, cancelJobs)
	case clientIdle, clientStarted:
	}
	// An idle client, or an insert-only one, has nothing running.
	running := c.state == clientStarted && c.producers != nil
	c.state = clientStopping
	c.mu.Unlock()

	var err error
	if running {
		err = c.shutdown(ctx, cancelJobs)
	} else {
		c.notifier.flushNow(ctx)
	}
	// Closed with the state set, so a call that finds the client stopped
	// never returns before Stopped is closed.
	c.mu.Lock()
	c.state = clientStopped
	close(c.stopped)
	c.mu.Unlock()
	return err
}

// awaitStop waits for another call's stop of the client, until ctx
// expires, canceling the running jobs first if cancelJobs is set; it
// returns at once for a client already stopped. Once the client has
// stopped, it flushes the notifier, for inserts made after the stopping
// call's flush.
func (c *Client[TTx]) awaitStop(ctx context.Context, cancelJobs bool) error {
	if cancelJobs {
		c.cancelJobs()
	}
	select {
	case <-c.stopped:
	case <-ctx.Done():
		select {
		case <-c.stopped:
		default:
			return ctx.Err()
		}
	}
	c.notifier.flushNow(ctx)
	return nil
}

// cancelJobs cancels the context of every job the client runs, with
// ErrClientStopped as its cause.
func (c *Client[TTx]) cancelJobs() {
	c.mu.Lock()
	cancel := c.workCancel
	c.mu.Unlock()
	if cancel != nil {
		cancel(ErrClientStopped)
	}
}

// shutdown stops a started client that works queues, canceling its jobs at
// once if cancelJobs is set.
func (c *Client[TTx]) shutdown(ctx context.Context, cancelJobs bool) error {
	// 1. Stop claiming and give up leadership. Producer loops exit once any
	// in-flight claim returns, and the jobs from that claim are started, so
	// nothing claimed is lost: canceled first, they return at once.
	c.claimCancel()
	if cancelJobs {
		c.cancelJobs()
	}
	c.producerWG.Wait()
	c.leaderWG.Wait()

	// 2. Wait for running jobs. The lease keeps being renewed meanwhile, so
	// a long drain cannot let it lapse.
	var stopErr error
	if !c.waitForJobs(ctx) {
		stopErr = ctx.Err()
		if cancelJobs {
			c.logger.WarnContext(ctx, "hopper: stop deadline reached with canceled jobs still running")
		} else {
			c.logger.WarnContext(ctx, "hopper: stop deadline reached; canceling running jobs")
		}
		c.cancelJobs()
		graceCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.tuning.stopGrace)
		c.waitForJobs(graceCtx)
		cancel()
	}
	c.finCancel()
	<-c.finalizer.done
	c.notifier.flushNow(ctx)

	// 3. Release the lease.
	c.cancelJobs()
	c.bgCancel()
	c.listenWG.Wait()
	c.leaseWG.Wait()
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := c.exec.ClientDelete(releaseCtx, c.clientID.Load()); err != nil {
		c.logger.WarnContext(ctx, "hopper: release client lease", "error", err)
	}
	c.logger.InfoContext(ctx, "hopper: client stopped", "client_id", c.clientID.Load())
	return stopErr
}

// waitForJobs blocks until every producer's jobs have returned or ctx is
// done, and reports which. It waits for the producers as they are when it
// is called: Queues().Remove can take one out meanwhile, and waits for its
// jobs itself.
func (c *Client[TTx]) waitForJobs(ctx context.Context) bool {
	c.mu.Lock()
	producers := slices.Collect(maps.Values(c.producers))
	c.mu.Unlock()
	done := make(chan struct{})
	go func() {
		for _, p := range producers {
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

// Run starts the client and blocks until ctx is canceled, then stops it,
// waiting up to Config.StopTimeout for running jobs. It returns nil after a
// clean drain, so it fits errgroup. It also returns nil, before ctx is
// canceled, once a Stop or StopAndCancel from elsewhere has stopped the
// client; that call reports how the stop went.
func (c *Client[TTx]) Run(ctx context.Context) error {
	if err := c.Start(ctx); err != nil {
		if ctx.Err() != nil {
			// Canceled during startup: a clean stop, nothing to drain.
			return nil
		}
		return err
	}
	select {
	case <-ctx.Done():
	case <-c.stopped:
		return nil
	}
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
//
// A renewal waits until the lease it renews would expire, and at least one
// renewal interval: on an engine that runs one writer at a time, a
// transaction that holds the database holds every renewal up, and a renewal
// that gives up is reported, with what the engine says of who holds the
// lock, and tried again, rather than left waiting past the lease without a
// word.
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
		seq := c.controlSeq.Load()
		start := time.Now()
		rctx, cancel := context.WithTimeout(ctx, c.renewalWait())
		res, err := c.exec.ClientRenew(rctx, driver.ClientRenewParams{ClientID: id, TTL: c.tuning.leaseTTL})
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				c.logger.WarnContext(ctx, "hopper: renew client lease", "client_id", id, "error", err)
			}
			continue
		}
		if !res.Renewed {
			c.fence(ctx, id) // which registers again, and so records the new lease
			continue
		}
		c.leaseFrom.Store(&start)
		// Control state rides along with every renewal, in case a
		// notification was missed.
		for _, jobID := range res.CancelRequested {
			c.cancelLocal(jobID)
		}
		c.applyRenewed(seq, res.PausedQueues, res.LimitedQueues)
	}
}

// renewalWait is how long a renewal may wait: until the lease would expire,
// and at least one renewal interval.
func (c *Client[TTx]) renewalWait() time.Duration {
	wait := c.tuning.leaseRenew
	if from := c.leaseFrom.Load(); from != nil {
		wait = max(wait, time.Until(from.Add(c.tuning.leaseTTL)))
	}
	return wait
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
		// Keep the canceled generation: nothing runs until the next
		// renewal attempt succeeds in registering.
		c.logger.ErrorContext(ctx, "hopper: re-register client", "error", err)
		return
	}
	c.clientID.Store(newID)
	c.genCtx, c.genCancel = context.WithCancel(c.workCtx) //nolint:gosec // canceled by fence or Stop
	// Recorded before the leader is woken, so a term this client wins
	// first reads its own jobs under the new lease.
	c.recordPeriodic(ctx)
	c.pokeLeader()
}
