package hopper

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// producer claims jobs for one queue and runs them on a bounded pool.
//
// It claims when at least a quarter of its slots are free, or when any slot
// is free and a short cooldown has passed since the last claim. Busy queues
// are therefore claimed in batches rather than one row at a time, while idle
// queues still start jobs immediately. It claims on a local wake-up (an
// insert by this client), when slots free up, and on an adaptive poll timer
// that is the safety net for work arriving without a wake-up.
type producer struct {
	queue        string
	cfg          QueueConfig
	logger       *slog.Logger
	pollInterval time.Duration
	cooldown     time.Duration

	claim  func(ctx context.Context, limit int, limited bool) (driver.JobClaimResult, error)
	work   func(row *driver.JobRow) driver.JobFinalize
	submit func(job *driver.JobRow, result driver.JobFinalize)

	wake  chan struct{} // an insert happened on this client
	freed chan struct{} // a slot freed up

	// cancel stops the claim loop; done is closed when it has stopped.
	cancel context.CancelFunc
	done   chan struct{}
	paused atomic.Bool
	// limited selects the claim path that applies the queue's limits.
	limited atomic.Bool

	mu      sync.Mutex
	running int
	jobs    sync.WaitGroup // running job goroutines
}

// setPaused stops or resumes claiming.
func (p *producer) setPaused(paused bool) {
	if p.paused.Swap(paused) != paused {
		p.logger.Info("hopper: queue " + map[bool]string{true: "paused", false: "resumed"}[paused])
		if !paused {
			p.wakeUp()
		}
	}
}

func (p *producer) wakeUp() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *producer) runningCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// run is the claim loop. It returns when ctx is done; jobs started by then
// keep running and are waited for through p.jobs.
func (p *producer) run(ctx context.Context) {
	defer close(p.done)
	threshold := max(1, p.cfg.MaxWorkers/4)
	var lastClaim time.Time
	errBackoff := p.pollInterval
	for {
		free := p.cfg.MaxWorkers - p.runningCount()
		if p.paused.Load() {
			free = 0
		}
		var cooldown <-chan time.Time
		if free > 0 {
			since := time.Since(lastClaim)
			if free >= threshold || since >= p.cooldown {
				n, wait, err := p.claimAndStart(ctx, free)
				lastClaim = time.Now()
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					p.logger.ErrorContext(ctx, "hopper: claim jobs", "error", err)
					select {
					case <-ctx.Done():
						return
					case <-time.After(errBackoff):
					}
					continue
				}
				if wait > 0 {
					// The rate limit allowed nothing yet; there is no point
					// in asking again before a token is due.
					select {
					case <-ctx.Done():
						return
					case <-time.After(min(wait, p.pollInterval)):
					}
					continue
				}
				if n == free {
					// A full batch: the queue probably has more. Go again as
					// soon as a slot frees, without waiting for the poll.
					continue
				}
			} else {
				cooldown = time.After(p.cooldown - since)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-p.wake:
		case <-p.freed:
		case <-cooldown:
		case <-time.After(p.pollInterval):
		}
	}
}

// claimTimeout bounds one claim statement.
const claimTimeout = 30 * time.Second

// claimAndStart claims up to limit jobs and starts each on its own
// goroutine. The statement runs on a context that ctx does not cancel: a
// claim interrupted mid-flight can leave rows marked running that this
// client never sees, which then wait for rescue, and cancelling a query
// also costs the pool a connection. Stopping waits for the claim instead,
// which takes milliseconds, and every claimed job is started.
func (p *producer) claimAndStart(ctx context.Context, limit int) (int, time.Duration, error) {
	claimCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimTimeout)
	defer cancel()
	res, err := p.claim(claimCtx, limit, p.limited.Load())
	for _, job := range res.Jobs {
		p.start(job)
	}
	return len(res.Jobs), res.Wait, err
}

func (p *producer) start(job *driver.JobRow) {
	p.mu.Lock()
	p.running++
	p.mu.Unlock()
	p.jobs.Add(1)
	go func() {
		defer p.jobs.Done()
		result := p.work(job)
		// Submit before freeing the slot, so that a backed-up finalizer
		// applies back-pressure to claiming instead of piling up results.
		p.submit(job, result)
		p.mu.Lock()
		p.running--
		p.mu.Unlock()
		select {
		case p.freed <- struct{}{}:
		default:
		}
	}()
}
