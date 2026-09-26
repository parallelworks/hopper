package hopper

import (
	"context"
	"log/slog"
	"sync"
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

	claim  func(ctx context.Context, limit int) ([]*driver.JobRow, error)
	work   func(row *driver.JobRow) driver.JobFinalize
	submit func(queue string, result driver.JobFinalize)

	wake  chan struct{} // an insert happened on this client
	freed chan struct{} // a slot freed up

	mu      sync.Mutex
	running int
	jobs    sync.WaitGroup // running job goroutines
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
	threshold := max(1, p.cfg.MaxWorkers/4)
	var lastClaim time.Time
	errBackoff := p.pollInterval
	for {
		free := p.cfg.MaxWorkers - p.runningCount()
		var cooldown <-chan time.Time
		if free > 0 {
			since := time.Since(lastClaim)
			if free >= threshold || since >= p.cooldown {
				n, err := p.claimAndStart(ctx, free)
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

// claimAndStart claims up to limit jobs and starts each on its own
// goroutine. Jobs returned by a claim are always started, even if ctx was
// cancelled meanwhile: they are marked running in the database and would
// otherwise wait for rescue.
func (p *producer) claimAndStart(ctx context.Context, limit int) (int, error) {
	jobs, err := p.claim(ctx, limit)
	for _, job := range jobs {
		p.start(job)
	}
	return len(jobs), err
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
		p.submit(p.queue, result)
		p.mu.Lock()
		p.running--
		p.mu.Unlock()
		select {
		case p.freed <- struct{}{}:
		default:
		}
	}()
}
