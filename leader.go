package hopper

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// leaderLoop competes for the leader lease and, while holding it, runs the
// singleton duties: rescuing jobs from lost clients, pruning expired client
// rows and maintaining history retention. Duties are idempotent, because two
// leaders can briefly overlap during a partition.
func (c *Client[TTx]) leaderLoop(ctx context.Context) {
	ticker := time.NewTicker(c.tuning.leaderInterval)
	defer ticker.Stop()

	// term is the current leadership: the periodic loop runs under it and
	// stops when it ends.
	var term *leaderTerm
	defer func() {
		term.end()
		c.resign(ctx)
	}()

	var lastMaintenance time.Time
	for {
		ok, err := c.exec.LeaderAttempt(ctx, driver.LeaderParams{ClientID: c.clientID.Load(), TTL: c.tuning.leaderTTL})
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return
			}
			c.logger.WarnContext(ctx, "hopper: leader attempt", "error", err)
		case ok:
			if !c.isLeader.Swap(true) {
				c.logger.InfoContext(ctx, "hopper: elected leader", "client_id", c.clientID.Load())
				c.emit(EventLeaderElected, nil, "")
				lastMaintenance = time.Time{}
				term = c.startTerm(ctx)
			}
			c.rescue(ctx)
			c.expire(ctx)
			c.age(ctx)
			c.pruneClients(ctx)
			if time.Since(lastMaintenance) >= c.tuning.maintenanceInterval {
				c.maintainHistory(ctx)
				lastMaintenance = time.Now()
			}
		default:
			if c.isLeader.Swap(false) {
				c.logger.WarnContext(ctx, "hopper: lost leadership", "client_id", c.clientID.Load())
				c.emit(EventLeaderLost, nil, "")
				term.end()
				term = nil
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-c.leaderPoke:
		}
	}
}

// leaderTerm is one stretch of leadership. The periodic and stream loops
// run for its duration.
type leaderTerm struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (c *Client[TTx]) startTerm(ctx context.Context) *leaderTerm {
	termCtx, cancel := context.WithCancel(ctx)
	t := &leaderTerm{cancel: cancel, done: make(chan struct{})}
	var wg sync.WaitGroup
	wg.Go(func() {
		if len(c.cfg.Periodic) > 0 {
			c.periodicLoop(termCtx)
		}
	})
	wg.Go(func() { c.streamLoop(termCtx) })
	go func() {
		defer close(t.done)
		wg.Wait()
	}()
	return t
}

// end stops the term's loops and waits for them. It is safe on a nil term.
func (t *leaderTerm) end() {
	if t == nil {
		return
	}
	t.cancel()
	<-t.done
}

// pokeLeader asks the leader loop to attempt an election now, for example
// because the leader resigned.
func (c *Client[TTx]) pokeLeader() {
	select {
	case c.leaderPoke <- struct{}{}:
	default:
	}
}

// resign gives up leadership on stop so a replacement takes over at once.
func (c *Client[TTx]) resign(ctx context.Context) {
	if !c.isLeader.Swap(false) {
		return
	}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := c.exec.LeaderResign(rctx, c.clientID.Load()); err != nil {
		c.logger.WarnContext(ctx, "hopper: resign leadership", "error", err)
		return
	}
	c.logger.InfoContext(ctx, "hopper: resigned leadership", "client_id", c.clientID.Load())
}

// rescue moves running jobs whose client has lost its lease back to
// retryable, or to discarded when they are out of attempts. The transition
// goes through the same fenced finalize statement as a worker's result, so
// a client that finalizes its own result first wins and the rescue of that
// job is dropped.
func (c *Client[TTx]) rescue(ctx context.Context) {
	for {
		jobs, err := c.exec.JobRescueCandidates(ctx, driver.JobRescueParams{
			StuckAfter: c.cfg.RescueStuckAfter,
			Limit:      c.tuning.rescueBatch,
		})
		if err != nil {
			if ctx.Err() == nil {
				c.logger.ErrorContext(ctx, "hopper: find jobs to rescue", "error", err)
			}
			return
		}
		if len(jobs) == 0 {
			return
		}
		// Finding our own jobs among the candidates, other than as stuck,
		// means our lease has lapsed and the lease loop has not noticed
		// yet. Fence first, so the jobs stop before they are handed back.
		if me := c.clientID.Load(); slices.ContainsFunc(jobs, func(j *driver.JobRow) bool { return j.AttemptedBy == me && !c.stuck(j) }) {
			c.fence(ctx, me)
		}

		fin := make([]driver.JobFinalize, len(jobs))
		queueOf := make(map[JobID]string, len(jobs))
		for i, j := range jobs {
			reason := "hopper: client lost"
			if c.stuck(j) {
				reason = "hopper: job ran longer than RescueStuckAfter"
			}
			f := driver.JobFinalize{
				ID:          j.ID,
				AttemptedBy: j.AttemptedBy,
				State:       driver.JobStateRetryable,
				Error:       &driver.AttemptError{Attempt: j.Attempt, Error: reason},
				Archive:     true,
			}
			if j.Attempt >= j.MaxAttempts {
				f.State = driver.JobStateDiscarded
			}
			fin[i] = f
			queueOf[j.ID] = j.Queue
		}
		applied, err := c.exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: fin})
		if err != nil {
			if ctx.Err() == nil {
				c.logger.ErrorContext(ctx, "hopper: rescue jobs", "error", err)
			}
			return
		}

		var retried, discarded int
		queues := map[string]struct{}{}
		byID := make(map[JobID]driver.JobFinalize, len(fin))
		for _, f := range fin {
			byID[f.ID] = f
		}
		for _, id := range applied {
			if byID[id].State == driver.JobStateDiscarded {
				discarded++
				continue
			}
			retried++
			queues[queueOf[id]] = struct{}{}
		}
		c.logger.WarnContext(ctx, "hopper: rescued jobs from lost clients", "retried", retried, "discarded", discarded, "dropped", len(jobs)-len(applied))
		for _, j := range jobs {
			if f, was := byID[j.ID]; was && slices.Contains(applied, j.ID) {
				c.emit(EventJobRescued, j, f.Error.Error)
			}
		}
		for q := range queues {
			c.wakeQueue(q)
			c.notifier.mark(q)
		}
		if len(jobs) < c.tuning.rescueBatch {
			return
		}
	}
}

// age promotes long-waiting jobs on queues with PriorityAging.
func (c *Client[TTx]) age(ctx context.Context) {
	queues, err := c.exec.QueueList(ctx)
	if err != nil {
		if ctx.Err() == nil {
			c.logger.WarnContext(ctx, "hopper: list queues for aging", "error", err)
		}
		return
	}
	for _, q := range queues {
		if q.Limits.Aging <= 0 {
			continue
		}
		n, err := c.exec.JobAge(ctx, q.Name, q.Limits.Aging)
		if err != nil {
			if ctx.Err() == nil {
				c.logger.WarnContext(ctx, "hopper: age jobs", "queue", q.Name, "error", err)
			}
			continue
		}
		if n > 0 {
			c.logger.DebugContext(ctx, "hopper: promoted waiting jobs", "queue", q.Name, "count", n)
			c.wakeQueue(q.Name)
		}
	}
}

// expire dead-letters waiting jobs whose TTL has passed.
func (c *Client[TTx]) expire(ctx context.Context) {
	for {
		jobs, err := c.exec.JobDiscardExpired(ctx, c.tuning.rescueBatch)
		if err != nil {
			if ctx.Err() == nil {
				c.logger.ErrorContext(ctx, "hopper: discard expired jobs", "error", err)
			}
			return
		}
		for _, j := range jobs {
			c.emit(EventJobDiscarded, j, "hopper: expired")
			c.signalDone(j.ID)
		}
		if len(jobs) > 0 {
			c.logger.InfoContext(ctx, "hopper: discarded expired jobs", "count", len(jobs))
		}
		if len(jobs) < c.tuning.rescueBatch {
			return
		}
	}
}

// stuck reports whether a rescue candidate qualifies by RescueStuckAfter,
// as opposed to by a lost lease.
func (c *Client[TTx]) stuck(j *driver.JobRow) bool {
	return c.cfg.RescueStuckAfter > 0 && time.Since(j.AttemptedAt) >= c.cfg.RescueStuckAfter
}

func (c *Client[TTx]) pruneClients(ctx context.Context) {
	n, err := c.exec.ClientPruneExpired(ctx)
	if err != nil {
		if ctx.Err() == nil {
			c.logger.WarnContext(ctx, "hopper: prune expired clients", "error", err)
		}
		return
	}
	if n > 0 {
		c.logger.InfoContext(ctx, "hopper: pruned expired clients", "count", n)
	}
}

func (c *Client[TTx]) maintainHistory(ctx context.Context) {
	res, err := c.exec.HistoryMaintain(ctx, driver.HistoryMaintainParams{
		CompletedRetention: c.cfg.CompletedRetention,
		FailedRetention:    c.cfg.FailedRetention,
		StreamRetention:    c.cfg.StreamRetention,
	})
	if err != nil {
		if ctx.Err() == nil {
			c.logger.ErrorContext(ctx, "hopper: maintain history", "error", err)
		}
		return
	}
	if len(res.Created) > 0 || len(res.Dropped) > 0 || res.Pruned > 0 {
		c.logger.InfoContext(ctx, "hopper: maintained history", "created", res.Created, "dropped", res.Dropped, "pruned", res.Pruned)
	}
}
