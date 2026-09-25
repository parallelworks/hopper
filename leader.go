package hopper

import (
	"context"
	"slices"
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
	defer c.resign(ctx)

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
				lastMaintenance = time.Time{}
			}
			c.rescue(ctx)
			c.pruneClients(ctx)
			if time.Since(lastMaintenance) >= c.tuning.maintenanceInterval {
				c.maintainHistory(ctx)
				lastMaintenance = time.Now()
			}
		default:
			if c.isLeader.Swap(false) {
				c.logger.WarnContext(ctx, "hopper: lost leadership", "client_id", c.clientID.Load())
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
		// Finding our own jobs among the candidates means our lease has
		// lapsed and the lease loop has not noticed yet. Fence first, so the
		// jobs stop before they are handed back.
		if me := c.clientID.Load(); slices.ContainsFunc(jobs, func(j *driver.JobRow) bool { return j.AttemptedBy == me }) {
			c.fence(ctx, me)
		}

		fin := make([]driver.JobFinalize, len(jobs))
		queueOf := make(map[JobID]string, len(jobs))
		for i, j := range jobs {
			reason := "hopper: client lost"
			if c.cfg.RescueStuckAfter > 0 && time.Since(j.AttemptedAt) > c.cfg.RescueStuckAfter {
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
		for q := range queues {
			c.wakeQueue(q)
			c.notifier.mark(q)
		}
		if len(jobs) < c.tuning.rescueBatch {
			return
		}
	}
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
