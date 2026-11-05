package hopper

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// leaderLoop competes for the leader lease and renews it, and runs nothing
// else: while it holds the lease, the term's loops run the singleton duties
// beside it (dutyLoop, maintenanceLoop and the others startTerm starts), so
// that none of them holds up the renewal. Duties are idempotent, because two
// leaders can briefly overlap during a partition.
func (c *Client[TTx]) leaderLoop(ctx context.Context) {
	ticker := time.NewTicker(c.tuning.leaderInterval)
	defer ticker.Stop()

	// term is the current leadership: the periodic loop runs under it and
	// stops when it ends.
	var term *leaderTerm
	// leaseID is the client ID the lease was last taken or renewed under,
	// which a stop gives it up for: a fence may re-register the client
	// under another before the next attempt notices.
	var leaseID int64
	// electedAt identifies the term the client leads (the lease's
	// elected_at): an attempt that returns another has started a new one.
	var electedAt time.Time
	// held is whether the last attempt that returned found the lease this
	// client's, which a term ended by the local clock leaves set: the lease
	// may still be live, and a stop gives it up.
	var held bool
	defer func() {
		term.end()
		c.resign(ctx, leaseID, held)
	}()

	// lose ends the term the client has stopped leading, with its event,
	// logging why.
	lose := func(msg string, args ...any) {
		c.logger.WarnContext(ctx, msg, args...)
		c.emit(EventLeaderLost, nil, "")
		term.end()
		term = nil
	}
	// endLapsed ends the term once its lease may have expired without a
	// renewal, by the local clock, with its event. Its loops have been
	// canceled by then (leaseFence), even while a renewal that kept failing
	// or waiting kept the loop from seeing the lease go. It reports whether
	// it ended the term.
	endLapsed := func() bool {
		if term != nil && term.lease.expired() && c.isLeader.Swap(false) {
			lose("hopper: leader lease may have expired without a renewal; its term ended", "client_id", leaseID)
			return true
		}
		return false
	}
	for {
		// The attempt's ID is the one a resign gives the lease up for: a
		// fence may re-register the client under another meanwhile.
		id := c.clientID.Load()
		// A term whose lease has gone ends before the attempt, which then
		// tries for the lease as any election does, at once, rather than
		// send a renewal with no time left and wait a leader interval more.
		endLapsed()
		// The lease the attempt takes or renews expires a TTL after the
		// server's now(), which is no earlier than this: until.
		attempted := time.Now()
		until := attempted.Add(c.tuning.leaderTTL)
		// An attempt waits no longer than the lease it would leave lasts: an
		// election's lease is gone a TTL after it was sent, and a renewal's
		// term ends once the lease it renews may have expired (below),
		// whatever the attempt returns.
		deadline := until
		if term != nil {
			deadline = term.lease.expiry()
		}
		actx, cancel := context.WithDeadline(ctx, deadline)
		res, err := c.exec.LeaderAttempt(actx, driver.LeaderParams{ClientID: id, TTL: c.tuning.leaderTTL})
		cancel()
		// A renewal that came back too late to extend the lease still ends
		// the term: its loops are gone, and the next term, of the same lease,
		// starts below. One that reports a resign request does not extend
		// the lease, and the term ends below.
		lapsed := false
		if term != nil {
			renewed := err == nil && res.Leader && res.ElectedAt.Equal(electedAt)
			if renewed && !res.ResignRequested {
				renewed = term.lease.renew(until)
			}
			if !renewed {
				lapsed = endLapsed()
			}
		}
		// A term is one live lease, identified by its elected_at. A client
		// that took over a lease that had lapsed, even its own and under the
		// same ID, or its old one under the ID a fence gave it, left its term
		// when the lease lapsed, and another client may have led since: that
		// term ends, with its event, before the new one starts or, if the new
		// one was asked to resign, is resigned. Comparing elected_at rather
		// than asking whether this attempt took the lease catches a takeover
		// whose response was lost, which the next renewal reports with the
		// new term's.
		if err == nil && res.Leader && !res.ElectedAt.Equal(electedAt) && c.isLeader.Swap(false) {
			lose("hopper: leader lease lapsed; its term ended", "client_id", leaseID, "new_client_id", id)
		}
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return
			}
			c.logger.WarnContext(ctx, "hopper: leader attempt", "error", err)
			// A renewal that failed once its lease may have expired, as one
			// sent with little of the lease left does at its deadline, ended
			// the term: the lease is tried for at once, as an election, as a
			// term that lapsed before its attempt is, not a leader interval
			// later. The term is gone, so this retries once.
			if lapsed {
				continue
			}
		case res.ResignRequested:
			// Asked to resign (Client.LeaderResign): no duty runs again in
			// this term. Its loops end before the lease is given up, and
			// within the lease it holds (the renewal that reported the
			// request did not extend it), so no duty of this client overlaps
			// the next leader's.
			term.end()
			term = nil
			held = false
			c.resignOnRequest(ctx, id)
			// It stays out of the election for a lease's TTL, so that
			// another client takes the lease rather than it again, and
			// attempts again as soon as the hold-off ends, not at the next
			// tick after it.
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.tuning.leaderTTL):
			}
			// A tick or poke from during the hold-off is stale: the attempt
			// that follows covers it, so it must not repeat that attempt.
			ticker.Reset(c.tuning.leaderInterval)
			select {
			case <-c.leaderPoke:
			default:
			}
			continue
		case res.Leader:
			leaseID, electedAt, held = id, res.ElectedAt, true
			if !c.isLeader.Swap(true) {
				c.logger.InfoContext(ctx, "hopper: elected leader", "client_id", id)
				c.emit(EventLeaderElected, nil, "")
				term = c.startTerm(ctx, until)
			}
		default:
			held = false
			if c.isLeader.Swap(false) {
				// The ID it led under: a fence may have moved the client on.
				lose("hopper: lost leadership", "client_id", leaseID)
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

// leaderTerm is one stretch of leadership. The periodic, stream, history
// and other term loops run for its duration, and no longer than its lease
// is known to be held.
type leaderTerm struct {
	cancel context.CancelFunc
	done   chan struct{}
	// maintained is closed once the term's first history maintenance has
	// run, which the index loop waits for (see indexLoop).
	maintained chan struct{}
	once       sync.Once
	// lease is how long the term's lease is known to be held. It cancels
	// the term's loops once that has passed.
	lease leaseFence
}

func (t *leaderTerm) maintainedOnce() { t.once.Do(func() { close(t.maintained) }) }

// startTerm starts a term whose lease expires no earlier than until.
func (c *Client[TTx]) startTerm(ctx context.Context, until time.Time) *leaderTerm {
	termCtx, cancel := context.WithCancel(ctx)
	t := &leaderTerm{cancel: cancel, done: make(chan struct{}), maintained: make(chan struct{})}
	t.lease.renew(until)
	var wg sync.WaitGroup
	wg.Go(func() { t.lease.fence(termCtx, cancel) })
	wg.Go(func() { c.dutyLoop(termCtx, ctx) })
	wg.Go(func() { c.maintenanceLoop(termCtx, t) })
	wg.Go(func() { c.periodicLoop(termCtx) })
	wg.Go(func() { c.streamLoop(termCtx) })
	wg.Go(func() { c.liveLoop(termCtx) })
	wg.Go(func() { c.indexLoop(termCtx, t.maintained) })
	wg.Go(func() { c.logRetentionLoop(termCtx) })
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
	select {
	case c.dutyPoke <- struct{}{}:
	default:
	}
}

// resign gives up leadership on stop so a replacement takes over at once.
// id is the client ID the lease was taken or last renewed under. held says
// whether the lease may still be this client's although its term ended:
// one whose renewal the local clock gave up on may have committed it.
func (c *Client[TTx]) resign(ctx context.Context, id int64, held bool) {
	if !c.isLeader.Swap(false) && !held {
		return
	}
	c.giveUpLease(ctx, id, "")
}

// giveUpLease deletes the leader lease if client id holds it, and then
// signals the other clients to elect a replacement, and logs which way the
// lease went; why ("" or " on request") says what ended the term. It runs
// on a context of its own, so that a stopping client still gives the lease
// up. If the statement fails, the lease expires instead, within the TTL
// its last renewal left it.
func (c *Client[TTx]) giveUpLease(ctx context.Context, id int64, why string) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	deleted, err := c.exec.LeaderResign(rctx, id)
	switch {
	case err != nil:
		c.logger.WarnContext(ctx, "hopper: resign leadership"+why+"; the lease expires instead", "client_id", id, "error", err)
	case !deleted:
		c.logger.InfoContext(ctx, "hopper: stopped leading"+why+"; the lease had already passed on", "client_id", id)
	default:
		c.logger.InfoContext(ctx, "hopper: resigned leadership"+why, "client_id", id)
	}
}

// resignOnRequest ends a term on a resign request, once the term's loops
// have ended: it gives the lease client id holds up and signals the other
// clients to elect a replacement. The event marks the end of the term,
// whichever way the lease went, and only of a term the client announced
// (EventLeaderElected): an attempt that took the lease but failed to
// report it started none.
func (c *Client[TTx]) resignOnRequest(ctx context.Context, id int64) {
	led := c.isLeader.Swap(false)
	c.giveUpLease(ctx, id, " on request")
	if led {
		c.emit(EventLeaderResigned, nil, "")
	}
}

// rescue moves running jobs whose client has lost its lease back to
// retryable, or to discarded when they are out of attempts. The transition
// goes through the same fenced finalize statement as a worker's result,
// fenced to the claim each job was found in, so a client that finalizes its
// own result first wins and the rescue of that job is dropped, even if the
// job has been claimed again since. A fence of this client's own lease
// runs under clientCtx (see dutyLoop).
func (c *Client[TTx]) rescue(ctx, clientCtx context.Context) {
	for {
		candidates, err := c.exec.JobRescueCandidates(ctx, driver.JobRescueParams{
			StuckAfter: c.cfg.RescueStuckAfter,
			Limit:      c.tuning.rescueBatch,
		})
		if err != nil {
			if ctx.Err() == nil {
				c.logger.ErrorContext(ctx, "hopper: find jobs to rescue", "error", err)
			}
			return
		}
		if len(candidates) == 0 {
			return
		}
		// Finding our own jobs among the candidates, other than as stuck,
		// means our lease has lapsed and the lease loop has not noticed
		// yet. Fence first, so the jobs stop before they are handed back.
		if me := c.clientID.Load(); slices.ContainsFunc(candidates, func(cand driver.RescueCandidate) bool { return cand.Job.AttemptedBy == me && !cand.Stuck }) {
			c.fence(clientCtx, me)
		}

		fin := make([]driver.JobFinalize, len(candidates))
		queueOf := make(map[JobID]string, len(candidates))
		// inBatch holds the jobs of a batch the rescue discards, whose
		// dependents it follows up.
		inBatch := map[JobID]struct{}{}
		noBatches := true
		for i, cand := range candidates {
			j := cand.Job
			noBatches = noBatches && j.BatchID.IsZero()
			reason, why := "hopper: client lost", driver.RescueClientLost
			if cand.Stuck {
				reason, why = "hopper: job ran longer than RescueStuckAfter", driver.RescueStuck
			}
			f := driver.JobFinalize{
				ID:          j.ID,
				AttemptedBy: j.AttemptedBy,
				Fence:       j.Fence,
				State:       driver.JobStateRetryable,
				Error:       &driver.AttemptError{Attempt: j.Attempt, Fence: j.Fence, Error: reason, Rescue: why},
				Archive:     true,
			}
			if j.Attempt >= j.MaxAttempts {
				f.State = driver.JobStateDiscarded
				if !j.BatchID.IsZero() {
					inBatch[j.ID] = struct{}{}
				}
			}
			fin[i] = f
			queueOf[j.ID] = j.Queue
		}
		applied, err := c.exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: fin, NoBatches: noBatches, SkipLocked: true, BriefWait: true})
		if errors.Is(err, driver.ErrRowsHeld) {
			// A transaction held open holds a row the batch's finalize
			// reaches, a dependent of one of its jobs: the next pass tries
			// again, rather than hold the leader's other duties up while it
			// bisects.
			c.logger.WarnContext(ctx, "hopper: rescue deferred: another transaction holds a row it reaches", "candidates", len(candidates))
			return
		}
		if err != nil {
			if ctx.Err() == nil {
				c.logger.ErrorContext(ctx, "hopper: rescue jobs", "error", err)
			}
			return
		}

		// The dependents of the jobs it discarded of a batch, and of those it
		// canceled by cascade, are promoted or canceled as a client's
		// follow-up does (their batches are left to settleBatches): the
		// finalize leaves a dependent with an any-of group to the follow-up,
		// and one of the cascade's own dependents can wait on a group the
		// cascade exhausted, which the sweep, from where its last pass
		// stopped, may not reach in this pass.
		applied, cascaded := splitCascaded(applied)
		var followUp []JobID
		for _, a := range applied {
			if _, ok := inBatch[a.ID]; ok {
				followUp = append(followUp, a.ID)
			}
		}
		if followUp = append(followUp, cascaded...); len(followUp) > 0 {
			if _, err := c.exec.WorkflowPromoteReady(ctx, driver.WorkflowPromoteParams{DependsOn: followUp}); err != nil && ctx.Err() == nil {
				c.logger.WarnContext(ctx, "hopper: promote dependents of rescued jobs; the sweep will", "count", len(followUp), "error", err)
			}
		}
		var retried, discarded int
		queues := map[string]struct{}{}
		byID := make(map[JobID]driver.JobFinalize, len(fin))
		for _, f := range fin {
			byID[f.ID] = f
		}
		rescued := make(map[JobID]struct{}, len(applied))
		for _, a := range applied {
			id := a.ID
			rescued[id] = struct{}{}
			if byID[id].State == driver.JobStateDiscarded {
				discarded++
				continue
			}
			retried++
			queues[queueOf[id]] = struct{}{}
		}
		c.logger.WarnContext(ctx, "hopper: rescued jobs from lost clients", "retried", retried, "discarded", discarded, "dropped", len(candidates)-len(applied))
		for _, cand := range candidates {
			if _, was := rescued[cand.Job.ID]; was {
				c.emit(EventJobRescued, cand.Job, byID[cand.Job.ID].Error.Error)
			}
		}
		for q := range queues {
			c.wakeQueue(q)
			c.notifier.mark(q)
		}
		// A full batch that rescued nothing would be read again as it is:
		// rows that transactions hold, which the finalize skips, or that it
		// cannot apply to. The next pass tries them again.
		if len(candidates) < c.tuning.rescueBatch || len(applied) == 0 {
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

// expireJobLeases ends the attempts of jobs claimed by ID whose job leases
// were not renewed in time: retryable at once, or discarded on their last
// attempt, or canceled if a cancel was requested. The statement locks each
// job's row and skips those another statement holds, such as a Finish in
// flight, and a renewal that commits first keeps its lease.
func (c *Client[TTx]) expireJobLeases(ctx context.Context) {
	for {
		jobs, cascaded, err := c.exec.JobExpireLeases(ctx, driver.JobExpireLeasesParams{
			Limit: c.tuning.rescueBatch, ClientID: c.clientID.Load(), Grace: c.cfg.JobLeaseGrace,
		})
		if errors.Is(err, driver.ErrRowsHeld) {
			c.logger.WarnContext(ctx, "hopper: job lease expiry deferred: another transaction holds a row it reaches")
			return
		}
		if err != nil {
			if ctx.Err() == nil {
				c.logger.ErrorContext(ctx, "hopper: expire job leases", "error", err)
			}
			return
		}
		var retried, ended int
		queues := map[string]struct{}{}
		for _, j := range jobs {
			c.emit(EventJobRescued, j, "hopper: job lease expired")
			if j.State.Terminal() {
				ended++
				c.signalDone(j.ID)
				continue
			}
			retried++
			queues[j.Queue] = struct{}{}
		}
		if len(jobs) > 0 {
			c.logger.WarnContext(ctx, "hopper: job leases expired", "retried", retried, "ended", ended)
		}
		for q := range queues {
			c.wakeQueue(q)
			c.notifier.mark(q)
		}
		c.followUpEnded(ctx, jobs, cascaded)
		if len(jobs) < c.tuning.rescueBatch {
			return
		}
	}
}

// expire dead-letters waiting jobs whose TTL has passed.
func (c *Client[TTx]) expire(ctx context.Context) {
	for {
		jobs, cascaded, err := c.exec.JobDiscardExpired(ctx, c.tuning.rescueBatch)
		if errors.Is(err, driver.ErrRowsHeld) {
			c.logger.WarnContext(ctx, "hopper: discard of expired jobs deferred: another transaction holds a row it reaches")
			return
		}
		if err != nil {
			if ctx.Err() == nil {
				c.logger.ErrorContext(ctx, "hopper: discard expired jobs", "error", err)
			}
			return
		}
		for _, j := range jobs {
			c.emit(EventJobDiscarded, j, j.Errors[len(j.Errors)-1].Error)
			c.signalDone(j.ID)
		}
		if len(jobs) > 0 {
			c.logger.InfoContext(ctx, "hopper: discarded expired jobs", "count", len(jobs))
		}
		c.followUpEnded(ctx, jobs, cascaded)
		if len(jobs) < c.tuning.rescueBatch {
			return
		}
	}
}

// followUpEnded follows up the jobs the expiry of job leases or of TTLs
// ended, as a client follows up its flush: it settles their batches and
// promotes or cancels the dependents of those of a workflow and of the
// dependents the statement canceled by cascade. The statement's tail
// leaves a dependent with an any-of group to it, and one of the cascade's
// own dependents can wait on a group the cascade exhausted; without it,
// they wait for the leader's sweeps.
func (c *Client[TTx]) followUpEnded(ctx context.Context, jobs []*JobRow, cascaded []JobID) {
	if len(jobs) == 0 {
		return
	}
	seen := map[JobID]struct{}{}
	var batches, ended []JobID
	for _, j := range jobs {
		if !j.State.Terminal() || j.BatchID.IsZero() {
			continue
		}
		ended = append(ended, j.ID)
		if _, dup := seen[j.BatchID]; !dup {
			seen[j.BatchID] = struct{}{}
			batches = append(batches, j.BatchID)
		}
	}
	if ended = append(ended, cascaded...); len(ended) > 0 {
		settleAndPromote(ctx, c.exec, c.logger, batches, ended)
	}
}

// settleBatches completes the batches whose last jobs were finalized at the
// same time by statements that could not see each other's progress, and
// that their clients did not settle (a client that stopped, a batch row
// held too long, or a previous release), and folds any progress left. It
// walks the batches in ID order. Progress keeps arriving while batches are
// worked, so a pass stops after one leader interval, and the next resumes
// where it stopped.
func (c *Client[TTx]) settleBatches(ctx context.Context) {
	deadline := time.Now().Add(c.tuning.leaderInterval)
	// The sweep of orphaned dependency results rides on the first settle
	// of a pass once per maintenance interval. It counts as run once tried:
	// a sweep that kept failing, retried on every pass, would fail each
	// pass's first settle and so keep the walk from ever getting past it.
	results := time.Since(c.resultsSweptAt) >= c.tuning.maintenanceInterval
	if results {
		c.resultsSweptAt = time.Now()
	}
	for {
		res, err := c.exec.BatchSettle(ctx, driver.BatchSettleParams{After: c.settleAfter, Limit: c.tuning.rescueBatch, Results: results})
		results = false
		if err != nil {
			if ctx.Err() == nil {
				c.logger.ErrorContext(ctx, "hopper: settle batches", "error", err)
			}
			return
		}
		if res.Completed > 0 {
			c.logger.DebugContext(ctx, "hopper: completed batches", "count", res.Completed)
		}
		if res.Folded < int64(c.tuning.rescueBatch) {
			c.settleAfter = driver.JobID{}
			return
		}
		c.settleAfter = res.Last
		if time.Now().After(deadline) {
			return
		}
	}
}

// promoteReady promotes workflow jobs whose last dependencies finished in
// concurrent transactions and that the finalizing clients did not promote
// (a client that stopped first, or a previous release). It walks the edges
// a chunk at a time, in ID order, for at most one leader interval per pass,
// and the next pass resumes where it stopped.
func (c *Client[TTx]) promoteReady(ctx context.Context) {
	deadline := time.Now().Add(c.tuning.leaderInterval)
	for {
		res, err := c.exec.WorkflowPromoteReady(ctx, driver.WorkflowPromoteParams{After: c.promoteAfter, Limit: c.tuning.rescueBatch})
		if err != nil {
			if ctx.Err() == nil {
				c.logger.ErrorContext(ctx, "hopper: promote ready workflow jobs", "error", err)
			}
			return
		}
		if res.Promoted > 0 {
			c.logger.InfoContext(ctx, "hopper: promoted workflow jobs whose dependencies finished concurrently", "count", res.Promoted)
		}
		if res.Canceled > 0 {
			c.logger.InfoContext(ctx, "hopper: canceled workflow jobs whose any-of groups failed", "count", res.Canceled)
		}
		if res.Scanned < int64(c.tuning.rescueBatch) {
			c.promoteAfter = driver.JobID{}
			return
		}
		c.promoteAfter = res.Last
		if time.Now().After(deadline) {
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

// maxIndexBackoffShift caps the retry interval of a timed-out index build
// at 64 maintenance intervals.
const maxIndexBackoffShift = 6

// maxInvalidBackoffShift caps the retry interval of an index reported
// invalid at 8 maintenance intervals: the minor release that repairs it
// arrives without a new term, and the next try repairs it.
const maxInvalidBackoffShift = 3

// indexLoop builds history indexes an upgrade declared (HistoryIndex) once
// the term's first history maintenance has run, and then every maintenance
// interval. It runs beside the leader loop, not in it: a concurrent index
// build can take minutes, and the leader loop must keep renewing the lease
// meanwhile. It waits for that first maintenance because partition creation
// skips a partition while a build runs on its table's DEFAULT partition
// (HistoryIndexLockKey), and the term starts by creating the partitions
// ahead. A build cut short by the end of the term is redone by the next
// leader. A partition whose build outlasts the session's statement_timeout
// is retried at doubling intervals, so that it neither holds up the others
// nor costs a futile build every interval, and so is an index the server
// cannot repair, with a warning each time. Index work that cannot get its
// lock waits longer on each pass, as deferred partition work does, and is
// warned about once some has been deferred ddlDeferralsWarn passes in a row.
func (c *Client[TTx]) indexLoop(ctx context.Context, maintained <-chan struct{}) {
	select {
	case <-ctx.Done():
		return
	case <-maintained:
	}
	ticker := time.NewTicker(c.tuning.maintenanceInterval)
	defer ticker.Stop()
	// builds holds the partitions whose build timed out, and repairs the
	// indexes reported invalid, each retried once its next try is due.
	builds, repairs := map[string]indexBackoff{}, map[string]indexBackoff{}
	retryIn := func(backoffs map[string]indexBackoff, name string, maxShift int) time.Duration {
		b := backoffs[name]
		b.failures++
		wait := c.tuning.maintenanceInterval << min(b.failures, maxShift)
		b.next = time.Now().Add(wait)
		backoffs[name] = b
		return wait
	}
	deferrals, busy := 0, 0
	for {
		skip, skipRepair := notDue(builds), notDue(repairs)
		timeout := ddlLockTimeout(deferrals)
		res, err := c.exec.HistoryIndex(ctx, driver.HistoryIndexParams{Skip: skip, SkipRepair: skipRepair, LockTimeout: timeout})
		if err != nil && ctx.Err() == nil {
			c.logger.WarnContext(ctx, "hopper: index history", "error", err)
		}
		for _, name := range res.TimedOut {
			c.logger.WarnContext(ctx, "hopper: a history partition's index build was canceled, by statement_timeout or an operator; if by the timeout, raise it for this role to let the build finish",
				"partition", name, "retry_in", retryIn(builds, name, maxIndexBackoffShift))
		}
		for _, name := range res.Invalid {
			c.logger.WarnContext(ctx, "hopper: a history index stays invalid although every partition is indexed, so queries "+
				"that need it read every partition; Postgres before its May 2026 minor releases (18.4, 17.10, 16.14, 15.18, 14.23) "+
				"cannot repair it, and a repair that failed is logged with its error",
				"index", name, "retry_in", retryIn(repairs, name, maxInvalidBackoffShift))
		}
		settleBackoffs(builds, skip, res.TimedOut, res, err)
		settleBackoffs(repairs, skipRepair, res.Invalid, res, err)
		// Only waits for a lock lengthen the next pass's lock timeout; a
		// pass that found another session's index pass under way waited for
		// nothing, and ended there, maybe before the work deferred last time.
		deferrals, busy = nextIndexRuns(deferrals, busy, res, err)
		if len(res.Deferred) > 0 {
			c.logger.Log(ctx, repeatLevel(deferrals), "hopper: history index work deferred: a transaction that used the partition, one left open with its table's index build lock, or another leader's history maintenance held its lock",
				"indexes", res.Deferred, "lock_timeout", timeout, "passes", deferrals)
		}
		if res.Busy != "" {
			c.logger.Log(ctx, repeatLevel(busy), "hopper: history indexing held off: another session holds the schema's index build lock, another leader's pass or one that stalled",
				"index", res.Busy, "passes", busy)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// liveLoop runs on the leader, apart from its other duties since a pass
// over a large backlog can take seconds: it keeps the live table vacuumed
// and its statistics fresh, because a burst can make both stale within
// seconds while the engine's own schedule may not look for a minute.
func (c *Client[TTx]) liveLoop(ctx context.Context) {
	ticker := time.NewTicker(c.tuning.leaderInterval)
	defer ticker.Stop()
	for {
		res, err := c.exec.JobsMaintain(ctx)
		if err != nil {
			if ctx.Err() == nil {
				c.logger.WarnContext(ctx, "hopper: maintain live table", "error", err)
			}
		} else if res.Vacuumed || res.Analyzed {
			c.logger.DebugContext(ctx, "hopper: maintained live table", "vacuumed", res.Vacuumed, "analyzed", res.Analyzed)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// dutyLoop runs the leader's singleton duties at the start of its term and
// then every leader interval: rescuing jobs from lost clients, expiring job
// leases and jobs past their TTL, settling batches, promoting workflow jobs,
// aging and pruning stale client rows. It runs beside the leader loop, not
// in it, so that no duty holds up the lease's renewal: a duty that waits,
// for a row a caller's transaction holds or behind history maintenance's
// DDL, waits on its own, and the lease fences it as it does the term's
// other loops.
//
// clientCtx is the client's own context, outliving the term, which a fence
// of the client's lease that the rescue finds lapsed runs under: once it has
// canceled the client's jobs, its registration under a new lease must not
// end with the term. A poke of the leader (pokeLeader) runs the duties at
// once, as it runs an attempt.
func (c *Client[TTx]) dutyLoop(ctx, clientCtx context.Context) {
	ticker := time.NewTicker(c.tuning.leaderInterval)
	defer ticker.Stop()
	// A poke from before the term, such as the one that started the
	// election it won or one sent while another client led, is covered by
	// the term's first pass, so it must not repeat that pass.
	select {
	case <-c.dutyPoke:
	default:
	}
	for {
		c.rescue(ctx, clientCtx)
		c.expireJobLeases(ctx)
		c.expire(ctx)
		c.settleBatches(ctx)
		c.promoteReady(ctx)
		c.age(ctx)
		c.pruneClients(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-c.dutyPoke:
		}
	}
}

// maintenanceLoop maintains history (maintainHistory) on the leader at
// the start of its term and then every maintenance interval, or at the
// next leader interval while a pass deferred partition work. It runs beside
// the leader loop, not in it, so that no retention statement holds up the
// lease's renewal: a prune of a DEFAULT partition that grew during an
// outage deletes a backlog in one statement, and every DDL waits up to its
// lock timeout. Like the term's other loops, it stops, statement and all,
// when the term ends, and once the term's lease may have expired without a
// renewal (leaseFence), even before the leader loop sees it go.
func (c *Client[TTx]) maintenanceLoop(ctx context.Context, t *leaderTerm) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		wait := c.tuning.maintenanceInterval
		if c.maintainHistory(ctx) {
			t.maintainedOnce()
		} else {
			wait = c.tuning.leaderInterval
		}
		timer.Reset(wait)
	}
}

// leaseFence tracks how long a leader term's lease is known to be held, and
// fences the term by it.
type leaseFence struct {
	mu sync.Mutex
	// until is when the lease last taken or renewed expires at the
	// earliest, by the local clock: a TTL after the attempt was sent, no
	// later than the server's now() and the TTL that expires_at holds. Once
	// it has passed it stays put, since no renewal extends it then, so the
	// lease stays lapsed.
	until time.Time
}

// renew records a renewal of the lease that lasts until until, and reports
// whether it extended it: not once the lease has lapsed, whose term's loops
// have been canceled.
func (f *leaseFence) renew(until time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.until.IsZero() && !time.Now().Before(f.until) {
		return false
	}
	f.until = until
	return true
}

// expiry is when the lease expires at the earliest, unless renewed.
func (f *leaseFence) expiry() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.until
}

// expired reports whether the lease may have expired without a renewal.
func (f *leaseFence) expired() bool {
	return f.left() <= 0
}

// left is how long the lease is known to be held. The clock is read under
// f.mu, as renew reads it, so that a renewal either extends the lease before
// left reads it or comes after and finds it lapsed: a fence that read the
// clock after releasing the lock could cancel the term's loops just as a
// renewal extended the lease, which would leave the term running on, its
// loops gone, for as long as its renewals succeed.
func (f *leaseFence) left() time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return time.Until(f.until)
}

// fence calls cancel once the lease may have expired without a renewal, or
// returns when ctx ends.
func (f *leaseFence) fence(ctx context.Context, cancel context.CancelFunc) {
	timer := time.NewTimer(f.left())
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		left := f.left()
		if left <= 0 {
			cancel()
			return
		}
		timer.Reset(left)
	}
}

// maintainHistory creates and drops history partitions. It reports false
// when a partition's creation or drop was deferred, for the next leader
// interval to retry rather than the next maintenance interval: a partition
// ahead that is missing when its period starts sends that period's history
// to the DEFAULT partition.
func (c *Client[TTx]) maintainHistory(ctx context.Context) bool {
	timeout := ddlLockTimeout(c.ddlDeferrals)
	res, err := c.exec.HistoryMaintain(ctx, driver.HistoryMaintainParams{
		CompletedRetention: c.cfg.CompletedRetention,
		FailedRetention:    c.cfg.FailedRetention,
		StreamRetention:    c.cfg.StreamRetention,
		LockTimeout:        timeout,
	})
	// Each group is maintained on its own, so a pass that failed in one may
	// still have done the others' work, which is logged as usual. It is
	// retried at the next maintenance interval, its deferred work with it,
	// rather than at every leader interval, but work it deferred still lengthens
	// the next try's lock timeout: a failure that lasts must not hold the
	// other groups' deferred work at the shortest timeout for good.
	c.ddlDeferrals = nextDeferrals(c.ddlDeferrals, len(res.Deferred) > 0, err != nil)
	if err != nil && ctx.Err() == nil {
		c.logger.ErrorContext(ctx, "hopper: maintain history", "error", err, "deferred", res.Deferred)
	}
	// A group's failure leaves the others maintained (HistoryMaintain goes
	// on past it), so what they did and what they deferred is in res
	// either way.
	if len(res.Created) > 0 || len(res.Dropped) > 0 || res.Pruned > 0 {
		c.logger.InfoContext(ctx, "hopper: maintained history", "created", res.Created, "dropped", res.Dropped, "pruned", res.Pruned)
	}
	// A prune that could not get its locks does not make the leader retry at
	// its next interval: it is not urgent, and a pass that retried it would
	// wait the lock timeout again. A pass that retries
	// deferred partitions retries it too, so, as kept partitions are, it is
	// logged once per maintenance interval.
	if len(res.PruneDeferred) > 0 && time.Since(c.pruneDeferredLogged) >= c.tuning.maintenanceInterval {
		c.pruneDeferredLogged = time.Now()
		c.logger.InfoContext(ctx, "hopper: history DEFAULT partitions' prune deferred to the next maintenance: a lock request, such as a DDL statement waiting for a long transaction, held their tables",
			"deferred", res.PruneDeferred)
	}
	// A kept partition waits for its build, whose failures the index loop
	// warns about, so it is only logged, once per maintenance interval: the
	// passes that retry deferred work in between, every leader interval, are
	// left out, without silencing a run of them that outlasts the interval.
	if len(res.Kept) > 0 && time.Since(c.keptLogged) >= c.tuning.maintenanceInterval {
		c.keptLogged = time.Now()
		c.logger.InfoContext(ctx, "hopper: history partitions past retention kept until their index is built, or while an index build or another leader's maintenance holds their table's index build lock",
			"kept", res.Kept)
	}
	if err != nil || len(res.Deferred) == 0 {
		return true
	}
	c.logger.Log(ctx, repeatLevel(c.ddlDeferrals), "hopper: history partitions deferred: a transaction that wrote history, or other history maintenance, held their lock",
		"deferred", res.Deferred, "lock_timeout", timeout, "passes", c.ddlDeferrals)
	return false
}

// nextDeferrals counts the passes in a row that deferred work, after a pass
// that deferred some or not: a pass that ended early (failed, or held off by
// another session's build lock) may have ended before the work deferred last
// time, so it neither ends the run nor extends it unless it deferred work
// itself. maintainHistory counts its passes the same way.
func nextDeferrals(n int, deferred, endedEarly bool) int {
	switch {
	case deferred:
		return n + 1
	case endedEarly:
		return n
	}
	return 0
}

// nextIndexRuns counts, after an index pass, the passes in a row that
// deferred work and those held off by another session's build lock. A pass
// held off ended before the rest of its work, as a failed one did.
func nextIndexRuns(deferrals, busy int, res driver.HistoryIndexResult, err error) (int, int) {
	failed := indexPassFailed(err)
	return nextDeferrals(deferrals, len(res.Deferred) > 0, failed || res.Busy != ""),
		nextDeferrals(busy, res.Busy != "", failed)
}

// indexPassFailed reports whether a HistoryIndex pass that returned err
// failed, and so may have ended before the rest of its work. A failed repair
// is reported once the rest is done (driver.ErrHistoryIndexRepair).
func indexPassFailed(err error) bool {
	return err != nil && !errors.Is(err, driver.ErrHistoryIndexRepair)
}

// indexBackoff is when indexLoop next tries a partition whose build timed
// out, or an index reported invalid, after failures in a row.
type indexBackoff struct {
	failures int
	next     time.Time
}

// notDue lists the names in backoffs whose next try is not due yet.
func notDue(backoffs map[string]indexBackoff) []string {
	var names []string
	for name, b := range backoffs {
		if time.Now().Before(b.next) {
			names = append(names, name)
		}
	}
	return names
}

// settleBackoffs forgets the backoffs of the names a pass tried, not
// skipping them, without reporting them again, so that a later failure
// starts over at the shortest wait. A pass that failed, was held off or
// deferred work may not have reached them, and forgets none.
// An index whose repair found its partition ahead dropped mid-pass, which
// only an operator's DROP does, is settled as if it had been repaired; its
// next failure starts the backoff again.
func settleBackoffs(backoffs map[string]indexBackoff, skipped, reported []string, res driver.HistoryIndexResult, err error) {
	if indexPassFailed(err) || res.Busy != "" || len(res.Deferred) > 0 {
		return
	}
	for name := range backoffs {
		if !slices.Contains(skipped, name) && !slices.Contains(reported, name) {
			delete(backoffs, name)
		}
	}
}

// repeatLevel is the level of a record about something that has lasted
// passes passes in a row: debug, and a warning from the ddlDeferralsWarn-th
// on.
func repeatLevel(passes int) slog.Level {
	if passes >= ddlDeferralsWarn {
		return slog.LevelWarn
	}
	return slog.LevelDebug
}

// Deferred partition work is retried every leader interval, each time
// waiting longer for its lock: doubling from 200 ms to at most 2 s, so that
// transactions that keep overlapping cannot hold retention and the
// partitions ahead off forever, while each attempt holds the table's
// writers up for no longer than its timeout.
const (
	ddlLockTimeoutBase = 200 * time.Millisecond
	ddlLockTimeoutMax  = 2 * time.Second
	ddlDeferralsWarn   = 5
)

func ddlLockTimeout(deferrals int) time.Duration {
	return min(ddlLockTimeoutBase<<min(deferrals, 4), ddlLockTimeoutMax)
}
