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
			c.settleBatches(ctx)
			c.promoteReady(ctx)
			c.age(ctx)
			c.pruneClients(ctx)
			if time.Since(lastMaintenance) >= c.tuning.maintenanceInterval && c.maintainHistory(ctx) {
				lastMaintenance = time.Now()
				term.maintainedOnce()
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
	// maintained is closed once the term's first history maintenance has
	// run, which the index loop waits for (see indexLoop).
	maintained chan struct{}
	once       sync.Once
}

func (t *leaderTerm) maintainedOnce() { t.once.Do(func() { close(t.maintained) }) }

func (c *Client[TTx]) startTerm(ctx context.Context) *leaderTerm {
	termCtx, cancel := context.WithCancel(ctx)
	t := &leaderTerm{cancel: cancel, done: make(chan struct{}), maintained: make(chan struct{})}
	var wg sync.WaitGroup
	wg.Go(func() { c.periodicLoop(termCtx) })
	wg.Go(func() { c.streamLoop(termCtx) })
	wg.Go(func() { c.liveLoop(termCtx) })
	wg.Go(func() { c.indexLoop(termCtx, t.maintained) })
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
// goes through the same fenced finalize statement as a worker's result,
// fenced to the claim each job was found in, so a client that finalizes its
// own result first wins and the rescue of that job is dropped, even if the
// job has been claimed again since.
func (c *Client[TTx]) rescue(ctx context.Context) {
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
			c.fence(ctx, me)
		}

		fin := make([]driver.JobFinalize, len(candidates))
		queueOf := make(map[JobID]string, len(candidates))
		noBatches := true
		for i, cand := range candidates {
			j := cand.Job
			noBatches = noBatches && j.BatchID.IsZero()
			reason := "hopper: client lost"
			if cand.Stuck {
				reason = "hopper: job ran longer than RescueStuckAfter"
			}
			f := driver.JobFinalize{
				ID:          j.ID,
				AttemptedBy: j.AttemptedBy,
				Fence:       j.Fence,
				State:       driver.JobStateRetryable,
				Error:       &driver.AttemptError{Attempt: j.Attempt, Fence: j.Fence, Error: reason},
				Archive:     true,
			}
			if j.Attempt >= j.MaxAttempts {
				f.State = driver.JobStateDiscarded
			}
			fin[i] = f
			queueOf[j.ID] = j.Queue
		}
		applied, err := c.exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: fin, NoBatches: noBatches, SkipLocked: true})
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
		c.logger.WarnContext(ctx, "hopper: rescued jobs from lost clients", "retried", retried, "discarded", discarded, "dropped", len(candidates)-len(applied))
		for _, cand := range candidates {
			if f, was := byID[cand.Job.ID]; was && slices.Contains(applied, cand.Job.ID) {
				c.emit(EventJobRescued, cand.Job, f.Error.Error)
			}
		}
		for q := range queues {
			c.wakeQueue(q)
			c.notifier.mark(q)
		}
		if len(candidates) < c.tuning.rescueBatch {
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
			c.emit(EventJobDiscarded, j, j.Errors[len(j.Errors)-1].Error)
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

// settleBatches completes the batches whose last jobs were finalized at the
// same time by statements that could not see each other's progress, and
// that their clients did not settle (a client that stopped, a batch row
// held too long, or a previous release), and folds any progress left. It
// walks the batches in ID order. Progress keeps arriving while batches are
// worked, so a pass stops after one leader interval, and the next resumes
// where it stopped.
func (c *Client[TTx]) settleBatches(ctx context.Context) {
	deadline := time.Now().Add(c.tuning.leaderInterval)
	for {
		res, err := c.exec.BatchSettle(ctx, driver.BatchSettleParams{After: c.settleAfter, Limit: c.tuning.rescueBatch})
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

// maintainHistory creates and drops history partitions. It reports false
// when a partition's creation or drop was deferred, for the next leader pass
// to retry rather than the next maintenance interval: a partition ahead that
// is missing when its period starts sends that period's history to the
// DEFAULT partition.
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
	// rather than on every leader pass, but work it deferred still lengthens
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
	// A kept partition waits for its build, whose failures the index loop
	// warns about, so it is only logged, once per maintenance interval: the
	// passes that retry deferred work in between, on every leader pass, are
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

// Deferred partition work is retried on every leader pass, each time
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
