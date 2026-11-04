package hopper

import (
	"cmp"
	"context"
	"time"
)

// Tuning exposes internal timings to tests in package hopper_test.
type Tuning struct {
	LeaseTTL         time.Duration
	LeaseRenew       time.Duration
	ClaimCooldown    time.Duration
	FinalizeInterval time.Duration
	FinalizeBatch    int
	FinalizeBuffer   int // 0: 20 * FinalizeBatch
	StopGrace        time.Duration
	CopyThreshold    int
	NotifyInterval   time.Duration
	LeaderTTL        time.Duration
	LeaderInterval   time.Duration
	Maintenance      time.Duration
	RescueBatch      int
	LogStoreClose    time.Duration // 0: the default
}

// SetTuning overrides the client's internal timings. Call it before Start.
func (c *Client[TTx]) SetTuning(t Tuning) {
	c.tuning = tuning{
		leaseTTL:            t.LeaseTTL,
		leaseRenew:          t.LeaseRenew,
		claimCooldown:       t.ClaimCooldown,
		finalizeInterval:    t.FinalizeInterval,
		finalizeBatch:       t.FinalizeBatch,
		finalizeBuffer:      cmp.Or(t.FinalizeBuffer, 20*t.FinalizeBatch),
		stopGrace:           t.StopGrace,
		copyThreshold:       t.CopyThreshold,
		notifyInterval:      t.NotifyInterval,
		leaderTTL:           t.LeaderTTL,
		leaderInterval:      t.LeaderInterval,
		maintenanceInterval: t.Maintenance,
		rescueBatch:         t.RescueBatch,
		streamBatch:         defaultTuning.streamBatch,
		logCloseTimeout:     cmp.Or(t.LogStoreClose, defaultTuning.logCloseTimeout),
		finalizeDrain:       defaultTuning.finalizeDrain,
	}
	c.notifier.interval = c.tuning.notifyInterval
}

// LeaseTuning returns the client lease's length and renewal interval.
func (c *Client[TTx]) LeaseTuning() (ttl, renew time.Duration) {
	return c.tuning.leaseTTL, c.tuning.leaseRenew
}

// IsLeader reports whether the client currently holds the leader lease.
func (c *Client[TTx]) IsLeader() bool { return c.isLeader.Load() }

// Listening reports whether the notification connection is up.
func (c *Client[TTx]) Listening() bool { return c.listening.Load() }

// Abandon simulates a process crash: every loop stops without finalizing
// results, resigning leadership or releasing the lease.
func (c *Client[TTx]) Abandon() {
	c.mu.Lock()
	// A call that is stopping the client closes Stopped itself. Otherwise
	// the client is stopping until Abandon has closed it, so that a stop
	// call meanwhile waits for it, as for any stop.
	closeStopped := c.state != clientStopping && c.state != clientStopped
	if closeStopped {
		c.state = clientStopping
	}
	c.mu.Unlock()
	c.finalizer.crashed.Store(true)
	c.claimCancel()
	c.workCancel(nil)
	c.bgCancel()
	c.finCancel()
	c.producerWG.Wait()
	c.leaderWG.Wait()
	c.listenWG.Wait()
	c.leaseWG.Wait()
	<-c.finalizer.done
	if closeStopped {
		c.mu.Lock()
		c.state = clientStopped
		close(c.stopped)
		c.mu.Unlock()
	}
}

// UniqueKey exposes key construction for tests.
func UniqueKey(o *UniqueOpts, args JobArgs, encoded []byte, queue string, now time.Time) (string, error) {
	return o.key(args, encoded, queue, now)
}

// ClientID returns the current lease ID.
func (c *Client[TTx]) ClientID() int64 { return c.clientID.Load() }

// DefaultBackoff exposes the retry backoff for tests.
var DefaultBackoff = defaultBackoff

// FenceTo moves the client to another client ID as a fence's
// re-registration does, without the loops noticing yet.
func (c *Client[TTx]) FenceTo(id int64) { c.clientID.Store(id) }

// ExpireJobs runs the leader's expiry of job leases and of TTLs once,
// whether or not the client leads.
func (c *Client[TTx]) ExpireJobs(ctx context.Context) {
	c.expireJobLeases(ctx)
	c.expire(ctx)
}
