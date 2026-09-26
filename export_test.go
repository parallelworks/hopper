package hopper

import "time"

// Tuning exposes internal timings to tests in package hopper_test.
type Tuning struct {
	LeaseTTL         time.Duration
	LeaseRenew       time.Duration
	ClaimCooldown    time.Duration
	FinalizeInterval time.Duration
	FinalizeBatch    int
	StopGrace        time.Duration
	CopyThreshold    int
	NotifyInterval   time.Duration
	LeaderTTL        time.Duration
	LeaderInterval   time.Duration
	Maintenance      time.Duration
	RescueBatch      int
}

// SetTuning overrides the client's internal timings. Call it before Start.
func (c *Client[TTx]) SetTuning(t Tuning) {
	c.tuning = tuning{
		leaseTTL:            t.LeaseTTL,
		leaseRenew:          t.LeaseRenew,
		claimCooldown:       t.ClaimCooldown,
		finalizeInterval:    t.FinalizeInterval,
		finalizeBatch:       t.FinalizeBatch,
		stopGrace:           t.StopGrace,
		copyThreshold:       t.CopyThreshold,
		notifyInterval:      t.NotifyInterval,
		leaderTTL:           t.LeaderTTL,
		leaderInterval:      t.LeaderInterval,
		maintenanceInterval: t.Maintenance,
		rescueBatch:         t.RescueBatch,
	}
	c.notifier.interval = c.tuning.notifyInterval
}

// IsLeader reports whether the client currently holds the leader lease.
func (c *Client[TTx]) IsLeader() bool { return c.isLeader.Load() }

// Listening reports whether the notification connection is up.
func (c *Client[TTx]) Listening() bool { return c.listening.Load() }

// Abandon simulates a process crash: every loop stops without finalizing
// results, resigning leadership or releasing the lease.
func (c *Client[TTx]) Abandon() {
	c.mu.Lock()
	c.state = clientStopped
	c.mu.Unlock()
	c.finalizer.crashed.Store(true)
	c.claimCancel()
	c.workCancel()
	c.bgCancel()
	c.finCancel()
	c.producerWG.Wait()
	c.leaderWG.Wait()
	c.listenWG.Wait()
	c.leaseWG.Wait()
	<-c.finalizer.done
}

// UniqueKey exposes key construction for tests.
func UniqueKey(o *UniqueOpts, args JobArgs, encoded []byte, queue string, now time.Time) (string, error) {
	return o.key(args, encoded, queue, now)
}

// ClientID returns the current lease ID.
func (c *Client[TTx]) ClientID() int64 { return c.clientID.Load() }

// DefaultBackoff exposes the retry backoff for tests.
var DefaultBackoff = defaultBackoff
