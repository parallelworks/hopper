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
}

// SetTuning overrides the client's internal timings. Call it before Start.
func (c *Client[TTx]) SetTuning(t Tuning) {
	c.tuning = tuning{
		leaseTTL:         t.LeaseTTL,
		leaseRenew:       t.LeaseRenew,
		claimCooldown:    t.ClaimCooldown,
		finalizeInterval: t.FinalizeInterval,
		finalizeBatch:    t.FinalizeBatch,
		stopGrace:        t.StopGrace,
		copyThreshold:    t.CopyThreshold,
	}
}

// ClientID returns the current lease ID.
func (c *Client[TTx]) ClientID() int64 { return c.clientID.Load() }

// DefaultBackoff exposes the retry backoff for tests.
var DefaultBackoff = defaultBackoff
