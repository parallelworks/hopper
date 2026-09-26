package hopper

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// QueueDefault is the queue jobs go to when none is given.
const QueueDefault = "default"

// Config configures a Client. Every field has a production default; the zero
// Config is an insert-only client.
type Config struct {
	// Queues lists the queues this client works, with per-queue settings. A
	// client with no queues only inserts jobs.
	Queues map[string]QueueConfig
	// Workers is the registry of workers. Required when Queues is set.
	Workers *Workers
	// Periodic lists jobs inserted on a schedule by the leader. See Cron
	// and Every.
	Periodic []PeriodicJob
	// Middleware wraps job execution and inserts, outermost first.
	Middleware []Middleware
	// Logger defaults to slog.Default().
	Logger *slog.Logger
	// Codec encodes args and outputs. Defaults to JSONCodec.
	Codec Codec
	// JobTimeout is how long a job may run before its context is cancelled,
	// unless its worker's Timeout says otherwise. Defaults to one minute.
	JobTimeout time.Duration
	// MaxAttempts is the default number of attempts before a job is
	// discarded. Defaults to 25.
	MaxAttempts int
	// StopTimeout bounds how long Run waits for running jobs after its
	// context is cancelled, before cancelling them. Defaults to 30 seconds.
	StopTimeout time.Duration
	// PollInterval is how often an idle producer checks its queue for work
	// that arrived without a wake-up. Defaults to one second.
	PollInterval time.Duration
	// StrictKinds rejects inserts whose kind has no worker on this client.
	// By default any kind can be inserted, since another service may work it.
	StrictKinds bool
	// Hostname identifies this process in the clients table. Defaults to
	// os.Hostname().
	Hostname string
	// CompletedRetention is how long completed jobs stay in history.
	// Defaults to 24 hours; negative keeps them forever. Retention is
	// enforced by the leader, so the leader's setting applies cluster-wide.
	CompletedRetention time.Duration
	// FailedRetention is how long cancelled and discarded jobs stay in
	// history, which is the dead-letter queue. Defaults to 7 days; negative
	// keeps them forever.
	FailedRetention time.Duration
	// RescueStuckAfter, if set, rescues jobs that have been running longer
	// than this even though their client's lease is live, for workers that
	// ignore their context. Off by default; timeouts cover most cases.
	RescueStuckAfter time.Duration
}

// QueueConfig configures one queue on a client.
type QueueConfig struct {
	// MaxWorkers is the number of jobs this client runs at once from the
	// queue. Required.
	MaxWorkers int
	// DeleteCompleted deletes completed jobs instead of archiving them to
	// history, for maximum throughput. Cancelled and discarded jobs are
	// always archived, because history is the dead-letter queue.
	DeleteCompleted bool

	// The limits below are cluster-wide: they are recorded on the queue
	// row when the client starts and enforced for every client. A client
	// that declares none leaves the recorded ones alone; change them at
	// runtime with Queues().SetLimits.

	// GlobalLimit caps the queue's running jobs across all clients.
	GlobalLimit int
	// RateLimit caps claims per second across all clients, in a token
	// bucket of RateBurst tokens (defaulting to one second's worth).
	RateLimit float64
	RateBurst int
	// PartitionLimit caps running jobs per InsertOpts.PartitionKey.
	PartitionLimit int
	// PriorityAging raises a waiting job's priority by one level each time
	// it has waited this long, so low priorities cannot starve.
	PriorityAging time.Duration
}

// limits returns the queue's cluster-wide limits.
func (q QueueConfig) limits(name string) driver.QueueLimits {
	return driver.QueueLimits{
		Name: name, GlobalLimit: q.GlobalLimit, RatePerSec: q.RateLimit, RateBurst: q.RateBurst,
		PartitionLimit: q.PartitionLimit, Aging: q.PriorityAging,
	}
}

// PerSecond is a readable RateLimit value.
func PerSecond(n float64) float64 { return n }

// Defaults for Config fields.
const (
	DefaultJobTimeout         = time.Minute
	DefaultMaxAttempts        = 25
	DefaultStopTimeout        = 30 * time.Second
	DefaultPollInterval       = time.Second
	DefaultCompletedRetention = 24 * time.Hour
	DefaultFailedRetention    = 7 * 24 * time.Hour
)

// withDefaults validates cfg and fills in defaults. It does not modify cfg.
func (cfg *Config) withDefaults() (Config, error) {
	var out Config
	if cfg != nil {
		out = *cfg
	}
	if out.Logger == nil {
		out.Logger = slog.Default()
	}
	if out.Codec == nil {
		out.Codec = JSONCodec{}
	}
	if out.JobTimeout == 0 {
		out.JobTimeout = DefaultJobTimeout
	}
	if out.MaxAttempts == 0 {
		out.MaxAttempts = DefaultMaxAttempts
	}
	if out.StopTimeout == 0 {
		out.StopTimeout = DefaultStopTimeout
	}
	if out.PollInterval == 0 {
		out.PollInterval = DefaultPollInterval
	}
	if out.CompletedRetention == 0 {
		out.CompletedRetention = DefaultCompletedRetention
	}
	if out.FailedRetention == 0 {
		out.FailedRetention = DefaultFailedRetention
	}
	if out.Hostname == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "unknown"
		}
		out.Hostname = host
	}

	if out.MaxAttempts < 0 {
		return out, errors.New("hopper: Config.MaxAttempts must be positive")
	}
	if out.JobTimeout < 0 || out.StopTimeout < 0 || out.PollInterval < 0 || out.RescueStuckAfter < 0 {
		return out, errors.New("hopper: Config durations must be positive")
	}
	if len(out.Queues) > 0 && out.Workers == nil {
		return out, errors.New("hopper: Config.Workers is required to work queues")
	}
	for name, q := range out.Queues {
		if name == "" {
			return out, errors.New("hopper: queue name must not be empty")
		}
		if q.MaxWorkers <= 0 {
			return out, fmt.Errorf("hopper: queue %q: MaxWorkers must be positive", name)
		}
		if q.GlobalLimit < 0 || q.RateLimit < 0 || q.RateBurst < 0 || q.PartitionLimit < 0 || q.PriorityAging < 0 {
			return out, fmt.Errorf("hopper: queue %q: limits must not be negative", name)
		}
	}
	if out.StrictKinds && out.Workers == nil {
		return out, errors.New("hopper: Config.StrictKinds requires Workers")
	}
	out.Periodic = append([]PeriodicJob(nil), out.Periodic...)
	names := map[string]struct{}{}
	for i := range out.Periodic {
		p := &out.Periodic[i]
		if err := p.resolve(); err != nil {
			return out, err
		}
		if _, dup := names[p.Name]; dup {
			return out, fmt.Errorf("hopper: periodic job %q is listed twice; set PeriodicOpts.Name to tell them apart", p.Name)
		}
		names[p.Name] = struct{}{}
	}
	for i, m := range out.Middleware {
		if m == nil {
			return out, fmt.Errorf("hopper: Config.Middleware[%d] is nil", i)
		}
	}
	return out, nil
}

// tuning holds internal timings that are fixed in production and shortened
// by tests.
type tuning struct {
	leaseTTL         time.Duration
	leaseRenew       time.Duration
	claimCooldown    time.Duration
	finalizeInterval time.Duration
	finalizeBatch    int
	// stopGrace is how long a hard stop waits for cancelled jobs to return
	// before flushing what has finished and giving up on the rest.
	stopGrace time.Duration
	// copyThreshold is the InsertMany batch size from which the COPY path
	// is used for batches without unique keys.
	copyThreshold int
	// notifyInterval coalesces insert notifications from this process.
	notifyInterval time.Duration
	leaderTTL      time.Duration
	// leaderInterval is how often the leader renews and runs its duties,
	// and how often other clients try for the lease.
	leaderInterval time.Duration
	// maintenanceInterval is how often the leader maintains history
	// partitions and retention.
	maintenanceInterval time.Duration
	rescueBatch         int
}

var defaultTuning = tuning{
	leaseTTL:            15 * time.Second,
	leaseRenew:          5 * time.Second,
	claimCooldown:       20 * time.Millisecond,
	finalizeInterval:    25 * time.Millisecond,
	finalizeBatch:       500,
	stopGrace:           2 * time.Second,
	copyThreshold:       256,
	notifyInterval:      10 * time.Millisecond,
	leaderTTL:           15 * time.Second,
	leaderInterval:      5 * time.Second,
	maintenanceInterval: 5 * time.Minute,
	rescueBatch:         1000,
}
