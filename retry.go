package hopper

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"runtime/debug"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// ErrorOutcome is what Config.OnError decides for a failed attempt. The zero
// ErrorOutcome keeps the outcome the client would otherwise choose. If more
// than one field is set, Discard wins over Snooze, and Snooze over RetryAt.
type ErrorOutcome struct {
	// RetryAt retries the job at this time, using up the attempt as any
	// failure does: on its last attempt the job is discarded all the same.
	RetryAt time.Time
	// Snooze reschedules the job after this long without using up the
	// attempt, as if the worker had returned Snooze. The error is kept as the
	// snooze reason rather than in the job's errors.
	Snooze time.Duration
	// Discard discards the job now, with the error recorded, as if the
	// worker had returned Cancel.
	Discard bool
}

// RetryPolicy chooses when a failed job runs again, for workers whose
// NextRetry returns the zero time. job.Attempt is the attempt that just
// failed, starting at 1. Returning the zero time passes the choice on: from
// a queue's policy to the client's, and from the client's to the default
// backoff. It is called from job goroutines, concurrently; a panic in it is
// logged and also passes the choice on.
type RetryPolicy interface {
	NextRetry(job *JobRow) time.Time
}

// ExponentialRetry is a RetryPolicy that waits Base, doubling with each
// attempt, with ±10% jitter, up to Max. Base defaults to one second and Max
// to 24 hours.
type ExponentialRetry struct {
	Base time.Duration
	Max  time.Duration
}

// NextRetry implements RetryPolicy.
func (r ExponentialRetry) NextRetry(job *JobRow) time.Time {
	base, ceiling := r.Base, r.Max
	if base <= 0 {
		base = time.Second
	}
	if ceiling <= 0 {
		ceiling = maxBackoff
	}
	d := jitter(float64(base) * math.Pow(2, float64(max(job.Attempt, 1)-1)))
	// Compared as floats, so a ceiling near the largest Duration cannot
	// overflow on the way back.
	if d >= float64(ceiling) {
		return time.Now().Add(ceiling)
	}
	return time.Now().Add(time.Duration(d))
}

// onError calls Config.OnError for a failed attempt, with the client in its
// context as a worker has it, recovering a panic in the hook as the zero
// ErrorOutcome.
func (c *Client[TTx]) onError(ctx context.Context, row *driver.JobRow, err error) (out ErrorOutcome) {
	if c.cfg.OnError == nil {
		return ErrorOutcome{}
	}
	defer func() {
		if r := recover(); r != nil {
			c.logger.ErrorContext(ctx, "hopper: Config.OnError panicked; keeping the default outcome",
				"job_id", row.ID, "kind", row.Kind, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			out = ErrorOutcome{}
		}
	}()
	return c.cfg.OnError(context.WithValue(ctx, clientKey{}, c), row, err)
}

// retryDelay is how long a failed job waits before it runs again: the
// worker's NextRetry, else the queue's RetryPolicy, else the client's, else
// the default backoff.
func (c *Client[TTx]) retryDelay(row *driver.JobRow, qcfg QueueConfig, unit workUnit) time.Duration {
	d := c.chosenDelay(row, qcfg, unit)
	if capped, ok := unit.(retryCapped); ok {
		if ceiling := capped.retryCap(); ceiling > 0 {
			d = min(d, ceiling)
		}
	}
	return d
}

// retryCapped is a unit whose retries wait at most retryCap, whatever
// chose the delay: an ordered delivery, which blocks its key while it waits.
type retryCapped interface {
	retryCap() time.Duration
}

func (c *Client[TTx]) chosenDelay(row *driver.JobRow, qcfg QueueConfig, unit workUnit) time.Duration {
	if unit != nil {
		if at := c.nextRetry(row, unit.NextRetry); !at.IsZero() {
			return max(time.Until(at), 0)
		}
	}
	for _, p := range []RetryPolicy{qcfg.RetryPolicy, c.cfg.RetryPolicy} {
		if p == nil {
			continue
		}
		if at := c.nextRetry(row, func() time.Time { return p.NextRetry(row) }); !at.IsZero() {
			return max(time.Until(at), 0)
		}
	}
	return defaultBackoff(row.Attempt)
}

// nextRetry calls a worker's NextRetry or a RetryPolicy, recovering a panic
// as the zero time, which passes the choice on: it runs on the job's
// goroutine, outside the worker's panic guard.
func (c *Client[TTx]) nextRetry(row *driver.JobRow, next func() time.Time) (at time.Time) {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("hopper: NextRetry panicked; using the next retry policy",
				"job_id", row.ID, "kind", row.Kind, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			at = time.Time{}
		}
	}()
	return next()
}

// maxBackoff caps the default retry delay.
const maxBackoff = 24 * time.Hour

// defaultBackoff returns attempt^4 + 5s with ±10% jitter, capped at 24h.
// attempt is the attempt that just failed, starting at 1.
func defaultBackoff(attempt int) time.Duration {
	secs := jitter(math.Pow(float64(attempt), 4) + 5)
	if secs > maxBackoff.Seconds() {
		return maxBackoff
	}
	return time.Duration(secs * float64(time.Second))
}

// jitter spreads x by ±10%.
func jitter(x float64) float64 {
	return x * (0.9 + rand.Float64()*0.2)
}
