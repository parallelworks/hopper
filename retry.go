package hopper

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
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
// failed, starting at 1. Returning the zero time selects the default
// backoff.
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
	d := float64(base) * math.Pow(2, float64(max(job.Attempt, 1)-1))
	d *= 0.9 + rand.Float64()*0.2
	return time.Now().Add(time.Duration(min(d, float64(ceiling))))
}

// onError calls Config.OnError for a failed attempt, recovering a panic in
// the hook as the zero ErrorOutcome.
func (c *Client[TTx]) onError(ctx context.Context, row *driver.JobRow, err error) (out ErrorOutcome) {
	if c.cfg.OnError == nil {
		return ErrorOutcome{}
	}
	defer func() {
		if r := recover(); r != nil {
			c.logger.ErrorContext(ctx, "hopper: Config.OnError panicked; keeping the default outcome",
				"job_id", row.ID, "kind", row.Kind, "panic", fmt.Sprint(r))
			out = ErrorOutcome{}
		}
	}()
	return c.cfg.OnError(ctx, row, err)
}

// retryDelay is how long a failed job waits before it runs again: the
// worker's NextRetry, else the queue's RetryPolicy, else the client's, else
// the default backoff.
func (c *Client[TTx]) retryDelay(row *driver.JobRow, qcfg QueueConfig, unit workUnit) time.Duration {
	if unit != nil {
		if at := unit.NextRetry(); !at.IsZero() {
			return max(time.Until(at), 0)
		}
	}
	for _, p := range []RetryPolicy{qcfg.RetryPolicy, c.cfg.RetryPolicy} {
		if p == nil {
			continue
		}
		if at := p.NextRetry(row); !at.IsZero() {
			return max(time.Until(at), 0)
		}
	}
	return defaultBackoff(row.Attempt)
}

// maxBackoff caps the default retry delay.
const maxBackoff = 24 * time.Hour

// defaultBackoff returns attempt^4 + 5s with ±10% jitter, capped at 24h.
// attempt is the attempt that just failed, starting at 1.
func defaultBackoff(attempt int) time.Duration {
	secs := math.Pow(float64(attempt), 4) + 5
	secs *= 0.9 + rand.Float64()*0.2
	if secs > maxBackoff.Seconds() {
		return maxBackoff
	}
	return time.Duration(secs * float64(time.Second))
}
