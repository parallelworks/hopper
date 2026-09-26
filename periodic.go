package hopper

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// PeriodicJob inserts a job on a schedule. The leader computes the slots
// and inserts each at most once cluster-wide, even across leader changes.
// Build one with Cron or Every.
type PeriodicJob struct {
	// Name identifies the job in the periodic table. It defaults to the
	// args' kind and must be unique among a client's periodic jobs.
	Name     string
	Schedule Schedule
	Args     JobArgs
	Opts     *PeriodicOpts

	err error
}

// PeriodicOpts customizes a periodic job.
type PeriodicOpts struct {
	// Name overrides the periodic job's name.
	Name string
	// RunOnStart inserts a job as soon as a leader starts scheduling, in
	// addition to the regular slots.
	RunOnStart bool
	// CatchUp inserts up to this many slots missed while no leader was
	// running (for example during a full outage), oldest first. By default
	// missed slots are skipped, except a slot due in the last few seconds,
	// which covers a leader failover.
	CatchUp int
	// Location evaluates a cron expression in this zone when the
	// expression does not name one. Defaults to UTC.
	Location *time.Location
	// InsertOpts are applied to every inserted job.
	InsertOpts *InsertOpts
}

// Cron returns a periodic job on a cron expression (see ParseCron). An
// invalid expression is reported by NewClient.
func Cron(spec string, args JobArgs, opts *PeriodicOpts) PeriodicJob {
	p := PeriodicJob{Args: args, Opts: opts}
	sched, err := ParseCron(spec)
	if err != nil {
		p.err = err
		return p
	}
	if opts != nil && opts.Location != nil && !hasTimeZone(spec) {
		sched = sched.In(opts.Location)
	}
	p.Schedule = sched
	return p
}

func hasTimeZone(spec string) bool {
	spec = trimLeft(spec)
	return len(spec) > 3 && (spec[:3] == "TZ=" || (len(spec) > 8 && spec[:8] == "CRON_TZ="))
}

func trimLeft(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	return s
}

// Every returns a periodic job that fires every interval, at slots aligned
// to the interval.
func Every(interval time.Duration, args JobArgs, opts *PeriodicOpts) PeriodicJob {
	p := PeriodicJob{Args: args, Opts: opts}
	if interval <= 0 {
		p.err = fmt.Errorf("hopper: Every: interval %s must be positive", interval)
		return p
	}
	p.Schedule = IntervalSchedule{Interval: interval}
	return p
}

// resolve validates the job and fills its name.
func (p *PeriodicJob) resolve() error {
	if p.err != nil {
		return p.err
	}
	if p.Args == nil {
		return errors.New("hopper: periodic job has nil Args")
	}
	if p.Schedule == nil {
		return fmt.Errorf("hopper: periodic job for %q has no Schedule", p.Args.Kind())
	}
	if p.Opts != nil && p.Opts.Name != "" {
		p.Name = p.Opts.Name
	}
	if p.Name == "" {
		p.Name = p.Args.Kind()
	}
	if p.Name == "" {
		return errors.New("hopper: periodic job has an empty name")
	}
	return nil
}

// periodicLoop runs on the leader. It inserts each periodic job's slots as
// they come due, sleeping until the soonest one.
func (c *Client[TTx]) periodicLoop(ctx context.Context) {
	now, err := c.exec.Now(ctx)
	if err != nil {
		if ctx.Err() == nil {
			c.logger.ErrorContext(ctx, "hopper: periodic: read clock", "error", err)
		}
		return
	}
	lastSlots, err := c.exec.PeriodicLastSlots(ctx)
	if err != nil {
		if ctx.Err() == nil {
			c.logger.ErrorContext(ctx, "hopper: periodic: read slots", "error", err)
		}
		return
	}

	type entry struct {
		job  *PeriodicJob
		next time.Time
	}
	entries := make([]*entry, 0, len(c.cfg.Periodic))
	for i := range c.cfg.Periodic {
		p := &c.cfg.Periodic[i]
		var opts PeriodicOpts
		if p.Opts != nil {
			opts = *p.Opts
		}
		if opts.RunOnStart {
			c.insertPeriodic(ctx, p, now)
		}
		// Slots missed while no leader ran are skipped, except those due
		// since the last recorded slot within the last lease TTL, which
		// covers a failover. CatchUp back-fills the most recent missed
		// slots instead. A schedule with no recorded slot starts fresh.
		e := &entry{job: p, next: p.Schedule.Next(now)}
		last, known := lastSlots[p.Name]
		if known && last.Before(now) {
			e.next = p.Schedule.Next(maxTime(last, now.Add(-c.tuning.leaderTTL)))
		}
		if known && opts.CatchUp > 0 {
			var missed []time.Time
			for slot := p.Schedule.Next(last); !slot.IsZero() && !slot.After(now) && len(missed) < 10000; slot = p.Schedule.Next(slot) {
				missed = append(missed, slot)
			}
			if len(missed) > opts.CatchUp {
				missed = missed[len(missed)-opts.CatchUp:]
			}
			for _, slot := range missed {
				c.insertPeriodic(ctx, p, slot)
			}
			e.next = p.Schedule.Next(now)
		}
		entries = append(entries, e)
	}

	for {
		var soonest time.Time
		for _, e := range entries {
			if !e.next.IsZero() && (soonest.IsZero() || e.next.Before(soonest)) {
				soonest = e.next
			}
		}
		if soonest.IsZero() {
			<-ctx.Done()
			return
		}
		// Sleep on the local clock, then act on the database clock.
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(soonest)):
		}
		now, err := c.exec.Now(ctx)
		if err != nil {
			if ctx.Err() == nil {
				c.logger.ErrorContext(ctx, "hopper: periodic: read clock", "error", err)
			}
			continue
		}
		for _, e := range entries {
			for !e.next.IsZero() && !e.next.After(now) {
				c.insertPeriodic(ctx, e.job, e.next)
				e.next = e.job.Schedule.Next(e.next)
			}
		}
	}
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// insertPeriodic inserts the job for one slot, unless another leader
// already has.
func (c *Client[TTx]) insertPeriodic(ctx context.Context, p *PeriodicJob, slot time.Time) {
	var insertOpts *InsertOpts
	if p.Opts != nil {
		insertOpts = p.Opts.InsertOpts
	}
	params, _, err := c.buildInsertParams(InsertParams{Args: p.Args, Opts: insertOpts}, time.Now())
	if err != nil {
		c.logger.ErrorContext(ctx, "hopper: periodic: build job", "periodic", p.Name, "error", err)
		return
	}
	job, inserted, err := c.exec.PeriodicInsert(ctx, driver.PeriodicInsertParams{Name: p.Name, Slot: slot, Job: params})
	if err != nil {
		if ctx.Err() == nil {
			c.logger.ErrorContext(ctx, "hopper: periodic: insert job", "periodic", p.Name, "slot", slot, "error", err)
		}
		return
	}
	if inserted {
		c.logger.DebugContext(ctx, "hopper: periodic: inserted job", "periodic", p.Name, "slot", slot, "job_id", job.ID)
		c.wakeQueue(job.Queue)
	}
}
