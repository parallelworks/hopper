package hopper

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"sync"
	"sync/atomic"
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

// clone returns a copy of the job that shares no options with it. The args
// and schedule are shared.
func (p PeriodicJob) clone() PeriodicJob {
	if p.Opts != nil {
		opts := *p.Opts
		if opts.InsertOpts != nil {
			insertOpts := *opts.InsertOpts
			insertOpts.Metadata = slices.Clone(insertOpts.Metadata)
			insertOpts.Tags = slices.Clone(insertOpts.Tags)
			if insertOpts.Unique != nil {
				unique := *insertOpts.Unique
				unique.Finished = slices.Clone(unique.Finished)
				insertOpts.Unique = &unique
			}
			opts.InsertOpts = &insertOpts
		}
		p.Opts = &opts
	}
	return p
}

// PeriodicJobs is a client's set of periodic jobs, which starts as
// Config.Periodic. Only the leader schedules, from its own set: a job added
// on one client is not seen by the others, so a job that must keep running
// across a leader change is added on every client that can lead, as
// Config.Periodic is. A change takes effect at once on the leader, and on
// another client when it becomes leader. The set is not persisted. It is safe
// for concurrent use.
type PeriodicJobs struct {
	logger     *slog.Logger
	neverLeads bool

	mu      sync.Mutex
	jobs    []*registeredPeriodic
	byName  map[string]*registeredPeriodic
	changed chan struct{}
}

// registeredPeriodic is one Add of a periodic job. A job removed and added
// again is a new registration.
type registeredPeriodic struct {
	job     PeriodicJob
	removed atomic.Bool
}

func newPeriodicJobs(jobs []PeriodicJob, logger *slog.Logger, neverLeads bool) *PeriodicJobs {
	p := &PeriodicJobs{logger: logger, neverLeads: neverLeads, byName: map[string]*registeredPeriodic{}, changed: make(chan struct{}, 1)}
	for _, job := range jobs {
		r := &registeredPeriodic{job: job.clone()}
		p.jobs = append(p.jobs, r)
		p.byName[job.Name] = r
	}
	return p
}

// PeriodicJobs returns the client's periodic jobs.
func (c *Client[TTx]) PeriodicJobs() *PeriodicJobs {
	return c.periodic
}

// Add registers a periodic job. It fails if the job is invalid or its name
// (PeriodicOpts.Name, or else the args' kind) is already registered; to
// change a job, Remove it and Add it again. A job added while this client is
// leader is scheduled at once, as if the leader had just started: RunOnStart
// inserts a job and CatchUp back-fills missed slots.
func (p *PeriodicJobs) Add(job PeriodicJob) error {
	return p.AddMany([]PeriodicJob{job})
}

// AddMany registers every job, or none if any is invalid or its name is
// taken, including by another of jobs.
func (p *PeriodicJobs) AddMany(jobs []PeriodicJob) error {
	regs := make([]*registeredPeriodic, len(jobs))
	names := make(map[string]struct{}, len(jobs))
	for i, job := range jobs {
		job = job.clone()
		if err := job.resolve(); err != nil {
			return err
		}
		if _, dup := names[job.Name]; dup {
			return fmt.Errorf("hopper: periodic job %q is listed twice; set PeriodicOpts.Name to tell them apart", job.Name)
		}
		names[job.Name] = struct{}{}
		regs[i] = &registeredPeriodic{job: job}
	}
	p.mu.Lock()
	for _, r := range regs {
		if _, dup := p.byName[r.job.Name]; dup {
			p.mu.Unlock()
			return fmt.Errorf("hopper: periodic job %q is already registered", r.job.Name)
		}
	}
	for _, r := range regs {
		p.jobs = append(p.jobs, r)
		p.byName[r.job.Name] = r
	}
	p.mu.Unlock()
	if p.neverLeads && len(regs) > 0 {
		p.logger.Debug("hopper: periodic jobs added on a client that never leads are not scheduled by it", "count", len(regs))
	}
	p.notify()
	return nil
}

// Remove unregisters the job with this name. A slot being inserted as it is
// removed may still be inserted. The job's last slot stays in the database,
// so a leader that still has the job does not insert a slot twice.
func (p *PeriodicJobs) Remove(name string) error {
	p.mu.Lock()
	r := p.byName[name]
	if r == nil {
		p.mu.Unlock()
		return fmt.Errorf("hopper: periodic job %q: %w", name, ErrNotFound)
	}
	r.removed.Store(true)
	delete(p.byName, name)
	p.jobs = slices.DeleteFunc(p.jobs, func(j *registeredPeriodic) bool { return j == r })
	p.mu.Unlock()
	p.notify()
	return nil
}

// Clear unregisters every job, including those from Config.Periodic.
func (p *PeriodicJobs) Clear() {
	p.mu.Lock()
	for _, r := range p.jobs {
		r.removed.Store(true)
	}
	p.jobs, p.byName = nil, map[string]*registeredPeriodic{}
	p.mu.Unlock()
	p.notify()
}

// All returns the registered jobs, in the order they were added, with their
// names filled in.
func (p *PeriodicJobs) All() []PeriodicJob {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]PeriodicJob, len(p.jobs))
	for i, r := range p.jobs {
		out[i] = r.job.clone()
	}
	return out
}

func (p *PeriodicJobs) snapshot() []*registeredPeriodic {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.jobs)
}

func (p *PeriodicJobs) notify() {
	select {
	case p.changed <- struct{}{}:
	default:
	}
}

type periodicEntry struct {
	next time.Time
}

// periodicLoop runs on the leader. It inserts each periodic job's slots as
// they come due, sleeping until the soonest one or until the jobs change.
func (c *Client[TTx]) periodicLoop(ctx context.Context) {
	entries := map[*registeredPeriodic]*periodicEntry{}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-c.periodic.changed:
		}
		timer.Reset(c.periodicPass(ctx, entries))
	}
}

// periodicPass starts scheduling the jobs added since the last pass, stops
// those removed, inserts the slots that have come due and returns how long
// to sleep. A failed read of the clock or the last slots is retried after a
// leader interval.
func (c *Client[TTx]) periodicPass(ctx context.Context, entries map[*registeredPeriodic]*periodicEntry) time.Duration {
	regs := c.periodic.snapshot()
	live := make(map[*registeredPeriodic]struct{}, len(regs))
	var added []*registeredPeriodic
	for _, r := range regs {
		live[r] = struct{}{}
		if entries[r] == nil {
			added = append(added, r)
		}
	}
	maps.DeleteFunc(entries, func(r *registeredPeriodic, _ *periodicEntry) bool {
		_, ok := live[r]
		return !ok
	})
	if len(regs) == 0 {
		return math.MaxInt64
	}

	now, err := c.exec.Now(ctx)
	if err != nil {
		if ctx.Err() == nil {
			c.logger.ErrorContext(ctx, "hopper: periodic: read clock", "error", err)
		}
		return c.tuning.leaderInterval
	}
	if len(added) > 0 {
		lastSlots, err := c.exec.PeriodicLastSlots(ctx)
		if err != nil {
			if ctx.Err() == nil {
				c.logger.ErrorContext(ctx, "hopper: periodic: read slots", "error", err)
			}
			return c.tuning.leaderInterval
		}
		for _, r := range added {
			entries[r] = c.startPeriodic(ctx, r, now, lastSlots)
		}
	}

	var soonest time.Time
	for _, r := range regs {
		e := entries[r]
		for !e.next.IsZero() && !e.next.After(now) {
			c.insertPeriodic(ctx, r, e.next)
			e.next = r.job.Schedule.Next(e.next)
		}
		if !e.next.IsZero() && (soonest.IsZero() || e.next.Before(soonest)) {
			soonest = e.next
		}
	}
	if soonest.IsZero() {
		return math.MaxInt64
	}
	// Sleep on the local clock, then act on the database clock.
	return time.Until(soonest)
}

// startPeriodic starts scheduling a job at the database time now: it runs
// RunOnStart and CatchUp, and returns the job's next slot.
func (c *Client[TTx]) startPeriodic(ctx context.Context, r *registeredPeriodic, now time.Time, lastSlots map[string]time.Time) *periodicEntry {
	p := &r.job
	var opts PeriodicOpts
	if p.Opts != nil {
		opts = *p.Opts
	}
	if opts.RunOnStart {
		c.insertPeriodic(ctx, r, now)
	}
	// Slots missed while no leader ran are skipped, except those due since
	// the last recorded slot within the last lease TTL, which covers a
	// failover. CatchUp back-fills the most recent missed slots instead. A
	// schedule with no recorded slot starts fresh.
	e := &periodicEntry{next: p.Schedule.Next(now)}
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
			c.insertPeriodic(ctx, r, slot)
		}
		e.next = p.Schedule.Next(now)
	}
	return e
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// insertPeriodic inserts the job for one slot, unless another leader
// already has or the job has been removed.
func (c *Client[TTx]) insertPeriodic(ctx context.Context, r *registeredPeriodic, slot time.Time) {
	if r.removed.Load() {
		return
	}
	p := &r.job
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
