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
	// RunOnStart inserts a job as soon as a leader starts scheduling the
	// periodic job, at the start of its term or when the job is added on
	// it, in addition to the regular slots. Its slot is the database time,
	// so an insert that committed but reported an error, as when the
	// connection drops before the reply, is inserted again when retried.
	RunOnStart bool
	// CatchUp inserts up to this many slots missed while no leader was
	// running (for example during a full outage), oldest first, at most
	// 10000. By default missed slots are skipped, except those due since the
	// last one inserted within the last lease TTL, which covers a leader
	// failover and a job removed and added again; CatchUp never inserts
	// fewer than those.
	CatchUp int
	// Location evaluates a cron expression in this zone when the
	// expression does not name one. Defaults to UTC.
	Location *time.Location
	// InsertOpts are applied to every inserted job.
	InsertOpts *InsertOpts
}

// Cron returns a periodic job on a cron expression (see ParseCron). An
// invalid expression is reported by NewClient or PeriodicJobs.Add.
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

// resolvePeriodic returns copies of the jobs, validated and with their
// names filled in. It fails if a job is invalid or two share a name.
func resolvePeriodic(jobs []PeriodicJob) ([]PeriodicJob, error) {
	out := make([]PeriodicJob, len(jobs))
	names := make(map[string]struct{}, len(jobs))
	for i, job := range jobs {
		job = job.clone()
		if err := job.resolve(); err != nil {
			return nil, err
		}
		if _, dup := names[job.Name]; dup {
			return nil, fmt.Errorf("hopper: periodic job %q is listed twice; set PeriodicOpts.Name to tell them apart", job.Name)
		}
		names[job.Name] = struct{}{}
		out[i] = job
	}
	return out, nil
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
	// validate, if set, checks that a job's insert can be built, so that
	// Add rejects a job that would fail at every slot.
	validate func(*PeriodicJob) error

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

// newPeriodicJobs returns a set that starts as jobs, which resolvePeriodic
// has already validated and copied.
func newPeriodicJobs(jobs []PeriodicJob, logger *slog.Logger, neverLeads bool) *PeriodicJobs {
	p := &PeriodicJobs{logger: logger, neverLeads: neverLeads, byName: map[string]*registeredPeriodic{}, changed: make(chan struct{}, 1)}
	for _, job := range jobs {
		r := &registeredPeriodic{job: job}
		p.jobs = append(p.jobs, r)
		p.byName[job.Name] = r
	}
	return p
}

// PeriodicJobs returns the client's periodic jobs.
func (c *Client[TTx]) PeriodicJobs() *PeriodicJobs {
	return c.periodic
}

// Add registers a periodic job. It fails if the job is invalid, including
// if the job it inserts could not be built (as Insert would fail with its
// args and InsertOpts), or if its name (PeriodicOpts.Name, or else the
// args' kind) is already registered; to change a job, Remove it and Add it
// again. A job added while this client is leader is scheduled at once, as if
// the leader had just started: RunOnStart inserts a job and CatchUp
// back-fills missed slots.
func (p *PeriodicJobs) Add(job PeriodicJob) error {
	return p.AddMany([]PeriodicJob{job})
}

// AddMany registers every job, or none if any is invalid or its name is
// taken, including by another of jobs.
func (p *PeriodicJobs) AddMany(jobs []PeriodicJob) error {
	resolved, err := resolvePeriodic(jobs)
	if err != nil {
		return err
	}
	regs := make([]*registeredPeriodic, len(resolved))
	for i, job := range resolved {
		if err := p.check(&job); err != nil {
			return err
		}
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

// Remove unregisters the job with this name on this client only: remove it
// on every client that can lead, or a leader change to a client that still
// has it brings it back. A slot being inserted as it is removed may still be
// inserted. The job's last slot stays in the database, so a leader that
// still has the job does not insert a slot twice.
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

// check reports whether the job's insert can be built.
func (p *PeriodicJobs) check(job *PeriodicJob) error {
	if p.validate == nil {
		return nil
	}
	if err := p.validate(job); err != nil {
		return fmt.Errorf("hopper: periodic job %q: %w", job.Name, err)
	}
	return nil
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

// periodicLoop runs on the leader. It inserts each periodic job's slots as
// they come due, sleeping until the soonest one or until the jobs change.
// next holds the next slot of each job it has started scheduling, zero for
// a job with no more slots.
func (c *Client[TTx]) periodicLoop(ctx context.Context) {
	next := map[*registeredPeriodic]time.Time{}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-c.periodic.changed:
		}
		timer.Reset(c.periodicPass(ctx, next))
	}
}

// periodicPass starts scheduling the jobs added since the last pass, stops
// those removed, inserts the slots that have come due and returns how long
// to sleep. A failed read of the clock or the last slots, and a failed
// insert, is retried after a leader interval; while the slots cannot be
// read, the jobs already started go on. A slot that falls more than a lease
// TTL behind, as while the database cannot be reached, is skipped, as
// missed slots are at the start of a term.
func (c *Client[TTx]) periodicPass(ctx context.Context, next map[*registeredPeriodic]time.Time) time.Duration {
	regs := c.periodic.snapshot()
	var added []*registeredPeriodic
	for _, r := range regs {
		if _, ok := next[r]; !ok {
			added = append(added, r)
		}
	}
	// next holds a removed job only if it holds more than the started jobs
	// still registered.
	if len(next) > len(regs)-len(added) {
		live := make(map[*registeredPeriodic]struct{}, len(regs))
		for _, r := range regs {
			live[r] = struct{}{}
		}
		maps.DeleteFunc(next, func(r *registeredPeriodic, _ time.Time) bool {
			_, ok := live[r]
			return !ok
		})
	}
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
	read := time.Now()
	sleep := time.Duration(math.MaxInt64)
	if len(added) > 0 {
		lastSlots, err := c.exec.PeriodicLastSlots(ctx)
		if err != nil {
			if ctx.Err() == nil {
				c.logger.ErrorContext(ctx, "hopper: periodic: read slots", "error", err)
			}
			sleep = c.tuning.leaderInterval
		} else {
			for _, r := range added {
				if n, ok := c.startPeriodic(ctx, r, now, lastSlots); ok {
					next[r] = n
				} else {
					sleep = c.tuning.leaderInterval
				}
			}
		}
	}

	var soonest time.Time
	floor := now.Add(-c.tuning.leaderTTL)
	for _, r := range regs {
		n, ok := next[r]
		if !ok {
			continue
		}
		// A pass held up past the lease TTL, by a failing database, skips
		// the slots due before it rather than inserting them all at once.
		if !n.IsZero() && n.Before(floor) {
			n = r.job.Schedule.Next(floor)
		}
		for !n.IsZero() && !n.After(now) {
			if !c.insertPeriodic(ctx, r, n) {
				sleep = c.tuning.leaderInterval
				break
			}
			n = r.job.Schedule.Next(n)
		}
		next[r] = n
		if n.After(now) && (soonest.IsZero() || n.Before(soonest)) {
			soonest = n
		}
	}
	if !soonest.IsZero() {
		// Sleep until the slot comes due on the database clock, measured
		// from the read, so that a local clock ahead of or behind the
		// database's neither spins nor oversleeps.
		sleep = min(sleep, soonest.Sub(now)-time.Since(read))
	}
	return sleep
}

// startPeriodic starts scheduling a job at the database time now and
// returns its next slot. It inserts the missed slots that CatchUp or a
// failover back-fills, then RunOnStart's job. RunOnStart goes last because
// its slot is now, and a slot older than the last one recorded is never
// inserted. It reports false if an insert failed, and stops there: the job
// is started again on the next pass, from the slots then recorded.
func (c *Client[TTx]) startPeriodic(ctx context.Context, r *registeredPeriodic, now time.Time, lastSlots map[string]time.Time) (time.Time, bool) {
	p := &r.job
	var opts PeriodicOpts
	if p.Opts != nil {
		opts = *p.Opts
	}
	// Slots missed while no leader ran are skipped, except those due since
	// the last recorded slot within the last lease TTL, which covers a
	// failover. CatchUp back-fills the most recent missed slots instead. A
	// schedule with no recorded slot starts fresh.
	next := p.Schedule.Next(now)
	last, known := lastSlots[p.Name]
	switch {
	case known && opts.CatchUp > 0:
		// CatchUp adds to the failover window's slots, which are the most
		// recent ones, rather than replacing them.
		window := 0
		for slot := p.Schedule.Next(maxTime(last, now.Add(-c.tuning.leaderTTL))); !slot.IsZero() && !slot.After(now) && window < maxCatchUp; slot = p.Schedule.Next(slot) {
			window++
		}
		for _, slot := range recentSlots(p.Schedule, last, now, min(max(opts.CatchUp, window), maxCatchUp)) {
			if !c.insertPeriodic(ctx, r, slot) {
				return time.Time{}, false
			}
		}
	case known && last.Before(now):
		next = p.Schedule.Next(maxTime(last, now.Add(-c.tuning.leaderTTL)))
		for !next.IsZero() && !next.After(now) {
			if !c.insertPeriodic(ctx, r, next) {
				return time.Time{}, false
			}
			next = p.Schedule.Next(next)
		}
	}
	if opts.RunOnStart && !c.insertPeriodic(ctx, r, now) {
		return time.Time{}, false
	}
	return next, true
}

// maxCatchUp bounds the missed slots a job back-fills when it starts.
const maxCatchUp = 10000

// recentSlots returns the up to n most recent slots of s after last and not
// after now, oldest first. It looks back from now over a window that
// doubles until it holds n slots or reaches last, so that a long outage
// costs a few windows of slots rather than every slot since last.
func recentSlots(s Schedule, last, now time.Time, n int) []time.Time {
	span := now.Sub(last)
	for w := min(time.Millisecond, span); ; {
		from := now.Add(-w)
		if w >= span {
			from = last
		}
		var slots []time.Time
		for slot := s.Next(from); !slot.IsZero() && !slot.After(now); slot = s.Next(slot) {
			if len(slots) == 2*n {
				slots = append(slots[:0], slots[n:]...)
			}
			slots = append(slots, slot)
		}
		if len(slots) >= n || !from.After(last) {
			return slots[max(0, len(slots)-n):]
		}
		if w > span/2 {
			w = span
		} else {
			w *= 2
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
// already has or the job has been removed. It reports false if the insert
// failed and the slot is to be tried again. A job that cannot be built is
// logged and its slot skipped, since trying again would fail the same way.
func (c *Client[TTx]) insertPeriodic(ctx context.Context, r *registeredPeriodic, slot time.Time) bool {
	if r.removed.Load() {
		return true
	}
	p := &r.job
	params, err := c.periodicParams(p)
	if err != nil {
		c.logger.ErrorContext(ctx, "hopper: periodic: build job", "periodic", p.Name, "error", err)
		return true
	}
	job, inserted, err := c.exec.PeriodicInsert(ctx, driver.PeriodicInsertParams{Name: p.Name, Slot: slot, Job: params})
	if err != nil {
		if ctx.Err() == nil {
			c.logger.ErrorContext(ctx, "hopper: periodic: insert job", "periodic", p.Name, "slot", slot, "error", err)
		}
		return false
	}
	if inserted {
		c.logger.DebugContext(ctx, "hopper: periodic: inserted job", "periodic", p.Name, "slot", slot, "job_id", job.ID)
		c.wakeQueue(job.Queue)
	}
	return true
}

// periodicParams builds the job a periodic job inserts.
func (c *Client[TTx]) periodicParams(p *PeriodicJob) (driver.JobInsertParams, error) {
	var insertOpts *InsertOpts
	if p.Opts != nil {
		insertOpts = p.Opts.InsertOpts
	}
	params, _, err := c.buildInsertParams(InsertParams{Args: p.Args, Opts: insertOpts}, time.Now())
	return params, err
}
