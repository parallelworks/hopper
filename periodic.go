package hopper

import (
	"context"
	"encoding/json"
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
	// fewer than those. Either way at most the 10000 most recent missed
	// slots are inserted.
	CatchUp int
	// Location evaluates a cron expression in this zone when the
	// expression does not name one. Defaults to UTC.
	Location *time.Location
	// InsertOpts are applied to every inserted job. A UniqueOpts.OnConflict
	// of UniqueReplace is treated as UniqueSkip: a slot whose job is a
	// duplicate keeps the existing job.
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
// Config.Periodic. The client records the set under its lease, and the
// leader schedules the jobs of every live client, for each name the newest
// client's definition: a job runs while any live client registers it. A
// change applies at once on the leader, and within a leader interval on
// another client. A job whose schedule cannot be recorded, one other than
// Cron's or Every's, or a Cron in time.Local or in a zone that does not load
// by its name, such as a time.FixedZone, is scheduled only from its own
// client, while that client leads. So is a job whose insert differs each
// time it is built, as with a Codec that encrypts with a random nonce or an
// InsertOpts method that reads the clock, or whose name, args or options
// hold a NUL. A Cron in a named zone runs only while a client that can load
// that zone leads, so every client that can lead needs the zone database.
// A client with no queues holds no lease and records nothing. It is safe for
// concurrent use.
type PeriodicJobs struct {
	logger     *slog.Logger
	insertOnly bool
	// validate, if set, checks that a job's insert can be built, so that
	// Add rejects a job that would fail at every slot.
	validate func(*PeriodicJob) error
	// define, if set, encodes a job's stored definition, nil for one that
	// cannot be stored.
	define func(*PeriodicJob) (json.RawMessage, error)
	// registered is signaled when the set changes, for the client to record
	// it.
	registered chan struct{}

	// recordable, if set, reports whether the client records a job with a
	// definition; a client with StrictKinds records only kinds it works.
	recordable func(*registeredPeriodic) bool

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
	// def is the job's stored definition, empty for a job whose schedule
	// cannot be stored, and digest identifies it (definitionDigest).
	def    string
	digest string
	// params builds the insert for a slot of a job defined by its stored
	// definition; nil for the client's own job, built from its args.
	params func(slot time.Time) (driver.JobInsertParams, error)
	// owner is, for a job defined by its stored definition, the leader's
	// own job of that definition, if the leader has one, so that removing
	// it stops the job at once rather than at the next pass.
	owner *registeredPeriodic
}

// newPeriodicJobs returns a set that starts as jobs, which resolvePeriodic
// has already validated and copied.
func newPeriodicJobs(jobs []PeriodicJob, logger *slog.Logger, insertOnly bool) *PeriodicJobs {
	p := &PeriodicJobs{logger: logger, insertOnly: insertOnly, byName: map[string]*registeredPeriodic{}, changed: make(chan struct{}, 1), registered: make(chan struct{}, 1)}
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
// again. The leader starts scheduling an added job at once when it is added
// on the leader, and within a leader interval when added on another client,
// as if its term had just started: RunOnStart inserts a job and CatchUp
// back-fills missed slots. A job the leader already schedules with the same
// definition, from this client or another, goes on as it was.
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
		if err := p.setDefinition(regs[i]); err != nil {
			return err
		}
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
	if p.insertOnly && len(regs) > 0 {
		p.logger.Debug("hopper: periodic jobs added on a client with no queues are never scheduled; it holds no lease to record them under", "count", len(regs))
	}
	p.notify()
	return nil
}

// Remove unregisters the job with this name on this client. The job stops
// once no live client registers it. A slot being inserted as it is removed
// may still be inserted. The job's last slot stays in the database, so a
// job registered again does not insert a slot twice.
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
	p.record()
}

// record asks the client to record the set now, rather than at its next
// lease renewal interval.
func (p *PeriodicJobs) record() {
	select {
	case p.registered <- struct{}{}:
	default:
	}
}

// setDefinition sets r's stored definition and its digest, empty for a job
// that cannot be stored.
func (p *PeriodicJobs) setDefinition(r *registeredPeriodic) error {
	if p.define == nil {
		return nil
	}
	def, err := p.define(&r.job)
	if err != nil {
		return fmt.Errorf("hopper: periodic job %q: %w", r.job.Name, err)
	}
	r.def, r.digest = string(def), definitionDigest(string(def))
	return nil
}

// registrations returns the definitions of the jobs the client records.
func (p *PeriodicJobs) registrations() []driver.PeriodicRegistration {
	p.mu.Lock()
	defer p.mu.Unlock()
	regs := []driver.PeriodicRegistration{}
	for _, r := range p.jobs {
		if p.records(r) {
			regs = append(regs, driver.PeriodicRegistration{Name: r.job.Name, Definition: json.RawMessage(r.def), Digest: r.digest})
		}
	}
	return regs
}

// records reports whether the client records r, rather than only scheduling
// it from its own set while it leads.
func (p *PeriodicJobs) records(r *registeredPeriodic) bool {
	return r.def != "" && (p.recordable == nil || p.recordable(r))
}

// periodicLoop runs on the leader. It inserts each periodic job's slots as
// they come due, sleeping until the soonest one or until the jobs change.
func (c *Client[TTx]) periodicLoop(ctx context.Context) {
	st := &periodicState{
		next:    map[*registeredPeriodic]time.Time{},
		retryAt: map[*registeredPeriodic]time.Time{},
		stored:  storedPeriodic{rejected: map[string]string{}},
		defined: map[string]*registeredPeriodic{},
	}
	// The first read comes before the first pass, so that a job another
	// client defines differently does not start from this client's own
	// definition first.
	conflicts := map[string]string{}
	st.stored.byName, _ = c.readStored(ctx, conflicts)
	stored := make(chan map[string][]storedDef, 1)
	var wg sync.WaitGroup
	defer wg.Wait()
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wg.Go(func() { c.watchStored(watchCtx, st.stored.byName, conflicts, stored) })

	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-c.periodic.changed:
		case st.stored.byName = <-stored:
		}
		timer.Reset(c.periodicPass(ctx, st))
	}
}

// periodicState is what the periodic loop keeps between its passes in a
// term.
type periodicState struct {
	// next holds the next slot of each job the loop has started scheduling,
	// zero for a job with no more slots.
	next map[*registeredPeriodic]time.Time
	// retryAt holds, on the local clock, when a job whose start or insert
	// failed is tried again. Until then the job waits, so that a failure is
	// retried after a leader interval rather than on every pass that another
	// job's slot or a change wakes.
	retryAt map[*registeredPeriodic]time.Time
	// clockFailed is set when the last pass could not read the clock.
	clockFailed bool
	// stored is the registrations other clients recorded, as last read.
	stored storedPeriodic
	// defined holds, by name, the registrations built from stored
	// definitions that the loop schedules.
	defined map[string]*registeredPeriodic
	// wakeAt is when, on the database clock, the last pass expected the
	// next one, zero if it expected none. A pass that starts more than a
	// lease TTL after it, as when the process was suspended, skips slots as
	// after a failure. It is not on the local clock, whose monotonic reading
	// stops on some systems while they sleep.
	wakeAt time.Time
}

// periodicPass inserts the slots that have come due, starts scheduling the
// jobs added since the last pass, stops those removed and returns how long
// to sleep. A failed read of the clock or the last slots, and a failed
// insert, is retried after a leader interval; while the slots cannot be
// read, the jobs already started go on. A slot that falls more than a lease
// TTL behind because the database failed, as while it cannot be reached, or
// while the loop does not run, as while the process is suspended, is
// treated as missed slots are at the start of a term (see missedSlots). A job behind only because a pass was slow, such as one that
// started another job with many missed slots, inserts every slot.
func (c *Client[TTx]) periodicPass(ctx context.Context, st *periodicState) time.Duration {
	regs := c.effectivePeriodic(ctx, st)
	var added []*registeredPeriodic
	for _, r := range regs {
		if _, ok := st.next[r]; !ok {
			added = append(added, r)
		}
	}
	// Remove, Clear and a changed stored definition mark every registration
	// they drop; one a stored definition shadows is no longer in regs.
	scheduled := make(map[*registeredPeriodic]bool, len(regs))
	for _, r := range regs {
		scheduled[r] = true
	}
	dropped := func(r *registeredPeriodic, _ time.Time) bool { return r.removed.Load() || !scheduled[r] }
	maps.DeleteFunc(st.next, dropped)
	maps.DeleteFunc(st.retryAt, dropped)
	if len(regs) == 0 {
		st.wakeAt = time.Time{}
		return math.MaxInt64
	}

	now, err := c.exec.Now(ctx)
	if err != nil {
		if ctx.Err() == nil {
			c.logger.ErrorContext(ctx, "hopper: periodic: read clock", "error", err)
		}
		st.clockFailed = true
		return c.tuning.leaderInterval
	}
	read := time.Now()
	stale := st.clockFailed || (!st.wakeAt.IsZero() && now.Sub(st.wakeAt) > c.tuning.leaderTTL)
	st.clockFailed = false
	// waiting reports whether a job that failed is not yet due to be tried
	// again.
	waiting := func(r *registeredPeriodic) bool {
		at, ok := st.retryAt[r]
		return ok && read.Before(at)
	}
	retry := func(r *registeredPeriodic) {
		// A job whose owner was removed during the pass did not fail: the
		// removal wakes the next pass, which drops the job or, if another
		// client registers it too, goes on with it at once.
		if r.owner != nil && r.owner.removed.Load() {
			return
		}
		st.retryAt[r] = time.Now().Add(c.tuning.leaderInterval)
	}

	// The jobs already started go first, so that starting an added job with
	// many slots to back-fill does not hold up their slots.
	var soonest time.Time
	floor := now.Add(-c.tuning.leaderTTL)
	for _, r := range regs {
		n, ok := st.next[r]
		if !ok || waiting(r) {
			continue
		}
		// A job held up past the lease TTL by a failing database, or by a
		// loop that did not run, inserts the slots it missed as it would at
		// the start of a term, rather than all of them at once.
		inserted := true
		if _, failed := st.retryAt[r]; (stale || failed) && !n.IsZero() && n.Before(floor) {
			missed := c.missedSlots(&r.job, n.Add(-time.Nanosecond), now)
			n = r.job.Schedule.Next(floor)
			for _, slot := range missed {
				if !c.insertPeriodic(ctx, r, slot) {
					n, inserted = slot, false
					break
				}
				n = r.job.Schedule.Next(slot)
			}
		}
		for inserted && !n.IsZero() && !n.After(now) {
			if !c.insertPeriodic(ctx, r, n) {
				inserted = false
				break
			}
			n = r.job.Schedule.Next(n)
		}
		st.next[r] = n
		if !inserted {
			retry(r)
			continue
		}
		delete(st.retryAt, r)
		if n.After(now) && (soonest.IsZero() || n.Before(soonest)) {
			soonest = n
		}
	}

	var starting []*registeredPeriodic
	for _, r := range added {
		if !waiting(r) {
			starting = append(starting, r)
		}
	}
	if len(starting) > 0 {
		lastSlots, err := c.exec.PeriodicLastSlots(ctx)
		if err != nil {
			if ctx.Err() == nil {
				c.logger.ErrorContext(ctx, "hopper: periodic: read slots", "error", err)
			}
			for _, r := range starting {
				retry(r)
			}
		} else {
			for _, r := range starting {
				n, ok := c.startPeriodic(ctx, r, now, lastSlots)
				if !ok {
					retry(r)
					continue
				}
				delete(st.retryAt, r)
				st.next[r] = n
				if n.After(now) && (soonest.IsZero() || n.Before(soonest)) {
					soonest = n
				}
			}
		}
	}

	sleep := time.Duration(math.MaxInt64)
	if !soonest.IsZero() {
		// Sleep until the slot comes due on the database clock, measured
		// from the read, so that a local clock ahead of or behind the
		// database's neither spins nor oversleeps.
		sleep = soonest.Sub(now) - time.Since(read)
	}
	for _, at := range st.retryAt {
		sleep = min(sleep, time.Until(at))
	}
	st.wakeAt = time.Time{}
	if sleep < math.MaxInt64 {
		st.wakeAt = now.Add(time.Since(read)).Add(max(sleep, 0))
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
	// A schedule with no recorded slot starts fresh. The missed slots end at
	// the latest one not after now, so either way the next slot is the one
	// after now.
	if last, known := lastSlots[p.Name]; known {
		for _, slot := range c.missedSlots(p, last, now) {
			if !c.insertPeriodic(ctx, r, slot) {
				return time.Time{}, false
			}
		}
	}
	if opts.RunOnStart && !c.insertPeriodic(ctx, r, now) {
		return time.Time{}, false
	}
	return p.Schedule.Next(now), true
}

// missedSlots returns the slots of a job after last and not after now that
// it inserts when it starts, oldest first. Missed slots are skipped, except
// those within the last lease TTL, which covers a failover; CatchUp
// back-fills the most recent missed slots instead, never fewer than that
// window holds. Either way they are the most recent, up to maxCatchUp, so
// the job's next slot is after now.
func (c *Client[TTx]) missedSlots(p *PeriodicJob, last, now time.Time) []time.Time {
	window := recentSlots(p.Schedule, maxTime(last, now.Add(-c.tuning.leaderTTL)), now, maxCatchUp)
	if p.Opts == nil || p.Opts.CatchUp <= len(window) {
		return window
	}
	return recentSlots(p.Schedule, last, now, min(p.Opts.CatchUp, maxCatchUp))
}

// maxCatchUp bounds the missed slots a job back-fills when it starts: the
// most recent are kept.
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
	if r.owner != nil && r.owner.removed.Load() {
		// The next pass, which the removal wakes, drops the job, or goes on
		// from this slot if another client registers it too.
		return false
	}
	p := &r.job
	var (
		params driver.JobInsertParams
		err    error
	)
	if r.params != nil {
		params, err = r.params(slot)
	} else {
		params, err = c.periodicParams(p, slot)
	}
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

// periodicParams builds the job a periodic job inserts for a slot. The slot
// is the time a UniqueOpts.ByPeriod key is computed from, so that each slot
// back-filled or retried keys its own period.
func (c *Client[TTx]) periodicParams(p *PeriodicJob, slot time.Time) (driver.JobInsertParams, error) {
	params, _, err := c.buildInsertParams(p.insertParams(), slot)
	return params, err
}

func (p *PeriodicJob) insertParams() InsertParams {
	var insertOpts *InsertOpts
	if p.Opts != nil {
		insertOpts = p.Opts.InsertOpts
	}
	return InsertParams{Args: p.Args, Opts: insertOpts}
}
