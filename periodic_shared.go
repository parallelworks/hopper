package hopper

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// periodicDefinitionVersion is the version of the definition a client
// stores. A leader skips a definition of a later version than it knows.
const periodicDefinitionVersion = 1

// periodicDefinition is a periodic job as a client stores it for the
// leader: everything the leader needs to insert its slots without the
// job's Go types.
type periodicDefinition struct {
	Version    int           `json:"v"`
	Cron       string        `json:"cron,omitempty"`
	Location   string        `json:"location,omitempty"`
	Every      time.Duration `json:"every,omitempty"`
	RunOnStart bool          `json:"run_on_start,omitempty"`
	CatchUp    int           `json:"catch_up,omitempty"`
	Job        definedJob    `json:"job"`
}

// definedJob is the slot-independent part of a periodic job's insert.
type definedJob struct {
	Kind         string          `json:"kind"`
	Queue        string          `json:"queue"`
	Priority     int             `json:"priority"`
	MaxAttempts  int             `json:"max_attempts"`
	ScheduledAt  *time.Time      `json:"scheduled_at,omitempty"`
	Args         json.RawMessage `json:"args"`
	Metadata     json.RawMessage `json:"metadata,omitempty"`
	Tags         []string        `json:"tags,omitempty"`
	TTL          time.Duration   `json:"ttl,omitempty"`
	Await        bool            `json:"await,omitempty"`
	OrderingKey  string          `json:"ordering_key,omitempty"`
	PartitionKey string          `json:"partition_key,omitempty"`
	// Unique is set for a unique job. Its key is the key without the
	// period, which the leader adds from each slot.
	Unique *definedUnique `json:"unique,omitempty"`
}

type definedUnique struct {
	Key      string        `json:"key"`
	ByPeriod time.Duration `json:"by_period,omitempty"`
	Finished []JobState    `json:"finished,omitempty"`
}

// periodicDefinition encodes p for the leader. It returns nil for a job
// whose schedule cannot be stored, which only its own client schedules.
func (c *Client[TTx]) periodicDefinition(p *PeriodicJob) (json.RawMessage, error) {
	d := periodicDefinition{Version: periodicDefinitionVersion}
	switch s := p.Schedule.(type) {
	case *CronSchedule:
		// time.Local names the zone of whichever process reads it.
		if s.loc == time.Local {
			return nil, nil
		}
		d.Cron, d.Location = s.spec, s.loc.String()
	case IntervalSchedule:
		d.Every = s.Interval
	default:
		return nil, nil
	}
	if p.Opts != nil {
		d.RunOnStart, d.CatchUp = p.Opts.RunOnStart, p.Opts.CatchUp
	}

	params := p.insertParams()
	opts := params.effectiveOpts()
	var byPeriod time.Duration
	if opts.Unique != nil {
		unique := *opts.Unique
		byPeriod, unique.ByPeriod = unique.ByPeriod, 0
		opts.Unique = &unique
	}
	job, _, err := c.buildInsertParams(InsertParams{Args: params.Args, Opts: &opts}, time.Time{})
	if err != nil {
		return nil, err
	}
	d.Job = definedJob{
		Kind:         job.Kind,
		Queue:        job.Queue,
		Priority:     job.Priority,
		MaxAttempts:  job.MaxAttempts,
		Args:         job.Args,
		Metadata:     job.Metadata,
		Tags:         job.Tags,
		TTL:          job.TTL,
		Await:        job.Await,
		OrderingKey:  job.OrderingKey,
		PartitionKey: job.PartitionKey,
	}
	if !job.ScheduledAt.IsZero() {
		d.Job.ScheduledAt = &job.ScheduledAt
	}
	if opts.Unique != nil {
		d.Job.Unique = &definedUnique{Key: job.UniqueKey, ByPeriod: byPeriod, Finished: job.UniqueFinished}
	}
	return json.Marshal(d)
}

// definedPeriodic decodes a definition another client stored into a
// registration the leader schedules.
func definedPeriodic(name string, raw json.RawMessage) (*registeredPeriodic, error) {
	var d periodicDefinition
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	if d.Version > periodicDefinitionVersion {
		return nil, fmt.Errorf("definition version %d is newer than this release's %d", d.Version, periodicDefinitionVersion)
	}
	var sched Schedule
	switch {
	case d.Cron != "":
		s, err := ParseCron(d.Cron)
		if err != nil {
			return nil, err
		}
		loc, err := time.LoadLocation(d.Location)
		if err != nil {
			return nil, err
		}
		sched = s.In(loc)
	case d.Every > 0:
		sched = IntervalSchedule{Interval: d.Every}
	default:
		return nil, fmt.Errorf("definition has no schedule")
	}
	job := d.Job
	return &registeredPeriodic{
		job:    PeriodicJob{Name: name, Schedule: sched, Opts: &PeriodicOpts{RunOnStart: d.RunOnStart, CatchUp: d.CatchUp}},
		def:    string(raw),
		params: job.params,
	}, nil
}

// params returns the job's insert for a slot.
func (j definedJob) params(slot time.Time) (driver.JobInsertParams, error) {
	p := driver.JobInsertParams{
		Kind:         j.Kind,
		Queue:        j.Queue,
		Priority:     j.Priority,
		MaxAttempts:  j.MaxAttempts,
		Args:         j.Args,
		Metadata:     j.Metadata,
		Tags:         j.Tags,
		TTL:          j.TTL,
		Await:        j.Await,
		OrderingKey:  j.OrderingKey,
		PartitionKey: j.PartitionKey,
	}
	if j.ScheduledAt != nil {
		p.ScheduledAt = *j.ScheduledAt
	}
	if u := j.Unique; u != nil {
		p.UniqueKey, p.UniqueFinished = u.Key, u.Finished
		if u.ByPeriod > 0 {
			period := "p=" + slot.UTC().Truncate(u.ByPeriod).Format(time.RFC3339)
			if p.UniqueKey == "*" {
				p.UniqueKey = period
			} else {
				p.UniqueKey += uniqueKeySeparator + period
			}
		}
		if len(p.UniqueKey) > maxUniqueKey {
			return driver.JobInsertParams{}, fmt.Errorf("hopper: unique key of %d bytes exceeds the %d byte limit", len(p.UniqueKey), maxUniqueKey)
		}
	}
	return p, nil
}

// registrationLoop records the client's periodic jobs under its lease, so
// that whichever client leads schedules them. It writes again when the jobs
// change or the client registers a new lease, and retries a failed write
// every lease renewal interval.
func (c *Client[TTx]) registrationLoop(ctx context.Context) {
	ticker := time.NewTicker(c.tuning.leaseRenew)
	defer ticker.Stop()
	var (
		writtenID      int64
		writtenVersion uint64
		lastErr        string
	)
	for {
		id := c.clientID.Load()
		version, regs := c.periodic.registrations()
		if id != writtenID || version != writtenVersion {
			err := c.exec.PeriodicRegister(ctx, driver.PeriodicRegisterParams{ClientID: id, Registrations: regs})
			switch {
			case err == nil:
				writtenID, writtenVersion, lastErr = id, version, ""
				c.logger.DebugContext(ctx, "hopper: recorded periodic jobs", "client_id", id, "count", len(regs))
			case ctx.Err() != nil:
				return
			case err.Error() != lastErr:
				lastErr = err.Error()
				c.logger.WarnContext(ctx, "hopper: record periodic jobs", "client_id", id, "error", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-c.periodic.registered:
		}
	}
}

// storedPeriodic is the leader's view of the definitions clients recorded.
type storedPeriodic struct {
	// byName holds each name's definitions, newest live client first.
	byName map[string][]storedDef
	// rejected holds definitions the leader could not decode, logged once.
	rejected map[string]string
}

type storedDef struct {
	client int64
	def    string
}

func equalStored(a, b map[string][]storedDef) bool {
	return maps.EqualFunc(a, b, slices.Equal[[]storedDef])
}

// readStored returns each name's definitions, newest live client first, or
// ok false if the read failed. conflicts holds the names live clients
// define differently, so that each is logged once per newest definition.
func (c *Client[TTx]) readStored(ctx context.Context, conflicts map[string]string) (map[string][]storedDef, bool) {
	rows, err := c.exec.PeriodicRegistrations(ctx)
	if err != nil {
		if ctx.Err() == nil {
			c.logger.ErrorContext(ctx, "hopper: periodic: read registrations", "error", err)
		}
		return nil, false
	}
	byName := make(map[string][]storedDef, len(rows))
	for _, r := range rows {
		def := string(r.Definition)
		if defs := byName[r.Name]; len(defs) > 0 && def != defs[0].def && conflicts[r.Name] != defs[0].def {
			conflicts[r.Name] = defs[0].def
			c.logger.WarnContext(ctx, "hopper: periodic: live clients define a job differently; the newest client's definition is scheduled", "periodic", r.Name)
		}
		byName[r.Name] = append(byName[r.Name], storedDef{client: r.ClientID, def: def})
	}
	return byName, true
}

// watchStored reads the definitions every leader interval and sends them on
// out when they change from last, keeping only the latest unsent.
func (c *Client[TTx]) watchStored(ctx context.Context, last map[string][]storedDef, conflicts map[string]string, out chan map[string][]storedDef) {
	ticker := time.NewTicker(c.tuning.leaderInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		byName, ok := c.readStored(ctx, conflicts)
		if !ok || equalStored(byName, last) {
			continue
		}
		last = byName
		select {
		case <-out:
		default:
		}
		out <- byName
	}
}

// effectivePeriodic returns the jobs the leader schedules: for each name,
// the definition of the newest live client that registers it. For itself
// the leader reads its own set, not what it recorded, so that its own
// changes apply at once; its jobs not recorded yet, or whose schedule cannot
// be stored, come from its own set too. A stored job keeps its registration
// while its definition is unchanged, whichever client it comes from, so a
// job moving between clients is not started again.
func (c *Client[TTx]) effectivePeriodic(ctx context.Context, st *periodicState) []*registeredPeriodic {
	self := c.clientID.Load()
	own := map[string]*registeredPeriodic{}
	for _, r := range c.periodic.snapshot() {
		own[r.job.Name] = r
	}
	wanted := map[string]string{}
	var out []*registeredPeriodic
	names := slices.Sorted(maps.Keys(st.stored.byName))
	for name := range own {
		if _, stored := st.stored.byName[name]; !stored {
			names = append(names, name)
		}
	}
	for _, name := range names {
		var source *registeredPeriodic
		for _, d := range st.stored.byName[name] {
			if d.client != self {
				wanted[name] = d.def
				break
			}
			if r, ok := own[name]; ok {
				source = r
				break
			}
		}
		if _, ok := wanted[name]; ok {
			continue
		}
		if source == nil {
			source = own[name]
		}
		switch {
		case source == nil:
		case source.def == "":
			out = append(out, source)
		default:
			wanted[name] = source.def
		}
	}
	added := map[string]*registeredPeriodic{}
	for _, name := range slices.Sorted(maps.Keys(wanted)) {
		def := wanted[name]
		if r, ok := st.defined[name]; ok && r.def == def {
			continue
		}
		r, err := definedPeriodic(name, json.RawMessage(def))
		if err == nil {
			added[name] = r
			continue
		}
		if st.stored.rejected[name] != def {
			st.stored.rejected[name] = def
			c.logger.ErrorContext(ctx, "hopper: periodic: a stored definition cannot be read; scheduling this client's own, if it has one", "periodic", name, "error", err)
		}
		delete(wanted, name)
		fallback := own[name]
		switch {
		case fallback == nil:
		case fallback.def == "":
			out = append(out, fallback)
		case st.defined[name] != nil && st.defined[name].def == fallback.def:
			wanted[name] = fallback.def
		default:
			if r, err := definedPeriodic(name, json.RawMessage(fallback.def)); err == nil {
				wanted[name], added[name] = fallback.def, r
			}
		}
	}
	for name, r := range st.defined {
		if wanted[name] != r.def {
			r.removed.Store(true)
			delete(st.defined, name)
		}
	}
	maps.Copy(st.defined, added)
	for _, name := range slices.Sorted(maps.Keys(wanted)) {
		out = append(out, st.defined[name])
	}
	return out
}
