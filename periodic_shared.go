package hopper

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

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
// that cannot be stored, which only its own client schedules: one whose
// schedule cannot be, whose insert differs each time it is built, as with
// a Codec that encrypts with a random nonce, whose insert sets ScheduledAt,
// which is usually read from the clock and would be frozen at Add, or whose
// name or definition has a NUL or invalid UTF-8, which Postgres refuses and
// encoding/json replaces in a string.
func (c *Client[TTx]) periodicDefinition(p *PeriodicJob) (json.RawMessage, error) {
	d := periodicDefinition{Version: periodicDefinitionVersion}
	switch s := p.Schedule.(type) {
	case *CronSchedule:
		if !loadsByName(s.loc) {
			return nil, nil
		}
		d.Cron, d.Location = s.spec, s.loc.String()
	case IntervalSchedule:
		d.Every = s.Interval
	case *IntervalSchedule:
		d.Every = s.Interval
	default:
		return nil, nil
	}
	if p.Opts != nil {
		d.RunOnStart, d.CatchUp = p.Opts.RunOnStart, p.Opts.CatchUp
	}

	job, byPeriod, err := c.definedInsert(p)
	if err != nil {
		return nil, err
	}
	again, _, err := c.definedInsert(p)
	if err != nil || !reflect.DeepEqual(again, job) || !job.ScheduledAt.IsZero() {
		return nil, err
	}
	if slices.ContainsFunc([]string{p.Name, job.Kind, job.Queue, job.OrderingKey, job.PartitionKey, job.UniqueKey}, func(s string) bool { return !storableName(s) }) {
		return nil, nil
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
	if job.UniqueKey != "" {
		d.Job.Unique = &definedUnique{Key: job.UniqueKey, ByPeriod: byPeriod, Finished: job.UniqueFinished}
	}
	def, err := json.Marshal(d)
	if err != nil || !utf8.Valid(def) || hasNULEscape(def) {
		return nil, err
	}
	return def, nil
}

// definedInsert builds p's insert with its unique key's period left out,
// which the leader adds from each slot, and returns that period.
func (c *Client[TTx]) definedInsert(p *PeriodicJob) (driver.JobInsertParams, time.Duration, error) {
	params := p.insertParams()
	opts := params.effectiveOpts()
	var byPeriod time.Duration
	if opts.Unique != nil {
		unique := *opts.Unique
		byPeriod, unique.ByPeriod = unique.ByPeriod, 0
		opts.Unique = &unique
	}
	job, _, err := c.buildInsertParams(InsertParams{Args: params.Args, Opts: &opts}, time.Time{})
	return job, byPeriod, err
}

// hasNULEscape reports whether JSON text holds a \u0000 escape, telling it
// from an escaped backslash followed by "u0000".
func hasNULEscape(b []byte) bool {
	for i := 0; i < len(b); i++ {
		if b[i] == '\\' {
			i++
			if bytes.HasPrefix(b[i:], []byte("u0000")) {
				return true
			}
		}
	}
	return false
}

// loadsByName reports whether the leader, loading loc by its name, gets the
// same zone, as far as its offsets over the next two years show. time.Local
// names the zone of whichever process reads it, and a time.FixedZone's name
// may load as no zone or as one with other offsets, such as "CET".
func loadsByName(loc *time.Location) bool {
	if loc == nil || loc == time.UTC {
		return true
	}
	if loc == time.Local {
		return false
	}
	named, err := time.LoadLocation(loc.String())
	if err != nil {
		return false
	}
	t := time.Now()
	for range 25 {
		_, want := t.In(loc).Zone()
		if _, got := t.In(named).Zone(); got != want {
			return false
		}
		t = t.AddDate(0, 1, 0)
	}
	return true
}

// definitionDigest identifies a definition by the bytes its client encoded.
// The engine may store the JSON reformatted, as Postgres's jsonb does, so
// definitions are compared by digest, never by the JSON read back. It is
// FNV-1a, not a cryptographic hash: digests are compared only among the
// definitions live clients register for one name.
func definitionDigest(def string) string {
	if def == "" {
		return ""
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(def))
	return strconv.FormatUint(h.Sum64(), 16)
}

// definedPeriodic decodes a definition another client stored into a
// registration the leader schedules.
func definedPeriodic(name string, stored storedDef) (*registeredPeriodic, error) {
	raw := json.RawMessage(stored.def)
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
			return nil, fmt.Errorf("time zone %q does not load on this client; a client that can lead needs the zone database, from the system or from importing time/tzdata: %w", d.Location, err)
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
		def:    stored.def,
		digest: stored.digest,
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
	if u := j.Unique; u != nil {
		p.UniqueKey, p.UniqueFinished = u.Key, u.Finished
		if u.ByPeriod > 0 {
			period := periodPart(slot, u.ByPeriod)
			if p.UniqueKey == "*" {
				p.UniqueKey = period
			} else {
				p.UniqueKey += uniqueKeySeparator + period
			}
		}
		if err := checkUniqueKey(p.UniqueKey); err != nil {
			return driver.JobInsertParams{}, err
		}
	}
	return p, nil
}

// periodicRecord is what a client last recorded of its periodic jobs.
type periodicRecord struct {
	mu       sync.Mutex
	clientID int64
	regs     []driver.PeriodicRegistration
	lastErr  string
}

// recordPeriodic writes the client's periodic jobs unless it last wrote
// them under its current lease. They are compared each time rather than on
// change alone, since a worker registered later changes what a StrictKinds
// client records.
func (c *Client[TTx]) recordPeriodic(ctx context.Context) {
	r := &c.recorded
	r.mu.Lock()
	defer r.mu.Unlock()
	id := c.clientID.Load()
	regs := c.periodic.registrations()
	if id == r.clientID && slices.EqualFunc(regs, r.regs, sameRegistration) {
		return
	}
	err := c.exec.PeriodicRegister(ctx, driver.PeriodicRegisterParams{ClientID: id, Registrations: regs})
	switch {
	case err == nil:
		r.clientID, r.regs, r.lastErr = id, regs, ""
		c.logger.DebugContext(ctx, "hopper: recorded periodic jobs", "client_id", id, "count", len(regs))
	case ctx.Err() != nil:
	case err.Error() != r.lastErr:
		r.lastErr = err.Error()
		c.logger.WarnContext(ctx, "hopper: record periodic jobs", "client_id", id, "error", err)
	}
}

// registrationLoop records the client's periodic jobs under its lease, so
// that whichever client leads schedules them. It writes again when the jobs
// change or the client registers a new lease, and retries a failed write
// every lease renewal interval.
func (c *Client[TTx]) registrationLoop(ctx context.Context) {
	ticker := time.NewTicker(c.tuning.leaseRenew)
	defer ticker.Stop()
	for {
		c.recordPeriodic(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-c.periodic.registered:
		}
	}
}

func sameRegistration(a, b driver.PeriodicRegistration) bool {
	return a.Name == b.Name && string(a.Definition) == string(b.Definition)
}

// storedPeriodic is the leader's view of the definitions clients recorded.
type storedPeriodic struct {
	// byName holds each name's definitions, newest live client first.
	byName map[string][]storedDef
	// rejected holds definitions the leader could not decode, logged once.
	rejected map[string]string
}

// storedDef is a definition with the digest its client computed.
type storedDef struct {
	client int64
	digest string
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
	slices.SortStableFunc(rows, func(a, b driver.PeriodicRegistrationRow) int {
		return cmp.Or(b.ClientStartedAt.Compare(a.ClientStartedAt), cmp.Compare(b.ClientID, a.ClientID))
	})
	byName := make(map[string][]storedDef, len(rows))
	for _, r := range rows {
		if defs := byName[r.Name]; len(defs) > 0 && r.Digest != defs[0].digest && conflicts[r.Name] != defs[0].digest {
			conflicts[r.Name] = defs[0].digest
			c.logger.WarnContext(ctx, "hopper: periodic: live clients define a job differently; the newest client's definition is scheduled", "periodic", r.Name)
		}
		byName[r.Name] = append(byName[r.Name], storedDef{client: r.ClientID, digest: r.Digest, def: string(r.Definition)})
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
// the leader reads its own set, not what it recorded, so that its changes
// to the names it recorded apply at once. Its jobs not recorded yet, or
// whose schedule cannot be stored, come from its own set too, unless
// another client records that name. A job the leader registers with the
// definition it schedules is built from its own args at each slot, whichever
// client's row is the newest. A stored job keeps its registration
// while its definition is unchanged, whichever client it comes from, so a
// job moving between clients is not started again.
func (c *Client[TTx]) effectivePeriodic(ctx context.Context, st *periodicState) []*registeredPeriodic {
	self := c.clientID.Load()
	snapshot := c.periodic.snapshot()
	own := make(map[string]*registeredPeriodic, len(snapshot))
	names := slices.Sorted(maps.Keys(st.stored.byName))
	for _, r := range snapshot {
		own[r.job.Name] = r
		if _, stored := st.stored.byName[r.job.Name]; !stored {
			names = append(names, r.job.Name)
		}
	}
	wanted := map[string]storedDef{}
	// owner holds the leader's own jobs whose definitions are wanted.
	owner := map[string]*registeredPeriodic{}
	var out []*registeredPeriodic
	for _, name := range names {
		var source *registeredPeriodic
		for _, d := range st.stored.byName[name] {
			if d.client != self {
				wanted[name] = d
				break
			}
			if r, ok := own[name]; ok && c.periodic.records(r) {
				source = r
				break
			}
		}
		if d, ok := wanted[name]; ok {
			if r, ok := own[name]; ok && r.digest == d.digest && c.periodic.records(r) {
				owner[name] = r
			}
			continue
		}
		if source == nil {
			source = own[name]
		}
		switch {
		case source == nil:
		case !c.periodic.records(source):
			out = append(out, source)
		default:
			wanted[name], owner[name] = ownDef(source), source
		}
	}
	added := map[string]*registeredPeriodic{}
	for _, name := range slices.Sorted(maps.Keys(wanted)) {
		want := wanted[name]
		if r, ok := st.defined[name]; ok && r.digest == want.digest {
			continue
		}
		if st.stored.rejected[name] != want.digest {
			r, err := definedPeriodic(name, want)
			if err == nil {
				added[name] = r
				continue
			}
			st.stored.rejected[name] = want.digest
			c.logger.ErrorContext(ctx, "hopper: periodic: a stored definition cannot be read; scheduling this client's own, if it has one", "periodic", name, "error", err)
		}
		delete(wanted, name)
		delete(owner, name)
		fallback := own[name]
		switch {
		case fallback == nil:
		case !c.periodic.records(fallback):
			out = append(out, fallback)
		case st.defined[name] != nil && st.defined[name].digest == fallback.digest:
			wanted[name], owner[name] = ownDef(fallback), fallback
		default:
			if st.stored.rejected[name] != fallback.digest {
				if r, err := definedPeriodic(name, ownDef(fallback)); err == nil {
					wanted[name], owner[name], added[name] = ownDef(fallback), fallback, r
					continue
				}
			}
			out = append(out, fallback)
		}
	}
	for name, r := range st.defined {
		if wanted[name].digest != r.digest {
			r.removed.Store(true)
			delete(st.defined, name)
		}
	}
	maps.Copy(st.defined, added)
	for _, name := range slices.Sorted(maps.Keys(wanted)) {
		r := st.defined[name]
		r.owner = owner[name]
		out = append(out, r)
	}
	return out
}

func ownDef(r *registeredPeriodic) storedDef {
	return storedDef{digest: r.digest, def: r.def}
}
