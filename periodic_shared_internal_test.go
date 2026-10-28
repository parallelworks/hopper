package hopper

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

type reportArgs struct {
	Tenant string `json:"tenant" hopper:"unique"`
	Note   string `json:"note"`
}

func (reportArgs) Kind() string { return "report" }

func (reportArgs) InsertOpts() InsertOpts {
	return InsertOpts{Queue: "reports", Priority: PriorityHigh, Tags: []string{"from-args"}}
}

// A job inserted from its stored definition is the job its own client
// would insert for the same slot.
func TestPeriodicDefinitionInsertsWhatTheClientWould(t *testing.T) {
	c := newPeriodicTestClient(t, &periodicExec{})
	jobs := map[string]PeriodicJob{
		"plain": Every(time.Minute, tickArgs{}, nil),
		"args options": Every(time.Hour, reportArgs{Tenant: "acme", Note: "n"}, &PeriodicOpts{
			InsertOpts: &InsertOpts{MaxAttempts: 3},
		}),
		"every option": Cron("0 * * * *", reportArgs{Tenant: "acme"}, &PeriodicOpts{InsertOpts: &InsertOpts{
			Queue: "q", Priority: PriorityLow, MaxAttempts: 7,
			Metadata: json.RawMessage(`{"trace":"t"}`), Tags: []string{"a", "b"}, TTL: time.Hour, Await: true,
			OrderingKey: "o", PartitionKey: "p",
			Unique: &UniqueOpts{ByArgs: true, ByQueue: true, ByPeriod: 24 * time.Hour, Finished: []JobState{JobStateCompleted}},
		}}),
		"unique by period only": Every(time.Hour, tickArgs{}, &PeriodicOpts{InsertOpts: &InsertOpts{Unique: &UniqueOpts{ByPeriod: time.Hour}}}),
		"unique with no parts":  Every(time.Hour, tickArgs{}, &PeriodicOpts{InsertOpts: &InsertOpts{Unique: &UniqueOpts{}}}),
	}
	slots := []time.Time{time.Date(2030, 5, 6, 7, 0, 0, 0, time.UTC), time.Date(2030, 5, 7, 23, 0, 0, 0, time.UTC)}
	for name, job := range jobs {
		if err := job.resolve(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		def, err := c.periodicDefinition(&job)
		if err != nil || def == nil {
			t.Fatalf("%s: definition = %s, %v", name, def, err)
		}
		r, err := definedPeriodic(job.Name, storedDef{digest: definitionDigest(string(def)), def: string(def)})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, slot := range slots {
			want, err := c.periodicParams(&job, slot)
			if err != nil {
				t.Fatal(err)
			}
			got, err := r.params(slot)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s at %s:\n got %+v\nwant %+v", name, slot, got, want)
			}
		}
	}
}

func TestPeriodicDefinitionKeepsTheSchedule(t *testing.T) {
	c := newPeriodicTestClient(t, &periodicExec{})
	chicago, err := time.LoadLocation("America/Chicago")
	if err != nil {
		t.Skip(err)
	}
	from := time.Date(2030, 3, 9, 12, 0, 0, 0, time.UTC)
	for name, job := range map[string]PeriodicJob{
		"every":          Every(90*time.Second, tickArgs{}, nil),
		"every pointer":  {Name: "tick", Schedule: &IntervalSchedule{Interval: 90 * time.Second}, Args: tickArgs{}},
		"cron in UTC":    Cron("30 9 * * MON-FRI", tickArgs{}, nil),
		"cron with zone": Cron("CRON_TZ=Asia/Tokyo 0 9 * * *", tickArgs{}, nil),
		"cron with opts": Cron("0 2 * * *", tickArgs{}, &PeriodicOpts{Location: chicago, RunOnStart: true, CatchUp: 3}),
	} {
		if err := job.resolve(); err != nil {
			t.Fatal(err)
		}
		def, err := c.periodicDefinition(&job)
		if err != nil {
			t.Fatal(err)
		}
		r, err := definedPeriodic(job.Name, storedDef{digest: definitionDigest(string(def)), def: string(def)})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for a, b := from, from; a.Before(from.Add(72 * time.Hour)); {
			a, b = job.Schedule.Next(a), r.job.Schedule.Next(b)
			if !a.Equal(b) {
				t.Errorf("%s: next slot %s, from the definition %s", name, a, b)
				break
			}
		}
		if job.Opts != nil && (r.job.Opts.RunOnStart != job.Opts.RunOnStart || r.job.Opts.CatchUp != job.Opts.CatchUp) {
			t.Errorf("%s: options %+v, from the definition %+v", name, job.Opts, r.job.Opts)
		}
	}
}

func TestPeriodicDefinitionLeavesOutWhatCannotBeStored(t *testing.T) {
	c := newPeriodicTestClient(t, &periodicExec{})
	for name, job := range map[string]PeriodicJob{
		"custom schedule": {Name: "custom", Schedule: onStartOnly{}, Args: tickArgs{}},
		"local time zone": Cron("0 9 * * *", tickArgs{}, &PeriodicOpts{Location: time.Local}),
		"fixed zone":      Cron("0 9 * * *", tickArgs{}, &PeriodicOpts{Location: time.FixedZone("UTC+3", 3*3600)}),
		"fixed zone named like one with daylight saving": Cron("0 9 * * *", tickArgs{}, &PeriodicOpts{Location: time.FixedZone("CET", 3600)}),
		"options read from the clock":                    Every(time.Hour, delayedArgs{}, nil),
		"a scheduled time that reads the same twice":     Every(time.Hour, tickArgs{}, &PeriodicOpts{InsertOpts: &InsertOpts{ScheduledAt: time.Now().Truncate(time.Hour).Add(time.Hour)}}),
		"name with a NUL":                                Every(time.Hour, tickArgs{}, &PeriodicOpts{Name: "a\x00b"}),
		"name that is not UTF-8":                         Every(time.Hour, tickArgs{}, &PeriodicOpts{Name: "a\xffb"}),
		"ordering key that is not UTF-8":                 Every(time.Hour, tickArgs{}, &PeriodicOpts{InsertOpts: &InsertOpts{OrderingKey: "a\xffb"}}),
		"metadata that is not UTF-8":                     Every(time.Hour, tickArgs{}, &PeriodicOpts{InsertOpts: &InsertOpts{Metadata: json.RawMessage("{\"k\":\"a\xffb\"}")}}),
	} {
		if err := job.resolve(); err != nil {
			t.Fatal(err)
		}
		if def, err := c.periodicDefinition(&job); err != nil || def != nil {
			t.Errorf("%s: definition = %s, %v; want none", name, def, err)
		}
	}
}

// delayedArgs delays each job it inserts by an hour from its insert.
type delayedArgs struct{}

func (delayedArgs) Kind() string { return "delayed" }

func (delayedArgs) InsertOpts() InsertOpts { return InsertOpts{ScheduledAt: time.Now().Add(time.Hour)} }

// nonceCodec encodes the same args differently each time, as an encrypting
// codec with a random nonce does.
type nonceCodec struct{ n *atomic.Int64 }

func (c nonceCodec) Marshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(fmt.Sprintf("%d:%s", c.n.Add(1), b))
}

func (nonceCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

type textArgs struct{ Text string }

func (textArgs) Kind() string { return "text" }

func TestPeriodicDefinitionLeavesOutUnstableOrNULArgs(t *testing.T) {
	c := newPeriodicTestClient(t, &periodicExec{})
	nul := Every(time.Hour, textArgs{Text: "a\x00b"}, nil)
	if err := nul.resolve(); err != nil {
		t.Fatal(err)
	}
	if def, err := c.periodicDefinition(&nul); err != nil || def != nil {
		t.Errorf("args with a NUL: definition = %s, %v; want none", def, err)
	}
	backslash := Every(time.Hour, textArgs{Text: `a\u0000b`}, nil)
	if err := backslash.resolve(); err != nil {
		t.Fatal(err)
	}
	if def, err := c.periodicDefinition(&backslash); err != nil || def == nil {
		t.Errorf("args with a backslash before u0000, which is no NUL: definition = %s, %v; want one", def, err)
	}

	c.cfg.Codec = nonceCodec{n: new(atomic.Int64)}
	job := Every(time.Hour, textArgs{Text: "a"}, nil)
	if err := job.resolve(); err != nil {
		t.Fatal(err)
	}
	if def, err := c.periodicDefinition(&job); err != nil || def != nil {
		t.Errorf("args a codec encodes differently each time: definition = %s, %v; want none", def, err)
	}
}

// scheduleFunc is a Schedule that a function computes.
type scheduleFunc func(time.Time) time.Time

func (f scheduleFunc) Next(t time.Time) time.Time { return f(t) }

// A nil schedule pointer or function is no schedule, refused when the job
// is added rather than a panic when it is recorded or scheduled.
func TestPeriodicJobWithNilSchedulePointer(t *testing.T) {
	for name, s := range map[string]Schedule{"cron": (*CronSchedule)(nil), "interval": (*IntervalSchedule)(nil), "func": scheduleFunc(nil)} {
		job := PeriodicJob{Schedule: s, Args: tickArgs{}}
		if err := job.resolve(); err == nil {
			t.Errorf("%s: a job with a nil schedule resolved", name)
		}
	}
}

// storedRow is another client's registration of job.
func storedRow(t *testing.T, c *Client[struct{}], client int64, job PeriodicJob) driver.PeriodicRegistrationRow {
	t.Helper()
	if err := job.resolve(); err != nil {
		t.Fatal(err)
	}
	def, err := c.periodicDefinition(&job)
	if err != nil {
		t.Fatal(err)
	}
	return driver.PeriodicRegistrationRow{ClientID: client, PeriodicRegistration: driver.PeriodicRegistration{Name: job.Name, Definition: def, Digest: definitionDigest(string(def))}}
}

// ownDefinition sets the definition of the client's only periodic job, as
// NewClient does, and returns the job.
func ownDefinition(t *testing.T, c *Client[struct{}]) *registeredPeriodic {
	t.Helper()
	c.periodic.define = c.periodicDefinition
	r := c.periodic.jobs[0]
	if err := c.periodic.setDefinition(r); err != nil {
		t.Fatal(err)
	}
	return r
}

// The leader schedules another client's job, starts it again only when its
// definition changes, and stops it when no client registers it.
func TestPeriodicLoopSchedulesStoredDefinitions(t *testing.T) {
	exec := &periodicExec{}
	c := newPeriodicTestClient(t, exec)
	c.clientID.Store(1)
	onStart := func(n int) PeriodicJob {
		return Every(time.Hour, tickArgs{}, &PeriodicOpts{Name: "remote", RunOnStart: true, InsertOpts: &InsertOpts{MaxAttempts: n}})
	}
	exec.set(func(e *periodicExec) {
		e.registrations = []driver.PeriodicRegistrationRow{storedRow(t, c, 2, onStart(1))}
	})
	runPeriodicLoop(t, c)
	eventually(t, func() bool { return exec.inserted("remote") == 1 })

	exec.set(func(e *periodicExec) {
		e.registrations = []driver.PeriodicRegistrationRow{storedRow(t, c, 3, onStart(1)), storedRow(t, c, 2, onStart(1))}
	})
	time.Sleep(100 * time.Millisecond)
	if n := exec.inserted("remote"); n != 1 {
		t.Errorf("the same definition from another client started the job again: %d inserts", n)
	}

	exec.set(func(e *periodicExec) {
		e.registrations = []driver.PeriodicRegistrationRow{storedRow(t, c, 3, onStart(5))}
	})
	eventually(t, func() bool { return exec.inserted("remote") == 2 })
	exec.mu.Lock()
	attempts := exec.inserts[1].Job.MaxAttempts
	exec.mu.Unlock()
	if attempts != 5 {
		t.Errorf("a changed definition inserted MaxAttempts %d, want 5", attempts)
	}

	exec.set(func(e *periodicExec) { e.registrations = nil })
	time.Sleep(100 * time.Millisecond)
	exec.set(func(e *periodicExec) {
		e.registrations = []driver.PeriodicRegistrationRow{storedRow(t, c, 3, onStart(5))}
	})
	eventually(t, func() bool { return exec.inserted("remote") == 3 })
}

// The leader's own changes apply at once, ahead of what it recorded, and a
// job another client also registers keeps running when the leader drops it.
func TestPeriodicLoopOwnChangesApplyAtOnce(t *testing.T) {
	exec := &periodicExec{}
	job := Every(10*time.Millisecond, tickArgs{}, nil)
	c := newPeriodicTestClient(t, exec, job)
	c.clientID.Store(1)
	r := ownDefinition(t, c)
	own := driver.PeriodicRegistrationRow{ClientID: 1, PeriodicRegistration: driver.PeriodicRegistration{Name: "tick", Definition: json.RawMessage(r.def), Digest: r.digest}}
	exec.set(func(e *periodicExec) { e.registrations = []driver.PeriodicRegistrationRow{own} })
	runPeriodicLoop(t, c)
	eventually(t, func() bool { return exec.inserted("tick") >= 2 })

	if err := c.periodic.Remove("tick"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	before := exec.inserted("tick")
	time.Sleep(50 * time.Millisecond)
	if after := exec.inserted("tick"); after > before+1 {
		t.Errorf("a job the leader removed, still in its recorded set, inserted %d more", after-before)
	}

	other := own
	other.ClientID = 2
	exec.set(func(e *periodicExec) { e.registrations = []driver.PeriodicRegistrationRow{own, other} })
	before = exec.inserted("tick")
	eventually(t, func() bool { return exec.inserted("tick") >= before+2 })
}

// A definition the leader cannot read, as from a later release, is skipped,
// and the leader schedules its own job of that name, once.
func TestPeriodicLoopFallsBackFromUnreadableDefinition(t *testing.T) {
	exec := &periodicExec{}
	c := newPeriodicTestClient(t, exec, Every(time.Hour, tickArgs{}, &PeriodicOpts{RunOnStart: true}))
	c.clientID.Store(1)
	ownDefinition(t, c)
	later := driver.PeriodicRegistrationRow{ClientID: 2, PeriodicRegistration: driver.PeriodicRegistration{Name: "tick", Definition: json.RawMessage(`{"v":99}`), Digest: "99"}}
	exec.set(func(e *periodicExec) { e.registrations = []driver.PeriodicRegistrationRow{later} })
	runPeriodicLoop(t, c)
	eventually(t, func() bool { return exec.inserted("tick") == 1 })
	time.Sleep(100 * time.Millisecond)
	if n := exec.inserted("tick"); n != 1 {
		t.Errorf("the fallback started %d times", n)
	}
}

// Removing the leader's own job stops it at once, even within a pass that
// is back-filling its missed slots.
func TestPeriodicLoopOwnRemovalStopsBackFill(t *testing.T) {
	exec := &periodicExec{}
	job := Every(time.Minute, tickArgs{}, &PeriodicOpts{CatchUp: 100})
	c := newPeriodicTestClient(t, exec, job)
	c.clientID.Store(1)
	ownDefinition(t, c)
	exec.set(func(e *periodicExec) {
		e.last = map[string]time.Time{"tick": time.Now().Add(-3 * time.Hour)}
		e.onInsert = func() {
			if err := c.periodic.Remove("tick"); err != nil && !errors.Is(err, ErrNotFound) {
				t.Error(err)
			}
		}
	})
	runPeriodicLoop(t, c)
	eventually(t, func() bool { return exec.inserted("tick") >= 1 })
	time.Sleep(100 * time.Millisecond)
	if n := exec.inserted("tick"); n != 1 {
		t.Errorf("a job removed during its back-fill inserted %d slots, want 1", n)
	}
}

// Removing the leader's own job during its back-fill, while an older client
// registers the same definition, hands the job over at once rather than
// after a leader interval.
func TestPeriodicLoopOwnRemovalHandsOverAtOnce(t *testing.T) {
	exec := &periodicExec{}
	job := Every(time.Minute, tickArgs{}, &PeriodicOpts{CatchUp: 100})
	c := newPeriodicTestClient(t, exec, job)
	c.tuning.leaderInterval = time.Hour
	c.clientID.Store(1)
	ownDefinition(t, c)
	started := time.Now()
	own, other := storedRow(t, c, 1, job), storedRow(t, c, 2, job)
	own.ClientStartedAt, other.ClientStartedAt = started.Add(time.Second), started
	exec.set(func(e *periodicExec) {
		e.registrations = []driver.PeriodicRegistrationRow{own, other}
		e.last = map[string]time.Time{"tick": time.Now().Add(-3 * time.Hour)}
		e.onInsert = func() {
			if err := c.periodic.Remove("tick"); err != nil && !errors.Is(err, ErrNotFound) {
				t.Error(err)
			}
		}
	})
	runPeriodicLoop(t, c)
	eventually(t, func() bool { return exec.inserted("tick") >= 3 })
}

// The newest live client's definition of a name comes first, whatever order
// the driver returns the rows in.
func TestReadStoredNewestClientFirst(t *testing.T) {
	exec := &periodicExec{}
	c := newPeriodicTestClient(t, exec)
	started := time.Now()
	row := func(client int64, at time.Time, n int) driver.PeriodicRegistrationRow {
		r := storedRow(t, c, client, Every(time.Hour, tickArgs{}, &PeriodicOpts{InsertOpts: &InsertOpts{MaxAttempts: n}}))
		r.ClientStartedAt = at
		return r
	}
	exec.set(func(e *periodicExec) {
		e.registrations = []driver.PeriodicRegistrationRow{row(3, started, 1), row(1, started.Add(time.Second), 2), row(2, started, 3)}
	})
	byName, ok := c.readStored(t.Context(), map[string]string{})
	if !ok {
		t.Fatal("read failed")
	}
	var clients []int64
	for _, d := range byName["tick"] {
		clients = append(clients, d.client)
	}
	if want := []int64{1, 3, 2}; !slices.Equal(clients, want) {
		t.Errorf("clients %v, want %v: the latest started first, then the highest ID", clients, want)
	}
}

// The leader's own row that its job no longer records, as after the job was
// removed and added again with a schedule that cannot be stored, does not
// shadow another client's definition of the name until the next read.
func TestEffectivePeriodicSkipsOwnRowItNoLongerRecords(t *testing.T) {
	exec := &periodicExec{}
	c := newPeriodicTestClient(t, exec, PeriodicJob{Name: "tick", Schedule: onStartOnly{}, Args: tickArgs{}})
	c.clientID.Store(1)
	ownDefinition(t, c)
	started := time.Now()
	stale, other := storedRow(t, c, 1, Every(time.Hour, tickArgs{}, nil)), storedRow(t, c, 2, Every(time.Minute, tickArgs{}, nil))
	stale.ClientStartedAt, other.ClientStartedAt = started.Add(time.Second), started
	exec.set(func(e *periodicExec) { e.registrations = []driver.PeriodicRegistrationRow{stale, other} })
	byName, ok := c.readStored(t.Context(), map[string]string{})
	if !ok {
		t.Fatal("read failed")
	}
	st := &periodicState{stored: storedPeriodic{byName: byName, rejected: map[string]string{}}, defined: map[string]*registeredPeriodic{}}
	regs := c.effectivePeriodic(t.Context(), st)
	if len(regs) != 1 {
		t.Fatalf("scheduled %d jobs, want 1", len(regs))
	}
	if regs[0].digest != other.Digest {
		t.Errorf("scheduled the definition of digest %q, want the other client's, %q", regs[0].digest, other.Digest)
	}
}

// stampArgs takes its MaxAttempts from n each time its insert is built.
type stampArgs struct{ n *atomic.Int64 }

func (stampArgs) Kind() string { return "stamp" }

func (a stampArgs) InsertOpts() InsertOpts { return InsertOpts{MaxAttempts: int(a.n.Load())} }

// A job the leader registers with the definition it schedules is built from
// its own args at each slot even when a newer client's row of that
// definition is read first, as a leader built its own jobs before.
func TestPeriodicLoopBuildsOwnJobBehindANewerClient(t *testing.T) {
	exec := &periodicExec{}
	attempts := new(atomic.Int64)
	attempts.Store(1)
	c := newPeriodicTestClient(t, exec, Every(time.Hour, stampArgs{n: attempts}, &PeriodicOpts{RunOnStart: true}))
	c.clientID.Store(1)
	r := ownDefinition(t, c)
	started := time.Now()
	own := driver.PeriodicRegistrationRow{ClientID: 1, ClientStartedAt: started, PeriodicRegistration: driver.PeriodicRegistration{Name: "stamp", Definition: json.RawMessage(r.def), Digest: r.digest}}
	newer := own
	newer.ClientID, newer.ClientStartedAt = 2, started.Add(time.Second)
	exec.set(func(e *periodicExec) { e.registrations = []driver.PeriodicRegistrationRow{newer, own} })
	attempts.Store(5)
	runPeriodicLoop(t, c)
	eventually(t, func() bool { return exec.inserted("stamp") == 1 })
	exec.mu.Lock()
	got := exec.inserts[0].Job.MaxAttempts
	exec.mu.Unlock()
	if got != 5 {
		t.Errorf("the leader inserted MaxAttempts %d, frozen in the definition, want 5 from its own args", got)
	}
}
