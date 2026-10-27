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
	scheduled := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	jobs := map[string]PeriodicJob{
		"plain": Every(time.Minute, tickArgs{}, nil),
		"args options": Every(time.Hour, reportArgs{Tenant: "acme", Note: "n"}, &PeriodicOpts{
			InsertOpts: &InsertOpts{MaxAttempts: 3},
		}),
		"every option": Cron("0 * * * *", reportArgs{Tenant: "acme"}, &PeriodicOpts{InsertOpts: &InsertOpts{
			Queue: "q", Priority: PriorityLow, MaxAttempts: 7, ScheduledAt: scheduled,
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
	} {
		if err := job.resolve(); err != nil {
			t.Fatal(err)
		}
		if def, err := c.periodicDefinition(&job); err != nil || def != nil {
			t.Errorf("%s: definition = %s, %v; want none", name, def, err)
		}
	}
}

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

	c.cfg.Codec = nonceCodec{n: new(atomic.Int64)}
	job := Every(time.Hour, textArgs{Text: "a"}, nil)
	if err := job.resolve(); err != nil {
		t.Fatal(err)
	}
	if def, err := c.periodicDefinition(&job); err != nil || def != nil {
		t.Errorf("args a codec encodes differently each time: definition = %s, %v; want none", def, err)
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
	c.periodic.define = c.periodicDefinition
	r := c.periodic.jobs[0]
	def, err := c.periodicDefinition(&r.job)
	if err != nil {
		t.Fatal(err)
	}
	r.def, r.digest = string(def), definitionDigest(string(def))
	own := driver.PeriodicRegistrationRow{ClientID: 1, PeriodicRegistration: driver.PeriodicRegistration{Name: "tick", Definition: def, Digest: definitionDigest(string(def))}}
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
	r := c.periodic.jobs[0]
	def, err := c.periodicDefinition(&r.job)
	if err != nil {
		t.Fatal(err)
	}
	r.def, r.digest = string(def), definitionDigest(string(def))
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
	c.periodic.define = c.periodicDefinition
	r := c.periodic.jobs[0]
	def, err := c.periodicDefinition(&r.job)
	if err != nil {
		t.Fatal(err)
	}
	r.def, r.digest = string(def), definitionDigest(string(def))
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
	c.periodic.define = c.periodicDefinition
	r := c.periodic.jobs[0]
	def, err := c.periodicDefinition(&r.job)
	if err != nil {
		t.Fatal(err)
	}
	r.def, r.digest = string(def), definitionDigest(string(def))
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
