package drivertest

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

func testLease[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	id, err := exec.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "h", TTL: time.Minute, Info: json.RawMessage(`{"pid":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	res, err := exec.ClientRenew(ctx, driver.ClientRenewParams{ClientID: id, TTL: time.Hour})
	if err != nil || !res.Renewed {
		t.Fatalf("renew = %+v, %v", res, err)
	}
	clients, err := exec.ClientList(ctx)
	if err != nil || len(clients) != 1 || clients[0].ID != id || clients[0].Hostname != "h" || clients[0].ExpiresAt.Before(time.Now().Add(30*time.Minute)) {
		t.Errorf("clients = %v, %v", clients, err)
	}
	f.ExpireLease(ctx, t, d, id)
	res, err = exec.ClientRenew(ctx, driver.ClientRenewParams{ClientID: id, TTL: time.Hour})
	if err != nil || res.Renewed {
		t.Fatalf("renew of expired lease = %+v, %v; want not renewed", res, err)
	}
	if n, err := exec.ClientPruneExpired(ctx); err != nil || n != 1 {
		t.Errorf("prune = %d, %v", n, err)
	}
	other := register(ctx, t, exec)
	if err := exec.ClientDelete(ctx, other); err != nil {
		t.Fatal(err)
	}
	if clients, _ := exec.ClientList(ctx); len(clients) != 0 {
		t.Errorf("%d client rows after delete and prune", len(clients))
	}
}

func testLeader[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	a, b := register(ctx, t, exec), register(ctx, t, exec)
	attempt := func(id int64) bool {
		t.Helper()
		ok, err := exec.LeaderAttempt(ctx, driver.LeaderParams{ClientID: id, TTL: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !attempt(a) {
		t.Fatal("first attempt failed")
	}
	if attempt(b) {
		t.Fatal("b took the lease from a live leader")
	}
	if !attempt(a) {
		t.Fatal("holder could not renew")
	}
	stats, err := exec.Stats(ctx)
	if err != nil || stats.Leader != a {
		t.Errorf("leader in stats = %d, %v; want %d", stats.Leader, err, a)
	}
	// A non-holder's resign is a no-op.
	if err := exec.LeaderResign(ctx, b); err != nil {
		t.Fatal(err)
	}
	if attempt(b) {
		t.Fatal("non-holder's resign freed the lease")
	}
	if err := exec.LeaderResign(ctx, a); err != nil {
		t.Fatal(err)
	}
	if !attempt(b) {
		t.Fatal("b could not take the lease after a resigned")
	}
	f.ExpireLeader(ctx, t, d)
	if !attempt(a) {
		t.Fatal("expired lease could not be taken over")
	}
}

func testRescue[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	insert(ctx, t, exec, params("k", 3))
	live := register(ctx, t, exec)
	dead := register(ctx, t, exec)
	liveJobs := claim(ctx, t, exec, live, 2)
	deadJobs := claim(ctx, t, exec, dead, 1)
	if len(liveJobs) != 2 || len(deadJobs) != 1 {
		t.Fatal("claims")
	}
	candidates := func(stuck time.Duration) []driver.RescueCandidate {
		t.Helper()
		got, err := exec.JobRescueCandidates(ctx, driver.JobRescueParams{StuckAfter: stuck, Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := candidates(0); len(got) != 0 {
		t.Fatalf("candidates with live leases = %v", got)
	}
	f.ExpireLease(ctx, t, d, dead)
	if got := candidates(0); len(got) != 1 || got[0].Job.ID != deadJobs[0].ID || got[0].Job.AttemptedBy != dead ||
		got[0].Job.Fence != deadJobs[0].Fence || got[0].Stuck {
		t.Fatalf("candidates = %v", got)
	}
	if _, err := exec.ClientPruneExpired(ctx); err != nil {
		t.Fatal(err)
	}
	if got := candidates(0); len(got) != 1 {
		t.Fatalf("candidates after prune = %v", got)
	}
	// A job is stuck by database time: its attempt began StuckAfter ago by
	// the database's clock, whatever the caller's.
	f.AgeAttempt(ctx, t, d, liveJobs[0].ID, 10*time.Minute)
	if got := candidates(5 * time.Minute); len(got) != 2 || got[0].Job.ID != liveJobs[0].ID || !got[0].Stuck || got[1].Stuck {
		t.Fatalf("candidates with StuckAfter = %v (oldest first, the first stuck)", got)
	}
	if got := candidates(15 * time.Minute); len(got) != 1 || got[0].Stuck {
		t.Fatalf("candidates with a longer StuckAfter = %v", got)
	}
	// The rescue itself is the fenced finalize.
	applied := finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: deadJobs[0].ID, AttemptedBy: dead, State: driver.JobStateRetryable, Error: &driver.AttemptError{Attempt: 1, Error: "hopper: client lost"}})
	if len(applied) != 1 {
		t.Fatal("rescue finalize not applied")
	}
	if got := candidates(0); len(got) != 0 {
		t.Errorf("rescued job still a candidate: %v", got)
	}
}

func testQueues[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
	if err := exec.QueueEnsure(ctx, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if err := exec.QueueEnsure(ctx, []string{"a"}); err != nil {
		t.Fatal(err)
	}
	if err := exec.QueueEnsure(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := exec.QueuePause(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	if err := exec.QueuePause(ctx, "c"); err != nil { // unknown queue: created paused
		t.Fatal(err)
	}
	queues, err := exec.QueueList(ctx)
	if err != nil || len(queues) != 3 {
		t.Fatalf("queues = %v, %v", queues, err)
	}
	if queues[0].Name != "a" || !queues[0].PausedAt.IsZero() || queues[1].Name != "b" || queues[1].PausedAt.IsZero() || queues[2].PausedAt.IsZero() {
		t.Errorf("queues = %+v %+v %+v", queues[0], queues[1], queues[2])
	}
	res, err := exec.ClientRenew(ctx, driver.ClientRenewParams{ClientID: register(ctx, t, exec), TTL: time.Hour})
	if err != nil || len(res.PausedQueues) != 2 {
		t.Errorf("renew paused queues = %v, %v", res.PausedQueues, err)
	}
	if err := exec.QueueResume(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	queues, _ = exec.QueueList(ctx)
	if !queues[1].PausedAt.IsZero() {
		t.Error("b still paused after resume")
	}
}

func testPeriodic[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	slot := time.Date(2026, 9, 25, 18, 0, 0, 0, time.UTC)
	job := driver.JobInsertParams{Kind: "tick", Queue: "default", Priority: 2, MaxAttempts: 1, Args: json.RawMessage(`{}`)}
	type result struct {
		inserted bool
		err      error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			_, inserted, err := exec.PeriodicInsert(ctx, driver.PeriodicInsertParams{Name: "tick", Slot: slot, Job: job})
			results <- result{inserted, err}
		}()
	}
	var inserted int
	for range 2 {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.inserted {
			inserted++
		}
	}
	if inserted != 1 {
		t.Errorf("slot inserted %d times", inserted)
	}
	if _, ins, err := exec.PeriodicInsert(ctx, driver.PeriodicInsertParams{Name: "tick", Slot: slot.Add(-time.Hour), Job: job}); err != nil || ins {
		t.Errorf("older slot inserted = %v, %v", ins, err)
	}
	if _, ins, err := exec.PeriodicInsert(ctx, driver.PeriodicInsertParams{Name: "tick", Slot: slot.Add(time.Hour), Job: job}); err != nil || !ins {
		t.Errorf("newer slot inserted = %v, %v", ins, err)
	}
	if n := f.CountLive(ctx, t, d); n != 2 {
		t.Errorf("jobs = %d, want 2", n)
	}
	slots, err := exec.PeriodicLastSlots(ctx)
	if err != nil || !slots["tick"].Equal(slot.Add(time.Hour)) {
		t.Errorf("last slots = %v, %v", slots, err)
	}
}

func testPeriodicRegistrations[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	reg := func(name, def string) driver.PeriodicRegistration {
		return driver.PeriodicRegistration{Name: name, Definition: json.RawMessage(def), Digest: "digest of " + def}
	}
	set := func(clientID int64, regs ...driver.PeriodicRegistration) {
		t.Helper()
		if err := exec.PeriodicRegister(ctx, driver.PeriodicRegisterParams{ClientID: clientID, Registrations: regs}); err != nil {
			t.Fatal(err)
		}
	}
	type entry struct {
		client    int64
		name, def string
	}
	listed := func() []entry {
		t.Helper()
		rows, err := exec.PeriodicRegistrations(ctx)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]entry, len(rows))
		for i, r := range rows {
			var def any
			if err := json.Unmarshal(r.Definition, &def); err != nil {
				t.Fatalf("definition %q: %v", r.Definition, err)
			}
			canonical, _ := json.Marshal(def)
			if r.Digest != "digest of "+string(canonical) {
				t.Errorf("registration %q digest = %q, definition %s", r.Name, r.Digest, canonical)
			}
			out[i] = entry{r.ClientID, r.Name, string(canonical)}
		}
		return out
	}

	older, newer := register(ctx, t, exec), register(ctx, t, exec)
	set(older, reg("a", `{"n":1}`), reg("b", `{"n":2}`))
	set(newer, reg("a", `{"n":3}`))
	if got, want := listed(), []entry{{newer, "a", `{"n":3}`}, {older, "a", `{"n":1}`}, {older, "b", `{"n":2}`}}; !slices.Equal(got, want) {
		t.Errorf("registrations = %v, want %v: by name, newest client first", got, want)
	}

	set(older, reg("b", `{"n":4}`))
	if got, want := listed(), []entry{{newer, "a", `{"n":3}`}, {older, "b", `{"n":4}`}}; !slices.Equal(got, want) {
		t.Errorf("after replacing a client's set, registrations = %v, want %v", got, want)
	}

	f.ExpireLease(ctx, t, d, newer)
	if got, want := listed(), []entry{{older, "b", `{"n":4}`}}; !slices.Equal(got, want) {
		t.Errorf("with a lease expired, registrations = %v, want %v", got, want)
	}
	if err := exec.ClientDelete(ctx, older); err != nil {
		t.Fatal(err)
	}
	if got := listed(); len(got) != 0 {
		t.Errorf("registrations of expired and deleted clients = %v", got)
	}
}

func testStats[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
	p := params("k", 4)
	p[3].Queue = "other"
	p[2].ScheduledAt = time.Now().Add(time.Hour)
	insert(ctx, t, exec, p)
	clientID := register(ctx, t, exec)
	claimed := claim(ctx, t, exec, clientID, 1)
	if err := exec.QueueEnsure(ctx, []string{"default", "other"}); err != nil {
		t.Fatal(err)
	}
	if err := exec.QueuePause(ctx, "other"); err != nil {
		t.Fatal(err)
	}
	if _, err := exec.LeaderAttempt(ctx, driver.LeaderParams{ClientID: clientID, TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: claimed[0].ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	if got := claim(ctx, t, exec, clientID, 1); len(got) != 1 {
		t.Fatal("claim")
	}
	stats, err := exec.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	def := stats.Queues["default"]
	if def == nil || def.Available != 0 || def.Scheduled != 1 || def.Running != 1 || def.CompletedLastMinute != 1 || def.Paused {
		t.Errorf("default stats = %+v", def)
	}
	other := stats.Queues["other"]
	if other == nil || other.Available != 1 || !other.Paused || other.OldestAvailable <= 0 {
		t.Errorf("other stats = %+v", other)
	}
	if stats.LiveClients != 1 || stats.Leader != clientID || stats.RunningByClient[clientID] != 1 {
		t.Errorf("cluster stats = %+v", stats)
	}
}

func testNotify[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	if !d.Capabilities().Listen {
		if _, err := d.Listener(ctx); !errors.Is(err, driver.ErrNotSupported) {
			t.Errorf("Listener without the capability = %v, want ErrNotSupported", err)
		}
		t.Skip("driver has no notifications")
	}
	exec := d.Executor()
	l, err := d.Listener(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close(ctx) //nolint:errcheck // cleanup
	if err := l.Listen(ctx, driver.ChannelInsert, driver.ChannelControl, driver.ChannelDone, driver.ChannelLeader); err != nil {
		t.Fatal(err)
	}
	// Channels may be shared with other tests in the same database, so
	// payloads carry a prefix unique to this test and foreign ones are
	// ignored.
	prefix := "dt" + time.Now().Format("150405.000000") + "-"
	next := func(wait time.Duration) map[string]int {
		got := map[string]int{}
		deadline := time.Now().Add(wait)
		for {
			nctx, cancel := context.WithDeadline(ctx, deadline)
			n, err := l.Next(nctx)
			cancel()
			if err != nil {
				return got
			}
			key := n.Channel + ":" + n.Payload
			if idx := indexOf(key, prefix); idx >= 0 {
				got[key[:idx]+key[idx+len(prefix):]]++
			}
		}
	}

	// A transactional insert notifies on commit, once per queue.
	tx, commit, _ := f.Begin(ctx, t, d)
	p := params("k", 3)
	for i := range p {
		p[i].Queue = prefix + "q"
	}
	p[2].Queue = prefix + "r"
	if _, err := d.UnwrapTx(tx).JobInsertMany(ctx, p, driver.JobInsertOpts{Notify: true}); err != nil {
		t.Fatal(err)
	}
	if got := next(300 * time.Millisecond); len(got) != 0 {
		t.Fatalf("notifications %v before commit", got)
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	if got := next(time.Second); got[driver.ChannelInsert+":q"] != 1 || got[driver.ChannelInsert+":r"] != 1 || len(got) != 2 {
		t.Errorf("insert notifications = %v", got)
	}
	// Notify sends one per payload.
	if err := exec.Notify(ctx, driver.ChannelInsert, []string{prefix + "a", prefix + "b"}); err != nil {
		t.Fatal(err)
	}
	if got := next(time.Second); got[driver.ChannelInsert+":a"] != 1 || got[driver.ChannelInsert+":b"] != 1 {
		t.Errorf("Notify payloads = %v", got)
	}
	// Pause and resume announce themselves on the control channel.
	if err := exec.QueuePause(ctx, prefix+"p"); err != nil {
		t.Fatal(err)
	}
	if err := exec.QueueResume(ctx, prefix+"p"); err != nil {
		t.Fatal(err)
	}
	if got := next(time.Second); got[driver.ChannelControl+":pause:p"] != 1 || got[driver.ChannelControl+":resume:p"] != 1 {
		t.Errorf("control notifications = %v", got)
	}
	// Canceling a running job announces it too, and a finalize of an
	// awaited job announces its ID on the done channel.
	res := insert(ctx, t, exec, []driver.JobInsertParams{{Kind: "k", Queue: "default", Priority: 2, MaxAttempts: 1, Await: true}})
	clientID := register(ctx, t, exec)
	if jobs := claim(ctx, t, exec, clientID, 1); len(jobs) != 1 {
		t.Fatal("claim")
	}
	if _, err := exec.JobCancel(ctx, res[0].Job.ID); err != nil {
		t.Fatal(err)
	}
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: res[0].Job.ID, AttemptedBy: clientID, State: driver.JobStateCanceled})
	deadline := time.Now().Add(2 * time.Second)
	var sawCancel, sawDone bool
	for !sawCancel || !sawDone {
		nctx, cancel := context.WithDeadline(ctx, deadline)
		n, err := l.Next(nctx)
		cancel()
		if err != nil {
			t.Fatalf("cancel notification %v, done notification %v", sawCancel, sawDone)
		}
		sawCancel = sawCancel || (n.Channel == driver.ChannelControl && n.Payload == "cancel:"+res[0].Job.ID.String())
		sawDone = sawDone || (n.Channel == driver.ChannelDone && n.Payload == res[0].Job.ID.String())
	}
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// testHistoryIndexes checks that maintenance builds and attaches the
// ordering-key (schema v13) and unique-key (v14) indexes of every history
// partition, including ones a canceled build left invalid, while leaders
// overlap and history is read throughout: no attach may deadlock with a
// reader.
func testHistoryIndexes[TTx any](t *testing.T, f PostgresFixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	p := params("k", 2)
	p[0].OrderingKey = "lane"
	p[1].UniqueKey = "once"
	insert(ctx, t, exec, p)
	clientID := register(ctx, t, exec)
	for _, j := range claim(ctx, t, exec, clientID, 2) {
		finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	}
	unattached := func() string {
		t.Helper()
		return f.QueryText(ctx, t, d, unattachedHistoryIndexes)
	}
	if n := unattached(); n == "0" {
		t.Fatal("no partition is missing its index before maintenance")
	}
	skipped := driver.HistoryIndexParams{Skip: []string{"hopper_job_history_completed_default", "hopper_job_history_failed_default"}}
	if _, err := exec.HistoryIndex(ctx, skipped); err != nil {
		t.Fatal(err)
	}
	if v := f.QueryText(ctx, t, d, `SELECT count(to_regclass(n))::text FROM unnest(ARRAY[
		'hopper_job_history_completed_default_ordering', 'hopper_job_history_failed_default_ordering',
		'hopper_job_history_completed_default_unique', 'hopper_job_history_failed_default_unique']) AS n`); v != "0" {
		t.Error("a skipped partition was indexed")
	}

	// A transaction that read history holds off the attach of an index
	// built (here, before the reader, which a concurrent build would wait
	// for): the pass reports it deferred without waiting for the reader,
	// and leaves the index, valid, for a later pass to attach without
	// building it again. The partition's other index is left for that pass
	// too, rather than built for its attach to wait behind the same reader.
	f.Exec(ctx, t, d, "CREATE INDEX hopper_job_history_failed_default_ordering ON hopper_job_history_failed_default (ordering_key, id) WHERE ordering_key IS NOT NULL")
	reader, commitReader, rollbackReader := f.Begin(ctx, t, d)
	defer rollbackReader() //nolint:errcheck // already committed on the happy path
	if _, err := d.UnwrapTx(reader).JobList(ctx, driver.JobListParams{Queue: "default", OrderingKey: "lane", Limit: 10}); err != nil {
		t.Fatal(err)
	}
	failedOnly := driver.HistoryIndexParams{Skip: []string{"hopper_job_history_completed_default"}, LockTimeout: 50 * time.Millisecond}
	start := time.Now()
	res, err := exec.HistoryIndex(ctx, failedOnly)
	if err != nil || !slices.Equal(res.Deferred, []string{"hopper_job_history_failed_default_ordering"}) {
		t.Errorf("attach behind a reader = %+v, %v", res, err)
	}
	if v := f.QueryText(ctx, t, d, "SELECT (to_regclass('hopper_job_history_failed_default_unique') IS NULL)::text"); v != "true" {
		t.Error("the partition's next index was built behind its deferred attach")
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("attach behind a reader took %s", took)
	}
	built := f.QueryText(ctx, t, d, "SELECT 'hopper_job_history_failed_default_ordering'::regclass::oid::text")
	if v := f.QueryText(ctx, t, d, `SELECT (x.indisvalid AND NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = x.indexrelid))::text
		FROM pg_index x WHERE x.indexrelid = 'hopper_job_history_failed_default_ordering'::regclass`); v != "true" {
		t.Errorf("deferred index valid and unattached = %s", v)
	}
	if err := commitReader(); err != nil {
		t.Fatal(err)
	}
	if res, err := exec.HistoryIndex(ctx, failedOnly); err != nil || len(res.Deferred) != 0 {
		t.Errorf("attach after the reader = %+v, %v", res, err)
	}
	if v := f.QueryText(ctx, t, d, `SELECT (EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = $1::oid))::text`, built); v != "true" {
		t.Error("the deferred index was not attached as built")
	}

	// What a canceled CREATE INDEX CONCURRENTLY leaves: an invalid index
	// under the name maintenance uses.
	for _, index := range []string{"hopper_job_history_completed_default_ordering", "hopper_job_history_completed_default_unique"} {
		f.Exec(ctx, t, d, "CREATE INDEX "+index+" ON hopper_job_history_completed_default (kind)")
		f.Exec(ctx, t, d, "UPDATE pg_index SET indisvalid = false WHERE indexrelid = $1::regclass", index)
	}

	maintainWhileReading(ctx, t, exec)
	if _, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{}); err != nil {
		t.Fatal(err)
	}
	if n := unattached(); n != "0" {
		t.Errorf("%s partitions without an attached index", n)
	}
	for _, parent := range []string{
		"hopper_job_history_completed_ordering", "hopper_job_history_failed_ordering",
		"hopper_job_history_completed_unique", "hopper_job_history_failed_unique",
	} {
		if v := f.QueryText(ctx, t, d, "SELECT indisvalid::text FROM pg_index WHERE indexrelid = $1::regclass", parent); v != "true" {
			t.Errorf("%s valid = %s", parent, v)
		}
	}
	if def := f.QueryText(ctx, t, d, "SELECT pg_get_indexdef('hopper_job_history_completed_default_ordering'::regclass)"); !strings.Contains(def, "(ordering_key, id) WHERE (ordering_key IS NOT NULL)") {
		t.Errorf("rebuilt index = %s", def)
	}
	if def := f.QueryText(ctx, t, d, "SELECT pg_get_indexdef('hopper_job_history_completed_default_unique'::regclass)"); !strings.Contains(def, "(kind, unique_key, finalized_at) WHERE (unique_key IS NOT NULL)") {
		t.Errorf("rebuilt index = %s", def)
	}
	jobs, err := exec.JobList(ctx, driver.JobListParams{Queue: "default", OrderingKey: "lane", Limit: 10})
	if err != nil || len(jobs) != 1 || jobs[0].OrderingKey != "lane" {
		t.Errorf("history by key = %+v, %v", jobs, err)
	}
	if res := insert(ctx, t, exec, []driver.JobInsertParams{onceAgain}); !res[0].Duplicate || res[0].Job.State != driver.JobStateCompleted {
		t.Errorf("history by unique key = %+v", res[0].Job)
	}

	// What an earlier release's retention could leave by dropping the last
	// partition without an index: every partition's index attached, and
	// the outcome table's not valid. Only releases whose attach counts again
	// can repair it; on others, HistoryIndex reports it, and leaves it alone
	// when told to skip it.
	repairs := reattachRevalidates(ctx, t, f, d)
	f.Exec(ctx, t, d, "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'hopper_job_history_failed_ordering'::regclass")

	// A probe that fails, here on a table in its way, is reported once the
	// pass has gone on to the indexes after it: both repairs report it, as
	// a failed repair (driver.ErrHistoryIndexRepair), not a failed pass, and
	// their indexes as invalid, for the leader to back off. The driver
	// keeps a probe's answer until the server restarts, so this is its
	// first: no repair has run before in this test.
	f.Exec(ctx, t, d, "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'hopper_job_history_failed_unique'::regclass")
	f.Exec(ctx, t, d, "CREATE TABLE hopper_revalidate_probe ()")
	if res, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{}); !errors.Is(err, driver.ErrHistoryIndexRepair) ||
		strings.Count(err.Error(), "probe index revalidation") != 2 || len(res.Invalid) != 2 || len(res.Deferred) != 0 {
		t.Errorf("revalidation with its probe failing = %+v, %v", res, err)
	}
	f.Exec(ctx, t, d, "DROP TABLE hopper_revalidate_probe")
	f.Exec(ctx, t, d, "UPDATE pg_index SET indisvalid = true WHERE indexrelid = 'hopper_job_history_failed_unique'::regclass")

	// A transaction that read every history partition holds the re-attach
	// off: where the attach repairs, the pass reports the index deferred,
	// for the next to retry, without waiting for the reader; where it
	// cannot, the pass reports it invalid without trying.
	tx, commit, rollback := f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // already committed on the happy path
	if _, err := d.UnwrapTx(tx).JobList(ctx, driver.JobListParams{Queue: "default", OrderingKey: "lane", Limit: 10}); err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	held, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{LockTimeout: 50 * time.Millisecond})
	wantInvalid, wantDeferred := []string(nil), []string{"hopper_job_history_failed_ordering"}
	if !repairs {
		wantInvalid, wantDeferred = wantDeferred, nil
	}
	if err != nil || !slices.Equal(held.Invalid, wantInvalid) || !slices.Equal(held.Deferred, wantDeferred) {
		t.Errorf("revalidation behind a reader = %+v, %v", held, err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("revalidation behind a reader took %s", took)
	}
	if v := f.QueryText(ctx, t, d, "SELECT indisvalid::text FROM pg_index WHERE indexrelid = 'hopper_job_history_failed_ordering'::regclass"); v != "false" {
		t.Errorf("hopper_job_history_failed_ordering valid behind a reader = %s", v)
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}

	// The re-attach takes the partition furthest ahead, which finalizes do
	// not write to yet: a statement reading every other partition, the
	// DEFAULT one and the current period's among them, does not hold it off.
	// A reader that cannot narrow by finalized_at reads that partition too,
	// and holds the re-attach off for up to the lock timeout.
	if repairs {
		others := f.QueryText(ctx, t, d, `SELECT string_agg(format('(SELECT count(*) FROM %I)', c.relname), ', ')
			FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = 'hopper_job_history_failed'::regclass
			  AND c.relname <> (SELECT max(c.relname) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
			    WHERE i.inhparent = 'hopper_job_history_failed'::regclass AND c.relname <> 'hopper_job_history_failed_default')`)
		released := make(chan struct{})
		go func() {
			defer close(released)
			f.Exec(ctx, t, d, "SELECT pg_sleep(2), "+others)
		}()
		t.Cleanup(func() { <-released })
		for deadline := time.Now().Add(10 * time.Second); f.QueryText(ctx, t, d, `SELECT EXISTS (SELECT FROM pg_locks
			WHERE relation = 'hopper_job_history_failed_default_ordering'::regclass AND pid <> pg_backend_pid())::text`) != "true"; time.Sleep(10 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal("the reader of the other partitions did not start")
			}
		}
		res, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{LockTimeout: 50 * time.Millisecond})
		if err != nil || len(res.Deferred)+len(res.Invalid) != 0 {
			t.Errorf("revalidation beside readers of the other partitions = %+v, %v", res, err)
		}
		<-released
	}

	res, err = exec.HistoryIndex(ctx, driver.HistoryIndexParams{})
	if err != nil {
		t.Fatal(err)
	}
	want := "true"
	if !repairs {
		want = "false"
		if !slices.Equal(res.Invalid, []string{"hopper_job_history_failed_ordering"}) {
			t.Errorf("unrepairable indexes = %q", res.Invalid)
		}
		if res, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{SkipRepair: res.Invalid}); err != nil || len(res.Invalid) != 0 {
			t.Errorf("skipped unrepairable index reported = %+v, %v", res, err)
		}
	} else if len(res.Invalid) != 0 {
		t.Errorf("repaired index reported unrepairable: %q", res.Invalid)
	}
	if v := f.QueryText(ctx, t, d, "SELECT indisvalid::text FROM pg_index WHERE indexrelid = 'hopper_job_history_failed_ordering'::regclass"); v != want {
		t.Errorf("hopper_job_history_failed_ordering valid after revalidation = %s, want %s", v, want)
	}
}

// unattachedHistoryIndexes counts the indexes history partitions lack.
const unattachedHistoryIndexes = `
	SELECT count(*)::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
	CROSS JOIN (VALUES ('_ordering'), ('_unique')) AS s(suffix)
	WHERE i.inhparent IN ('hopper_job_history_completed'::regclass, 'hopper_job_history_failed'::regclass)
	  AND NOT EXISTS (
	    SELECT 1 FROM pg_index x JOIN pg_inherits xi ON xi.inhrelid = x.indexrelid
	    WHERE x.indrelid = c.oid AND xi.inhparent = to_regclass(i.inhparent::regclass::text || s.suffix))`

// onceAgain repeats a unique job that counts its completed duplicates.
var onceAgain = driver.JobInsertParams{
	Kind: "k", Queue: "default", Priority: 2, MaxAttempts: 3, UniqueKey: "once",
	UniqueFinished: []driver.JobState{driver.JobStateCompleted},
}

// reattachRevalidates runs ReattachRevalidates on the fixture's database.
func reattachRevalidates[TTx any](ctx context.Context, t *testing.T, f PostgresFixture[TTx], d driver.Driver[TTx]) bool {
	t.Helper()
	return ReattachRevalidates(
		func(query string) { f.Exec(ctx, t, d, query) },
		func(query string) string { return f.QueryText(ctx, t, d, query) })
}

// ReattachRevalidates reports whether attaching a partition's index again
// marks its parent valid, as Postgres does from the May 2026 releases (18.4,
// 17.10, 16.14, 15.18, 14.23) on, which decides whether HistoryIndex can
// repair an outcome table's index or reports it in
// HistoryIndexResult.Invalid. It runs the driver's own probe: the parent's
// index left invalid by dropping the one partition without an index, then
// attached again. exec runs a statement and queryText returns the single
// text value a query selects, both on a Postgres database a test owns.
func ReattachRevalidates(exec func(query string), queryText func(query string) string) bool {
	exec("CREATE TABLE revalidate_probe (k int) PARTITION BY LIST (k)")
	exec("CREATE TABLE revalidate_probe_1 PARTITION OF revalidate_probe FOR VALUES IN (1)")
	exec("CREATE TABLE revalidate_probe_2 PARTITION OF revalidate_probe FOR VALUES IN (2)")
	exec("CREATE INDEX revalidate_probe_k ON ONLY revalidate_probe (k)")
	exec("CREATE INDEX revalidate_probe_1_k ON revalidate_probe_1 (k)")
	exec("ALTER INDEX revalidate_probe_k ATTACH PARTITION revalidate_probe_1_k")
	exec("DROP TABLE revalidate_probe_2")
	exec("ALTER INDEX revalidate_probe_k ATTACH PARTITION revalidate_probe_1_k")
	valid := queryText("SELECT indisvalid::text FROM pg_index WHERE indexrelid = 'revalidate_probe_k'::regclass") == "true"
	exec("DROP TABLE revalidate_probe")
	return valid
}

// testHistoryRetentionCompletesIndex covers retention dropping the last
// partitions an outcome table's index lacks, after a schema upgrade
// declared it: the index must end valid, which nothing can do afterwards on
// releases before May 2026.
func testHistoryRetentionCompletesIndex[TTx any](t *testing.T, f PostgresFixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	keep := driver.HistoryMaintainParams{FailedRetention: 48 * time.Hour}
	if _, err := exec.HistoryMaintain(ctx, driver.HistoryMaintainParams{}); err != nil {
		t.Fatal(err)
	}
	// Past partitions attached before the ordering-key index was declared,
	// as on an installation upgraded to schema v13.
	redeclare := func(days ...string) []string {
		t.Helper()
		f.Exec(ctx, t, d, "DROP INDEX hopper_job_history_failed_ordering")
		var names []string
		for _, day := range days {
			from, err := time.Parse("20060102", day)
			if err != nil {
				t.Fatal(err)
			}
			name := "hopper_job_history_failed_" + day
			f.Exec(ctx, t, d, "CREATE TABLE "+name+" (LIKE hopper_job_history INCLUDING DEFAULTS INCLUDING CONSTRAINTS)")
			f.Exec(ctx, t, d, "ALTER TABLE hopper_job_history_failed ATTACH PARTITION "+name+" FOR VALUES FROM ('"+
				from.Format(time.RFC3339)+"') TO ('"+from.AddDate(0, 0, 1).Format(time.RFC3339)+"')")
			names = append(names, name)
		}
		f.Exec(ctx, t, d, "CREATE INDEX hopper_job_history_failed_ordering ON ONLY hopper_job_history_failed (ordering_key, id) WHERE ordering_key IS NOT NULL")
		return names
	}
	valid := func() string {
		t.Helper()
		return f.QueryText(ctx, t, d, "SELECT indisvalid::text FROM pg_index WHERE indexrelid = 'hopper_job_history_failed_ordering'::regclass")
	}
	// failedOnly leaves out the other groups' partitions, whose next period
	// (the completed table's hour, the stream's day) may come due during
	// the test and be created, or deferred behind a lock it holds.
	failedOnly := func(names []string) []string {
		return slices.DeleteFunc(slices.Clone(names), func(n string) bool { return !strings.HasPrefix(n, "hopper_job_history_failed_") })
	}

	old := redeclare("20000101")
	if _, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{Skip: old}); err != nil {
		t.Fatal(err)
	}
	if v := valid(); v != "false" {
		t.Fatalf("valid with a partition unindexed = %s", v)
	}
	// Retention keeps the partition until it is indexed.
	res, err := exec.HistoryMaintain(ctx, keep)
	if err != nil || !slices.Contains(res.Kept, old[0]) || slices.Contains(res.Dropped, old[0]) {
		t.Fatalf("maintain with the last unindexed partition past retention = %+v, %v", res, err)
	}
	if v := f.QueryText(ctx, t, d, "SELECT (to_regclass($1) IS NOT NULL)::text", old[0]); v != "true" {
		t.Fatalf("kept partition exists = %s", v)
	}
	if _, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{}); err != nil {
		t.Fatal(err)
	}
	if v := valid(); v != "true" {
		t.Errorf("valid once the kept partition is indexed = %s", v)
	}
	if res, err := exec.HistoryMaintain(ctx, keep); err != nil || !slices.Contains(res.Dropped, old[0]) || len(res.Kept) != 0 {
		t.Errorf("maintain after indexing = %+v, %v", res, err)
	}
	if v := valid(); v != "true" {
		t.Errorf("valid after retention dropped the indexed partition = %s", v)
	}

	// While the DEFAULT partition, which retention never drops, still lacks
	// the index, retention drops the partitions past it that lack it too:
	// the DEFAULT partition's attach completes the index.
	pending := redeclare("20000106", "20000107")
	res, err = exec.HistoryMaintain(ctx, keep)
	if err != nil || !slices.Equal(slices.Sorted(slices.Values(failedOnly(res.Dropped))), pending) || len(res.Kept) != 0 {
		t.Errorf("maintain with the DEFAULT partition unindexed = %+v, %v", res, err)
	}
	if _, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{}); err != nil {
		t.Fatal(err)
	}
	if v := valid(); v != "true" {
		t.Errorf("valid after the DEFAULT partition's build = %s", v)
	}

	// Once the DEFAULT partition has the index, every partition it lacks is
	// kept, not only the last: a partition only ever goes from lacking the
	// index to having it, so retention needs no lock to keep it from
	// dropping the last one while an attach elsewhere commits.
	pending = redeclare("20000108", "20000109")
	if _, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{Skip: pending}); err != nil {
		t.Fatal(err)
	}
	res, err = exec.HistoryMaintain(ctx, keep)
	if err != nil || !slices.Equal(slices.Sorted(slices.Values(res.Kept)), pending) || len(failedOnly(res.Dropped)) != 0 {
		t.Errorf("maintain with two unindexed partitions past retention = %+v, %v", res, err)
	}
	if _, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{}); err != nil {
		t.Fatal(err)
	}
	if res, err := exec.HistoryMaintain(ctx, keep); err != nil || !slices.Equal(slices.Sorted(slices.Values(failedOnly(res.Dropped))), pending) || len(res.Kept) != 0 {
		t.Errorf("maintain after the builds = %+v, %v", res, err)
	}
	if v := valid(); v != "true" {
		t.Errorf("valid after the builds = %s", v)
	}

	// A pass builds the DEFAULT partition last, so that partitions that
	// expire during a long pass are dropped, the DEFAULT partition lacking
	// the index too, rather than kept for a build. A statement whose
	// snapshot the first build waits for shows which partition that is.
	ordered := redeclare("20000110")
	snapshot := make(chan struct{})
	go func() { defer close(snapshot); f.Exec(ctx, t, d, "SELECT pg_sleep(2)") }()
	t.Cleanup(func() { <-snapshot })
	for deadline := time.Now().Add(10 * time.Second); f.QueryText(ctx, t, d,
		"SELECT count(*)::text FROM pg_stat_activity WHERE query = 'SELECT pg_sleep(2)' AND datname = current_database()") == "0"; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the snapshot holder did not start")
		}
	}
	built := make(chan error, 1)
	go func() { _, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{}); built <- err }()
	var first string
	for deadline := time.Now().Add(10 * time.Second); first == ""; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("no build started")
		}
		first = f.QueryText(ctx, t, d, `SELECT coalesce(min(c.relname), '') FROM pg_stat_progress_create_index p
			JOIN pg_class c ON c.oid = p.relid WHERE c.relnamespace = current_schema()::regnamespace`)
	}
	if strings.HasSuffix(first, "_default") {
		t.Errorf("first build of a pass = %s, want a time partition", first)
	}
	if err := <-built; err != nil {
		t.Fatal(err)
	}
	if res, err := exec.HistoryMaintain(ctx, keep); err != nil || !slices.Equal(failedOnly(res.Dropped), ordered) {
		t.Errorf("maintain after the ordered pass = %+v, %v", res, err)
	}

	const (
		buildLock       = "hashtext(current_schema() || '.' || $1), 1752133737" // HistoryIndexLockKey
		maintenanceLock = "hashtext(current_schema()), 1752133736"              // HistoryLockKey
		schemaBuildLock = "hashtext(current_schema()), 1752133737"              // HistoryIndexLockKey, schema-wide
	)
	// holdLock holds the advisory lock key, with args for its parameters,
	// for two seconds, through lock (pg_advisory_xact_lock, exclusively, or
	// pg_advisory_xact_lock_shared), and returns once it is held, with a
	// channel closed once it is released.
	holdLock := func(lock, key string, args ...any) <-chan struct{} {
		t.Helper()
		released := make(chan struct{})
		go func() {
			defer close(released)
			f.Exec(ctx, t, d, "SELECT "+lock+"("+key+"), pg_sleep(2)", args...)
		}()
		// A test that stops early waits for the holder, whose own failure
		// must be reported before the test ends.
		t.Cleanup(func() { <-released })
		for deadline := time.Now().Add(10 * time.Second); f.QueryText(ctx, t, d,
			"SELECT (NOT pg_try_advisory_xact_lock("+key+"))::text", args...) != "true"; time.Sleep(10 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("%s(%s) was not taken", lock, key)
			}
		}
		return released
	}
	// holdTableLock holds HistoryIndexLockKey combined with the failed
	// table through lock: exclusively, as its DEFAULT partition's build
	// does, or in share mode, as retention, partition creation and the
	// builds of its time partitions do.
	holdTableLock := func(lock string) <-chan struct{} {
		t.Helper()
		return holdLock(lock, buildLock, "hopper_job_history_failed")
	}
	// holdMaintenanceLock holds HistoryLockKey, as a stalled leader would.
	holdMaintenanceLock := func() <-chan struct{} { t.Helper(); return holdLock("pg_advisory_xact_lock", maintenanceLock) }
	// indexAllButDefault indexes the failed table's partitions except its
	// DEFAULT partition, which the index then still lacks.
	indexAllButDefault := func() {
		t.Helper()
		if _, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{Skip: []string{"hopper_job_history_failed_default"}}); err != nil {
			t.Fatal(err)
		}
	}

	// A build on the DEFAULT partition holds the outcome table's lock:
	// retention keeps every partition past retention, indexed or not,
	// rather than lock the table and then wait for the DEFAULT partition,
	// which the build holds.
	building := redeclare("20000108")
	indexAllButDefault()
	released := holdTableLock("pg_advisory_xact_lock")
	res, err = exec.HistoryMaintain(ctx, keep)
	if err != nil || !slices.Equal(res.Kept, building) || len(res.Dropped) != 0 {
		t.Errorf("maintain while a build holds the DEFAULT partition = %+v, %v", res, err)
	}
	<-released
	if _, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{}); err != nil {
		t.Fatal(err)
	}
	if res, err := exec.HistoryMaintain(ctx, keep); err != nil || !slices.Equal(res.Dropped, building) || len(res.Kept) != 0 {
		t.Errorf("maintain after the DEFAULT partition's build = %+v, %v", res, err)
	}
	if v := valid(); v != "true" {
		t.Errorf("valid after the DEFAULT partition's build = %s", v)
	}

	// An overlapping leader's retention or partition creation holds the
	// table's lock in share mode: retention drops an indexed partition
	// rather than keep it for a build no one runs, and partition creation
	// creates the furthest one ahead, dropped here, rather than defer it.
	dropping := redeclare("20000109")
	indexAllButDefault()
	// dropAhead drops the failed table's partition furthest ahead, for
	// partition creation to recreate, and returns its name.
	dropAhead := func() string {
		t.Helper()
		ahead := f.QueryText(ctx, t, d, `SELECT max(c.relname) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
			WHERE i.inhparent = 'hopper_job_history_failed'::regclass AND c.relname <> 'hopper_job_history_failed_default'`)
		f.Exec(ctx, t, d, "DROP TABLE "+ahead)
		return ahead
	}
	ahead := dropAhead()
	released = holdTableLock("pg_advisory_xact_lock_shared")
	if res, err := exec.HistoryMaintain(ctx, keep); err != nil || !slices.Equal(res.Dropped, dropping) || !slices.Equal(failedOnly(res.Created), []string{ahead}) ||
		len(res.Kept)+len(failedOnly(res.Deferred)) != 0 {
		t.Errorf("maintain beside another leader's retention = %+v, %v", res, err)
	}
	<-released

	// A build of a time partition holds the table's lock in share mode:
	// retention keeps a partition past it that lacks the index, which may be
	// the one being built, rather than lock the table and wait for the
	// build, though the DEFAULT partition lacks the index too.
	unbuiltPast := redeclare("20000112")
	released = holdTableLock("pg_advisory_xact_lock_shared")
	if res, err := exec.HistoryMaintain(ctx, keep); err != nil || !slices.Equal(failedOnly(res.Kept), unbuiltPast) || len(failedOnly(res.Dropped)) != 0 {
		t.Errorf("maintain while a time partition is being built = %+v, %v", res, err)
	}
	<-released
	if _, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{}); err != nil {
		t.Fatal(err)
	}
	if res, err := exec.HistoryMaintain(ctx, keep); err != nil || !slices.Equal(failedOnly(res.Dropped), unbuiltPast) || len(res.Kept) != 0 {
		t.Errorf("maintain after the time partition's build = %+v, %v", res, err)
	}

	// A transaction left open holding the table's lock holds the DEFAULT
	// partition's build off no longer than the lock timeout: the pass
	// reports the build deferred rather than stall behind it.
	stalled := redeclare("20000110")
	indexAllButDefault()
	released = holdTableLock("pg_advisory_xact_lock_shared")
	if res, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{LockTimeout: 50 * time.Millisecond}); err != nil ||
		!slices.Contains(res.Deferred, "hopper_job_history_failed_default_ordering") {
		t.Errorf("index behind a held table lock = %+v, %v", res, err)
	}
	<-released
	if _, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{}); err != nil {
		t.Fatal(err)
	}
	if res, err := exec.HistoryMaintain(ctx, keep); err != nil || !slices.Equal(res.Dropped, stalled) || len(res.Kept) != 0 {
		t.Errorf("maintain after the deferred build = %+v, %v", res, err)
	}
	if v := valid(); v != "true" {
		t.Errorf("valid after the deferred build = %s", v)
	}

	// A session that holds the schema's build lock, another leader's pass
	// or one that stalled, holds every build off: the pass reports itself
	// held off, so that a holder that never lets go is warned about, and
	// not deferred, which would lengthen the next pass's lock waits.
	unbuilt := redeclare("20000111")
	released = holdLock("pg_advisory_xact_lock", schemaBuildLock)
	if res, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{}); err != nil || res.Busy == "" || len(res.Deferred) != 0 {
		t.Errorf("index while another session holds the schema's build lock = %+v, %v", res, err)
	}
	<-released
	if _, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{}); err != nil {
		t.Fatal(err)
	}
	if res, err := exec.HistoryMaintain(ctx, keep); err != nil || !slices.Equal(res.Dropped, unbuilt) || len(res.Kept) != 0 {
		t.Errorf("maintain after the schema's build lock was let go = %+v, %v", res, err)
	}

	// An overlapping leader that stalls while it holds the history
	// maintenance lock holds partition creation off no longer than the lock
	// timeout: the partition ahead is deferred, not waited for.
	ahead = dropAhead()
	released = holdMaintenanceLock()
	short := keep
	short.LockTimeout = 50 * time.Millisecond
	if res, err := exec.HistoryMaintain(ctx, short); err != nil || !slices.Contains(res.Deferred, ahead) || len(res.Created) != 0 {
		t.Errorf("maintain beside a stalled leader = %+v, %v", res, err)
	}
	<-released
	if res, err := exec.HistoryMaintain(ctx, keep); err != nil || !slices.Equal(failedOnly(res.Created), []string{ahead}) {
		t.Errorf("maintain after the stalled leader = %+v, %v", res, err)
	}

	// Retention dropping partitions while the leader builds the others'.
	for round := range 4 {
		old := redeclare("20000102", "20000103", "20000104", "20000105")
		var wg sync.WaitGroup
		errs := make(chan error, 2)
		wg.Go(func() {
			if _, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{}); err != nil {
				errs <- err
			}
		})
		wg.Go(func() {
			deadline := time.Now().Add(30 * time.Second)
			for {
				res, err := exec.HistoryMaintain(ctx, keep)
				if err != nil {
					errs <- err
					return
				}
				if len(res.Deferred)+len(res.Kept) == 0 || time.Now().After(deadline) {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		if v := f.QueryText(ctx, t, d, "SELECT count(to_regclass(n))::text FROM unnest($1::text[]) n", old); v != "0" {
			t.Fatalf("round %d: %s partitions past retention left", round, v)
		}
		if v := valid(); v != "true" {
			t.Fatalf("round %d: valid after retention and indexing = %s", round, v)
		}
	}

	// An index an earlier release left invalid with every partition indexed,
	// which Postgres before its May 2026 releases cannot repair: no partition
	// can be the last one it lacks, so a drop neither waits for the history
	// maintenance lock nor holds it, however long the index stays invalid.
	indexed := "hopper_job_history_failed_20000111"
	f.Exec(ctx, t, d, "CREATE TABLE "+indexed+" (LIKE hopper_job_history INCLUDING DEFAULTS INCLUDING CONSTRAINTS)")
	f.Exec(ctx, t, d, "ALTER TABLE hopper_job_history_failed ATTACH PARTITION "+indexed+" FOR VALUES FROM ('2000-01-11Z') TO ('2000-01-12Z')")
	f.Exec(ctx, t, d, "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'hopper_job_history_failed_ordering'::regclass")
	maintenanceReleased := holdMaintenanceLock()
	if res, err := exec.HistoryMaintain(ctx, keep); err != nil || !slices.Equal(res.Dropped, []string{indexed}) || len(res.Kept)+len(failedOnly(res.Deferred)) != 0 {
		t.Errorf("maintain with an index left invalid and every partition indexed = %+v, %v", res, err)
	}
	<-maintenanceReleased
}

// maintainWhileReading runs the history maintenance and indexing of three
// overlapping leaders while eight readers list a key's jobs from history and
// look for a unique key's duplicates there, and fails the test on any error
// either side returns: no attach may deadlock with a reader.
func maintainWhileReading(ctx context.Context, t *testing.T, exec driver.Executor) {
	t.Helper()
	var (
		readers  sync.WaitGroup
		stop     = make(chan struct{})
		readErrs = make(chan error, 8)
	)
	for range 8 {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := exec.JobList(ctx, driver.JobListParams{Queue: "default", OrderingKey: "lane", Limit: 10}); err != nil {
					readErrs <- err
					return
				}
				if _, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{onceAgain}, driver.JobInsertOpts{}); err != nil {
					readErrs <- err
					return
				}
			}
		})
	}
	errs := make(chan error, 6)
	for range 3 {
		go func() {
			_, err := exec.HistoryMaintain(ctx, driver.HistoryMaintainParams{CompletedRetention: time.Hour, FailedRetention: time.Hour})
			errs <- err
		}()
		go func() { _, err := exec.HistoryIndex(ctx, driver.HistoryIndexParams{}); errs <- err }()
	}
	for range 6 {
		if err := <-errs; err != nil {
			t.Errorf("concurrent maintenance: %v", err)
		}
	}
	close(stop)
	readers.Wait()
	close(readErrs)
	for err := range readErrs {
		t.Errorf("read during maintenance: %v", err)
	}
}

func testHistoryMaintain[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	insert(ctx, t, exec, params("k", 1))
	clientID := register(ctx, t, exec)
	jobs := claim(ctx, t, exec, clientID, 1)
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: jobs[0].ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	p := driver.HistoryMaintainParams{CompletedRetention: 24 * time.Hour, FailedRetention: 7 * 24 * time.Hour}
	if _, err := exec.HistoryMaintain(ctx, p); err != nil {
		t.Fatal(err)
	}
	// Idempotent, and recent history is untouched.
	res, err := exec.HistoryMaintain(ctx, p)
	if err != nil || len(res.Created) != 0 || len(res.Dropped) != 0 || res.Pruned != 0 {
		t.Errorf("second run = %+v, %v", res, err)
	}
	if _, err := exec.JobGet(ctx, jobs[0].ID); err != nil {
		t.Errorf("recent history row lost: %v", err)
	}
	// Concurrent leaders do not trip over each other.
	maintainWhileReading(ctx, t, exec)
}

func testNow[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	now, err := f.NewDriver(t).Executor().Now(ctx)
	if err != nil || time.Until(now).Abs() > time.Minute {
		t.Errorf("Now = %s, %v", now, err)
	}
}

func testMaintain[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
	// Nothing has changed: nothing to do.
	if res, err := exec.JobsMaintain(ctx); err != nil || res != (driver.JobsMaintainResult{}) {
		t.Fatalf("maintenance of a fresh table = %+v, %v", res, err)
	}
	// A burst of inserts into a table the planner believes empty is worth
	// fresh statistics; the engine's counters may lag by a second or so.
	if _, err := exec.JobsMaintain(ctx); err != nil {
		t.Fatal(err)
	}
	analyzeEmpty(ctx, t, exec)
	insert(ctx, t, exec, params("burst", 6000))
	res := waitMaintain(ctx, t, exec, func(r driver.JobsMaintainResult) bool { return r.Analyzed })
	if res.Vacuumed {
		t.Errorf("vacuumed with nothing dead: %+v", res)
	}
	if res.Analyzed {
		// Once the counters have caught up with the analyze, there is
		// nothing more to do.
		res = waitMaintain(ctx, t, exec, func(r driver.JobsMaintainResult) bool { return r == driver.JobsMaintainResult{} })
		if res != (driver.JobsMaintainResult{}) {
			t.Errorf("maintenance keeps repeating: %+v", res)
		}
	}
	// A burst that has run and left is worth a vacuum only once its dead
	// rows number in the hundreds of thousands, so finishing this one is
	// not.
	clientID := register(ctx, t, exec)
	running := claim(ctx, t, exec, clientID, 6000)
	fin := make([]driver.JobFinalize, len(running))
	for i, j := range running {
		fin[i] = driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateCompleted}
	}
	finalizeCurrent(ctx, t, exec, fin...)
	time.Sleep(time.Second)
	if res, err := exec.JobsMaintain(ctx); err != nil || res.Vacuumed {
		t.Errorf("vacuumed after a small burst: %+v, %v", res, err)
	}
}

// testDepsTableMaintained checks that the leader's live table maintenance
// analyzes the dependency table once a burst of workflows has filled it,
// even when a vacuum has already given the planner its row count: until
// the table has statistics, the planner takes each lookup of a job's edges
// for a two-hundredth of the table, and finalizes and the promotion sweep
// scan it instead.
func testDepsTableMaintained[TTx any](t *testing.T, f PostgresFixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	f.Exec(ctx, t, d, "ALTER TABLE hopper_job_deps SET (autovacuum_enabled = false)")
	deps := make([]driver.JobDependency, 1200)
	for i := range deps {
		deps[i] = driver.JobDependency{Job: i + 1, DependsOn: 0}
	}
	if _, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{Name: "fan-out", Jobs: params("step", len(deps)+1), Deps: deps}); err != nil {
		t.Fatal(err)
	}
	stat := func(q string) string {
		return f.QueryText(ctx, t, d, "SELECT ("+q+")::text FROM pg_stat_user_tables WHERE relid = 'hopper_job_deps'::regclass")
	}
	// Postgres 14's statistics collector reports the inserts a moment later.
	for deadline := time.Now().Add(10 * time.Second); stat("n_mod_since_analyze >= 1200") != "true"; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the dependency table's inserts were not counted")
		}
	}
	if stat("last_analyze IS NULL") != "true" {
		t.Fatal("dependency table analyzed before maintenance")
	}
	// A vacuum gives the planner the table's row count but no column
	// statistics: the table still needs its first analyze.
	f.Exec(ctx, t, d, "VACUUM hopper_job_deps")
	// The result's Analyzed would also report the analyze of hopper_jobs,
	// which the workflow's 1201 jobs earn, so the table's own statistics
	// are what tell.
	for deadline := time.Now().Add(10 * time.Second); stat("last_analyze IS NOT NULL") != "true"; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("dependency table not analyzed")
		}
		if _, err := exec.JobsMaintain(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Churn alone re-analyzes it only every ten minutes.
	analyzedAt := stat("last_analyze")
	if _, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{Name: "fan-out", Jobs: params("step", len(deps)+1), Deps: deps}); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); stat("n_mod_since_analyze >= 1200") != "true"; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the second workflow's edges were not counted")
		}
	}
	for range 3 {
		if _, err := exec.JobsMaintain(ctx); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(time.Second)
	if got := stat("last_analyze"); got != analyzedAt {
		t.Errorf("dependency table analyzed again on churn within ten minutes: %s, then %s", analyzedAt, got)
	}
}

// analyzeEmpty gives the engine statistics that say the live table is
// empty, as an idle installation has, through the Fixture when it can.
func analyzeEmpty(ctx context.Context, t *testing.T, exec driver.Executor) {
	t.Helper()
	if a, ok := exec.(interface{ AnalyzeForTest(context.Context) error }); ok {
		if err := a.AnalyzeForTest(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

// waitMaintain calls JobsMaintain until done reports what it waited for,
// or ten seconds pass.
func waitMaintain(ctx context.Context, t *testing.T, exec driver.Executor, done func(driver.JobsMaintainResult) bool) driver.JobsMaintainResult {
	t.Helper()
	var res driver.JobsMaintainResult
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		var err error
		if res, err = exec.JobsMaintain(ctx); err != nil {
			t.Fatal(err)
		}
		if done(res) {
			return res
		}
		time.Sleep(100 * time.Millisecond)
	}
	return res
}

// testHistoryMaintainWhileInUse runs maintenance while a transaction that
// wrote history (a worker's CompleteTx) is still open. Dropping a partition
// past retention needs that transaction to end; maintenance puts the drop
// off instead of waiting for it, which would queue every other history
// write behind the drop, and drops it once the transaction is over.
func testHistoryMaintainWhileInUse[TTx any](t *testing.T, f PostgresFixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	p := driver.HistoryMaintainParams{CompletedRetention: 24 * time.Hour, FailedRetention: 7 * 24 * time.Hour}
	first, err := exec.HistoryMaintain(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	// A partition ahead that is missing, as one the leader could not create
	// earlier would be: its attach scans the DEFAULT partition, where the
	// current period's history goes until its partition exists.
	ahead := ""
	for _, name := range first.Created {
		if strings.HasPrefix(name, "hopper_job_history_completed_") {
			ahead = name
		}
	}
	if ahead == "" {
		t.Fatalf("no completed-history partition created ahead: %+v", first)
	}
	f.Exec(ctx, t, d, `DROP TABLE `+ahead)
	const old = "hopper_job_history_completed_2020010100"
	f.Exec(ctx, t, d, `CREATE TABLE `+old+` PARTITION OF hopper_job_history_completed
		FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-01-01 01:00:00+00')`)
	insert(ctx, t, exec, params("k", 2))
	clientID := register(ctx, t, exec)
	jobs := claim(ctx, t, exec, clientID, 2)
	if len(jobs) != 2 {
		t.Fatalf("claimed %d", len(jobs))
	}

	tx, commit, rollback := f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // already committed on the happy path
	finalizeCurrent(ctx, t, d.UnwrapTx(tx), driver.JobFinalize{ID: jobs[0].ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	mctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	res, err := exec.HistoryMaintain(mctx, p)
	if err != nil {
		t.Fatalf("maintenance while history is in use: %v", err)
	}
	if len(res.Dropped) != 0 || len(res.Created) != 0 || !slices.Equal(res.Deferred, []string{ahead, old}) {
		t.Errorf("maintenance while history is in use = %+v; want %s and %s deferred", res, ahead, old)
	}
	// Other finalizes are not queued behind a waiting attach or drop.
	if applied := finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: jobs[1].ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true}); len(applied) != 1 {
		t.Errorf("finalize during maintenance applied %v", applied)
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	res, err = exec.HistoryMaintain(ctx, p)
	if err != nil || !slices.Equal(res.Created, []string{ahead}) || len(res.Dropped) != 1 || res.Dropped[0] != old || len(res.Deferred) != 0 {
		t.Errorf("maintenance after the transaction = %+v, %v", res, err)
	}
}
