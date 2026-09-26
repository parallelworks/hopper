package drivertest

import (
	"context"
	"encoding/json"
	"errors"
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
	candidates := func(stuck time.Duration) []*driver.JobRow {
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
	if got := candidates(0); len(got) != 1 || got[0].ID != deadJobs[0].ID || got[0].AttemptedBy != dead {
		t.Fatalf("candidates = %v", got)
	}
	if _, err := exec.ClientPruneExpired(ctx); err != nil {
		t.Fatal(err)
	}
	if got := candidates(0); len(got) != 1 {
		t.Fatalf("candidates after prune = %v", got)
	}
	f.AgeAttempt(ctx, t, d, liveJobs[0].ID, 10*time.Minute)
	if got := candidates(5 * time.Minute); len(got) != 2 || got[0].ID != liveJobs[0].ID {
		t.Fatalf("candidates with StuckAfter = %v (oldest first)", got)
	}
	// The rescue itself is the fenced finalize.
	applied := finalize(ctx, t, exec, driver.JobFinalize{ID: deadJobs[0].ID, AttemptedBy: dead, State: driver.JobStateRetryable, Error: &driver.AttemptError{Attempt: 1, Error: "hopper: client lost"}})
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
	finalize(ctx, t, exec, driver.JobFinalize{ID: claimed[0].ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
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
	// Cancelling a running job announces it too, and a finalize of an
	// awaited job announces its ID on the done channel.
	res := insert(ctx, t, exec, []driver.JobInsertParams{{Kind: "k", Queue: "default", Priority: 2, MaxAttempts: 1, Await: true}})
	clientID := register(ctx, t, exec)
	if jobs := claim(ctx, t, exec, clientID, 1); len(jobs) != 1 {
		t.Fatal("claim")
	}
	if _, err := exec.JobCancel(ctx, res[0].Job.ID); err != nil {
		t.Fatal(err)
	}
	finalize(ctx, t, exec, driver.JobFinalize{ID: res[0].Job.ID, AttemptedBy: clientID, State: driver.JobStateCancelled})
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

func testHistoryMaintain[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	insert(ctx, t, exec, params("k", 1))
	clientID := register(ctx, t, exec)
	jobs := claim(ctx, t, exec, clientID, 1)
	finalize(ctx, t, exec, driver.JobFinalize{ID: jobs[0].ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
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
	errs := make(chan error, 3)
	for range 3 {
		go func() {
			_, err := exec.HistoryMaintain(ctx, driver.HistoryMaintainParams{CompletedRetention: time.Hour, FailedRetention: time.Hour})
			errs <- err
		}()
	}
	for range 3 {
		if err := <-errs; err != nil {
			t.Errorf("concurrent maintenance: %v", err)
		}
	}
}

func testNow[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	now, err := f.NewDriver(t).Executor().Now(ctx)
	if err != nil || time.Until(now).Abs() > time.Minute {
		t.Errorf("Now = %s, %v", now, err)
	}
}
