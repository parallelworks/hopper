package hopperpgx_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

func TestJobInsertManyReplace(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	unique := func(v int, at time.Time) driver.JobInsertParams {
		return driver.JobInsertParams{
			Kind: "u", Queue: "default", Priority: 2, MaxAttempts: 5, UniqueKey: "k",
			Args: json.RawMessage(fmt.Sprintf(`{"v":%d}`, v)), ScheduledAt: at,
		}
	}
	first, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{unique(1, time.Time{})}, driver.JobInsertOpts{})
	if err != nil {
		t.Fatal(err)
	}

	// Replace updates the job in place and pushes it out.
	future := time.Now().Add(time.Hour)
	second, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{unique(2, future)}, driver.JobInsertOpts{OnConflict: driver.ConflictReplace})
	if err != nil {
		t.Fatal(err)
	}
	j := second[0].Job
	if !second[0].Duplicate || j.ID != first[0].Job.ID || string(j.Args) != `{"v": 2}` || j.State != driver.JobStateScheduled ||
		!j.ScheduledAt.Equal(future.Truncate(time.Microsecond)) {
		t.Errorf("replace result = %+v (dup %v)", j, second[0].Duplicate)
	}

	// A running job is not replaced.
	if _, err := d.Pool().Exec(ctx, "UPDATE hopper_jobs SET scheduled_at = now() WHERE id = $1", [16]byte(j.ID)); err != nil {
		t.Fatal(err)
	}
	clientID := register(t, ctx, exec)
	claimed, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 1})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v, %d", err, len(claimed))
	}
	third, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{unique(3, future)}, driver.JobInsertOpts{OnConflict: driver.ConflictReplace})
	if err != nil {
		t.Fatal(err)
	}
	if j := third[0].Job; !third[0].Duplicate || string(j.Args) != `{"v": 2}` || j.State != driver.JobStateRunning {
		t.Errorf("running job was replaced: %+v", j)
	}
}

func TestNotifyIsDeliveredOnCommit(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)

	// Notification channels are shared by every schema in the database,
	// so parallel tests see each other's. Queue names unique to this test
	// tell ours apart.
	prefix := fmt.Sprintf("t%d-", time.Now().UnixNano())
	l, err := d.Listener(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close(ctx) //nolint:errcheck // test cleanup
	if err := l.Listen(ctx, driver.ChannelInsert); err != nil {
		t.Fatal(err)
	}
	// next returns the payloads of this test's notifications received
	// within the wait, without the prefix.
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
			if p, ok := strings.CutPrefix(n.Payload, prefix); ok {
				got[p]++
			}
		}
	}

	// A transactional insert with Notify: nothing until commit, then one
	// notification per queue.
	tx, err := d.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // cleanup on failure
	params := insertParams("k", 4)
	for i := range params {
		params[i].Queue = prefix + "default"
	}
	params[1].Queue, params[3].Queue = prefix+"other", prefix+"other"
	if _, err := d.UnwrapTx(tx).JobInsertMany(ctx, params, driver.JobInsertOpts{Notify: true}); err != nil {
		t.Fatal(err)
	}
	if got := next(300 * time.Millisecond); len(got) != 0 {
		t.Fatalf("notifications %v delivered before commit", got)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if got := next(time.Second); got["default"] != 1 || got["other"] != 1 || len(got) != 2 {
		t.Errorf("notifications after commit = %v", got)
	}

	// Notify sends one per payload.
	if err := exec.Notify(ctx, driver.ChannelInsert, []string{prefix + "a", prefix + "b"}); err != nil {
		t.Fatal(err)
	}
	if got := next(time.Second); got["a"] != 1 || got["b"] != 1 || len(got) != 2 {
		t.Errorf("Notify payloads = %v", got)
	}

	// COPY with Notify.
	params = insertParams("k", 3)
	for i := range params {
		params[i].Queue = prefix + "copy"
	}
	if _, err := exec.JobInsertCopy(ctx, params, driver.JobInsertOpts{Notify: true}); err != nil {
		t.Fatal(err)
	}
	if got := next(time.Second); got["copy"] != 1 || len(got) != 1 {
		t.Errorf("COPY notifications = %v", got)
	}
}

func TestJobRescueCandidates(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	if _, err := exec.JobInsertMany(ctx, insertParams("k", 3), driver.JobInsertOpts{}); err != nil {
		t.Fatal(err)
	}
	live := register(t, ctx, exec)
	dead, err := exec.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "dead", TTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	liveJobs, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: live, Limit: 2})
	if err != nil || len(liveJobs) != 2 {
		t.Fatalf("claim: %v, %d", err, len(liveJobs))
	}
	deadJobs, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: dead, Limit: 1})
	if err != nil || len(deadJobs) != 1 {
		t.Fatalf("claim: %v, %d", err, len(deadJobs))
	}

	// Nothing to rescue while both leases are live.
	got, err := exec.JobRescueCandidates(ctx, driver.JobRescueParams{Limit: 10})
	if err != nil || len(got) != 0 {
		t.Fatalf("candidates with live leases = %v, %v", got, err)
	}

	// Expire the dead client's lease: only its job is a candidate.
	if _, err := d.Pool().Exec(ctx, "UPDATE hopper_clients SET expires_at = now() - interval '1s' WHERE id = $1", dead); err != nil {
		t.Fatal(err)
	}
	got, err = exec.JobRescueCandidates(ctx, driver.JobRescueParams{Limit: 10})
	if err != nil || len(got) != 1 || got[0].ID != deadJobs[0].ID || got[0].AttemptedBy != dead {
		t.Fatalf("candidates = %v, %v", got, err)
	}

	// Deleting the client row (as the leader's prune does) changes nothing.
	if n, err := exec.ClientPruneExpired(ctx); err != nil || n != 1 {
		t.Fatalf("prune = %d, %v", n, err)
	}
	got, err = exec.JobRescueCandidates(ctx, driver.JobRescueParams{Limit: 10})
	if err != nil || len(got) != 1 {
		t.Fatalf("candidates after prune = %v, %v", got, err)
	}

	// StuckAfter also selects jobs of live clients that have run too long.
	if _, err := d.Pool().Exec(ctx, "UPDATE hopper_jobs SET attempted_at = now() - interval '10 minutes' WHERE id = $1", [16]byte(liveJobs[0].ID)); err != nil {
		t.Fatal(err)
	}
	got, err = exec.JobRescueCandidates(ctx, driver.JobRescueParams{StuckAfter: 5 * time.Minute, Limit: 10})
	if err != nil || len(got) != 2 {
		t.Fatalf("candidates with StuckAfter = %d, %v", len(got), err)
	}
	if got[0].ID != liveJobs[0].ID {
		t.Errorf("candidates should be oldest first: %v", got[0].ID)
	}
}

func TestLeaderLease(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	a := register(t, ctx, exec)
	b := register(t, ctx, exec)

	l, err := d.Listener(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close(ctx) //nolint:errcheck // test cleanup
	if err := l.Listen(ctx, driver.ChannelLeader); err != nil {
		t.Fatal(err)
	}

	ok, err := exec.LeaderAttempt(ctx, driver.LeaderParams{ClientID: a, TTL: time.Hour})
	if err != nil || !ok {
		t.Fatalf("a's first attempt = %v, %v", ok, err)
	}
	ok, err = exec.LeaderAttempt(ctx, driver.LeaderParams{ClientID: b, TTL: time.Hour})
	if err != nil || ok {
		t.Fatalf("b took the lease from a live leader: %v, %v", ok, err)
	}
	// The holder renews, keeping its election time.
	var elected time.Time
	if err := d.Pool().QueryRow(ctx, "SELECT elected_at FROM hopper_leader").Scan(&elected); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if ok, err := exec.LeaderAttempt(ctx, driver.LeaderParams{ClientID: a, TTL: time.Hour}); err != nil || !ok {
		t.Fatalf("a's renewal = %v, %v", ok, err)
	}
	var elected2 time.Time
	if err := d.Pool().QueryRow(ctx, "SELECT elected_at FROM hopper_leader").Scan(&elected2); err != nil {
		t.Fatal(err)
	}
	if !elected.Equal(elected2) {
		t.Errorf("renewal changed elected_at from %s to %s", elected, elected2)
	}

	// Resigning by a non-holder does nothing.
	if err := exec.LeaderResign(ctx, b); err != nil {
		t.Fatal(err)
	}
	var holder int64
	if err := d.Pool().QueryRow(ctx, "SELECT client_id FROM hopper_leader").Scan(&holder); err != nil || holder != a {
		t.Fatalf("holder after non-holder's resign = %d, %v; want %d", holder, err, a)
	}

	// The holder's resign frees the lease and notifies. (The channel is
	// shared with parallel tests, so only the presence of a notification
	// can be asserted.)
	if err := exec.LeaderResign(ctx, a); err != nil {
		t.Fatal(err)
	}
	nctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	n, err := l.Next(nctx)
	cancel()
	if err != nil || n.Channel != driver.ChannelLeader {
		t.Fatalf("resign notification = %+v, %v", n, err)
	}
	if ok, err := exec.LeaderAttempt(ctx, driver.LeaderParams{ClientID: b, TTL: time.Hour}); err != nil || !ok {
		t.Fatalf("b's attempt after resign = %v, %v", ok, err)
	}

	// An expired lease can be taken over.
	if _, err := d.Pool().Exec(ctx, "UPDATE hopper_leader SET expires_at = now() - interval '1s'"); err != nil {
		t.Fatal(err)
	}
	if ok, err := exec.LeaderAttempt(ctx, driver.LeaderParams{ClientID: a, TTL: time.Hour}); err != nil || !ok {
		t.Fatalf("a's takeover of an expired lease = %v, %v", ok, err)
	}
}

func TestHistoryMaintain(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	pool := d.Pool()

	// A finalized job before any time partition exists lands in DEFAULT.
	if _, err := exec.JobInsertMany(ctx, insertParams("k", 2), driver.JobInsertOpts{}); err != nil {
		t.Fatal(err)
	}
	clientID := register(t, ctx, exec)
	jobs, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 2})
	if err != nil || len(jobs) != 2 {
		t.Fatalf("claim: %v, %d", err, len(jobs))
	}
	if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{
		{ID: jobs[0].ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true},
		{ID: jobs[1].ID, AttemptedBy: clientID, State: driver.JobStateDiscarded, Archive: true},
	}}); err != nil {
		t.Fatal(err)
	}
	count := func(table string) int {
		var n int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if count("hopper_job_history_completed_default") != 1 || count("hopper_job_history_failed_default") != 1 {
		t.Fatal("finalized rows did not land in the DEFAULT partitions")
	}

	// An old partition with an old row, to be dropped by retention.
	if _, err := pool.Exec(ctx, `CREATE TABLE hopper_job_history_completed_2020010100 PARTITION OF hopper_job_history_completed
		FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-01-01 01:00:00+00')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO hopper_job_history (id, seq, kind, queue, state, priority, attempt, max_attempts, scheduled_at, args, metadata, errors, await, created_at, finalized_at)
		VALUES (hopper_uuidv7(), 1, 'k', 'default', 'completed', 2, 1, 1, now(), '{}', '{}', '[]', false, now(), '2020-01-01 00:30:00+00')`); err != nil {
		t.Fatal(err)
	}
	// And an old row stuck in the DEFAULT partition.
	if _, err := pool.Exec(ctx, `INSERT INTO hopper_job_history (id, seq, kind, queue, state, priority, attempt, max_attempts, scheduled_at, args, metadata, errors, await, created_at, finalized_at)
		VALUES (hopper_uuidv7(), 2, 'k', 'default', 'discarded', 2, 1, 1, now(), '{}', '{}', '[]', false, now(), now() - interval '30 days')`); err != nil {
		t.Fatal(err)
	}

	res, err := exec.HistoryMaintain(ctx, driver.HistoryMaintainParams{CompletedRetention: 24 * time.Hour, FailedRetention: 7 * 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	// 3 hourly and 2 daily periods ahead of the current ones.
	if len(res.Created) != 3+2 {
		t.Errorf("created = %v", res.Created)
	}
	next := "hopper_job_history_completed_" + time.Now().UTC().Add(time.Hour).Format("2006010215")
	if !slices.Contains(res.Created, next) {
		t.Errorf("created %v does not include the next hour %s", res.Created, next)
	}
	if len(res.Dropped) != 1 || res.Dropped[0] != "hopper_job_history_completed_2020010100" {
		t.Errorf("dropped = %v", res.Dropped)
	}
	if res.Pruned != 1 {
		t.Errorf("pruned = %d, want the old DEFAULT row", res.Pruned)
	}

	// Rows already in DEFAULT stay there and stay visible.
	if count("hopper_job_history_completed_default") != 1 || count("hopper_job_history_failed_default") != 1 {
		t.Error("recent rows in DEFAULT were touched")
	}
	if count("hopper_job_history") != 2 {
		t.Errorf("history rows = %d, want the two finalized jobs", count("hopper_job_history"))
	}
	if _, err := exec.JobGet(ctx, jobs[0].ID); err != nil {
		t.Errorf("row in DEFAULT not found: %v", err)
	}

	// A row for a future period routes to its partition, not DEFAULT.
	if _, err := pool.Exec(ctx, `INSERT INTO hopper_job_history (id, seq, kind, queue, state, priority, attempt, max_attempts, scheduled_at, args, metadata, errors, await, created_at, finalized_at)
		VALUES (hopper_uuidv7(), 3, 'k', 'default', 'completed', 2, 1, 1, now(), '{}', '{}', '[]', false, now(), now() + interval '1 hour')`); err != nil {
		t.Fatal(err)
	}
	if count("hopper_job_history_completed_default") != 1 || count(next) != 1 {
		t.Error("row for the next hour did not route to its partition")
	}

	// Idempotent: a second run creates and drops nothing.
	res, err = exec.HistoryMaintain(ctx, driver.HistoryMaintainParams{CompletedRetention: 24 * time.Hour, FailedRetention: 7 * 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Created) != 0 || len(res.Dropped) != 0 || res.Pruned != 0 {
		t.Errorf("second run = %+v", res)
	}

	// Retention disabled keeps everything.
	if _, err := pool.Exec(ctx, `CREATE TABLE hopper_job_history_failed_20200101 PARTITION OF hopper_job_history_failed
		FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-01-02 00:00:00+00')`); err != nil {
		t.Fatal(err)
	}
	res, err = exec.HistoryMaintain(ctx, driver.HistoryMaintainParams{CompletedRetention: -1, FailedRetention: -1})
	if err != nil || len(res.Dropped) != 0 {
		t.Errorf("disabled retention dropped %v, %v", res.Dropped, err)
	}

	// Maintenance inside a transaction is refused.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // cleanup
	if _, err := d.UnwrapTx(tx).HistoryMaintain(ctx, driver.HistoryMaintainParams{}); err == nil {
		t.Error("maintenance in a transaction succeeded")
	}
}

func TestHistoryMaintainConcurrentLeaders(t *testing.T) {
	t.Parallel()
	ctx, _, exec := setup(t)
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

func TestListenerFailsWhenBackendTerminated(t *testing.T) {
	t.Parallel()
	ctx, d, _ := setup(t)
	l, err := d.Listener(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Listen(ctx, driver.ChannelInsert); err != nil {
		t.Fatal(err)
	}
	// Terminate the listener's backend, as a failover would.
	var pid int
	if err := d.Pool().QueryRow(ctx, "SELECT pid FROM pg_stat_activity WHERE application_name = 'hopper-listener:' || current_schema()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Pool().Exec(ctx, "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatal(err)
	}
	// Other tests' notifications may still be delivered first; the
	// connection must fail within the deadline.
	nctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		_, err := l.Next(nctx)
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("Next kept succeeding on a terminated connection")
		}
		if err != nil {
			break
		}
	}
	_ = l.Close(ctx)
}
