package hopperpgx_test

// Postgres-specific behavior. The engine-agnostic contract is covered by
// the drivertest suite (see conformance_test.go).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/hopperpgx"
	"github.com/parallelworks/hopper/internal/testdb"
)

func setup(t *testing.T) (context.Context, *hopperpgx.Driver, driver.Executor) {
	t.Helper()
	pool := testdb.Pool(t)
	d := hopperpgx.New(pool)
	return context.Background(), d, d.Executor()
}

func insertParams(kind string, n int) []driver.JobInsertParams {
	out := make([]driver.JobInsertParams, n)
	for i := range out {
		out[i] = driver.JobInsertParams{
			Kind: kind, Queue: "default", Priority: 2, MaxAttempts: 3,
			Args: json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)),
		}
	}
	return out
}

func register(t *testing.T, ctx context.Context, exec driver.Executor) int64 {
	t.Helper()
	id, err := exec.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "test", TTL: 15 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return id
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
