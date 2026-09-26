// Package drivertest is the conformance suite every hopper driver must pass.
//
// A driver's tests call Run with a Fixture that hands out isolated, migrated
// databases and implements the few engine-specific hooks the suite needs to
// set up situations (an expired lease, a stuck job) that no queue operation
// produces on its own.
//
//	func TestConformance(t *testing.T) {
//		drivertest.Run(t, pgxFixture{})
//	}
//
// The suite covers inserting (including unique keys and replace), claiming
// (order, ownership, expiry, SKIP LOCKED), every finalize transition and its
// fencing, leases, leader election, rescue candidates, cancel, retry, TTL
// expiry, listing, queues, periodic slots, stats, notifications and history
// maintenance. Engine-specific behavior, such as how partitions are managed,
// belongs in the driver's own tests.
package drivertest

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

// Fixture gives the suite a database per test and the hooks it needs.
type Fixture[TTx any] interface {
	// NewDriver returns a driver on a fresh database with hopper's schema
	// installed, torn down when the test ends.
	NewDriver(t *testing.T) driver.Driver[TTx]
	// Begin starts a transaction. The suite calls commit or rollback.
	Begin(ctx context.Context, t *testing.T, d driver.Driver[TTx]) (tx TTx, commit, rollback func() error)
	// ExpireLease makes a client's lease expired, as a partition or a long
	// pause would.
	ExpireLease(ctx context.Context, t *testing.T, d driver.Driver[TTx], clientID int64)
	// ExpireLeader makes the leader lease expired.
	ExpireLeader(ctx context.Context, t *testing.T, d driver.Driver[TTx])
	// MakeClaimable moves a job's scheduled time to now.
	MakeClaimable(ctx context.Context, t *testing.T, d driver.Driver[TTx], id driver.JobID)
	// AgeAttempt backdates a running job's attempted time by age.
	AgeAttempt(ctx context.Context, t *testing.T, d driver.Driver[TTx], id driver.JobID, age time.Duration)
	// CountLive and CountHistory count jobs in the live table and in history.
	CountLive(ctx context.Context, t *testing.T, d driver.Driver[TTx]) int
	CountHistory(ctx context.Context, t *testing.T, d driver.Driver[TTx]) int
}

// Run runs the suite.
func Run[TTx any](t *testing.T, f Fixture[TTx]) {
	t.Helper()
	tests := []struct {
		name string
		fn   func(*testing.T, Fixture[TTx])
	}{
		{"InsertMany", testInsertMany[TTx]},
		{"InsertUnique", testInsertUnique[TTx]},
		{"InsertCopy", testInsertCopy[TTx]},
		{"InsertTx", testInsertTx[TTx]},
		{"Claim", testClaim[TTx]},
		{"ClaimSkipsLocked", testClaimSkipsLocked[TTx]},
		{"Finalize", testFinalize[TTx]},
		{"FinalizeFences", testFinalizeFences[TTx]},
		{"Lease", testLease[TTx]},
		{"Leader", testLeader[TTx]},
		{"Rescue", testRescue[TTx]},
		{"Cancel", testCancel[TTx]},
		{"Retry", testRetry[TTx]},
		{"TTL", testTTL[TTx]},
		{"List", testList[TTx]},
		{"Queues", testQueues[TTx]},
		{"Periodic", testPeriodic[TTx]},
		{"Stats", testStats[TTx]},
		{"Notify", testNotify[TTx]},
		{"HistoryMaintain", testHistoryMaintain[TTx]},
		{"Now", testNow[TTx]},
		{"Subscriptions", testSubscriptions[TTx]},
		{"Publish", testPublish[TTx]},
		{"OrderingClaim", testOrderingClaim[TTx]},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, f)
		})
	}
}

func params(kind string, n int) []driver.JobInsertParams {
	out := make([]driver.JobInsertParams, n)
	for i := range out {
		out[i] = driver.JobInsertParams{
			Kind: kind, Queue: "default", Priority: 2, MaxAttempts: 3,
			Args: json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)),
		}
	}
	return out
}

func register(ctx context.Context, t *testing.T, exec driver.Executor) int64 {
	t.Helper()
	id, err := exec.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "test", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func insert(ctx context.Context, t *testing.T, exec driver.Executor, p []driver.JobInsertParams) []driver.JobInsertResult {
	t.Helper()
	res, err := exec.JobInsertMany(ctx, p, driver.JobInsertOpts{})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func claim(ctx context.Context, t *testing.T, exec driver.Executor, clientID int64, limit int) []*driver.JobRow {
	t.Helper()
	jobs, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: limit})
	if err != nil {
		t.Fatal(err)
	}
	return jobs
}

func finalize(ctx context.Context, t *testing.T, exec driver.Executor, jobs ...driver.JobFinalize) []driver.JobID {
	t.Helper()
	applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: jobs})
	if err != nil {
		t.Fatal(err)
	}
	return applied
}

// argsN reads the "i" field of a job's args, or -1.
func argsN(job *driver.JobRow) int {
	var a struct{ I int }
	if err := json.Unmarshal(job.Args, &a); err != nil {
		return -1
	}
	return a.I
}

func testInsertMany[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
	p := params("order", 50)
	for i := range p {
		p[i].Queue = fmt.Sprintf("q%d", i%3)
	}
	p[7].ScheduledAt = time.Now().Add(time.Hour)
	p[8].Metadata = json.RawMessage(`{"trace":"t"}`)
	results := insert(ctx, t, exec, p)
	if len(results) != len(p) {
		t.Fatalf("got %d results for %d params", len(results), len(p))
	}
	for i, r := range results {
		if r.Duplicate || argsN(r.Job) != i || r.Job.Queue != p[i].Queue || r.Job.Kind != "order" {
			t.Errorf("result %d = %+v (dup %v)", i, r.Job, r.Duplicate)
		}
		if r.Job.ID.IsZero() || r.Job.CreatedAt.IsZero() || r.Job.ScheduledAt.IsZero() || r.Job.Errors == nil || r.Job.Attempt != 0 || r.Job.MaxAttempts != 3 {
			t.Errorf("result %d incomplete: %+v", i, r.Job)
		}
		want := driver.JobStateAvailable
		if i == 7 {
			want = driver.JobStateScheduled
		}
		if r.Job.State != want {
			t.Errorf("result %d state = %s, want %s", i, r.Job.State, want)
		}
	}
	// IDs are distinct. (Claim order uses an internal sequence, not the
	// ID: several IDs minted in one millisecond need not sort in insertion
	// order.)
	seen := map[driver.JobID]bool{}
	for _, r := range results {
		if seen[r.Job.ID] {
			t.Errorf("duplicate ID %s", r.Job.ID)
		}
		seen[r.Job.ID] = true
	}
	got, err := exec.JobGet(ctx, results[8].Job.ID)
	if err != nil || !strings.Contains(string(got.Metadata), `"trace"`) {
		t.Errorf("JobGet = %+v, %v", got, err)
	}
	if _, err := exec.JobGet(ctx, driver.JobID{1, 2, 3}); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("JobGet unknown = %v", err)
	}
	if res, err := exec.JobInsertMany(ctx, nil, driver.JobInsertOpts{}); err != nil || len(res) != 0 {
		t.Errorf("empty insert = %v, %v", res, err)
	}
}

func testInsertUnique[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	unique := func(kind string, v int, at time.Time) driver.JobInsertParams {
		return driver.JobInsertParams{Kind: kind, Queue: "default", Priority: 2, MaxAttempts: 5, UniqueKey: "k", Args: json.RawMessage(fmt.Sprintf(`{"i":%d}`, v)), ScheduledAt: at}
	}
	first := insert(ctx, t, exec, []driver.JobInsertParams{unique("u", 1, time.Time{})})
	second := insert(ctx, t, exec, []driver.JobInsertParams{unique("u", 2, time.Time{}), unique("other", 3, time.Time{})})
	if !second[0].Duplicate || second[0].Job.ID != first[0].Job.ID || argsN(second[0].Job) != 1 {
		t.Errorf("duplicate not resolved to the existing job: %+v", second[0])
	}
	if second[1].Duplicate {
		t.Error("a different kind with the same key marked duplicate")
	}

	// Replace updates a non-running duplicate and pushes it out.
	future := time.Now().Add(time.Hour)
	rep, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{unique("u", 4, future)}, driver.JobInsertOpts{OnConflict: driver.ConflictReplace})
	if err != nil {
		t.Fatal(err)
	}
	if j := rep[0].Job; !rep[0].Duplicate || j.ID != first[0].Job.ID || argsN(j) != 4 || j.State != driver.JobStateScheduled {
		t.Errorf("replace = %+v", j)
	}
	// A running duplicate is left alone.
	f.MakeClaimable(ctx, t, d, first[0].Job.ID)
	clientID := register(ctx, t, exec)
	claimed := claim(ctx, t, exec, clientID, 10)
	if !slices.ContainsFunc(claimed, func(j *driver.JobRow) bool { return j.ID == first[0].Job.ID }) {
		t.Fatalf("unique job not claimed: %v", claimed)
	}
	rep, err = exec.JobInsertMany(ctx, []driver.JobInsertParams{unique("u", 5, future)}, driver.JobInsertOpts{OnConflict: driver.ConflictReplace})
	if err != nil {
		t.Fatal(err)
	}
	if j := rep[0].Job; !rep[0].Duplicate || argsN(j) != 4 || j.State != driver.JobStateRunning {
		t.Errorf("running job was replaced: %+v", j)
	}

	// Finalizing frees the key.
	finalize(ctx, t, exec, driver.JobFinalize{ID: first[0].Job.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	if third := insert(ctx, t, exec, []driver.JobInsertParams{unique("u", 6, time.Time{})}); third[0].Duplicate {
		t.Error("finalized job still blocks its unique key")
	}
}
