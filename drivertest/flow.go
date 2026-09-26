package drivertest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

func claimLimited(ctx context.Context, t *testing.T, exec driver.Executor, clientID int64, limit int) driver.JobClaimResult {
	t.Helper()
	res, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: limit, Limited: true})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func testQueueLimits[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
	if err := exec.QueueSetLimits(ctx, driver.QueueLimits{Name: "default", GlobalLimit: 3, RatePerSec: 10, RateBurst: 20, PartitionLimit: 2, Aging: time.Minute}); err != nil {
		t.Fatal(err)
	}
	queues, err := exec.QueueList(ctx)
	if err != nil || len(queues) != 1 {
		t.Fatalf("queues = %v, %v", queues, err)
	}
	l := queues[0].Limits
	if l.GlobalLimit != 3 || l.RatePerSec != 10 || l.RateBurst != 20 || l.PartitionLimit != 2 || l.Aging != time.Minute || !l.Limited() {
		t.Errorf("limits = %+v", l)
	}
	res, err := exec.ClientRenew(ctx, driver.ClientRenewParams{ClientID: register(ctx, t, exec), TTL: time.Hour})
	if err != nil || len(res.LimitedQueues) != 1 || res.LimitedQueues[0] != "default" {
		t.Errorf("renew limited queues = %v, %v", res.LimitedQueues, err)
	}
	// Zero values remove limits; aging alone does not make a queue limited.
	if err := exec.QueueSetLimits(ctx, driver.QueueLimits{Name: "default", Aging: time.Minute}); err != nil {
		t.Fatal(err)
	}
	queues, _ = exec.QueueList(ctx)
	if l := queues[0].Limits; l.Limited() || l.Aging != time.Minute {
		t.Errorf("limits after clearing = %+v", l)
	}
	// The limited path on an unlimited (or unknown) queue behaves like the plain one.
	insert(ctx, t, exec, params("k", 2))
	if got := claimLimited(ctx, t, exec, register(ctx, t, exec), 10); len(got.Jobs) != 2 || got.Wait != 0 {
		t.Errorf("limited claim on unlimited queue = %+v", got)
	}
}

func testGlobalLimit[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
	if err := exec.QueueSetLimits(ctx, driver.QueueLimits{Name: "default", GlobalLimit: 3}); err != nil {
		t.Fatal(err)
	}
	insert(ctx, t, exec, params("k", 10))
	a, b := register(ctx, t, exec), register(ctx, t, exec)
	first := claimLimited(ctx, t, exec, a, 10)
	if len(first.Jobs) != 3 {
		t.Fatalf("first claim = %d, want the global limit of 3", len(first.Jobs))
	}
	if second := claimLimited(ctx, t, exec, b, 10); len(second.Jobs) != 0 || second.Wait != 0 {
		t.Errorf("claim at the limit = %+v", second)
	}
	finalize(ctx, t, exec, driver.JobFinalize{ID: first.Jobs[0].ID, AttemptedBy: a, State: driver.JobStateCompleted, Archive: true})
	if third := claimLimited(ctx, t, exec, b, 10); len(third.Jobs) != 1 {
		t.Errorf("claim after one finished = %d, want 1", len(third.Jobs))
	}
}

func testRateLimit[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
	// 5 per second, burst of 2.
	if err := exec.QueueSetLimits(ctx, driver.QueueLimits{Name: "default", RatePerSec: 5, RateBurst: 2}); err != nil {
		t.Fatal(err)
	}
	insert(ctx, t, exec, params("k", 10))
	clientID := register(ctx, t, exec)
	if got := claimLimited(ctx, t, exec, clientID, 10); len(got.Jobs) != 2 {
		t.Fatalf("burst claim = %d, want 2", len(got.Jobs))
	}
	empty := claimLimited(ctx, t, exec, clientID, 10)
	if len(empty.Jobs) != 0 || empty.Wait <= 0 || empty.Wait > 250*time.Millisecond {
		t.Errorf("claim with an empty bucket = %d jobs, wait %s; want 0 and ~200ms", len(empty.Jobs), empty.Wait)
	}
	// Tokens refill at the rate.
	time.Sleep(450 * time.Millisecond)
	if got := claimLimited(ctx, t, exec, clientID, 10); len(got.Jobs) != 2 {
		t.Errorf("claim after refill = %d, want 2 (rate 5/s for 0.45s, capped by burst)", len(got.Jobs))
	}
	// Overall, no more than rate × time + burst are ever claimed.
	deadline := time.Now().Add(time.Second)
	claimed := 4
	for time.Now().Before(deadline) {
		claimed += len(claimLimited(ctx, t, exec, clientID, 10).Jobs)
		time.Sleep(50 * time.Millisecond)
	}
	if claimed > 2+5*2 {
		t.Errorf("claimed %d jobs in ~1.5s at 5/s with burst 2", claimed)
	}
}

func testPartitionLimit[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
	if err := exec.QueueSetLimits(ctx, driver.QueueLimits{Name: "default", PartitionLimit: 2}); err != nil {
		t.Fatal(err)
	}
	p := params("k", 9)
	for i := range p {
		p[i].PartitionKey = []string{"a", "a", "a", "a", "b", "b", "b", "", ""}[i]
	}
	inserted := insert(ctx, t, exec, p)
	clientID := register(ctx, t, exec)
	got := claimLimited(ctx, t, exec, clientID, 10)
	perKey := map[string]int{}
	for _, j := range got.Jobs {
		perKey[j.PartitionKey]++
	}
	if perKey["a"] != 2 || perKey["b"] != 2 || perKey[""] != 2 {
		t.Errorf("claimed per partition = %v, want 2 of a, 2 of b and both unkeyed", perKey)
	}
	// Nothing more for a and b while their jobs run; finishing one of a's frees one.
	if again := claimLimited(ctx, t, exec, register(ctx, t, exec), 10); len(again.Jobs) != 0 {
		t.Errorf("claimed %d over the partition limit", len(again.Jobs))
	}
	finalize(ctx, t, exec, driver.JobFinalize{ID: inserted[0].Job.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	next := claimLimited(ctx, t, exec, clientID, 10)
	if len(next.Jobs) != 1 || next.Jobs[0].PartitionKey != "a" {
		t.Errorf("claim after one of a finished = %v", next.Jobs)
	}
}

func testAging[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	p := params("k", 3)
	p[0].Priority, p[1].Priority, p[2].Priority = 4, 2, 1
	inserted := insert(ctx, t, exec, p)
	// Nothing has waited a minute yet.
	if n, err := exec.JobAge(ctx, "default", time.Minute); err != nil || n != 0 {
		t.Errorf("age = %d, %v", n, err)
	}
	f.AgeAttempt(ctx, t, d, inserted[0].Job.ID, 0) // no-op on a waiting job; keeps the hook exercised
	if n, err := exec.JobAge(ctx, "default", time.Millisecond); err != nil || n != 2 {
		t.Errorf("age after waiting = %d, %v; want the two jobs above priority 1", n, err)
	}
	for i, want := range []int{3, 1, 1} {
		j, _ := exec.JobGet(ctx, inserted[i].Job.ID)
		if j.Priority != want {
			t.Errorf("job %d priority = %d, want %d", i, j.Priority, want)
		}
	}
	if n, err := exec.JobAge(ctx, "default", 0); err != nil || n != 0 {
		t.Errorf("age with no period = %d, %v", n, err)
	}
}

func testBatches[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	cb := func(kind string) *driver.JobInsertParams {
		return &driver.JobInsertParams{Kind: kind, Queue: "callbacks", Priority: 1, MaxAttempts: 3, Args: json.RawMessage(`{"run":9}`)}
	}
	id, err := exec.BatchInsert(ctx, driver.BatchInsertParams{Total: 3, OnSuccess: cb("done"), OnFailure: cb("alert"), OnComplete: cb("always"), Metadata: json.RawMessage(`{"run":9}`)})
	if err != nil {
		t.Fatal(err)
	}
	b, err := exec.BatchGet(ctx, id)
	if err != nil || b.Pending != 3 || b.Total != 3 || b.Failed != 0 || !b.CompletedAt.IsZero() {
		t.Fatalf("batch = %+v, %v", b, err)
	}
	p := params("shard", 3)
	for i := range p {
		p[i].BatchID = id
	}
	jobs := insert(ctx, t, exec, p)
	if jobs[0].Job.BatchID != id {
		t.Errorf("job batch id = %s, want %s", jobs[0].Job.BatchID, id)
	}
	clientID := register(ctx, t, exec)
	running := claim(ctx, t, exec, clientID, 3)
	// Two complete in one flush; the batch counts them off but is not done.
	finalize(ctx, t, exec,
		driver.JobFinalize{ID: running[0].ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true},
		driver.JobFinalize{ID: running[1].ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true},
	)
	b, _ = exec.BatchGet(ctx, id)
	if b.Pending != 1 || b.Failed != 0 || !b.CompletedAt.IsZero() {
		t.Errorf("batch after two = %+v", b)
	}
	list := func(kinds ...string) []*driver.JobRow {
		t.Helper()
		out, err := exec.JobList(ctx, driver.JobListParams{Kinds: kinds, Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	if got := list("done", "alert", "always"); len(got) != 0 {
		t.Errorf("callbacks inserted early: %v", got)
	}
	// The last one is discarded: the failure and the completion callbacks run.
	finalize(ctx, t, exec, driver.JobFinalize{ID: running[2].ID, AttemptedBy: clientID, State: driver.JobStateDiscarded, Error: &driver.AttemptError{Attempt: 1, Error: "x"}})
	b, _ = exec.BatchGet(ctx, id)
	if b.Pending != 0 || b.Failed != 1 || b.CompletedAt.IsZero() {
		t.Errorf("completed batch = %+v", b)
	}
	got := list("done", "alert", "always")
	kinds := map[string]*driver.JobRow{}
	for _, j := range got {
		kinds[j.Kind] = j
	}
	if len(got) != 2 || kinds["alert"] == nil || kinds["always"] == nil {
		t.Fatalf("callbacks = %v", got)
	}
	var meta struct {
		BatchID     string `json:"batch_id"`
		BatchFailed int    `json:"batch_failed"`
	}
	if err := json.Unmarshal(kinds["alert"].Metadata, &meta); err != nil || meta.BatchID != id.String() || meta.BatchFailed != 1 {
		t.Errorf("callback metadata = %s (%v)", kinds["alert"].Metadata, err)
	}
	if j := kinds["alert"]; j.Queue != "callbacks" || j.Priority != 1 || j.MaxAttempts != 3 || string(j.Args) != `{"run": 9}` {
		t.Errorf("callback job = %+v", j)
	}

	// A batch whose jobs all succeed runs the success callback, and one
	// whose waiting job is cancelled counts that as a failure.
	id2, err := exec.BatchInsert(ctx, driver.BatchInsertParams{Total: 2, OnSuccess: cb("done2"), OnFailure: cb("alert2")})
	if err != nil {
		t.Fatal(err)
	}
	p = params("shard2", 2)
	p[0].BatchID, p[1].BatchID = id2, id2
	jobs = insert(ctx, t, exec, p)
	if _, err := exec.JobCancel(ctx, jobs[1].Job.ID); err != nil {
		t.Fatal(err)
	}
	running = claim(ctx, t, exec, clientID, 1)
	finalize(ctx, t, exec, driver.JobFinalize{ID: running[0].ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	b, _ = exec.BatchGet(ctx, id2)
	if b.Pending != 0 || b.Failed != 1 {
		t.Errorf("batch with a cancelled job = %+v", b)
	}
	if got := list("done2", "alert2"); len(got) != 1 || got[0].Kind != "alert2" {
		t.Errorf("callbacks for the cancelled batch = %v", got)
	}
	if _, err := exec.BatchGet(ctx, driver.JobID{7}); err == nil {
		t.Error("unknown batch found")
	}
}
