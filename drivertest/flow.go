package drivertest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
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
	insert(ctx, t, exec, params("k", 20))
	clientID := register(ctx, t, exec)
	// The bucket refills while the test runs, and a loaded machine can take
	// longer between two claims than a token takes to refill. What a claim
	// may return is therefore bounded by the time since the first one.
	start := time.Now()
	refilled := func() float64 { return 5 * time.Since(start).Seconds() }
	if got := claimLimited(ctx, t, exec, clientID, 10); len(got.Jobs) != 2 {
		t.Fatalf("burst claim = %d, want 2", len(got.Jobs))
	}
	claimed := 2
	empty := claimLimited(ctx, t, exec, clientID, 10)
	claimed += len(empty.Jobs)
	switch {
	case float64(len(empty.Jobs)) > refilled():
		t.Errorf("claim with an empty bucket = %d jobs after %s at 5/s", len(empty.Jobs), time.Since(start))
	case len(empty.Jobs) == 0 && (empty.Wait <= 0 || empty.Wait > 200*time.Millisecond):
		t.Errorf("claim with an empty bucket waits %s; want at most the 200ms a token takes", empty.Wait)
	}
	// Tokens refill at the rate.
	time.Sleep(450 * time.Millisecond)
	got := claimLimited(ctx, t, exec, clientID, 10)
	claimed += len(got.Jobs)
	if len(got.Jobs) != 2 {
		t.Errorf("claim after refill = %d, want 2 (rate 5/s for 0.45s, capped by burst)", len(got.Jobs))
	}
	// Overall, no more than rate × time + burst are ever claimed.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		claimed += len(claimLimited(ctx, t, exec, clientID, 10).Jobs)
		time.Sleep(50 * time.Millisecond)
	}
	if float64(claimed) > 2+refilled() {
		t.Errorf("claimed %d jobs in %s at 5/s with burst 2", claimed, time.Since(start))
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

// testBatchesConcurrent finalizes a batch's jobs in concurrent statements.
// Each records its progress without waiting on the others, so a
// transaction that finalized a job and is still open holds up none of its
// siblings' finalizes. The last two, each missing the other's uncommitted
// progress, leave the batch open, and BatchSettle, run by each finalizing
// client after its statement commits, completes it once; the sweep finds
// what a client could not settle.
func testBatchesConcurrent[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	clientID := register(ctx, t, exec)
	cb := func(kind string) *driver.JobInsertParams {
		return &driver.JobInsertParams{Kind: kind, Queue: "callbacks", Priority: 2, MaxAttempts: 3, Args: json.RawMessage(`{}`)}
	}
	callbacks := func(batch driver.JobID) map[string]int {
		t.Helper()
		jobs, err := exec.JobList(ctx, driver.JobListParams{Queue: "callbacks", Limit: 1000})
		if err != nil {
			t.Fatal(err)
		}
		kinds := map[string]int{}
		for _, j := range jobs {
			var meta struct {
				BatchID string `json:"batch_id"`
			}
			if json.Unmarshal(j.Metadata, &meta) == nil && meta.BatchID == batch.String() {
				kinds[j.Kind]++
			}
		}
		return kinds
	}
	newBatch := func(kind string, n int) driver.JobID {
		t.Helper()
		id, err := exec.BatchInsert(ctx, driver.BatchInsertParams{Total: n, OnSuccess: cb("ok"), OnFailure: cb("failed"), OnComplete: cb("always")})
		if err != nil {
			t.Fatal(err)
		}
		p := params(kind, n)
		for i := range p {
			p[i].BatchID = id
		}
		insert(ctx, t, exec, p)
		return id
	}
	settle := func(ids ...driver.JobID) driver.BatchSettleResult {
		t.Helper()
		p := driver.BatchSettleParams{IDs: ids, Limit: 100}
		res, err := exec.BatchSettle(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	sweep := func() driver.BatchSettleResult { t.Helper(); return settle() }
	completed := func(id driver.JobID) driver.JobFinalize {
		return driver.JobFinalize{ID: id, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true}
	}

	batch := newBatch("pair", 2)
	jobs := claim(ctx, t, exec, clientID, 2)
	if len(jobs) != 2 {
		t.Fatalf("claimed %d", len(jobs))
	}
	tx, commit, rollback := f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // already committed on the happy path
	finalize(ctx, t, d.UnwrapTx(tx), completed(jobs[0].ID))
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if applied, err := exec.JobFinalizeMany(sctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{completed(jobs[1].ID)}}); err != nil || len(applied) != 1 {
		t.Fatalf("a sibling's finalize while the transaction is open = %v, %v; it must not wait for it", applied, err)
	}
	if res := settle(batch); res.Completed != 0 {
		t.Errorf("BatchSettle with a job's finalize uncommitted = %+v", res)
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	if b, err := exec.BatchGet(ctx, batch); err != nil || b.Pending != 0 || !b.CompletedAt.IsZero() {
		t.Fatalf("batch after both finalizes = %+v, %v; the race should leave it open", b, err)
	}
	if res := settle(batch); res.Completed != 1 {
		t.Errorf("BatchSettle = %+v; want 1 completed", res)
	}
	if res := sweep(); res != (driver.BatchSettleResult{}) {
		t.Errorf("sweep after the batch was settled = %+v", res)
	}
	if b, err := exec.BatchGet(ctx, batch); err != nil || b.Pending != 0 || b.Failed != 0 || b.CompletedAt.IsZero() {
		t.Errorf("settled batch = %+v, %v", b, err)
	}
	if kinds := callbacks(batch); len(kinds) != 2 || kinds["ok"] != 1 || kinds["always"] != 1 {
		t.Errorf("callbacks = %v, want ok and always once", kinds)
	}

	// A settle that cannot get a batch row in time leaves the batch to the
	// sweep, without an error.
	held := newBatch("held", 3)
	jobs = claim(ctx, t, exec, clientID, 3)
	if len(jobs) != 3 {
		t.Fatalf("claimed %d", len(jobs))
	}
	finalize(ctx, t, exec, completed(jobs[0].ID))
	tx, commit, rollback = f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // already committed on the happy path
	if res, err := d.UnwrapTx(tx).BatchSettle(ctx, driver.BatchSettleParams{IDs: []driver.JobID{held}}); err != nil || res.Folded != 1 {
		t.Fatalf("BatchSettle in a transaction = %+v, %v", res, err)
	}
	finalize(ctx, t, exec, completed(jobs[1].ID))
	start := time.Now()
	if res := settle(held); res != (driver.BatchSettleResult{}) {
		t.Errorf("BatchSettle of a held batch row = %+v", res)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("BatchSettle of a held batch row took %s", took)
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	finalize(ctx, t, exec, completed(jobs[2].ID))
	if b, err := exec.BatchGet(ctx, held); err != nil || b.Pending != 0 || b.CompletedAt.IsZero() {
		t.Errorf("held batch = %+v, %v", b, err)
	}
	if kinds := callbacks(held); len(kinds) != 2 || kinds["ok"] != 1 || kinds["always"] != 1 {
		t.Errorf("callbacks of the held batch = %v", kinds)
	}

	// Of two clients that finalized a batch's last jobs at the same time,
	// the one settling second waits for the first's settle, which could not
	// see its progress, and completes the batch.
	raced := newBatch("raced", 2)
	jobs = claim(ctx, t, exec, clientID, 2)
	if len(jobs) != 2 {
		t.Fatalf("claimed %d", len(jobs))
	}
	txB, commitB, rollbackB := f.Begin(ctx, t, d)
	defer rollbackB() //nolint:errcheck // already committed on the happy path
	finalize(ctx, t, d.UnwrapTx(txB), completed(jobs[1].ID))
	finalize(ctx, t, exec, completed(jobs[0].ID))
	txA, commitA, rollbackA := f.Begin(ctx, t, d)
	defer rollbackA() //nolint:errcheck // already committed on the happy path
	if res, err := d.UnwrapTx(txA).BatchSettle(ctx, driver.BatchSettleParams{IDs: []driver.JobID{raced}}); err != nil || res.Completed != 0 {
		t.Fatalf("first settle = %+v, %v", res, err)
	}
	if err := commitB(); err != nil {
		t.Fatal(err)
	}
	late := make(chan driver.BatchSettleResult, 1)
	go func() {
		res, err := exec.BatchSettle(ctx, driver.BatchSettleParams{IDs: []driver.JobID{raced}})
		if err != nil {
			t.Error(err)
		}
		late <- res
	}()
	// Commit as soon as the second settle waits for the row, so that its
	// wait stays well within its lock timeout on a slow machine.
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if f.QueryText(ctx, t, d, `SELECT count(*)::text FROM pg_stat_activity
		    WHERE wait_event_type = 'Lock' AND position(current_schema() IN query) > 0
		      AND query LIKE '%hopper_batch_progress%'`) != "0" {
			break
		}
	}
	if err := commitA(); err != nil {
		t.Fatal(err)
	}
	if res := <-late; res.Completed != 1 {
		t.Errorf("second settle = %+v; want the batch completed", res)
	}

	// The sweep walks batches in ID order from After.
	var open [2]driver.JobID
	for i := range open {
		open[i] = newBatch(fmt.Sprintf("open%d", i), 2)
	}
	var rest []driver.JobFinalize
	for _, j := range claim(ctx, t, exec, clientID, 4) {
		if argsN(j) == 0 {
			finalize(ctx, t, exec, completed(j.ID))
		} else {
			rest = append(rest, completed(j.ID))
		}
	}
	slices.SortFunc(open[:], func(a, b driver.JobID) int { return bytes.Compare(a[:], b[:]) })
	first, err := exec.BatchSettle(ctx, driver.BatchSettleParams{Limit: 1})
	if err != nil || first.Folded != 1 || first.Last != open[0] {
		t.Fatalf("first chunk = %+v, %v; want %s", first, err, open[0])
	}
	second, err := exec.BatchSettle(ctx, driver.BatchSettleParams{After: first.Last, Limit: 1})
	if err != nil || second.Folded != 1 || second.Last != open[1] {
		t.Fatalf("second chunk = %+v, %v; want %s", second, err, open[1])
	}
	if res := sweep(); res.Folded != 0 {
		t.Errorf("sweep after both chunks = %+v", res)
	}
	for _, id := range open {
		if b, err := exec.BatchGet(ctx, id); err != nil || b.Pending != 1 || !b.CompletedAt.IsZero() {
			t.Errorf("half-done batch = %+v, %v", b, err)
		}
	}

	// BatchInsert rejects a callback the jobs table would refuse, and a
	// batch row from before it did, with such a callback, still completes,
	// its callback's values brought into range, without failing the sweep
	// for the other batches. Progress rows whose batch is gone are deleted.
	if _, err := exec.BatchInsert(ctx, driver.BatchInsertParams{Total: 1, OnComplete: &driver.JobInsertParams{Kind: "bad", Queue: "callbacks", MaxAttempts: 3}}); err == nil {
		t.Error("BatchInsert with a callback of priority 0 succeeded")
	}
	legacy := f.QueryText(ctx, t, d, `INSERT INTO hopper_batches (pending, total, on_complete)
	    VALUES (1, 1, '{"kind": "legacy", "queue": "callbacks", "priority": 0, "max_attempts": 99999}') RETURNING id::text`)
	f.Exec(ctx, t, d, `INSERT INTO hopper_batch_progress (batch_id, done, failed)
	    VALUES ($1::uuid, 1, 0), ('00000000-0000-7000-8000-000000000001', 1, 0)`, legacy)
	if res := sweep(); res.Completed != 1 {
		t.Errorf("sweep with a legacy batch = %+v; want it completed", res)
	}
	cbs, err := exec.JobList(ctx, driver.JobListParams{Queue: "callbacks", Kinds: []string{"legacy"}, Limit: 10})
	if err != nil || len(cbs) != 1 || cbs[0].Priority != 1 || cbs[0].MaxAttempts != 32767 {
		t.Errorf("legacy callback = %+v, %v", cbs, err)
	}
	finalize(ctx, t, exec, rest...)
	for _, id := range open {
		if b, err := exec.BatchGet(ctx, id); err != nil || b.CompletedAt.IsZero() {
			t.Errorf("batch finished after its sweep = %+v, %v", b, err)
		}
	}

	// Many batches, their jobs finalized by concurrent clients in mixed
	// sets of results, each client settling its batches after each
	// statement as the finalizer does: every batch completes exactly once,
	// with no sweep, its failures counted and the callbacks its outcome
	// calls for. Every third batch has its first two jobs discarded, which
	// may land in different statements.
	const batches, size, clients, discards = 12, 6, 4, 2
	ids := make([]driver.JobID, batches)
	wantFailed := map[string]int{}
	for i := range ids {
		kind := fmt.Sprintf("many%d", i)
		ids[i] = newBatch(kind, size)
		if i%3 == 0 {
			wantFailed[kind] = discards
		}
	}
	var wg sync.WaitGroup
	for range clients {
		owner := register(ctx, t, exec)
		wg.Go(func() {
			for {
				res, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: owner, Limit: 3})
				if err != nil {
					t.Error(err)
					return
				}
				if len(res.Jobs) == 0 {
					return
				}
				results := make([]driver.JobFinalize, len(res.Jobs))
				for i, j := range res.Jobs {
					results[i] = driver.JobFinalize{ID: j.ID, AttemptedBy: owner, State: driver.JobStateCompleted, Archive: true}
					if argsN(j) < wantFailed[j.Kind] {
						results[i].State, results[i].Error = driver.JobStateDiscarded, &driver.AttemptError{Attempt: 1, Error: "x"}
					}
				}
				if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: results}); err != nil {
					t.Error(err)
					return
				}
				var batches []driver.JobID
				for _, j := range res.Jobs {
					if !slices.Contains(batches, j.BatchID) {
						batches = append(batches, j.BatchID)
					}
				}
				if _, err := exec.BatchSettle(ctx, driver.BatchSettleParams{IDs: batches}); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	if res := sweep(); res.Completed != 0 {
		t.Errorf("the sweep completed %d batches the clients' settles left open", res.Completed)
	}
	for i, id := range ids {
		want := wantFailed[fmt.Sprintf("many%d", i)]
		b, err := exec.BatchGet(ctx, id)
		if err != nil || b.CompletedAt.IsZero() || b.Pending != 0 || b.Failed != want {
			t.Errorf("batch %d = %+v, %v; want %d failed", i, b, err, want)
			continue
		}
		outcome := "ok"
		if want > 0 {
			outcome = "failed"
		}
		if kinds := callbacks(id); len(kinds) != 2 || kinds["always"] != 1 || kinds[outcome] != 1 {
			t.Errorf("batch %d (%d failed): callbacks %v", i, want, kinds)
		}
	}

	// A failure is counted where retention does not reach: a batch whose
	// failed job's history is gone before the batch completes still fails.
	pruned := newBatch("pruned", 2)
	jobs = claim(ctx, t, exec, clientID, 2)
	if len(jobs) != 2 {
		t.Fatalf("claimed %d", len(jobs))
	}
	finalize(ctx, t, exec, driver.JobFinalize{ID: jobs[0].ID, AttemptedBy: clientID, State: driver.JobStateDiscarded, Error: &driver.AttemptError{Attempt: 1, Error: "x"}})
	sweep()
	f.Exec(ctx, t, d, "DELETE FROM hopper_job_history WHERE kind = 'pruned'")
	finalize(ctx, t, exec, completed(jobs[1].ID))
	if b, err := exec.BatchGet(ctx, pruned); err != nil || b.CompletedAt.IsZero() || b.Failed != 1 {
		t.Errorf("batch whose failure's history was pruned = %+v, %v", b, err)
	}
	if kinds := callbacks(pruned); kinds["failed"] != 1 || kinds["ok"] != 0 {
		t.Errorf("callbacks of the pruned batch = %v", kinds)
	}

	if left := f.QueryText(ctx, t, d, "SELECT count(*)::text FROM hopper_batch_progress"); left != "0" {
		t.Errorf("%s progress rows left after every batch completed and settled", left)
	}
}
