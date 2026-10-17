package drivertest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// testJobTags checks that a job's tags are stored with it on every insert
// path, returned wherever the job is, kept through history and a retry
// from it, and replaced by a replacing insert, and that a job without tags
// stores none.
func testJobTags[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	p := params("k", 3)
	p[0].Tags = []string{"tenant:acme", "nightly"}
	p[1].Tags = []string{`quote " and \ backslash`, "ünïcode"}
	res := insert(ctx, t, exec, p)
	for i, r := range res {
		if !slices.Equal(r.Job.Tags, p[i].Tags) {
			t.Errorf("inserted job %d tags = %q, want %q", i, r.Job.Tags, p[i].Tags)
		}
	}
	if n := f.QueryText(ctx, t, d, "SELECT count(*)::text FROM hopper_jobs WHERE tags IS NULL"); n != "1" {
		t.Errorf("%s jobs store NULL tags, want 1 (the untagged one)", n)
	}

	// The live table refuses tags of another shape.
	const oddValues = `(VALUES ('"tenant"'::jsonb), ('[1, "ok", null, "", "ok"]'), ('{"a": "b"}')) AS v(tags)`
	f.Exec(ctx, t, d, `
		CREATE FUNCTION refuses_tags(v jsonb) RETURNS boolean LANGUAGE plpgsql AS $$
		BEGIN
		  INSERT INTO hopper_jobs (kind, queue, tags) VALUES ('odd', 'odd-tags', v);
		  RETURN false;
		EXCEPTION WHEN check_violation THEN
		  RETURN true;
		END
		$$`)
	if v := f.QueryText(ctx, t, d, "SELECT bool_and(refuses_tags(tags))::text FROM "+oddValues); v != "true" {
		t.Errorf("hopper_jobs refuses tags that are not an array of strings: %s", v)
	}
	// Nested arrays too, which a lax path's filter would unwrap.
	if v := f.QueryText(ctx, t, d, `SELECT bool_and(refuses_tags(tags))::text FROM (VALUES ('[["a"]]'::jsonb), ('[[]]'), ('["a", ["b"]]')) AS v(tags)`); v != "true" {
		t.Errorf("hopper_jobs refuses tags that nest arrays: %s", v)
	}
	// History refuses them too.
	f.Exec(ctx, t, d, `
		CREATE FUNCTION history_refuses_tags(v jsonb) RETURNS boolean LANGUAGE plpgsql AS $$
		BEGIN
		  INSERT INTO hopper_job_history (id, seq, kind, queue, state, priority, attempt, max_attempts,
		  scheduled_at, args, metadata, errors, await, created_at, finalized_at, tags)
		SELECT hopper_uuidv7(), j.seq, 'odd', 'odd-tags', 'completed', j.priority, j.attempt, j.max_attempts,
		  j.scheduled_at, j.args, j.metadata, j.errors, j.await, j.created_at, now(), v
		  FROM (SELECT * FROM hopper_jobs LIMIT 1) j;
		  RETURN false;
		EXCEPTION WHEN check_violation THEN
		  RETURN true;
		END
		$$`)
	if v := f.QueryText(ctx, t, d, `SELECT bool_and(history_refuses_tags(tags))::text FROM (VALUES ('"tenant"'::jsonb), ('[1, "ok"]'), ('{"a": "b"}'), ('[["a"]]')) AS v(tags)`); v != "true" {
		t.Errorf("hopper_job_history refuses tags that are not an array of strings: %s", v)
	}
	// The CHECKs allow the empty string and repeats, which only a direct
	// SQL write stores. They read without them rather than failing the
	// listing.
	f.Exec(ctx, t, d, `INSERT INTO hopper_job_history (id, seq, kind, queue, state, priority, attempt, max_attempts,
		  scheduled_at, args, metadata, errors, await, created_at, finalized_at, tags)
		SELECT hopper_uuidv7(), j.seq, 'odd', 'odd-tags', 'completed', j.priority, j.attempt, j.max_attempts,
		  j.scheduled_at, j.args, j.metadata, j.errors, j.await, j.created_at, now(), v.tags
		FROM (SELECT * FROM hopper_jobs LIMIT 1) j CROSS JOIN (VALUES ('["", "ok", "ok"]'::jsonb), ('[""]')) AS v(tags)`)
	odd, err := exec.JobList(ctx, driver.JobListParams{Queue: "odd-tags", Limit: 10})
	if err != nil || len(odd) != 2 {
		t.Fatalf("jobs with odd tags = %+v, %v", odd, err)
	}
	var oddTags [][]string
	for _, j := range odd {
		if j.Tags != nil {
			oddTags = append(oddTags, j.Tags)
		}
	}
	if len(oddTags) != 1 || !slices.Equal(oddTags[0], []string{"ok"}) {
		t.Errorf("odd tags read as %q, want only [ok]", oddTags)
	}
	for _, p := range []driver.JobListParams{
		{Queue: "odd-tags", TagsAny: []string{"ok"}, Limit: 10},
		{Queue: "odd-tags", TagsAll: []string{"ok"}, Limit: 10},
	} {
		byTag, err := exec.JobList(ctx, p)
		if err != nil || len(byTag) != 1 || !slices.Equal(byTag[0].Tags, []string{"ok"}) {
			t.Errorf("odd tags listed by tag (any %q, all %q) = %+v, %v", p.TagsAny, p.TagsAll, byTag, err)
		}
	}
	// A retry brings each back to the live table with the tags it read as.
	for _, j := range odd {
		retried, err := exec.JobRetry(ctx, driver.JobRetryParams{ID: j.ID})
		if err != nil || !slices.Equal(retried.Tags, j.Tags) {
			t.Errorf("job with odd tags %q retried = %+v, %v", j.Tags, retried, err)
		}
	}
	if byTag, err := exec.JobList(ctx, driver.JobListParams{Queue: "odd-tags", TagsAny: []string{"ok"}, Limit: 10}); err != nil || len(byTag) != 1 {
		t.Errorf("retried job listed by tag = %+v, %v", byTag, err)
	}
	if n := f.QueryText(ctx, t, d, "SELECT (SELECT count(*) FROM hopper_job_tags WHERE tag = '') + (SELECT count(*) FROM hopper_job_history_tags WHERE tag = '')"); n != "0" {
		t.Errorf("%s tags rows for the empty string", n)
	}
	f.Exec(ctx, t, d, "DELETE FROM hopper_jobs WHERE queue = 'odd-tags'")

	clientID := register(ctx, t, exec)
	running := claim(ctx, t, exec, clientID, 3)
	for _, j := range running {
		if want := p[argsN(j)].Tags; !slices.Equal(j.Tags, want) {
			t.Errorf("claimed job %d tags = %q, want %q", argsN(j), j.Tags, want)
		}
		finalize(ctx, t, exec, driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	}
	if n := f.QueryText(ctx, t, d, "SELECT count(*)::text FROM hopper_job_history_tags_completed_default"); n != "0" {
		t.Errorf("%s tags rows of jobs finished on a new installation in the DEFAULT partition, want 0", n)
	}
	got, err := exec.JobGet(ctx, res[1].Job.ID)
	if err != nil || got.State != driver.JobStateCompleted || !slices.Equal(got.Tags, p[1].Tags) {
		t.Fatalf("job in history = %+v, %v", got, err)
	}
	retried, err := exec.JobRetry(ctx, driver.JobRetryParams{ID: res[0].Job.ID})
	if err != nil || !slices.Equal(retried.Tags, p[0].Tags) {
		t.Errorf("job retried from history = %+v, %v", retried, err)
	}

	u := params("u", 1)[0]
	u.UniqueKey, u.Tags = "one", []string{"old"}
	insert(ctx, t, exec, []driver.JobInsertParams{u})
	u.Tags = []string{"new"}
	rep, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{u}, driver.JobInsertOpts{OnConflict: driver.ConflictReplace})
	if err != nil || !rep[0].Duplicate || !slices.Equal(rep[0].Job.Tags, []string{"new"}) {
		t.Errorf("replaced job = %+v, %v", rep, err)
	}
	u.Tags = nil
	rep, err = exec.JobInsertMany(ctx, []driver.JobInsertParams{u}, driver.JobInsertOpts{OnConflict: driver.ConflictReplace})
	if err != nil || rep[0].Job.Tags != nil {
		t.Errorf("job replaced without tags = %+v, %v", rep, err)
	}

	if d.Capabilities().Copy {
		c := params("copy", 2)
		c[1].Tags = []string{"copied"}
		copied, err := exec.JobInsertCopy(ctx, c, driver.JobInsertOpts{})
		if err != nil {
			t.Fatal(err)
		}
		for i, r := range copied {
			stored, err := exec.JobGet(ctx, r.Job.ID)
			if err != nil || !slices.Equal(stored.Tags, c[i].Tags) || !slices.Equal(r.Job.Tags, c[i].Tags) {
				t.Errorf("copied job %d tags = %q (result %q), want %q, %v", i, stored.Tags, r.Job.Tags, c[i].Tags, err)
			}
		}
	}

	w := params("wf", 2)
	w[0].Tags = []string{"flow"}
	wf, err := exec.WorkflowInsert(ctx, driver.WorkflowInsertParams{Jobs: w, Deps: []driver.JobDependency{{Job: 1, DependsOn: 0}}})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(wf.Jobs[0].Job.Tags, []string{"flow"}) || wf.Jobs[1].Job.Tags != nil {
		t.Errorf("workflow job tags = %q, %q", wf.Jobs[0].Job.Tags, wf.Jobs[1].Job.Tags)
	}

	// A batch's callback keeps its tags until the batch completes.
	cb := &driver.JobInsertParams{Kind: "done", Queue: "callbacks", Priority: 1, MaxAttempts: 3, Tags: []string{"callback"}}
	batch, err := exec.BatchInsert(ctx, driver.BatchInsertParams{Total: 1, OnComplete: cb})
	if err != nil {
		t.Fatal(err)
	}
	b := params("shard", 1)
	b[0].Queue, b[0].BatchID = "shards", batch
	insert(ctx, t, exec, b)
	shards, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "shards", ClientID: clientID, Limit: 1})
	if err != nil || len(shards.Jobs) != 1 {
		t.Fatalf("claim = %+v, %v", shards, err)
	}
	finalize(ctx, t, exec, driver.JobFinalize{ID: shards.Jobs[0].ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	callbacks, err := exec.JobList(ctx, driver.JobListParams{Queue: "callbacks", Limit: 10})
	if err != nil || len(callbacks) != 1 || !slices.Equal(callbacks[0].Tags, []string{"callback"}) {
		t.Errorf("callbacks = %+v, %v", callbacks, err)
	}
	// A callback whose tags a direct SQL write made odd gets the tags they
	// read as, rather than failing the finalize that completes its batch.
	for _, odd := range []struct {
		tags string
		want []string
	}{
		{`"tenant"`, nil},
		{`[1, "cb", "", "cb"]`, []string{"cb"}},
		// Past the tags table's largest index row, which would fail the
		// finalize; no insert takes a tag longer than 255 bytes.
		{`["cb", "` + incompressible(3000) + `"]`, []string{"cb"}},
	} {
		batch, err := exec.BatchInsert(ctx, driver.BatchInsertParams{Total: 1, OnComplete: &driver.JobInsertParams{Kind: "done", Queue: "odd-callbacks", Priority: 1, MaxAttempts: 3}})
		if err != nil {
			t.Fatal(err)
		}
		f.Exec(ctx, t, d, "UPDATE hopper_batches SET on_complete = jsonb_set(on_complete, '{tags}', $1::jsonb) WHERE id = $2::uuid", odd.tags, batch.String())
		b := params("shard", 1)
		b[0].Queue, b[0].BatchID = "shards", batch
		insert(ctx, t, exec, b)
		shards, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "shards", ClientID: clientID, Limit: 1})
		if err != nil || len(shards.Jobs) != 1 {
			t.Fatalf("claim = %+v, %v", shards, err)
		}
		finalize(ctx, t, exec, driver.JobFinalize{ID: shards.Jobs[0].ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
		callbacks, err := exec.JobList(ctx, driver.JobListParams{Queue: "odd-callbacks", Limit: 10})
		if err != nil || len(callbacks) != 1 || !slices.Equal(callbacks[0].Tags, odd.want) {
			t.Errorf("callback with tags %s = %+v, %v, want tags %q", odd.tags, callbacks, err, odd.want)
		}
		f.Exec(ctx, t, d, "DELETE FROM hopper_jobs WHERE queue = 'odd-callbacks'")
	}
}

// testJobListFilters checks the listing's ID, priority, tag and metadata
// filters, alone and together, over live jobs and history, in both orders
// and across pages.
func testJobListFilters[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	p := params("k", 8)
	for i := range p {
		p[i].Priority = 1 + i%4
		switch i % 4 {
		case 0:
			p[i].Tags = []string{"a", "b"}
		case 1:
			p[i].Tags = []string{"a"}
		case 2:
			p[i].Tags = []string{"c"}
		}
		p[i].Metadata = json.RawMessage(`{"tenant": "t` + strconv.Itoa(i%2) + `", "n": {"i": ` + strconv.Itoa(i) + `}, "list": [1, 2]}`)
	}
	res := insert(ctx, t, exec, p)
	ids := make([]driver.JobID, len(res))
	for i, r := range res {
		ids[i] = r.Job.ID
	}
	// The first half finishes, so every filter spans both tables, and job 2
	// is discarded, so that history's walks read both outcomes.
	clientID := register(ctx, t, exec)
	for _, j := range claim(ctx, t, exec, clientID, 8) {
		switch n := argsN(j); {
		case n == 2:
			discarded := driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateDiscarded, Archive: true}
			discarded.Error = &driver.AttemptError{Attempt: j.Attempt, Error: "x"}
			finalize(ctx, t, exec, discarded)
		case n < 4:
			finalize(ctx, t, exec, driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
		default:
			finalize(ctx, t, exec, driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateScheduled, Snooze: true, Delay: time.Hour})
		}
	}
	for _, c := range []struct {
		name   string
		params driver.JobListParams
		want   []int
	}{
		{"none", driver.JobListParams{}, []int{0, 1, 2, 3, 4, 5, 6, 7}},
		{"ids", driver.JobListParams{IDs: []driver.JobID{ids[6], ids[1], {1}}}, []int{1, 6}},
		{"priorities", driver.JobListParams{Priorities: []int{1, 4}}, []int{0, 3, 4, 7}},
		{"any", driver.JobListParams{TagsAny: []string{"b", "c"}}, []int{0, 2, 4, 6}},
		// Jobs 0 and 4 have both tags, and come from both arms.
		{"overlapping any", driver.JobListParams{TagsAny: []string{"b", "a", "b"}}, []int{0, 1, 4, 5}},
		{"all", driver.JobListParams{TagsAll: []string{"a", "b"}}, []int{0, 4}},
		{"repeated all", driver.JobListParams{TagsAll: []string{"b", "a", "b", "a"}}, []int{0, 4}},
		{"any and all", driver.JobListParams{TagsAny: []string{"a", "c"}, TagsAll: []string{"a"}}, []int{0, 1, 4, 5}},
		{"unknown tag", driver.JobListParams{TagsAny: []string{"zzz"}}, nil},
		{"metadata", driver.JobListParams{Metadata: json.RawMessage(`{"tenant": "t1"}`)}, []int{1, 3, 5, 7}},
		{"nested metadata", driver.JobListParams{Metadata: json.RawMessage(`{"n": {"i": 6}, "list": [2]}`)}, []int{6}},
		{"empty metadata", driver.JobListParams{Metadata: json.RawMessage(`{}`)}, []int{0, 1, 2, 3, 4, 5, 6, 7}},
		{"together", driver.JobListParams{TagsAny: []string{"a"}, Priorities: []int{1, 2}, Metadata: json.RawMessage(`{"tenant": "t0"}`), Kinds: []string{"k"}}, []int{0, 4}},
		{"history only", driver.JobListParams{TagsAll: []string{"a"}, States: []driver.JobState{driver.JobStateCompleted}}, []int{0, 1}},
		{"live only", driver.JobListParams{TagsAll: []string{"a"}, States: []driver.JobState{driver.JobStateScheduled}}, []int{4, 5}},
		// History's walks read only the outcomes the states name.
		{"completed by tag", driver.JobListParams{TagsAny: []string{"c"}, States: []driver.JobState{driver.JobStateCompleted}}, nil},
		{"discarded by tag", driver.JobListParams{TagsAny: []string{"c", "a"}, States: []driver.JobState{driver.JobStateDiscarded}}, []int{2}},
		{"canceled by tag", driver.JobListParams{TagsAll: []string{"c"}, States: []driver.JobState{driver.JobStateCanceled}}, nil},
		{"finished and live by tag", driver.JobListParams{TagsAny: []string{"c"}, States: []driver.JobState{driver.JobStateDiscarded, driver.JobStateScheduled}}, []int{2, 6}},
		// The walks read only the IDs asked for.
		{"ids by tag", driver.JobListParams{IDs: []driver.JobID{ids[0], ids[5], ids[2], ids[6]}, TagsAny: []string{"a", "c"}}, []int{0, 2, 5, 6}},
		{"ids by all tags", driver.JobListParams{IDs: []driver.JobID{ids[0], ids[5], ids[4]}, TagsAll: []string{"b", "a"}}, []int{0, 4}},
		{"ids and states by tag", driver.JobListParams{IDs: []driver.JobID{ids[1], ids[2], ids[5]}, TagsAll: []string{"a"}, States: []driver.JobState{driver.JobStateCompleted, driver.JobStateDiscarded}}, []int{1}},
	} {
		for _, newest := range []bool{false, true} {
			params := c.params
			params.Queue, params.NewestFirst, params.Limit = "default", newest, 1
			var got []int
			for {
				page, err := exec.JobList(ctx, params)
				if err != nil {
					t.Fatalf("%s: %v", c.name, err)
				}
				for _, j := range page {
					got = append(got, argsN(j))
					params.After = j.ID
				}
				if len(page) < params.Limit {
					break
				}
			}
			// Jobs inserted in one statement share a millisecond, in which
			// IDs can sort either way.
			want := slices.Clone(c.want)
			slices.SortFunc(want, func(a, b int) int { return bytes.Compare(ids[a][:], ids[b][:]) })
			if newest {
				slices.Reverse(want)
			}
			if !slices.Equal(got, want) {
				t.Errorf("%s (newest first %t) = %v, want %v", c.name, newest, got, want)
			}
		}
	}
	// A parameter checked before the listing is named, so that a priority
	// out of smallint's range is not reported as the Metadata whose input
	// also fails with 22003. What the server's input refuses fails the
	// listing with ErrInvalidFilter wrapping the server's error, naming no
	// field, since a queue's input fails with the class of error Metadata's
	// does.
	for name, c := range map[string]struct {
		params driver.JobListParams
		field  string
	}{
		"priority beyond smallint": {driver.JobListParams{Priorities: []int{70000}, Metadata: json.RawMessage(`{}`), Limit: 1}, "Priorities"},
		"ordering key alone":       {driver.JobListParams{OrderingKey: "k", Limit: 1}, "ordering key"},
		"metadata beyond numeric":  {driver.JobListParams{Priorities: []int{1}, Metadata: json.RawMessage(`{"a": 1e1000000}`), Limit: 1}, ""},
		"metadata with NUL":        {driver.JobListParams{Metadata: json.RawMessage(`{"a": "\u0000"}`), Limit: 1}, ""},
		"metadata with surrogate":  {driver.JobListParams{Metadata: json.RawMessage(`{"a": "\ud800x"}`), Limit: 1}, ""},
		"metadata not UTF-8":       {driver.JobListParams{Metadata: json.RawMessage("{\"a\": \"\xff\"}"), Limit: 1}, ""},
		"queue with NUL":           {driver.JobListParams{Queue: "q\x00", Metadata: json.RawMessage(`{}`), Limit: 1}, ""},
		"state with NUL":           {driver.JobListParams{States: []driver.JobState{"avail\x00able"}, Limit: 1}, ""},
	} {
		_, err := exec.JobList(ctx, c.params)
		if !errors.Is(err, driver.ErrInvalidFilter) || !strings.Contains(err.Error(), c.field) || c.field == "" && strings.Contains(err.Error(), "Metadata") {
			t.Errorf("%s: JobList error = %v, want ErrInvalidFilter naming %q", name, err, c.field)
		}
	}
	// In the caller's transaction, a refused filter leaves the transaction
	// usable: the next listing in it runs, and it commits.
	tx, commit, rollback := f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // committed below
	txExec := d.UnwrapTx(tx)
	if _, err := txExec.JobList(ctx, driver.JobListParams{Metadata: json.RawMessage(`{"a": "\u0000"}`), Limit: 1}); !errors.Is(err, driver.ErrInvalidFilter) {
		t.Errorf("JobList in a transaction with a refused filter = %v, want ErrInvalidFilter", err)
	}
	if got, err := txExec.JobList(ctx, driver.JobListParams{Limit: 1}); err != nil || len(got) != 1 {
		t.Errorf("JobList in the same transaction afterwards = %d jobs, %v; want 1", len(got), err)
	}
	if err := commit(); err != nil {
		t.Errorf("commit after a refused filter: %v", err)
	}
}

// testTagsPartitionWaitsForHistory checks that a tags partition past
// retention is dropped only after its history partition: while a
// transaction that wrote history defers the history partition's drop, the
// tags partition beside it is kept, so that a job still in history is still
// listed by its tags. A read of the tags partition defers only the tags
// partition's drop, which is dropped once the read ends.
func testTagsPartitionWaitsForHistory[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	p := driver.HistoryMaintainParams{CompletedRetention: 24 * time.Hour, FailedRetention: 7 * 24 * time.Hour}
	if _, err := exec.HistoryMaintain(ctx, p); err != nil {
		t.Fatal(err)
	}
	const old, oldTags = "hopper_job_history_completed_2020010100", "hopper_job_history_tags_completed_20200101"
	f.Exec(ctx, t, d, `CREATE TABLE `+old+` PARTITION OF hopper_job_history_completed
		FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-01-01 01:00:00+00')`)
	f.Exec(ctx, t, d, `CREATE TABLE `+oldTags+` PARTITION OF hopper_job_history_tags_completed
		FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-01-02 00:00:00+00')`)
	insert(ctx, t, exec, params("k", 1))
	id := f.QueryText(ctx, t, d, `INSERT INTO hopper_job_history (id, seq, kind, queue, state, priority, attempt, max_attempts,
		  scheduled_at, args, metadata, errors, await, created_at, finalized_at, tags)
		SELECT hopper_uuidv7(), j.seq, 'old', 'old', 'completed', j.priority, j.attempt, j.max_attempts,
		  j.scheduled_at, j.args, j.metadata, j.errors, j.await, j.created_at, '2020-01-01 00:30:00+00', '["old"]'
		FROM (SELECT * FROM hopper_jobs LIMIT 1) j RETURNING id::text`)
	clientID := register(ctx, t, exec)
	jobs := claim(ctx, t, exec, clientID, 1)
	if len(jobs) != 1 {
		t.Fatalf("claimed %d", len(jobs))
	}
	tx, commit, rollback := f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // already committed on the happy path
	finalize(ctx, t, d.UnwrapTx(tx), driver.JobFinalize{ID: jobs[0].ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	mctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	res, err := exec.HistoryMaintain(mctx, p)
	if err != nil {
		t.Fatalf("maintenance while history is in use: %v", err)
	}
	if !slices.Contains(res.Deferred, old) || slices.Contains(res.Dropped, oldTags) || slices.Contains(res.Deferred, oldTags) {
		t.Errorf("maintenance while history is in use = %+v; want %s deferred and %s left for after it", res, old, oldTags)
	}
	listed, err := exec.JobList(ctx, driver.JobListParams{TagsAny: []string{"old"}, Limit: 10})
	if err != nil || len(listed) != 1 || listed[0].ID.String() != id {
		t.Errorf("job in a history partition whose drop was deferred, listed by tag = %+v, %v", listed, err)
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	// A read of the tags partition, held open while maintenance runs.
	reading := make(chan struct{})
	go func() {
		defer close(reading)
		f.Exec(ctx, t, d, `SELECT count(*), pg_sleep(3) FROM `+oldTags)
	}()
	for deadline := time.Now().Add(10 * time.Second); f.QueryText(ctx, t, d,
		`SELECT count(*) FROM pg_locks WHERE relation = to_regclass($1) AND granted`, oldTags) == "0"; {
		if time.Now().After(deadline) {
			t.Fatal("the read of the tags partition did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// History's drop takes no lock on the tags table, whose readers lock it
	// before history: holding history's while it waited for theirs, it
	// could deadlock with a listing by tag.
	res, err = exec.HistoryMaintain(ctx, p)
	if err != nil || !slices.Contains(res.Dropped, old) || slices.Contains(res.Deferred, old) || !slices.Contains(res.Deferred, oldTags) {
		t.Errorf("maintenance while the tags partition is in use = %+v, %v; want %s dropped and %s deferred", res, err, old, oldTags)
	}
	<-reading
	res, err = exec.HistoryMaintain(ctx, p)
	if err != nil || !slices.Contains(res.Dropped, oldTags) {
		t.Errorf("maintenance after the read = %+v, %v; want %s dropped", res, err, oldTags)
	}
}

// testTagsLeaveWithTheirHour drops, by retention, the first of a day's two
// hourly history partitions. Its job's tags rows go with it, though the
// day's tags partition stays for the second hour: left there, a listing by
// tag walked them before the day's jobs still in history.
func testTagsLeaveWithTheirHour[TTx any](t *testing.T, f Fixture[TTx]) {
	dropTaggedHour(t, f, false)
}

// testTagsHourOutlastsItsDelete drops an hourly history partition whose
// tags rows' delete the server cancels, as a statement_timeout shorter than
// a busy hour's delete does. The hour goes without deleting them, rather
// than staying, with every later hour, for each pass to try again, and its
// rows stay in the day's tags partition, which a listing by tag reads past.
func testTagsHourOutlastsItsDelete[TTx any](t *testing.T, f Fixture[TTx]) {
	dropTaggedHour(t, f, true)
}

// dropTaggedHour drops the first of a day's two hourly history partitions,
// each holding a tagged job, by retention, its tags rows' delete canceled
// if canceled, and checks what stays.
func dropTaggedHour[TTx any](t *testing.T, f Fixture[TTx], canceled bool) {
	t.Helper()
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	const first, second, day = "hopper_job_history_completed_2020010100", "hopper_job_history_completed_2020010101", "hopper_job_history_tags_completed_20200101"
	f.Exec(ctx, t, d, `CREATE TABLE `+first+` PARTITION OF hopper_job_history_completed FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-01-01 01:00:00+00')`)
	f.Exec(ctx, t, d, `CREATE TABLE `+second+` PARTITION OF hopper_job_history_completed FOR VALUES FROM ('2020-01-01 01:00:00+00') TO ('2020-01-01 02:00:00+00')`)
	f.Exec(ctx, t, d, `CREATE TABLE `+day+` PARTITION OF hopper_job_history_tags_completed FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-01-02 00:00:00+00')`)
	insert(ctx, t, exec, params("template", 1))
	finished := func(at string) string {
		t.Helper()
		return f.QueryText(ctx, t, d, `INSERT INTO hopper_job_history (id, seq, kind, queue, state, priority, attempt, max_attempts,
			  scheduled_at, args, metadata, errors, await, created_at, finalized_at, tags)
			SELECT hopper_uuidv7(), j.seq, 'hourly', 'hourly', 'completed', j.priority, j.attempt, j.max_attempts,
			  j.scheduled_at, j.args, j.metadata, j.errors, j.await, j.created_at, $1::timestamptz, '["hourly"]'
			FROM (SELECT * FROM hopper_jobs LIMIT 1) j RETURNING id::text`, at)
	}
	gone, kept := finished("2020-01-01 00:30:00+00"), finished("2020-01-01 01:30:00+00")
	left := "0"
	if canceled {
		f.Exec(ctx, t, d, `CREATE FUNCTION hopper_test_cancel() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN RAISE EXCEPTION 'canceling statement due to statement timeout' USING ERRCODE = 'query_canceled'; END $$`)
		f.Exec(ctx, t, d, `CREATE TRIGGER hopper_test_cancel BEFORE DELETE ON `+day+` FOR EACH ROW EXECUTE FUNCTION hopper_test_cancel()`)
		left = "1"
	}
	now, err := exec.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := exec.HistoryMaintain(ctx, driver.HistoryMaintainParams{CompletedRetention: now.Sub(time.Date(2020, 1, 1, 1, 30, 0, 0, time.UTC))})
	if err != nil || !slices.Contains(res.Dropped, first) || slices.Contains(res.Dropped, second) || slices.Contains(res.Dropped, day) {
		t.Fatalf("maintenance = %+v, %v; want %s dropped, and %s and %s kept", res, err, first, second, day)
	}
	for id, want := range map[string]string{gone: left, kept: "1"} {
		if n := f.QueryText(ctx, t, d, "SELECT count(*)::text FROM hopper_job_history_tags WHERE id = $1::uuid", id); n != want {
			t.Errorf("job %s has %s tags rows after its hour's drop, want %s", id, n, want)
		}
	}
	listed, err := exec.JobList(ctx, driver.JobListParams{TagsAny: []string{"hourly"}, States: []driver.JobState{driver.JobStateCompleted}, Limit: 10})
	if err != nil || len(listed) != 1 || listed[0].ID.String() != kept {
		t.Errorf("listing by tag after the hour's drop = %+v, %v; want the second hour's job", listed, err)
	}
}

// testTagsRowsFollowJobs runs inserts (some replacing a unique job's tags),
// claims, every kind of finalize, retries from history, cancels, deletes,
// history maintenance (whose failed history's retention is short enough to
// prune it as it goes) and live table maintenance concurrently on tagged
// jobs, then checks that the tags tables hold exactly the tags of the jobs
// beside them, and that a listing by each tag finds exactly the jobs that
// have it, in order, across pages.
func testTagsRowsFollowJobs[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	tags := []string{"a", "b", "c", "d"}
	f.Exec(ctx, t, d, `CREATE TABLE hopper_job_history_completed_2020010100 PARTITION OF hopper_job_history_completed FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-01-01 01:00:00+00')`)
	f.Exec(ctx, t, d, `CREATE TABLE hopper_job_history_completed_2020010101 PARTITION OF hopper_job_history_completed FOR VALUES FROM ('2020-01-01 01:00:00+00') TO ('2020-01-01 02:00:00+00')`)
	f.Exec(ctx, t, d, `CREATE TABLE hopper_job_history_tags_completed_20200101 PARTITION OF hopper_job_history_tags_completed FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-01-02 00:00:00+00')`)
	insert(ctx, t, exec, params("template", 1))
	f.Exec(ctx, t, d, `INSERT INTO hopper_job_history (id, seq, kind, queue, state, priority, attempt, max_attempts,
		  scheduled_at, args, metadata, errors, await, created_at, finalized_at, tags)
		SELECT hopper_uuidv7(), j.seq, 'old', 'default', 'completed', j.priority, j.attempt, j.max_attempts,
		  j.scheduled_at, j.args, j.metadata, j.errors, j.await, j.created_at,
		  '2020-01-01 00:00:00+00'::timestamptz + make_interval(mins => 3 * n), jsonb_build_array('a', 'b')
		FROM (SELECT * FROM hopper_jobs LIMIT 1) j, generate_series(1, 30) n`)
	f.Exec(ctx, t, d, "DELETE FROM hopper_jobs WHERE kind = 'template'")
	now, err := exec.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	maintain := driver.HistoryMaintainParams{CompletedRetention: now.Sub(time.Date(2020, 1, 1, 1, 30, 0, 0, time.UTC)), FailedRetention: 50 * time.Millisecond}
	var (
		wg   sync.WaitGroup
		stop = make(chan struct{})
		errs = make(chan error, 16)
	)
	run := func(fn func(i int) error) {
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if err := fn(i); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	for w := range 3 {
		run(func(i int) error {
			p := params("k", 4)
			for k := range p {
				p[k].Tags = []string{tags[(w+i+k)%4], tags[(i*k)%4]}
				if p[k].Tags[0] == p[k].Tags[1] {
					p[k].Tags = p[k].Tags[:1]
				}
				if k == 3 {
					// A unique job whose tags each replacing insert changes.
					p[k].UniqueKey, p[k].Tags = "u"+strconv.Itoa(i%5), tags[i%4:i%4+1]
					if i%3 == 0 {
						p[k].Tags = nil
					}
				}
			}
			opts := driver.JobInsertOpts{}
			if i%2 == 0 {
				opts.OnConflict = driver.ConflictReplace
			}
			_, err := exec.JobInsertMany(ctx, p, opts)
			return err
		})
	}
	clientID := register(ctx, t, exec)
	for range 3 {
		run(func(i int) error {
			res, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 8})
			if err != nil {
				return err
			}
			fin := make([]driver.JobFinalize, len(res.Jobs))
			for k, j := range res.Jobs {
				fin[k] = driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true}
				switch (i + k) % 5 {
				case 1:
					fin[k].State, fin[k].Error = driver.JobStateDiscarded, &driver.AttemptError{Attempt: j.Attempt, Error: "x"}
				case 2:
					fin[k].State, fin[k].Error = driver.JobStateRetryable, &driver.AttemptError{Attempt: j.Attempt, Error: "x"}
				case 3:
					fin[k].State, fin[k].Snooze = driver.JobStateScheduled, true
				case 4:
					fin[k].Archive = false
				}
			}
			_, err = exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: fin})
			return err
		})
	}
	run(func(i int) error {
		for _, tag := range tags {
			jobs, err := exec.JobList(ctx, driver.JobListParams{TagsAny: []string{tag}, Limit: 3, NewestFirst: i%2 == 0})
			if err != nil {
				return err
			}
			for k, j := range jobs {
				var err error
				switch k {
				case 0:
					_, err = exec.JobRetry(ctx, driver.JobRetryParams{ID: j.ID})
				case 1:
					_, err = exec.JobCancel(ctx, j.ID)
				case 2:
					_, err = exec.JobDelete(ctx, j.ID)
				}
				if err != nil && !errors.Is(err, driver.ErrJobRunning) && !errors.Is(err, driver.ErrNotFound) && !errors.Is(err, driver.ErrUniqueConflict) {
					return err
				}
			}
		}
		return nil
	})
	run(func(int) error {
		// Completed history stays, so that tagged jobs are left there to
		// check, but for the first of the old day's hours, whose drop deletes
		// its jobs' tags rows; the failed DEFAULT partitions' prune deletes
		// jobs and their tags rows while finalizes, retries and deletes reach
		// them.
		_, err := exec.HistoryMaintain(ctx, maintain)
		return err
	})
	run(func(int) error {
		_, err := exec.JobsMaintain(ctx)
		return err
	})
	time.Sleep(2 * time.Second)
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if _, err := exec.HistoryMaintain(ctx, maintain); err != nil {
		t.Fatal(err)
	}
	if n := f.QueryText(ctx, t, d, "SELECT (to_regclass('hopper_job_history_completed_2020010100') IS NULL AND to_regclass('hopper_job_history_tags_completed_20200101') IS NOT NULL)::text"); n != "true" {
		t.Fatal("the old day's first hour was not dropped apart from its tags partition")
	}

	if n := f.QueryText(ctx, t, d, "SELECT count(*)::text FROM hopper_job_history_tags"); n == "0" {
		t.Fatal("no tagged job reached history")
	}
	checkTagsRows(ctx, t, f, d, "EXCEPT")
	for _, tag := range tags {
		want := f.QueryText(ctx, t, d, `SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '') FROM (
			SELECT id FROM hopper_jobs WHERE tags ? $1 UNION ALL SELECT id FROM hopper_job_history WHERE tags ? $1) x`, tag)
		var got []string
		p := driver.JobListParams{TagsAny: []string{tag}, Limit: 7}
		for {
			page, err := exec.JobList(ctx, p)
			if err != nil {
				t.Fatal(err)
			}
			for _, j := range page {
				got = append(got, j.ID.String())
				p.After = j.ID
			}
			if len(page) < p.Limit {
				break
			}
		}
		if strings.Join(got, ",") != want {
			t.Errorf("listing by %q found %d jobs, want %d", tag, len(got), strings.Count(want, ",")+1)
		}
	}
}

// testTagsTableMaintained checks that the leader's live table maintenance
// vacuums the live tags table, whose index a listing by tag walks, once its
// dead rows number as many as the live table's would.
func testTagsTableMaintained[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	// The table's autovacuum settings (v17) would otherwise have autovacuum
	// vacuum it first, leaving the leader no dead rows to find, or hold it
	// while the leader's VACUUM (SKIP_LOCKED) skips it.
	f.Exec(ctx, t, d, "ALTER TABLE hopper_job_tags SET (autovacuum_enabled = false)")
	p := params("vacuum", 55000)
	for i := range p {
		p[i].Tags = []string{"a", "b"}
	}
	insert(ctx, t, exec, p)
	f.Exec(ctx, t, d, "DELETE FROM hopper_jobs WHERE kind = 'vacuum'")
	vacuumed := func() string {
		return f.QueryText(ctx, t, d, "SELECT (last_vacuum IS NOT NULL)::text FROM pg_stat_user_tables WHERE relid = 'hopper_job_tags'::regclass")
	}
	if v := vacuumed(); v != "false" {
		t.Fatalf("tags table vacuumed before maintenance = %s", v)
	}
	if res := waitMaintain(ctx, t, exec, func(r driver.JobsMaintainResult) bool { return r.Vacuumed }); !res.Vacuumed {
		t.Fatalf("maintenance after 110000 dead tags rows = %+v", res)
	}
	// Postgres 14's statistics collector reports the vacuum a moment later.
	for deadline := time.Now().Add(10 * time.Second); vacuumed() != "true"; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("tags table not vacuumed")
		}
	}
}

// testTagsTableMaintainedPastJobsError checks that the live tags table is
// maintained even when hopper_jobs' maintenance fails, here by an extended
// statistic whose expression its ANALYZE cannot compute.
func testTagsTableMaintainedPastJobsError[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	for _, table := range []string{"hopper_jobs", "hopper_job_tags"} {
		f.Exec(ctx, t, d, "ALTER TABLE "+table+" SET (autovacuum_enabled = false)")
	}
	f.Exec(ctx, t, d, "CREATE STATISTICS hopper_jobs_failing ON (1 / (priority - priority)) FROM hopper_jobs")
	p := params("analyze", 1200)
	for i := range p {
		p[i].Tags = []string{"a"}
	}
	insert(ctx, t, exec, p)
	stat := func(q string) string {
		return f.QueryText(ctx, t, d, "SELECT ("+q+")::text FROM pg_stat_user_tables WHERE relid = 'hopper_job_tags'::regclass")
	}
	// Postgres 14's statistics collector reports the inserts a moment later.
	for deadline := time.Now().Add(10 * time.Second); stat("n_mod_since_analyze >= 1200") != "true"; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the tags table's inserts were not counted")
		}
	}
	for deadline := time.Now().Add(10 * time.Second); stat("last_analyze IS NOT NULL") != "true"; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("tags table not analyzed while hopper_jobs' maintenance failed")
		}
		if _, err := exec.JobsMaintain(ctx); err == nil {
			t.Error("maintenance with hopper_jobs' ANALYZE failing reported no error")
		}
	}
}

// testTagsFollowHistoryRetention checks that retention's prune of the DEFAULT
// partitions keeps history's tags rows with their jobs: a job pruned from
// history's DEFAULT partition takes its tags rows along, wherever they are,
// and a tags row in the tags' DEFAULT partition stays while its job is in one
// of history's time partitions.
func testTagsFollowHistoryRetention[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	insert(ctx, t, exec, params("template", 1))
	now, err := exec.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	hour := now.UTC().Truncate(time.Hour)
	partition := func(parent, root string, from time.Time) {
		t.Helper()
		name := parent + "_" + from.Format("2006010215")
		f.Exec(ctx, t, d, "CREATE TABLE "+name+" (LIKE "+root+" INCLUDING DEFAULTS INCLUDING CONSTRAINTS)")
		f.Exec(ctx, t, d, "ALTER TABLE "+parent+" ATTACH PARTITION "+name+" FOR VALUES FROM ('"+
			from.Format(time.RFC3339)+"') TO ('"+from.Add(time.Hour).Format(time.RFC3339)+"')")
	}
	finished := func(tag string, at time.Time) string {
		t.Helper()
		return f.QueryText(ctx, t, d, `INSERT INTO hopper_job_history (id, seq, kind, queue, state, priority, attempt, max_attempts,
			  scheduled_at, args, metadata, errors, await, created_at, finalized_at, tags)
			SELECT hopper_uuidv7(), j.seq, 'retained', 'retained', 'completed', j.priority, j.attempt, j.max_attempts,
			  j.scheduled_at, j.args, j.metadata, j.errors, j.await, j.created_at, $1, jsonb_build_array($2::text)
			FROM (SELECT * FROM hopper_jobs LIMIT 1) j RETURNING id::text`, at, tag)
	}
	tagRows := func(id string) string {
		t.Helper()
		return f.QueryText(ctx, t, d, "SELECT count(*)::text FROM hopper_job_history_tags WHERE id = $1::uuid", id)
	}
	// retain keeps history from a minute after from on, so that a row at
	// from is past retention and from's period is not.
	retain := func(from time.Time) int64 {
		t.Helper()
		now, err := exec.Now(ctx)
		if err != nil {
			t.Fatal(err)
		}
		res, err := exec.HistoryMaintain(ctx, driver.HistoryMaintainParams{CompletedRetention: now.Sub(from) - time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		return res.Pruned
	}

	// History's DEFAULT partition holds the job, and a tags time partition
	// its tags row.
	pruned := hour.Add(-4 * time.Hour)
	day := pruned.Truncate(24 * time.Hour)
	f.Exec(ctx, t, d, "CREATE TABLE IF NOT EXISTS hopper_job_history_tags_completed_"+day.Format("20060102")+" PARTITION OF hopper_job_history_tags_completed FOR VALUES FROM ('"+
		day.Format(time.RFC3339)+"') TO ('"+day.Add(24*time.Hour).Format(time.RFC3339)+"')")
	id := finished("pruned", pruned)
	if n := tagRows(id); n != "1" {
		t.Fatalf("tags rows before retention = %s", n)
	}
	// A tags row in the tags' DEFAULT partition whose job is gone, as one
	// left by a dropped history partition, is pruned too, but Pruned counts
	// jobs: only the one.
	f.Exec(ctx, t, d, `INSERT INTO hopper_job_history_tags (state, finalized_at, tag, id) VALUES ('completed', $1, 'orphan', hopper_uuidv7())`, hour.Add(-96*time.Hour))
	if n := retain(pruned); n != 1 {
		t.Errorf("Pruned = %d, want the one job", n)
	}
	if n := f.QueryText(ctx, t, d, "SELECT count(*)::text FROM hopper_job_history_tags WHERE tag = 'orphan'"); n != "0" {
		t.Errorf("orphan tags rows after retention = %s", n)
	}
	if _, err := exec.JobGet(ctx, mustID(t, id)); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("pruned job = %v", err)
	}
	if n := tagRows(id); n != "0" {
		t.Errorf("tags rows of a pruned job = %s", n)
	}

	// A history time partition holds the job, and the tags' DEFAULT partition
	// its tags row.
	kept := hour.Add(-72 * time.Hour)
	partition("hopper_job_history_completed", "hopper_job_history", kept)
	id = finished("kept", kept)
	retain(kept)
	if _, err := exec.JobGet(ctx, mustID(t, id)); err != nil {
		t.Fatalf("job in a partition within retention = %v", err)
	}
	jobs, err := exec.JobList(ctx, driver.JobListParams{Queue: "retained", TagsAny: []string{"kept"}, Limit: 10})
	if err != nil || len(jobs) != 1 || jobs[0].ID.String() != id {
		t.Errorf("job kept in history listed by tag = %+v, %v", jobs, err)
	}
}

// testTagsFollowDirectUpdates updates jobs by direct SQL in every way that
// changes a tags row's key: a live job's tags and ID, and a finished job's
// tags, its finalized_at within its partition and across partitions, and its
// state across outcomes. It also writes to history through an outcome table
// and a partition, which fire none of a partitioned table's statement
// triggers. Each tags table then holds exactly its jobs' tags, and a listing
// by tag finds each job where it now is.
func testTagsFollowDirectUpdates[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	p := params("direct", 9)
	for i := range p {
		p[i].Tags = []string{"t" + strconv.Itoa(i), "shared"}
	}
	insert(ctx, t, exec, p)
	clientID := register(ctx, t, exec)
	for _, j := range claim(ctx, t, exec, clientID, 9) {
		if !slices.Contains(j.Tags, "t4") && !slices.Contains(j.Tags, "t5") {
			finalize(ctx, t, exec, driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
		}
	}
	id := func(tag string) string {
		t.Helper()
		return f.QueryText(ctx, t, d, `SELECT id::text FROM (SELECT id, tags FROM hopper_jobs UNION ALL SELECT id, tags FROM hopper_job_history) j
			WHERE tags ? $1`, tag)
	}
	f.Exec(ctx, t, d, "UPDATE hopper_job_history SET tags = '[\"moved\", \"shared\"]' WHERE id = $1::uuid", id("t0"))
	f.Exec(ctx, t, d, "UPDATE hopper_job_history SET finalized_at = finalized_at + interval '1 microsecond' WHERE id = $1::uuid", id("t1"))
	f.Exec(ctx, t, d, "UPDATE hopper_job_history SET finalized_at = finalized_at - interval '30 days' WHERE id = $1::uuid", id("t2"))
	f.Exec(ctx, t, d, "UPDATE hopper_job_history SET state = 'canceled' WHERE id = $1::uuid", id("t3"))
	f.Exec(ctx, t, d, "UPDATE hopper_jobs SET id = hopper_uuidv7() WHERE id = $1::uuid", id("t4"))
	f.Exec(ctx, t, d, "UPDATE hopper_jobs SET tags = '[\"t5\"]' WHERE id = $1::uuid", id("t5"))
	f.Exec(ctx, t, d, "UPDATE hopper_job_history_completed SET tags = '[\"t6b\"]' WHERE id = $1::uuid", id("t6"))
	leaf := func(tag string) string {
		t.Helper()
		return f.QueryText(ctx, t, d, "SELECT tableoid::regclass::text FROM hopper_job_history WHERE tags ? $1", tag)
	}
	f.Exec(ctx, t, d, "DELETE FROM "+leaf("t7")+" WHERE tags ? 't7'")
	f.Exec(ctx, t, d, "UPDATE "+leaf("t8")+" SET finalized_at = finalized_at + interval '1 microsecond' WHERE tags ? 't8'")
	// An update that changes no key leaves the rows as they are.
	f.Exec(ctx, t, d, "UPDATE hopper_job_history SET output = output")

	checkTagsRows(ctx, t, f, d, "EXCEPT ALL")
	canceled := []driver.JobState{driver.JobStateCanceled}
	for _, c := range []struct {
		tag    string
		all    []string
		states []driver.JobState
	}{
		{"moved", []string{"moved", "shared"}, nil},
		{"t1", []string{"t1", "shared"}, nil},
		{"t2", []string{"t2", "shared"}, nil},
		{"t3", []string{"t3", "shared"}, canceled},
		{"t4", []string{"t4", "shared"}, nil},
		{"t5", []string{"t5"}, nil},
		{"t6b", []string{"t6b"}, nil},
		{"t8", []string{"t8", "shared"}, nil},
	} {
		jobs, err := exec.JobList(ctx, driver.JobListParams{TagsAll: c.all, States: c.states, Limit: 10})
		if err != nil || len(jobs) != 1 || jobs[0].ID.String() != id(c.tag) {
			t.Errorf("listing by %s = %v, %v; want the job %s", c.tag, jobs, err, id(c.tag))
		}
	}
	for _, tag := range []string{"t0", "t6", "t7"} {
		if jobs, err := exec.JobList(ctx, driver.JobListParams{TagsAny: []string{tag}, Limit: 10}); err != nil || len(jobs) != 0 {
			t.Errorf("listing by the removed tag %s = %v, %v", tag, jobs, err)
		}
	}
	jobs, err := exec.JobList(ctx, driver.JobListParams{TagsAny: []string{"shared"}, Limit: 10})
	if err != nil || len(jobs) != 6 {
		t.Errorf("listing by shared = %d jobs, %v; want 6", len(jobs), err)
	}

	// A tags row whose job no longer has the tag, as a write the triggers
	// did not see leaves, lists nothing.
	f.Exec(ctx, t, d, "ALTER TABLE hopper_jobs DISABLE TRIGGER hopper_jobs_tags_update")
	f.Exec(ctx, t, d, "UPDATE hopper_jobs SET tags = '[\"t4x\"]' WHERE id = $1::uuid", id("t4"))
	f.Exec(ctx, t, d, "ALTER TABLE hopper_jobs ENABLE TRIGGER hopper_jobs_tags_update")
	for _, l := range []driver.JobListParams{{TagsAny: []string{"t4"}}, {TagsAny: []string{"t0", "t4"}}, {TagsAll: []string{"t4"}}} {
		l.Limit = 10
		if jobs, err := exec.JobList(ctx, l); err != nil || len(jobs) != 0 {
			t.Errorf("listing %+v by a tag only a stale tags row has = %v, %v", l, jobs, err)
		}
	}
}

// checkTagsRows checks that each tags table holds exactly the tags of the
// jobs beside it, comparing the two with except: "EXCEPT", or "EXCEPT ALL"
// to count a duplicate tags row as a mismatch too.
func checkTagsRows[TTx any](ctx context.Context, t *testing.T, f Fixture[TTx], d driver.Driver[TTx], except string) {
	t.Helper()
	const liveExpanded = `SELECT e.tag, j.id FROM hopper_jobs j, LATERAL jsonb_array_elements_text(j.tags) e(tag)`
	const historyExpanded = `SELECT h.state, h.finalized_at, e.tag, h.id FROM hopper_job_history h, LATERAL jsonb_array_elements_text(h.tags) e(tag)`
	for name, q := range map[string]string{
		"live tags rows without their job's tag":    "SELECT tag, id FROM hopper_job_tags " + except + " " + liveExpanded,
		"live jobs' tags without a tags row":        liveExpanded + " " + except + " SELECT tag, id FROM hopper_job_tags",
		"history tags rows without their job's tag": "SELECT state, finalized_at, tag, id FROM hopper_job_history_tags " + except + " " + historyExpanded,
		"history jobs' tags without a tags row":     historyExpanded + " " + except + " SELECT state, finalized_at, tag, id FROM hopper_job_history_tags",
	} {
		if n := f.QueryText(ctx, t, d, "SELECT count(*)::text FROM ("+q+") x"); n != "0" {
			t.Errorf("%s: %s", name, n)
		}
	}
}

// incompressible returns n letters and digits that Postgres's compression
// cannot shrink, so that a value of them is as large in an index row as it
// is long. A repeated character would compress to a few bytes.
func incompressible(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const size = uint64(len(alphabet))
	b := make([]byte, n)
	x := uint64(0x9e3779b97f4a7c15)
	for i := range b {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		b[i] = alphabet[x%size]
	}
	return string(b)
}

func mustID(t *testing.T, s string) driver.JobID {
	t.Helper()
	id, err := driver.ParseJobID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// testTagsFollowTruncate truncates hopper_jobs, then history, each alone and
// then together with every tags table, as a reset of every table does. The
// tags tables follow, though TRUNCATE fires no delete trigger.
func testTagsFollowTruncate[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	fill := func() {
		t.Helper()
		p := params("truncate", 4)
		for i := range p {
			p[i].Tags = []string{"truncated"}
		}
		insert(ctx, t, exec, p)
		clientID := register(ctx, t, exec)
		for _, j := range claim(ctx, t, exec, clientID, 2) {
			finalize(ctx, t, exec, driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
		}
	}
	rows := func(table string) string {
		t.Helper()
		return f.QueryText(ctx, t, d, "SELECT count(*)::text FROM "+table)
	}
	fill()
	f.Exec(ctx, t, d, "TRUNCATE hopper_jobs")
	if live, history := rows("hopper_job_tags"), rows("hopper_job_history_tags"); live != "0" || history != "2" {
		t.Errorf("after TRUNCATE hopper_jobs: %s live and %s history tags rows, want 0 and 2", live, history)
	}
	f.Exec(ctx, t, d, "TRUNCATE hopper_job_history")
	if n := rows("hopper_job_history_tags"); n != "0" {
		t.Errorf("after TRUNCATE hopper_job_history: %s history tags rows, want 0", n)
	}
	fill()
	f.Exec(ctx, t, d, "TRUNCATE hopper_jobs, hopper_job_history, hopper_job_tags, hopper_job_history_tags")
	if live, history := rows("hopper_job_tags"), rows("hopper_job_history_tags"); live != "0" || history != "0" {
		t.Errorf("after truncating every table: %s live and %s history tags rows, want 0", live, history)
	}
	if jobs, err := exec.JobList(ctx, driver.JobListParams{TagsAny: []string{"truncated"}, Limit: 10}); err != nil || len(jobs) != 0 {
		t.Errorf("listing by tag after the truncates = %v, %v", jobs, err)
	}
}

// testTagsPagePastFilteredHistory lists by tag and by a kind that the first
// thirty finished jobs with the tag lack, in pages smaller than that. The
// walk of history's tags rows returns a page of keys, whose jobs the filter
// then drops, so each page walks on until it is full: every job of the kind,
// three finished and one live, is listed once, in order, in both directions.
func testTagsPagePastFilteredHistory[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	tagged := func(kind string, n int) []driver.JobInsertParams {
		p := params(kind, n)
		for i := range p {
			p[i].Tags = []string{"p", "q"}
		}
		return p
	}
	insert(ctx, t, exec, tagged("skip", 30))
	var want []driver.JobID
	for _, r := range insert(ctx, t, exec, tagged("want", 3)) {
		want = append(want, r.Job.ID)
	}
	clientID := register(ctx, t, exec)
	for _, j := range claim(ctx, t, exec, clientID, 33) {
		finalize(ctx, t, exec, driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	}
	want = append(want, insert(ctx, t, exec, tagged("want", 1))[0].Job.ID)
	for _, c := range []struct {
		name string
		p    driver.JobListParams
	}{
		{"any", driver.JobListParams{TagsAny: []string{"p"}}},
		{"all", driver.JobListParams{TagsAll: []string{"p", "q"}}},
		{"any newest first", driver.JobListParams{TagsAny: []string{"p"}, NewestFirst: true}},
	} {
		c.p.Kinds, c.p.Limit = []string{"want"}, 2
		var got []driver.JobID
		for range 10 {
			page, err := exec.JobList(ctx, c.p)
			if err != nil {
				t.Fatalf("%s: %v", c.name, err)
			}
			if len(page) == 0 {
				break
			}
			for _, j := range page {
				got = append(got, j.ID)
			}
			c.p.After = page[len(page)-1].ID
		}
		expected := slices.Clone(want)
		slices.SortFunc(expected, func(a, b driver.JobID) int { return bytes.Compare(a[:], b[:]) })
		if c.p.NewestFirst {
			slices.Reverse(expected)
		}
		if !slices.Equal(got, expected) {
			t.Errorf("%s: listed %v, want %v", c.name, got, expected)
		}
	}
}
