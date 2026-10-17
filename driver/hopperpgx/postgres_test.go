package hopperpgx_test

// Postgres-specific behavior. The engine-agnostic contract is covered by
// the drivertest suite (see conformance_test.go).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/hopperpgx"
	"github.com/parallelworks/hopper/internal/testdb"
)

func setup(t *testing.T) (context.Context, *hopperpgx.Driver, driver.Executor) {
	t.Helper()
	pool := testdb.Pool(t)
	d := testdb.Driver(pool)
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
	claimed, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 2})
	jobs := claimed.Jobs
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

	var now time.Time
	if err := pool.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		t.Fatal(err)
	}
	res, err := exec.HistoryMaintain(ctx, driver.HistoryMaintainParams{CompletedRetention: 24 * time.Hour, FailedRetention: 7 * 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	// 3 hourly and 2 daily periods ahead of the current ones for history,
	// and 2 daily stream partitions. History's tags rows have the 2 days
	// ahead too, tomorrow's from the migration.
	tagsCreated := 0
	for _, name := range res.Created {
		if strings.HasPrefix(name, "hopper_job_history_tags_") {
			tagsCreated++
		}
	}
	if len(res.Created)-tagsCreated != 3+2+2 {
		t.Errorf("created = %v", res.Created)
	}
	for _, outcome := range []string{"completed", "failed"} {
		for days := 1; days <= 2; days++ {
			name := "hopper_job_history_tags_" + outcome + "_" + now.UTC().Truncate(24*time.Hour).Add(time.Duration(days)*24*time.Hour).Format("20060102")
			var exists bool
			if err := pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists); err != nil || !exists {
				t.Errorf("tags partition ahead %s exists = %t, %v", name, exists, err)
			}
		}
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

// TestHistoryMaintainGoesPastGroupError checks that a group whose
// maintenance fails leaves the others maintained: with the stream log's
// partition ahead failing to attach, history's tags still get theirs.
func TestHistoryMaintainGoesPastGroupError(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	pool := d.Pool()
	var now time.Time
	if err := pool.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		t.Fatal(err)
	}
	day := now.UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
	// The DEFAULT partition holds a row of the day ahead, which that day's
	// partition cannot be attached past.
	if _, err := pool.Exec(ctx, "INSERT INTO hopper_stream_events (topic, created_at) VALUES ('t', $1)", day.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := exec.HistoryMaintain(ctx, driver.HistoryMaintainParams{}); err == nil {
		t.Fatal("HistoryMaintain with a stream log partition that cannot attach = nil error")
	}
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", "hopper_job_history_tags_completed_"+day.Format("20060102")).Scan(&exists); err != nil || !exists {
		t.Errorf("history tags partition ahead exists = %t, %v; want it created past the stream log's error", exists, err)
	}
}

// TestHistoryMaintainRetainsPastCreateError has the stream log's partition
// ahead fail to attach on every pass, and checks that the group's retention
// still runs: its partition past retention is dropped, and its DEFAULT
// partition pruned.
func TestHistoryMaintainRetainsPastCreateError(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	pool := d.Pool()
	for _, q := range []string{
		// Rows of both days ahead, so that the pass fails whichever day it
		// runs in.
		`INSERT INTO hopper_stream_events (topic, created_at)
		 SELECT 't', date_trunc('day', now(), 'UTC') + make_interval(days => d, hours => 1) FROM generate_series(1, 2) d`,
		`INSERT INTO hopper_stream_events (topic, created_at) VALUES ('t', now() - interval '30 days')`,
		`CREATE TABLE hopper_stream_events_20200101 PARTITION OF hopper_stream_events
		 FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-01-02 00:00:00+00')`,
	} {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	res, err := exec.HistoryMaintain(ctx, driver.HistoryMaintainParams{StreamRetention: 24 * time.Hour})
	if err == nil {
		t.Fatal("HistoryMaintain with a stream log partition that cannot attach = nil error")
	}
	if !slices.Contains(res.Dropped, "hopper_stream_events_20200101") || res.Pruned != 1 {
		t.Errorf("stream log retention past its creation error = %+v; want hopper_stream_events_20200101 dropped and 1 row pruned", res)
	}
}

// TestTagsPartitionKeptWhileHistoryPruneFails has a tagged job of a period
// whose history partition never existed, so that it is in history's DEFAULT
// partition while its tags rows are in that period's tags partition. While
// the DEFAULT partition's prune fails, the tags partition past retention is
// kept, so that the job, still in history, is still listed by its tag; once
// the prune takes the job, the tags partition goes too.
func TestTagsPartitionKeptWhileHistoryPruneFails(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	pool := d.Pool()
	const tagsPartition = "hopper_job_history_tags_completed_20200101"
	var id string
	for _, q := range []string{
		`CREATE TABLE ` + tagsPartition + ` PARTITION OF hopper_job_history_tags_completed
		 FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-01-02 00:00:00+00')`,
		`CREATE FUNCTION refuse_prune() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'prune refused'; END $$`,
		`CREATE TRIGGER refuse_prune BEFORE DELETE ON hopper_job_history_completed_default
		 FOR EACH ROW EXECUTE FUNCTION refuse_prune()`,
	} {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	if err := pool.QueryRow(ctx, `INSERT INTO hopper_job_history (id, seq, kind, queue, state, priority, attempt, max_attempts,
		  scheduled_at, args, metadata, errors, await, created_at, finalized_at, tags)
		VALUES (hopper_uuidv7(), 1, 'k', 'default', 'completed', 2, 1, 1, now(), '{}', '{}', '[]', false, now(),
		  '2020-01-01 00:30:00+00', '["old"]') RETURNING id::text`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	p := driver.HistoryMaintainParams{CompletedRetention: 24 * time.Hour, FailedRetention: 7 * 24 * time.Hour}
	res, err := exec.HistoryMaintain(ctx, p)
	if err == nil || slices.Contains(res.Dropped, tagsPartition) {
		t.Errorf("maintenance while history's prune fails = %+v, %v; want an error and %s kept", res, err, tagsPartition)
	}
	listed, err := exec.JobList(ctx, driver.JobListParams{TagsAny: []string{"old"}, Limit: 10})
	if err != nil || len(listed) != 1 || listed[0].ID.String() != id {
		t.Errorf("job in history's DEFAULT partition listed by tag = %+v, %v", listed, err)
	}
	if _, err := pool.Exec(ctx, "DROP TRIGGER refuse_prune ON hopper_job_history_completed_default"); err != nil {
		t.Fatal(err)
	}
	res, err = exec.HistoryMaintain(ctx, p)
	if err != nil || !slices.Contains(res.Dropped, tagsPartition) {
		t.Errorf("maintenance once history's prune runs = %+v, %v; want %s dropped", res, err, tagsPartition)
	}
}

// TestTagsDayWaitsForEveryHourOfIt checks that a day's tags partition is
// dropped only once every hourly history partition of that day is gone: while
// a reader holds the drop of the day's second hour off, the day's tags
// partition, which holds that hour's jobs' tags rows too, stays.
func TestTagsDayWaitsForEveryHourOfIt(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	pool := d.Pool()
	const first, second, day = "hopper_job_history_completed_2020010100", "hopper_job_history_completed_2020010101", "hopper_job_history_tags_completed_20200101"
	for _, q := range []string{
		`CREATE TABLE ` + first + ` PARTITION OF hopper_job_history_completed FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-01-01 01:00:00+00')`,
		`CREATE TABLE ` + second + ` PARTITION OF hopper_job_history_completed FOR VALUES FROM ('2020-01-01 01:00:00+00') TO ('2020-01-01 02:00:00+00')`,
		`CREATE TABLE ` + day + ` PARTITION OF hopper_job_history_tags_completed FOR VALUES FROM ('2020-01-01 00:00:00+00') TO ('2020-01-02 00:00:00+00')`,
	} {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	reader, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback(ctx) //nolint:errcheck // read only
	if _, err := reader.Exec(ctx, "SELECT count(*) FROM "+second); err != nil {
		t.Fatal(err)
	}
	p := driver.HistoryMaintainParams{CompletedRetention: 24 * time.Hour, FailedRetention: 7 * 24 * time.Hour}
	res, err := exec.HistoryMaintain(ctx, p)
	if err != nil || slices.Contains(res.Dropped, second) || slices.Contains(res.Dropped, day) {
		t.Errorf("maintenance while %s is read = %+v, %v; want it and %s kept", second, res, err, day)
	}
	if err := reader.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	res, err = exec.HistoryMaintain(ctx, p)
	if err != nil || !slices.Contains(res.Dropped, day) {
		t.Errorf("maintenance once every hour of the day can go = %+v, %v; want %s dropped", res, err, day)
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

// TestTagsAllWalksRarerTag checks that TagsAll walks the rarer of two tags
// that both reach the count's cap, by the live tags table's statistics: the
// common tag's 1,900 jobs come first in ID order, so walking it would read
// them all before the first job that has both.
func TestTagsAllWalksRarerTag(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	pool := d.Pool()
	common := insertParams("common", 1900)
	for i := range common {
		common[i].Tags = []string{"every"}
	}
	both := insertParams("both", 1100)
	for i := range both {
		both[i].Tags = []string{"every", "some"}
	}
	for _, p := range [][]driver.JobInsertParams{common, both} {
		if _, err := exec.JobInsertMany(ctx, p, driver.JobInsertOpts{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, "ANALYZE hopper_job_tags"); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // read only
	jobs, err := d.UnwrapTx(tx).JobList(ctx, driver.JobListParams{TagsAll: []string{"every", "some"}, Limit: 20})
	if err != nil || len(jobs) != 20 {
		t.Fatalf("JobList = %d jobs, %v", len(jobs), err)
	}
	// The counts read up to 1,000 entries of each tag; the walk of "some"
	// reads a page's worth more, and that of "every" 1,900.
	var read int64
	if err := tx.QueryRow(ctx, "SELECT pg_stat_get_xact_tuples_returned('hopper_job_tags_pkey'::regclass)").Scan(&read); err != nil {
		t.Fatal(err)
	}
	if read > 3000 {
		t.Errorf("TagsAll read %d tags entries, as if it walked the common tag", read)
	}
}

// TestTagsAllWalksTagRareInEachTable lists by two tags across the live table
// and history, where the common tag has few live jobs: its count is under the
// cap in one table, but it has many more jobs than the other tag, which is
// past the cap in both. The listing walks the other one.
func TestTagsAllWalksTagRareInEachTable(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	pool := d.Pool()
	insert := func(kind string, n int, tags ...string) {
		t.Helper()
		p := insertParams(kind, n)
		for i := range p {
			p[i].Tags = tags
		}
		if _, err := exec.JobInsertMany(ctx, p, driver.JobInsertOpts{}); err != nil {
			t.Fatal(err)
		}
	}
	insert("old", 20000, "old")
	insert("new_done", 1200, "new")
	if _, err := pool.Exec(ctx, `WITH d AS (DELETE FROM hopper_jobs RETURNING *)
		INSERT INTO hopper_job_history (id, seq, kind, queue, state, priority, attempt, max_attempts, scheduled_at,
		  args, metadata, errors, await, created_at, finalized_at, tags)
		SELECT id, seq, kind, queue, 'completed', priority, attempt, max_attempts, scheduled_at,
		  args, metadata, errors, await, created_at, now(), tags FROM d`); err != nil {
		t.Fatal(err)
	}
	insert("new", 1200, "new")
	insert("both", 5, "old", "new")
	if _, err := pool.Exec(ctx, "ANALYZE hopper_job_tags; ANALYZE hopper_job_history_tags"); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // read only
	jobs, err := d.UnwrapTx(tx).JobList(ctx, driver.JobListParams{TagsAll: []string{"old", "new"}, Limit: 20})
	if err != nil || len(jobs) != 5 {
		t.Fatalf("JobList = %d jobs, %v; want 5", len(jobs), err)
	}
	// The counts read up to 1,000 entries of each tag in each table, and the
	// walk of "new" its 2,405; that of "old" would read its 20,000 in history.
	var read int64
	if err := tx.QueryRow(ctx, `SELECT coalesce(sum(pg_stat_get_xact_tuples_returned(x.indexrelid)), 0)::bigint
		FROM pg_index x JOIN pg_class c ON c.oid = x.indrelid
		WHERE c.relnamespace = current_schema()::regnamespace AND c.relname LIKE 'hopper\_job%tags%'`).Scan(&read); err != nil {
		t.Fatal(err)
	}
	if read > 10000 {
		t.Errorf("TagsAll read %d tags entries, as if it walked the tag with few live jobs and many in history", read)
	}
}

// TestTagsReinsertUnderSameIDFails deletes a tagged job and inserts it
// again under its ID in one statement. The live tags table's insert trigger
// adds the job's rows before the delete's statement trigger has removed the
// old ones, so the statement fails, rather than leave the job without its
// tags rows, and the job and its rows stay as they were.
func TestTagsReinsertUnderSameIDFails(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	pool := d.Pool()
	p := insertParams("reinsert", 1)
	p[0].Tags = []string{"a", "b"}
	if _, err := exec.JobInsertMany(ctx, p, driver.JobInsertOpts{}); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Exec(ctx, `WITH d AS (DELETE FROM hopper_jobs RETURNING *) INSERT INTO hopper_jobs (id, kind, queue, args, tags) SELECT id, kind, queue, args, tags FROM d`)
	var pgErr interface{ SQLState() string }
	if !errors.As(err, &pgErr) || pgErr.SQLState() != "23505" {
		t.Fatalf("reinsert under the same ID = %v, want a unique violation", err)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM hopper_job_tags t JOIN hopper_jobs j USING (id) WHERE j.tags ? t.tag`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Errorf("%d tags rows after the failed reinsert, want 2", rows)
	}
}

// TestTagsAndFiltersInLatin1 works in a database whose encoding lacks some
// characters. A listing by a queue, tag or Metadata in them fails with
// ErrInvalidFilter, since no job can match it, and hopper_tags counts a
// tag's bytes in UTF-8, as InsertOpts.Tags does.
func TestTagsAndFiltersInLatin1(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// The client sends UTF-8, which the server converts to LATIN1.
	u, err := url.Parse(testdb.Database(t, "ENCODING 'LATIN1' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0"))
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("client_encoding", "UTF8")
	u.RawQuery = q.Encode()
	pool := testdb.PoolIn(t, u.String())
	exec := testdb.Driver(pool).Executor()
	for name, p := range map[string]driver.JobListParams{
		"a queue":  {Queue: "日本", Metadata: json.RawMessage(`{"a": 1}`)},
		"a tag":    {TagsAny: []string{"日本"}},
		"tags":     {TagsAll: []string{"a", "日本"}},
		"Metadata": {Queue: "default", Metadata: json.RawMessage(`{"a": "日本"}`)},
	} {
		p.Limit = 1
		_, err := exec.JobList(ctx, p)
		if !errors.Is(err, driver.ErrInvalidFilter) {
			t.Errorf("JobList with %s LATIN1 lacks = %v, want ErrInvalidFilter", name, err)
		}
	}
	// A tag's 255 bytes are counted in UTF-8, as a Go client counts them,
	// though each é is one byte in LATIN1.
	for n, ok := range map[int]bool{127: true, 128: false} {
		_, err := pool.Exec(ctx, "SELECT hopper_tags(jsonb_build_array(repeat('é', $1)))", n)
		if (err == nil) != ok {
			t.Errorf("hopper_tags of %d é (%d UTF-8 bytes) = %v, want accepted %t", n, 2*n, err, ok)
		}
	}
	// So are a batch callback's, which a direct SQL write to the batch row
	// can make longer: a callback is not given a tag a listing refuses.
	long := strings.Repeat("é", 200)
	batch, err := exec.BatchInsert(ctx, driver.BatchInsertParams{Total: 1, OnComplete: &driver.JobInsertParams{Kind: "done", Queue: "callbacks", Priority: 1, MaxAttempts: 3}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE hopper_batches SET on_complete = jsonb_set(on_complete, '{tags}', jsonb_build_array('cb', $1::text)) WHERE id = $2::uuid", long, batch.String()); err != nil {
		t.Fatal(err)
	}
	shard := insertParams("shard", 1)
	shard[0].BatchID = batch
	if _, err := exec.JobInsertMany(ctx, shard, driver.JobInsertOpts{}); err != nil {
		t.Fatal(err)
	}
	client := register(t, ctx, exec)
	claimed, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: client, Limit: 1})
	if err != nil || len(claimed.Jobs) != 1 {
		t.Fatalf("claim = %+v, %v", claimed, err)
	}
	if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{{ID: claimed.Jobs[0].ID, AttemptedBy: client, State: driver.JobStateCompleted, Archive: true}}}); err != nil {
		t.Fatal(err)
	}
	callbacks, err := exec.JobList(ctx, driver.JobListParams{Queue: "callbacks", Limit: 10})
	if err != nil || len(callbacks) != 1 || !slices.Equal(callbacks[0].Tags, []string{"cb"}) {
		t.Errorf("callback whose batch row has a tag of 400 UTF-8 bytes = %+v, %v; want tags [cb]", callbacks, err)
	}
}

// TestTagsControlCharactersInWIN1252 checks hopper_tags in a database whose
// encoding gives printable characters, such as the euro sign, the bytes
// 0x80 to 0x9F, which are C1 controls as code points: it refuses the
// control characters InsertOpts.Tags refuses, and only those.
func TestTagsControlCharactersInWIN1252(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	u, err := url.Parse(testdb.Database(t, "ENCODING 'WIN1252' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0"))
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("client_encoding", "UTF8")
	u.RawQuery = q.Encode()
	pool := testdb.PoolIn(t, u.String())
	for tag, ok := range map[string]bool{"€100": true, "Š…‰": true, "a\nb": false, "a\x7fb": false} {
		_, err := pool.Exec(ctx, "SELECT hopper_tags(jsonb_build_array($1::text))", tag)
		if (err == nil) != ok {
			t.Errorf("hopper_tags of %q = %v, want accepted %t", tag, err, ok)
		}
	}
}

// TestJobListStoredRowErrorIsNotFilter checks that a listing failing on a
// stored row, not on its filters, is not reported as ErrInvalidFilter: a job
// whose args have no form in the session's client encoding fails the
// conversion of its row (22P05), a data exception like a refused filter's.
func TestJobListStoredRowErrorIsNotFilter(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	pool := d.Pool()
	p := insertParams("stored", 1)
	p[0].Args = json.RawMessage(`{"name": "日本"}`)
	if _, err := exec.JobInsertMany(ctx, p, driver.JobInsertOpts{}); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // read only
	if _, err := tx.Exec(ctx, "SET LOCAL client_encoding = 'LATIN1'"); err != nil {
		t.Fatal(err)
	}
	_, err = d.UnwrapTx(tx).JobList(ctx, driver.JobListParams{Queue: "default", Limit: 10})
	if err == nil || errors.Is(err, driver.ErrInvalidFilter) {
		t.Errorf("JobList of a row the client encoding lacks = %v, want the listing's own error", err)
	}
}

// statementCounter counts the statements a pool runs.
type statementCounter struct{ n atomic.Int64 }

func (c *statementCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	c.n.Add(1)
	return ctx
}

func (*statementCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// TestRetentionChecksTagsPartitionsTogether checks that retention finds which
// expired tags partitions still have their history partition in one
// statement for the group, not one for each: with history's drops deferred,
// a pass over 20 such partitions runs no more statements than one over one.
func TestRetentionChecksTagsPartitionsTogether(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	pool := d.Pool()
	cfg := pool.Config()
	counter := &statementCounter{}
	cfg.ConnConfig.Tracer = counter
	traced, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(traced.Close)
	maintain := testdb.Driver(traced).Executor()
	p := driver.HistoryMaintainParams{CompletedRetention: 24 * time.Hour, FailedRetention: 7 * 24 * time.Hour}
	if _, err := exec.HistoryMaintain(ctx, p); err != nil {
		t.Fatal(err)
	}
	// A day's first hour of history, and the day's tags partition.
	addDay := func(d int) {
		t.Helper()
		from := time.Date(2020, 1, 1+d, 0, 0, 0, 0, time.UTC)
		for _, p := range []struct {
			parent, format string
			period         time.Duration
		}{{"hopper_job_history_completed", "2006010215", time.Hour}, {"hopper_job_history_tags_completed", "20060102", 24 * time.Hour}} {
			if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s_%s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')`,
				p.parent, from.Format(p.format), p.parent, from.Format(time.RFC3339), from.Add(p.period).Format(time.RFC3339))); err != nil {
				t.Fatal(err)
			}
		}
	}
	// A transaction that wrote history holds every drop of its partitions
	// off, so each pass defers them and keeps their tags partitions.
	pass := func() int64 {
		t.Helper()
		held, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer held.Rollback(ctx) //nolint:errcheck // read only
		if _, err := held.Exec(ctx, "LOCK TABLE hopper_job_history_completed IN ROW EXCLUSIVE MODE"); err != nil {
			t.Fatal(err)
		}
		// A pass that created a partition ahead, as one does when the test
		// straddles the hour, ran that creation's statements too; the next
		// pass creates none.
		for {
			before := counter.n.Load()
			res, err := maintain.HistoryMaintain(ctx, p)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range res.Dropped {
				if strings.HasPrefix(name, "hopper_job_history_tags_completed_2020") {
					t.Fatalf("pass dropped %s while its history partition is held", name)
				}
			}
			if len(res.Created) == 0 {
				return counter.n.Load() - before
			}
		}
	}
	addDay(0)
	one := pass()
	for d := 1; d < 20; d++ {
		addDay(d)
	}
	if many := pass(); many != one {
		t.Errorf("retention over 20 tags partitions waiting for history ran %d statements, over one %d; want the same", many, one)
	}
}

// TestHistoryTagsRowsUnique checks that history's tags rows are unique: a
// TRUNCATE of a history partition fires none of history's triggers and leaves
// its jobs' tags rows, and reinserting its rows would add a second copy of
// them, which a listing by tag would show as the job twice.
func TestHistoryTagsRowsUnique(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	pool := d.Pool()
	if _, err := exec.HistoryMaintain(ctx, driver.HistoryMaintainParams{CompletedRetention: 24 * time.Hour, FailedRetention: 24 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	p := insertParams("restored", 1)
	p[0].Tags = []string{"restored"}
	if _, err := exec.JobInsertMany(ctx, p, driver.JobInsertOpts{}); err != nil {
		t.Fatal(err)
	}
	client := register(t, ctx, exec)
	claimed, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: client, Limit: 1})
	if err != nil || len(claimed.Jobs) != 1 {
		t.Fatalf("claim = %+v, %v", claimed, err)
	}
	id := claimed.Jobs[0].ID
	if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{{ID: id, AttemptedBy: client, State: driver.JobStateCompleted, Archive: true}}}); err != nil {
		t.Fatal(err)
	}
	var partition string
	if err := pool.QueryRow(ctx, "SELECT tableoid::regclass::text FROM hopper_job_history WHERE id = $1::uuid", id.String()).Scan(&partition); err != nil {
		t.Fatal(err)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	for _, q := range []string{
		"CREATE TEMP TABLE restore AS SELECT * FROM " + partition,
		"TRUNCATE " + partition,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	_, err = conn.Exec(ctx, "INSERT INTO hopper_job_history SELECT * FROM restore")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Errorf("reinsert of a truncated partition's tagged rows = %v, want a unique violation", err)
	}
	listed, err := exec.JobList(ctx, driver.JobListParams{TagsAny: []string{"restored"}, Limit: 10})
	if err != nil || len(listed) > 1 {
		t.Errorf("listing by tag after the reinsert = %d jobs, %v; want the job at most once", len(listed), err)
	}
}

// statementLog records the statements a pool runs, and calls after, once, at
// the end of the first that mentions afterSQL.
type statementLog struct {
	mu       sync.Mutex
	sql      []string
	args     [][]any
	afterSQL string
	after    func()
}

func (l *statementLog) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sql, l.args = append(l.sql, d.SQL), append(l.args, d.Args)
	return context.WithValue(ctx, statementSQL{}, d.SQL)
}

func (l *statementLog) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	sql, _ := ctx.Value(statementSQL{}).(string)
	l.mu.Lock()
	after := l.after
	if after != nil && strings.Contains(sql, l.afterSQL) {
		l.after = nil
	} else {
		after = nil
	}
	l.mu.Unlock()
	if after != nil {
		after()
	}
}

// statementSQL keys a statement's text in the context its trace carries.
type statementSQL struct{}

// tracedDriver is a driver on pool's schema whose statements log records.
func tracedDriver(t *testing.T, pool *pgxpool.Pool, log *statementLog) *hopperpgx.Driver {
	t.Helper()
	cfg := pool.Config()
	cfg.ConnConfig.Tracer = log
	traced, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(traced.Close)
	return testdb.Driver(traced)
}

// TestTagListingPlansWhatItReads lists finished jobs by one tag and by eight,
// with history holding thirty hourly partitions besides the jobs', and
// explains each statement the listing ran. The plans scan only the history
// partition the jobs are in, and the same tags partitions for eight tags as
// for one: an arm per tag, each looking jobs up in every partition of
// history, planned a scan of each of them for each tag.
func TestTagListingPlansWhatItReads(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	pool := d.Pool()
	var now time.Time
	if err := pool.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		t.Fatal(err)
	}
	start := now.UTC().Truncate(time.Hour)
	for h := 3; h < 33; h++ {
		from := start.Add(-time.Duration(h) * time.Hour)
		if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE hopper_job_history_completed_%s PARTITION OF hopper_job_history_completed
			FOR VALUES FROM ('%s') TO ('%s')`, from.Format("2006010215"), from.Format(time.RFC3339), from.Add(time.Hour).Format(time.RFC3339))); err != nil {
			t.Fatal(err)
		}
	}
	tags := []string{"t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7"}
	p := insertParams("planned", 16)
	for i := range p {
		p[i].Tags = tags
	}
	if _, err := exec.JobInsertMany(ctx, p, driver.JobInsertOpts{}); err != nil {
		t.Fatal(err)
	}
	clientID, err := exec.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "test", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 16})
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range claimed.Jobs {
		if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{
			{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	log := &statementLog{}
	listing := tracedDriver(t, pool, log).Executor()
	scans := func(params driver.JobListParams) (history, tagged int) {
		t.Helper()
		log.mu.Lock()
		log.sql, log.args = nil, nil
		log.mu.Unlock()
		params.Limit = 10
		page, err := listing.JobList(ctx, params)
		if err != nil || len(page) != 10 {
			t.Fatalf("listing %+v = %d jobs, %v", params, len(page), err)
		}
		log.mu.Lock()
		defer log.mu.Unlock()
		for i, q := range log.sql {
			if !strings.HasPrefix(strings.TrimSpace(q), "SELECT") && !strings.HasPrefix(strings.TrimSpace(q), "WITH") && !strings.HasPrefix(q, "(") {
				continue
			}
			var plan []byte
			if err := pool.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+q, log.args[i]...).Scan(&plan); err != nil {
				t.Fatalf("explain %s: %v", q, err)
			}
			for _, rel := range regexp.MustCompile(`"Relation Name": "([^"]+)"`).FindAllStringSubmatch(string(plan), -1) {
				switch name := rel[1]; {
				case strings.Contains(name, "_tags"):
					tagged++
				case strings.HasPrefix(name, "hopper_job_history_"):
					history++
				}
			}
		}
		return history, tagged
	}
	states := []driver.JobState{driver.JobStateCompleted}
	h1, t1 := scans(driver.JobListParams{TagsAny: tags[:1], States: states})
	h8, t8 := scans(driver.JobListParams{TagsAny: tags, States: states})
	if h1 > 1 || h8 > 1 || t8 != t1 {
		t.Errorf("listing by one tag planned %d history scans and %d of tags tables, by eight %d and %d; want at most 1 history scan, and the same tags scans",
			h1, t1, h8, t8)
	}
}

// TestTagListingPreparesOnce pages through finished jobs by tag, a job a
// page, on one connection. pgx prepares each statement text it runs and
// keeps up to 512 a connection, so the listing's texts are the same from
// page to page: with the page's finish times written into the statement,
// each page prepared another, which the server kept until pgx evicted it.
// The statement that reads the page's jobs is planned for each page's
// values, never generically, so that it reads only their partitions.
func TestTagListingPreparesOnce(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	p := insertParams("paged", 12)
	for i := range p {
		p[i].Tags = []string{"paged"}
	}
	if _, err := exec.JobInsertMany(ctx, p, driver.JobInsertOpts{}); err != nil {
		t.Fatal(err)
	}
	clientID := register(t, ctx, exec)
	claimed, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 12})
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range claimed.Jobs {
		if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{
			{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	cfg := d.Pool().Config()
	cfg.MaxConns = 1
	one, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer one.Close()
	listing := testdb.Driver(one).Executor()
	params := driver.JobListParams{TagsAny: []string{"paged"}, States: []driver.JobState{driver.JobStateCompleted}, Limit: 1}
	listed := 0
	for {
		page, err := listing.JobList(ctx, params)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		listed += len(page)
		params.After = page[0].ID
	}
	if listed != 12 {
		t.Fatalf("listed %d jobs, want 12", listed)
	}
	var statements, generic int
	if err := one.QueryRow(ctx, `SELECT count(*), coalesce(sum(generic_plans), 0) FROM pg_prepared_statements
		WHERE statement LIKE '%finalized_at = ANY(%' AND statement NOT LIKE '%pg_prepared_statements%'`).Scan(&statements, &generic); err != nil {
		t.Fatal(err)
	}
	if statements != 1 || generic != 0 {
		t.Errorf("12 pages prepared %d statements reading history's jobs, which ran %d generic plans; want 1 and 0", statements, generic)
	}
}

// TestTagListingRoundTrips lists live and finished jobs by tag in one page:
// the transaction's begin, its settings, the walk of history's tags rows, the
// read of the jobs and the commit, five round trips.
func TestTagListingRoundTrips(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	p := insertParams("trips", 4)
	for i := range p {
		p[i].Tags = []string{"trip"}
	}
	if _, err := exec.JobInsertMany(ctx, p, driver.JobInsertOpts{}); err != nil {
		t.Fatal(err)
	}
	clientID := register(t, ctx, exec)
	claimed, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range claimed.Jobs {
		if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{
			{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	log := &statementLog{}
	page, err := tracedDriver(t, d.Pool(), log).Executor().JobList(ctx, driver.JobListParams{TagsAny: []string{"trip"}, Limit: 10})
	if err != nil || len(page) != 4 {
		t.Fatalf("listing = %d jobs, %v; want 4", len(page), err)
	}
	if len(log.sql) != 5 {
		t.Errorf("a page by tag ran %d statements, want 5: %q", len(log.sql), log.sql)
	}
}

// TestTagListingWalksOnFasterPastFilteredHistory lists finished jobs by tag
// and a kind that only the last 4 of 404 have, in a page of 10. Each walk of
// history's tags rows after the first takes twice the keys of the one
// before: a walk of a page of keys each time took 41 walks, each with its own
// round trips.
func TestTagListingWalksOnFasterPastFilteredHistory(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	clientID := register(t, ctx, exec)
	finish := func(kind string, n int) {
		t.Helper()
		p := insertParams(kind, n)
		for i := range p {
			p[i].Queue, p[i].Tags = kind, []string{"f"}
		}
		if _, err := exec.JobInsertMany(ctx, p, driver.JobInsertOpts{}); err != nil {
			t.Fatal(err)
		}
		claimed, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: kind, ClientID: clientID, Limit: n})
		if err != nil || len(claimed.Jobs) != n {
			t.Fatalf("claimed %d of %d, %v", len(claimed.Jobs), n, err)
		}
		fin := make([]driver.JobFinalize, n)
		for i, j := range claimed.Jobs {
			fin[i] = driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true}
		}
		if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: fin}); err != nil {
			t.Fatal(err)
		}
	}
	finish("skip", 400)
	finish("want", 4)
	log := &statementLog{}
	page, err := tracedDriver(t, d.Pool(), log).Executor().JobList(ctx, driver.JobListParams{
		Kinds: []string{"want"}, TagsAny: []string{"f"}, States: []driver.JobState{driver.JobStateCompleted}, Limit: 10,
	})
	if err != nil || len(page) != 4 {
		t.Fatalf("listing = %d jobs, %v; want 4", len(page), err)
	}
	walks := 0
	for _, q := range log.sql {
		if strings.Contains(q, "LEFT JOIN LATERAL") {
			walks++
		}
	}
	if walks > 8 {
		t.Errorf("the page took %d walks of history's tags rows, want at most 8", walks)
	}
}

// TestTagListingSnapshotSpansStatements finalizes a tagged job into history
// right after a listing by its tag has walked history's tags rows, before
// the statement that reads the live table. The statements share one
// snapshot, so the job, live in it, is listed once; each on its own snapshot,
// the walk ran before the job was in history and the read after it had left
// the live table, and the job was in neither.
func TestTagListingSnapshotSpansStatements(t *testing.T) {
	t.Parallel()
	ctx, d, exec := setup(t)
	p := insertParams("moved", 1)
	p[0].Tags = []string{"m"}
	res, err := exec.JobInsertMany(ctx, p, driver.JobInsertOpts{})
	if err != nil {
		t.Fatal(err)
	}
	clientID, err := exec.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "test", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: 1}); err != nil {
		t.Fatal(err)
	}
	log := &statementLog{afterSQL: "LEFT JOIN LATERAL", after: func() {
		if _, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: []driver.JobFinalize{
			{ID: res[0].Job.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true},
		}}); err != nil {
			t.Error(err)
		}
	}}
	page, err := tracedDriver(t, d.Pool(), log).Executor().JobList(ctx, driver.JobListParams{TagsAny: []string{"m"}, Limit: 10})
	if err != nil || len(page) != 1 || page[0].ID != res[0].Job.ID {
		t.Errorf("listing by tag across the job's finalize = %+v, %v; want the job once", page, err)
	}
	if log.after != nil {
		t.Error("the job was not finalized during the listing")
	}
}
