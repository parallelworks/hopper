package hoppersql_test

import (
	"context"
	"database/sql"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/hoppersql"
	"github.com/parallelworks/hopper/drivertest"
	"github.com/parallelworks/hopper/hoppermigrate"
	"github.com/parallelworks/hopper/internal/testdb"
)

// sqlFixture runs the conformance suite against hoppersql, through pgx's
// database/sql adapter.
type sqlFixture struct{}

// open returns a database/sql handle on the pool's schema.
func open(t *testing.T, pool *pgxpool.Pool) *sql.DB {
	t.Helper()
	db := stdlib.OpenDB(*pool.Config().ConnConfig)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func (sqlFixture) NewDriver(t *testing.T) driver.Driver[*sql.Tx] {
	pool := testdb.Pool(t)
	return hoppersql.NewWithConfig(open(t, pool), &hoppersql.Config{Schema: pool.Config().ConnConfig.RuntimeParams["search_path"]})
}

func (sqlFixture) Begin(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx]) (*sql.Tx, func() error, func() error) {
	t.Helper()
	tx, err := d.(*hoppersql.Driver).DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	return tx, tx.Commit, tx.Rollback
}

func exec(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], query string, args ...any) {
	t.Helper()
	if _, err := d.(*hoppersql.Driver).DB().ExecContext(ctx, query, args...); err != nil {
		t.Fatal(err)
	}
}

func (sqlFixture) ExpireLease(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], clientID int64) {
	exec(ctx, t, d, "UPDATE hopper_clients SET expires_at = now() - interval '1s' WHERE id = $1", clientID)
}

func (sqlFixture) ExpireLeader(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx]) {
	exec(ctx, t, d, "UPDATE hopper_leader SET expires_at = now() - interval '1s'")
}

func (sqlFixture) MakeClaimable(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], id driver.JobID) {
	exec(ctx, t, d, "UPDATE hopper_jobs SET scheduled_at = now() WHERE id = $1", id)
}

func (sqlFixture) AgeAttempt(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], id driver.JobID, age time.Duration) {
	exec(ctx, t, d, "UPDATE hopper_jobs SET attempted_at = now() - make_interval(secs => $2) WHERE id = $1", id, age.Seconds())
}

func (sqlFixture) CountLive(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx]) int {
	return count(ctx, t, d, "hopper_jobs")
}

func (sqlFixture) CountHistory(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx]) int {
	return count(ctx, t, d, "hopper_job_history")
}

func (sqlFixture) Exec(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], query string, args ...any) {
	t.Helper()
	if _, err := d.(*hoppersql.Driver).DB().ExecContext(ctx, query, args...); err != nil {
		t.Error(err)
	}
}

func (sqlFixture) QueryText(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], query string, args ...any) string {
	t.Helper()
	var s string
	if err := d.(*hoppersql.Driver).DB().QueryRowContext(ctx, query, args...).Scan(&s); err != nil {
		t.Error(err)
	}
	return s
}

func count(ctx context.Context, t *testing.T, d driver.Driver[*sql.Tx], table string) int {
	t.Helper()
	var n int
	if err := d.(*hoppersql.Driver).DB().QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestConformance(t *testing.T) {
	t.Parallel()
	drivertest.Run(t, sqlFixture{})
}

// TestMigrate applies and reverts the schema through the database/sql
// migrator, which holds its lock on a connection reserved from the pool.
func TestMigrate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.EmptyPool(t, 0)
	d := hoppersql.NewWithConfig(open(t, pool), &hoppersql.Config{Schema: pool.Config().ConnConfig.RuntimeParams["search_path"]})
	opts := &hoppermigrate.Options{Logger: slog.New(slog.DiscardHandler)}
	if _, err := hoppermigrate.Up(ctx, d, opts); err != nil {
		t.Fatalf("up: %v", err)
	}
	v, err := hoppermigrate.Version(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	if v != hoppermigrate.Latest() {
		t.Fatalf("version after up = %d, want %d", v, hoppermigrate.Latest())
	}
	if _, err := hoppermigrate.Down(ctx, d, &hoppermigrate.Options{Logger: opts.Logger, Target: 0}); err != nil {
		t.Fatalf("down: %v", err)
	}
	if v, err := hoppermigrate.Version(ctx, d); err != nil || v != 0 {
		t.Fatalf("version after down = %d, %v; want 0", v, err)
	}
}

// TestTagListingPreparesOnce pages through finished jobs by tag, a job a
// page, on one connection. pgx's database/sql adapter prepares each
// statement text it runs and keeps it on the server, so the listing's texts
// are the same from page to page, and the statement that reads the page's
// jobs is planned for each page's values, never generically.
func TestTagListingPreparesOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.Pool(t)
	db := open(t, pool)
	db.SetMaxOpenConns(1)
	exec := hoppersql.NewWithConfig(db, &hoppersql.Config{Schema: pool.Config().ConnConfig.RuntimeParams["search_path"]}).Executor()
	p := make([]driver.JobInsertParams, 12)
	for i := range p {
		p[i] = driver.JobInsertParams{Kind: "paged", Queue: "default", Priority: 2, MaxAttempts: 3, Tags: []string{"paged"}}
	}
	if _, err := exec.JobInsertMany(ctx, p, driver.JobInsertOpts{}); err != nil {
		t.Fatal(err)
	}
	clientID, err := exec.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "test", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
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
	params := driver.JobListParams{TagsAny: []string{"paged"}, States: []driver.JobState{driver.JobStateCompleted}, Limit: 1}
	listed := 0
	for {
		page, err := exec.JobList(ctx, params)
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
	if err := db.QueryRowContext(ctx, `SELECT count(*), coalesce(sum(generic_plans), 0) FROM pg_prepared_statements
		WHERE statement LIKE '%finalized_at = ANY(%' AND statement NOT LIKE '%pg_prepared_statements%'`).Scan(&statements, &generic); err != nil {
		t.Fatal(err)
	}
	if statements != 1 || generic != 0 {
		t.Errorf("12 pages prepared %d statements reading history's jobs, which ran %d generic plans; want 1 and 0", statements, generic)
	}
}

// TestRetentionPreparesNoTagsDelete drops two hours of history, whose tags
// rows' deletes each name their hour's partition. They run unprepared: a
// prepared one stayed in the connection's statement cache, on the server
// too, never to run again.
func TestRetentionPreparesNoTagsDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.Pool(t)
	for h := range 2 {
		from := time.Date(2020, 1, 1, h, 0, 0, 0, time.UTC)
		if _, err := pool.Exec(ctx, `CREATE TABLE hopper_job_history_completed_`+from.Format("2006010215")+` PARTITION OF hopper_job_history_completed FOR VALUES FROM ('`+
			from.Format(time.RFC3339)+`') TO ('`+from.Add(time.Hour).Format(time.RFC3339)+`')`); err != nil {
			t.Fatal(err)
		}
	}
	db := open(t, pool)
	db.SetMaxOpenConns(1)
	exec := hoppersql.NewWithConfig(db, &hoppersql.Config{Schema: pool.Config().ConnConfig.RuntimeParams["search_path"]}).Executor()
	now, err := exec.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := exec.HistoryMaintain(ctx, driver.HistoryMaintainParams{CompletedRetention: now.Sub(time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC))})
	if err != nil || !slices.Contains(res.Dropped, "hopper_job_history_completed_2020010100") || !slices.Contains(res.Dropped, "hopper_job_history_completed_2020010101") {
		t.Fatalf("maintenance = %+v, %v; want both hours dropped", res, err)
	}
	var prepared int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_prepared_statements
		WHERE statement LIKE '%hopper_tags_of(h.tags)%' AND statement NOT LIKE '%pg_prepared_statements%'`).Scan(&prepared); err != nil {
		t.Fatal(err)
	}
	if prepared != 0 {
		t.Errorf("dropping 2 hours prepared %d deletes of their tags rows, want 0", prepared)
	}
}

// statementTrace records the statements a database/sql handle runs.
type statementTrace struct {
	mu  sync.Mutex
	sql []string
}

func (s *statementTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sql = append(s.sql, d.SQL)
	return ctx
}

func (*statementTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// tracedExecutor is an executor on pool's schema whose statements trace
// records.
func tracedExecutor(t *testing.T, pool *pgxpool.Pool, trace pgx.QueryTracer) driver.Executor {
	t.Helper()
	cfg := *pool.Config().ConnConfig
	cfg.Tracer = trace
	db := stdlib.OpenDB(cfg)
	t.Cleanup(func() { _ = db.Close() })
	return hoppersql.NewWithConfig(db, &hoppersql.Config{Schema: pool.Config().ConnConfig.RuntimeParams["search_path"]}).Executor()
}

// tagged inserts n jobs of kind with tag, and finalizes the first finished
// of them into history as completed.
func tagged(ctx context.Context, t *testing.T, x driver.Executor, kind, tag string, n, finished int) {
	t.Helper()
	p := make([]driver.JobInsertParams, n)
	for i := range p {
		p[i] = driver.JobInsertParams{Kind: kind, Queue: kind, Priority: 2, MaxAttempts: 3, Tags: []string{tag}}
	}
	if _, err := x.JobInsertMany(ctx, p, driver.JobInsertOpts{}); err != nil {
		t.Fatal(err)
	}
	if finished == 0 {
		return
	}
	clientID, err := x.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "test", TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := x.JobClaim(ctx, driver.JobClaimParams{Queue: kind, ClientID: clientID, Limit: finished})
	if err != nil {
		t.Fatal(err)
	}
	fin := make([]driver.JobFinalize, len(claimed.Jobs))
	for i, j := range claimed.Jobs {
		fin[i] = driver.JobFinalize{ID: j.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true}
	}
	if _, err := x.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: fin}); err != nil {
		t.Fatal(err)
	}
}

// TestTagListingRoundTrips lists live and finished jobs by tag in one page:
// the transaction's begin, its settings, the walk of history's tags rows, the
// read of the jobs and the commit, five round trips.
func TestTagListingRoundTrips(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.Pool(t)
	x := hoppersql.NewWithConfig(open(t, pool), &hoppersql.Config{Schema: pool.Config().ConnConfig.RuntimeParams["search_path"]}).Executor()
	tagged(ctx, t, x, "trips", "trip", 4, 2)
	trace := &statementTrace{}
	page, err := tracedExecutor(t, pool, trace).JobList(ctx, driver.JobListParams{TagsAny: []string{"trip"}, Limit: 10})
	if err != nil || len(page) != 4 {
		t.Fatalf("listing = %d jobs, %v; want 4", len(page), err)
	}
	if len(trace.sql) != 5 {
		t.Errorf("a page by tag ran %d statements, want 5: %q", len(trace.sql), trace.sql)
	}
}

// TestTagListingWalksOnFasterPastFilteredHistory lists finished jobs by tag
// and a kind that only the last 4 of 404 have, in a page of 10. Each walk of
// history's tags rows after the first takes twice the keys of the one
// before: a walk of a page of keys each time took 41 walks, each with its own
// round trips.
func TestTagListingWalksOnFasterPastFilteredHistory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.Pool(t)
	x := hoppersql.NewWithConfig(open(t, pool), &hoppersql.Config{Schema: pool.Config().ConnConfig.RuntimeParams["search_path"]}).Executor()
	tagged(ctx, t, x, "skip", "f", 400, 400)
	tagged(ctx, t, x, "want", "f", 4, 4)
	trace := &statementTrace{}
	page, err := tracedExecutor(t, pool, trace).JobList(ctx, driver.JobListParams{
		Kinds: []string{"want"}, TagsAny: []string{"f"}, States: []driver.JobState{driver.JobStateCompleted}, Limit: 10,
	})
	if err != nil || len(page) != 4 {
		t.Fatalf("listing = %d jobs, %v; want 4", len(page), err)
	}
	walks := 0
	for _, q := range trace.sql {
		if strings.Contains(q, "LEFT JOIN LATERAL") {
			walks++
		}
	}
	if walks > 8 {
		t.Errorf("the page took %d walks of history's tags rows, want at most 8", walks)
	}
}

// fetchRows records the rows returned by each statement that reads a tag
// listing's jobs from history by their keys.
type fetchRows struct {
	mu   sync.Mutex
	rows []int64
}

type fetchSQL struct{}

func (r *fetchRows) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, fetchSQL{}, d.SQL)
}

func (r *fetchRows) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryEndData) {
	if sql, _ := ctx.Value(fetchSQL{}).(string); strings.Contains(sql, "finalized_at = ANY(") && d.Err == nil {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.rows = append(r.rows, d.CommandTag.RowsAffected())
	}
}

// TestTagListingReadsAPageOfHistory lists finished jobs by tag and a kind
// that the 300 after the first 150 of them have, in a page of 10. The walks
// that double past the 150 end on one of 160 keys, all of that kind, but a
// walk's read of the jobs returns no more than the page.
func TestTagListingReadsAPageOfHistory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.Pool(t)
	x := hoppersql.NewWithConfig(open(t, pool), &hoppersql.Config{Schema: pool.Config().ConnConfig.RuntimeParams["search_path"]}).Executor()
	tagged(ctx, t, x, "skip", "f", 150, 150)
	tagged(ctx, t, x, "want", "f", 300, 300)
	read := &fetchRows{}
	page, err := tracedExecutor(t, pool, read).JobList(ctx, driver.JobListParams{
		Kinds: []string{"want"}, TagsAny: []string{"f"}, States: []driver.JobState{driver.JobStateCompleted}, Limit: 10,
	})
	if err != nil || len(page) != 10 {
		t.Fatalf("listing = %d jobs, %v; want 10", len(page), err)
	}
	rows, err := pool.Query(ctx, "SELECT id FROM hopper_job_history WHERE kind = 'want' ORDER BY id LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}
	first, err := pgx.CollectRows(rows, pgx.RowTo[driver.JobID])
	if err != nil {
		t.Fatal(err)
	}
	for i, j := range page {
		if j.ID != first[i] {
			t.Fatalf("job %d of the page = %s, want %s", i, j.ID, first[i])
		}
	}
	if len(read.rows) == 0 || slices.Max(read.rows) > 10 {
		t.Errorf("the reads of history's jobs returned %v rows, want at most 10 each", read.rows)
	}
}
