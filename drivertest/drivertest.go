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
// fencing to the claim (also inside a caller's transaction), snooze reasons, leases, claims by ID and their job leases, leader election, rescue candidates,
// cancel, retry, TTL expiry, listing, queues, periodic slots, stats,
// notifications and history maintenance. What depends on whether the engine
// locks rows or runs one writer at a time (driver.Capabilities.SingleWriter)
// is tested either way. Engine-specific behavior, such as how partitions are
// managed, belongs in the driver's own tests; RunPostgres has the tests of
// what hopper's Postgres drivers share.
package drivertest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// Fixture gives the suite a database per test and the hooks it needs. The
// hooks report a failure with t.Error rather than t.Fatal when the suite may
// call them from another goroutine, as SnoozeReason says.
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
	// ExpireJobLease makes the job lease of a job claimed by ID expired, as
	// a worker that stopped renewing it would. CountJobLeases counts the job
	// lease rows the engine keeps.
	ExpireJobLease(ctx context.Context, t *testing.T, d driver.Driver[TTx], id driver.JobID)
	CountJobLeases(ctx context.Context, t *testing.T, d driver.Driver[TTx]) int
	// AgeClientStart backdates a client's start by age.
	AgeClientStart(ctx context.Context, t *testing.T, d driver.Driver[TTx], clientID int64, age time.Duration)
	// CountLive and CountHistory count jobs in the live table and in history.
	CountLive(ctx context.Context, t *testing.T, d driver.Driver[TTx]) int
	CountHistory(ctx context.Context, t *testing.T, d driver.Driver[TTx]) int
	// ExpireJob moves a live job's TTL deadline a second into the past.
	ExpireJob(ctx context.Context, t *testing.T, d driver.Driver[TTx], id driver.JobID)
	// StaleWake marks a running job as woken by JobRetryOrderingKey a second
	// before its attempt began, as a wake during an earlier attempt would.
	StaleWake(ctx context.Context, t *testing.T, d driver.Driver[TTx], id driver.JobID)
	// MakePending puts a live job back in the pending state, as a client
	// that stopped before promoting it would leave it.
	MakePending(ctx context.Context, t *testing.T, d driver.Driver[TTx], id driver.JobID)
	// SnoozeReason reads a live job's stored snooze reason, or "" for none,
	// which the driver's reads show only for a scheduled job. The suite
	// calls it from any goroutine. SetSnoozeReason stores one, as a client
	// of an earlier release, which cleared it neither on claim nor on
	// finalize, can leave it on a job that is not snoozed.
	SnoozeReason(ctx context.Context, t *testing.T, d driver.Driver[TTx], id driver.JobID) string
	SetSnoozeReason(ctx context.Context, t *testing.T, d driver.Driver[TTx], id driver.JobID, reason string)
	// SetProgress stores a JSON value as a live job's progress as it is, as
	// a direct SQL write could.
	SetProgress(ctx context.Context, t *testing.T, d driver.Driver[TTx], id driver.JobID, value string)
	// CountEdges counts the dependency edges the engine keeps for workflow
	// jobs that have not finished.
	CountEdges(ctx context.Context, t *testing.T, d driver.Driver[TTx]) int
	// WaitForLockWait waits until a statement on the driver's database
	// waits for a lock another transaction holds, so that a test knows its
	// competing statement is blocked rather than not yet started.
	WaitForLockWait(ctx context.Context, t *testing.T, d driver.Driver[TTx])
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
		{"InsertUniqueFinished", testInsertUniqueFinished[TTx]},
		{"InsertUniqueFinishedRacesFinalize", testInsertUniqueFinishedRacesFinalize[TTx]},
		{"InsertCopy", testInsertCopy[TTx]},
		{"JobTags", testJobTags[TTx]},
		{"JobListFilters", testJobListFilters[TTx]},
		{"TagsPagePastFilteredHistory", testTagsPagePastFilteredHistory[TTx]},
		{"InsertTx", testInsertTx[TTx]},
		{"Claim", testClaim[TTx]},
		{"ClaimSkipsLocked", testClaimSkipsLocked[TTx]},
		{"Finalize", func(t *testing.T, f Fixture[TTx]) { testFinalize(t, f, false) }},
		{"FinalizeNoBatches", func(t *testing.T, f Fixture[TTx]) { testFinalize(t, f, true) }},
		{"FinalizeFences", func(t *testing.T, f Fixture[TTx]) { testFinalizeFences(t, f, false) }},
		{"FinalizeFencesNoBatches", func(t *testing.T, f Fixture[TTx]) { testFinalizeFences(t, f, true) }},
		{"FinalizeSkipLocked", func(t *testing.T, f Fixture[TTx]) { testFinalizeSkipLocked(t, f, false) }},
		{"FinalizeSkipLockedNoBatches", func(t *testing.T, f Fixture[TTx]) { testFinalizeSkipLocked(t, f, true) }},
		{"ClaimFence", testClaimFence[TTx]},
		{"FinalizeFencesReclaim", testFinalizeFencesReclaim[TTx]},
		{"FinalizeReportsTheAppliedFence", testFinalizeReportsTheAppliedFence[TTx]},
		{"RescueRacesReclaim", testRescueRacesReclaim[TTx]},
		{"RecordProgressFencesSnooze", testRecordProgressFencesSnooze[TTx]},
		{"ClaimByID", testClaimByID[TTx]},
		{"ClaimByIDInTx", testClaimByIDInTx[TTx]},
		{"ClaimByIDRacesClaim", testClaimByIDRacesClaim[TTx]},
		{"LeaseRenew", testLeaseRenew[TTx]},
		{"LeaseRenewConcurrent", testLeaseRenewConcurrent[TTx]},
		{"LeaseFinalize", func(t *testing.T, f Fixture[TTx]) { testLeaseFinalize(t, f, false) }},
		{"LeaseFinalizeNoBatches", func(t *testing.T, f Fixture[TTx]) { testLeaseFinalize(t, f, true) }},
		{"LeaseProgress", testLeaseProgress[TTx]},
		{"LeaseFencesClients", testLeaseFencesClients[TTx]},
		{"LeasesOutsideLimits", testLeasesOutsideLimits[TTx]},
		{"ExpireLeases", testExpireLeases[TTx]},
		{"ExpireLeasesAfterRestart", testExpireLeasesAfterRestart[TTx]},
		{"ExpireLeasesAfterRestartNoGrace", testExpireLeasesAfterRestartNoGrace[TTx]},
		{"ExpireRacesRenew", testExpireRacesRenew[TTx]},
		{"ExpireRacesFinalize", testExpireRacesFinalize[TTx]},
		{"ExpireRacesCancel", testExpireRacesCancel[TTx]},
		{"FinalizeTx", testFinalizeTx[TTx]},
		{"FinalizeTxConcurrent", testFinalizeTxConcurrent[TTx]},
		{"Lease", testLease[TTx]},
		{"Leader", testLeader[TTx]},
		{"Rescue", testRescue[TTx]},
		{"Cancel", testCancel[TTx]},
		{"Retry", testRetry[TTx]},
		{"RetryOrderingKey", testRetryOrderingKey[TTx]},
		{"RetryOrderingKeyConcurrent", testRetryOrderingKeyConcurrent[TTx]},
		{"RetryOrderingKeyTx", testRetryOrderingKeyTx[TTx]},
		{"TTL", testTTL[TTx]},
		{"SnoozeReason", func(t *testing.T, f Fixture[TTx]) { testSnoozeReason(t, f, false) }},
		{"SnoozeReasonNoBatches", func(t *testing.T, f Fixture[TTx]) { testSnoozeReason(t, f, true) }},
		{"SnoozeReasonConcurrent", testSnoozeReasonConcurrent[TTx]},
		{"List", testList[TTx]},
		{"ListOrderingKeyLane", testListOrderingKeyLane[TTx]},
		{"JobRecordProgress", testJobRecordProgress[TTx]},
		{"JobRecordProgressRacesFinalize", testJobRecordProgressRacesFinalize[TTx]},
		{"JobProgressReset", testJobProgressReset[TTx]},
		{"JobLogs", func(t *testing.T, f Fixture[TTx]) { testJobLogs(t, f, false) }},
		{"JobLogsNoBatches", func(t *testing.T, f Fixture[TTx]) { testJobLogs(t, f, true) }},
		{"JobLogsFenced", testJobLogsFenced[TTx]},
		{"JobDelete", testJobDelete[TTx]},
		{"JobDeleteRacesClaim", testJobDeleteRacesClaim[TTx]},
		{"JobDeleteRacesRetry", testJobDeleteRacesRetry[TTx]},
		{"Queues", testQueues[TTx]},
		{"Periodic", testPeriodic[TTx]},
		{"Stats", testStats[TTx]},
		{"Notify", testNotify[TTx]},
		{"HistoryMaintain", testHistoryMaintain[TTx]},
		{"Now", testNow[TTx]},
		{"Subscriptions", testSubscriptions[TTx]},
		{"Publish", testPublish[TTx]},
		{"OrderingClaim", testOrderingClaim[TTx]},
		{"QueueLimits", testQueueLimits[TTx]},
		{"GlobalLimit", testGlobalLimit[TTx]},
		{"RateLimit", testRateLimit[TTx]},
		{"PartitionLimit", testPartitionLimit[TTx]},
		{"Aging", testAging[TTx]},
		{"Batches", testBatches[TTx]},
		{"BatchesConcurrent", testBatchesConcurrent[TTx]},
		{"Workflows", testWorkflows[TTx]},
		{"WorkflowFanInConcurrent", testWorkflowFanInConcurrent[TTx]},
		{"WorkflowsConcurrent", testWorkflowsConcurrent[TTx]},
		{"Streams", testStreams[TTx]},
		{"StreamsRetention", testStreamsRetention[TTx]},
		{"Maintain", testMaintain[TTx]},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, f)
		})
	}
}

// PostgresFixture is the Fixture of one of hopper's Postgres drivers, which
// can also run SQL on hopper's schema. RunPostgres runs, with it, the tests
// of what those drivers share beyond the driver interface: history's
// partitions and their indexes, the tags tables and their triggers, the rows
// clients of earlier releases leave, and the planner statistics the leader
// keeps.
type PostgresFixture[TTx any] interface {
	Fixture[TTx]
	// Exec runs a SQL statement with hopper's tables on the search path, for
	// rows the suite writes directly, such as those a client of the previous
	// release leaves behind. QueryText returns the single text value a query
	// selects, for what the driver's reads do not show. Both report a
	// failure with t.Error rather than t.Fatal, so the suite may call them
	// from any goroutine.
	Exec(ctx context.Context, t *testing.T, d driver.Driver[TTx], query string, args ...any)
	QueryText(ctx context.Context, t *testing.T, d driver.Driver[TTx], query string, args ...any) string
}

// RunPostgres runs the tests of what hopper's Postgres drivers share, which
// a Postgres driver passes besides Run's.
func RunPostgres[TTx any](t *testing.T, f PostgresFixture[TTx]) {
	t.Helper()
	tests := []struct {
		name string
		fn   func(*testing.T, PostgresFixture[TTx])
	}{
		{"JobTagsStored", testJobTagsStored[TTx]},
		{"TagsRowsFollowJobs", testTagsRowsFollowJobs[TTx]},
		{"TagsTableMaintained", testTagsTableMaintained[TTx]},
		{"TagsTableMaintainedPastJobsError", testTagsTableMaintainedPastJobsError[TTx]},
		{"TagsFollowHistoryRetention", testTagsFollowHistoryRetention[TTx]},
		{"TagsPartitionWaitsForHistory", testTagsPartitionWaitsForHistory[TTx]},
		{"TagsLeaveWithTheirHour", testTagsLeaveWithTheirHour[TTx]},
		{"TagsHourOutlastsItsDelete", testTagsHourOutlastsItsDelete[TTx]},
		{"TagsHourDroppedMeanwhile", testTagsHourDroppedMeanwhile[TTx]},
		{"TagsDayGoesWithItsLastHour", testTagsDayGoesWithItsLastHour[TTx]},
		{"TagsFollowDirectUpdates", testTagsFollowDirectUpdates[TTx]},
		{"TagsFollowTruncate", testTagsFollowTruncate[TTx]},
		{"JobLogsStored", func(t *testing.T, f PostgresFixture[TTx]) { testJobLogsStored(t, f, false) }},
		{"JobLogsStoredNoBatches", func(t *testing.T, f PostgresFixture[TTx]) { testJobLogsStored(t, f, true) }},
		{"RetryOrderingKeyAgingOrder", testRetryOrderingKeyAgingOrder[TTx]},
		{"BatchProgressRaces", testBatchProgressRaces[TTx]},
		{"StreamsPartitioned", testStreamsPartitioned[TTx]},
		{"HistoryMaintainWhileInUse", testHistoryMaintainWhileInUse[TTx]},
		{"HistoryIndexes", testHistoryIndexes[TTx]},
		{"HistoryRetentionCompletesIndex", testHistoryRetentionCompletesIndex[TTx]},
		{"DepsTableMaintained", testDepsTableMaintained[TTx]},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, f)
		})
	}
}

// Postgres implements a PostgresFixture's hooks that are statements on
// hopper's Postgres schema, for the fixtures of hopper's Postgres drivers to
// embed, which then implement only NewDriver, Begin and SQL.
type Postgres[TTx any] struct {
	// SQL runs statements on a driver's database, as PostgresFixture's Exec
	// and QueryText describe.
	SQL interface {
		Exec(ctx context.Context, t *testing.T, d driver.Driver[TTx], query string, args ...any)
		QueryText(ctx context.Context, t *testing.T, d driver.Driver[TTx], query string, args ...any) string
	}
}

// Exec implements PostgresFixture.
func (p Postgres[TTx]) Exec(ctx context.Context, t *testing.T, d driver.Driver[TTx], query string, args ...any) {
	t.Helper()
	p.SQL.Exec(ctx, t, d, query, args...)
}

// QueryText implements PostgresFixture.
func (p Postgres[TTx]) QueryText(ctx context.Context, t *testing.T, d driver.Driver[TTx], query string, args ...any) string {
	t.Helper()
	return p.SQL.QueryText(ctx, t, d, query, args...)
}

// ExpireLease implements Fixture.
func (p Postgres[TTx]) ExpireLease(ctx context.Context, t *testing.T, d driver.Driver[TTx], clientID int64) {
	t.Helper()
	p.SQL.Exec(ctx, t, d, "UPDATE hopper_clients SET expires_at = now() - interval '1s' WHERE id = $1", clientID)
}

// ExpireLeader implements Fixture.
func (p Postgres[TTx]) ExpireLeader(ctx context.Context, t *testing.T, d driver.Driver[TTx]) {
	t.Helper()
	p.SQL.Exec(ctx, t, d, "UPDATE hopper_leader SET expires_at = now() - interval '1s'")
}

// MakeClaimable implements Fixture.
func (p Postgres[TTx]) MakeClaimable(ctx context.Context, t *testing.T, d driver.Driver[TTx], id driver.JobID) {
	t.Helper()
	p.SQL.Exec(ctx, t, d, "UPDATE hopper_jobs SET scheduled_at = now() WHERE id = $1::uuid", id.String())
}

// AgeAttempt implements Fixture.
func (p Postgres[TTx]) AgeAttempt(ctx context.Context, t *testing.T, d driver.Driver[TTx], id driver.JobID, age time.Duration) {
	t.Helper()
	p.SQL.Exec(ctx, t, d, "UPDATE hopper_jobs SET attempted_at = now() - make_interval(secs => $2::float8) WHERE id = $1::uuid", id.String(), age.Seconds())
}

// CountLive implements Fixture.
func (p Postgres[TTx]) CountLive(ctx context.Context, t *testing.T, d driver.Driver[TTx]) int {
	t.Helper()
	return p.count(ctx, t, d, "SELECT count(*)::text FROM hopper_jobs")
}

// CountHistory implements Fixture.
func (p Postgres[TTx]) CountHistory(ctx context.Context, t *testing.T, d driver.Driver[TTx]) int {
	t.Helper()
	return p.count(ctx, t, d, "SELECT count(*)::text FROM hopper_job_history")
}

// ExpireJob implements Fixture.
func (p Postgres[TTx]) ExpireJob(ctx context.Context, t *testing.T, d driver.Driver[TTx], id driver.JobID) {
	t.Helper()
	p.SQL.Exec(ctx, t, d, "UPDATE hopper_jobs SET expires_at = now() - interval '1s' WHERE id = $1::uuid", id.String())
}

// StaleWake implements Fixture.
func (p Postgres[TTx]) StaleWake(ctx context.Context, t *testing.T, d driver.Driver[TTx], id driver.JobID) {
	t.Helper()
	p.SQL.Exec(ctx, t, d, "UPDATE hopper_jobs SET wake_requested_at = attempted_at - interval '1s' WHERE id = $1::uuid", id.String())
}

// MakePending implements Fixture.
func (p Postgres[TTx]) MakePending(ctx context.Context, t *testing.T, d driver.Driver[TTx], id driver.JobID) {
	t.Helper()
	p.SQL.Exec(ctx, t, d, "UPDATE hopper_jobs SET state = 'pending' WHERE id = $1::uuid", id.String())
}

// SnoozeReason implements Fixture.
func (p Postgres[TTx]) SnoozeReason(ctx context.Context, t *testing.T, d driver.Driver[TTx], id driver.JobID) string {
	t.Helper()
	return p.SQL.QueryText(ctx, t, d, "SELECT coalesce(snooze_reason, '') FROM hopper_jobs WHERE id = $1::uuid", id.String())
}

// SetSnoozeReason implements Fixture.
func (p Postgres[TTx]) SetSnoozeReason(ctx context.Context, t *testing.T, d driver.Driver[TTx], id driver.JobID, reason string) {
	t.Helper()
	p.SQL.Exec(ctx, t, d, "UPDATE hopper_jobs SET snooze_reason = $2 WHERE id = $1::uuid", id.String(), reason)
}

// SetProgress implements Fixture.
func (p Postgres[TTx]) SetProgress(ctx context.Context, t *testing.T, d driver.Driver[TTx], id driver.JobID, value string) {
	t.Helper()
	p.SQL.Exec(ctx, t, d, "UPDATE hopper_jobs SET progress = $2::jsonb WHERE id = $1::uuid", id.String(), value)
}

// ExpireJobLease implements Fixture.
func (p Postgres[TTx]) ExpireJobLease(ctx context.Context, t *testing.T, d driver.Driver[TTx], id driver.JobID) {
	t.Helper()
	p.SQL.Exec(ctx, t, d, "UPDATE hopper_job_leases SET expires_at = now() - interval '1s', renewed_at = now() - interval '1s' - ttl WHERE id = $1::uuid", id.String())
}

// AgeClientStart implements Fixture.
func (p Postgres[TTx]) AgeClientStart(ctx context.Context, t *testing.T, d driver.Driver[TTx], clientID int64, age time.Duration) {
	t.Helper()
	p.SQL.Exec(ctx, t, d, "UPDATE hopper_clients SET started_at = started_at - make_interval(secs => $2::float8) WHERE id = $1::bigint", clientID, age.Seconds())
}

// CountJobLeases implements Fixture.
func (p Postgres[TTx]) CountJobLeases(ctx context.Context, t *testing.T, d driver.Driver[TTx]) int {
	t.Helper()
	return p.count(ctx, t, d, "SELECT count(*)::text FROM hopper_job_leases")
}

// CountEdges implements Fixture.
func (p Postgres[TTx]) CountEdges(ctx context.Context, t *testing.T, d driver.Driver[TTx]) int {
	t.Helper()
	return p.count(ctx, t, d, "SELECT count(*)::text FROM hopper_job_deps")
}

// WaitForLockWait implements Fixture: a statement on the driver's schema,
// whose name is in every statement's text, waits for a lock.
func (p Postgres[TTx]) WaitForLockWait(ctx context.Context, t *testing.T, d driver.Driver[TTx]) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for p.SQL.QueryText(ctx, t, d, `SELECT count(*)::text FROM pg_stat_activity
		WHERE wait_event_type = 'Lock' AND position(current_schema() IN query) > 0 AND pid <> pg_backend_pid()`) == "0" {
		if time.Now().After(deadline) {
			t.Fatal("no statement started waiting for a lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (p Postgres[TTx]) count(ctx context.Context, t *testing.T, d driver.Driver[TTx], query string) int {
	t.Helper()
	n, err := strconv.Atoi(p.SQL.QueryText(ctx, t, d, query))
	if err != nil {
		t.Error(err)
	}
	return n
}

// singleWriter reports whether d's engine runs one writing transaction at a
// time.
func singleWriter[TTx any](d driver.Driver[TTx]) bool { return d.Capabilities().SingleWriter }

// jsonEqual reports whether two JSON texts hold the same value: an engine
// need not keep the text as given (Postgres's jsonb orders an object's keys
// and spaces them), only the value. Numbers compare by their exact value,
// not as float64s.
func jsonEqual(got []byte, want string) bool {
	decode := func(b []byte) (any, bool) {
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber()
		var v any
		if dec.Decode(&v) != nil {
			return nil, false
		}
		_, err := dec.Token() // one value, and nothing after it
		return v, errors.Is(err, io.EOF)
	}
	a, ok := decode(got)
	b, ok2 := decode([]byte(want))
	return ok && ok2 && fmt.Sprint(normalizeJSON(a)) == fmt.Sprint(normalizeJSON(b))
}

// normalizeJSON gives a decoded JSON value a printed form that does not
// depend on its object's key order, nor on how its numbers are written.
func normalizeJSON(v any) any {
	switch v := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		out := make([]string, len(keys))
		for i, k := range keys {
			out[i] = strconv.Quote(k) + ":" + fmt.Sprint(normalizeJSON(v[k]))
		}
		return "{" + strings.Join(out, ",") + "}"
	case []any:
		out := make([]string, len(v))
		for i, e := range v {
			out[i] = fmt.Sprint(normalizeJSON(e))
		}
		return "[" + strings.Join(out, ",") + "]"
	case string:
		return strconv.Quote(v)
	case json.Number:
		if r, ok := new(big.Rat).SetString(string(v)); ok {
			return r.RatString()
		}
		return string(v)
	default:
		return v
	}
}

// noBatchesExec finalizes with JobFinalizeParams.NoBatches set, which a
// caller may when none of the jobs belongs to a batch: the tests of jobs
// outside batches see the same results through it.
type noBatchesExec struct{ driver.Executor }

func (e noBatchesExec) JobFinalizeMany(ctx context.Context, params driver.JobFinalizeParams) ([]driver.JobFinalized, error) {
	params.NoBatches = true
	return e.Executor.JobFinalizeMany(ctx, params)
}

// executor returns d's executor, finalizing with NoBatches if noBatches.
func executor[TTx any](d driver.Driver[TTx], noBatches bool) driver.Executor {
	if noBatches {
		return noBatchesExec{d.Executor()}
	}
	return d.Executor()
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
	res, err := exec.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clientID, Limit: limit})
	if err != nil {
		t.Fatal(err)
	}
	return res.Jobs
}

// finalize applies results as given, their fences included.
func finalize(ctx context.Context, t *testing.T, exec driver.Executor, jobs ...driver.JobFinalize) []driver.JobID {
	t.Helper()
	applied, err := exec.JobFinalizeMany(ctx, driver.JobFinalizeParams{Jobs: jobs})
	if err != nil {
		t.Fatal(err)
	}
	return appliedIDs(applied)
}

// appliedIDs returns the jobs of the results a finalize applied.
func appliedIDs(applied []driver.JobFinalized) []driver.JobID {
	ids := make([]driver.JobID, len(applied))
	for i, a := range applied {
		ids[i] = a.ID
	}
	return ids
}

// finalizeCurrent is finalize for tests of something other than the fence:
// a result without a Fence gets the job's current one, as if it came from
// the job's latest claim. It fences only by the other conditions, such as
// the client, so a test of the fence uses finalize.
func finalizeCurrent(ctx context.Context, t *testing.T, exec driver.Executor, jobs ...driver.JobFinalize) []driver.JobID {
	t.Helper()
	return finalize(ctx, t, exec, withFences(ctx, t, exec, jobs)...)
}

// withFences returns jobs with the current fence of each job filled in where
// a result has none.
func withFences(ctx context.Context, t *testing.T, exec driver.Executor, jobs []driver.JobFinalize) []driver.JobFinalize {
	t.Helper()
	out := slices.Clone(jobs)
	for i, f := range out {
		if f.Fence != 0 {
			continue
		}
		j, err := exec.JobGet(ctx, f.ID)
		if errors.Is(err, driver.ErrNotFound) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		out[i].Fence = j.Fence
	}
	return out
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
	finalizeCurrent(ctx, t, exec, driver.JobFinalize{ID: first[0].Job.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true})
	if third := insert(ctx, t, exec, []driver.JobInsertParams{unique("u", 6, time.Time{})}); third[0].Duplicate {
		t.Error("finalized job still blocks its unique key")
	}
}
