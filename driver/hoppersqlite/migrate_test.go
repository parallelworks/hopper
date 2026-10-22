package hoppersqlite_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/hoppersqlite"
	"github.com/parallelworks/hopper/hoppermigrate"
)

var quiet = &hoppermigrate.Options{Logger: slog.New(slog.DiscardHandler)}

// openEmpty returns a driver on a new database without hopper's schema.
func openEmpty(t *testing.T) *hoppersqlite.Driver {
	t.Helper()
	d, err := hoppersqlite.Open(filepath.Join(t.TempDir(), "hopper.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// objects counts hopper's tables, indexes and triggers in the database.
func objects(t *testing.T, d *hoppersqlite.Driver) int {
	t.Helper()
	var n int
	if err := d.DB().QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name LIKE 'hopper%'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestMigrate applies every version, reverts them all and applies them
// again: each script, up and down, runs, and down to zero leaves nothing of
// hopper's behind.
func TestMigrate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d := openEmpty(t)
	if v, err := hoppermigrate.Version(ctx, d); err != nil || v != 0 {
		t.Fatalf("version of an empty database = %d, %v", v, err)
	}
	if v, err := d.Executor().SchemaVersion(ctx); err != nil || v != 0 {
		t.Fatalf("schema version of an empty database = %d, %v", v, err)
	}
	for range 2 {
		res, err := hoppermigrate.Up(ctx, d, quiet)
		if err != nil || res.Version != hoppermigrate.Latest() || len(res.Applied) != hoppermigrate.Latest() {
			t.Fatalf("Up = %+v, %v", res, err)
		}
		if res, err := hoppermigrate.Up(ctx, d, quiet); err != nil || len(res.Applied) != 0 {
			t.Fatalf("a second Up = %+v, %v", res, err)
		}
		if v, err := d.Executor().SchemaVersion(ctx); err != nil || v != hoppermigrate.Latest() {
			t.Fatalf("schema version = %d, %v", v, err)
		}
		if _, err := hoppermigrate.Down(ctx, d, quiet); err != nil {
			t.Fatalf("Down: %v", err)
		}
		if n := objects(t, d); n != 0 {
			t.Fatalf("%d of hopper's objects left after Down to zero", n)
		}
	}
	// Down and up one version at a time.
	if _, err := hoppermigrate.Up(ctx, d, quiet); err != nil {
		t.Fatal(err)
	}
	for v := hoppermigrate.Latest() - 1; v >= 0; v-- {
		if _, err := hoppermigrate.Down(ctx, d, &hoppermigrate.Options{Logger: quiet.Logger, Target: v}); err != nil {
			t.Fatalf("Down to %d: %v", v, err)
		}
	}
	for v := 1; v <= hoppermigrate.Latest(); v++ {
		if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: quiet.Logger, Target: v}); err != nil {
			t.Fatalf("Up to %d: %v", v, err)
		}
	}
}

// TestMigrations checks the scripts Migrations returns: SQLite's, one up
// and one down for every version hoppermigrate knows, under its names,
// which applied in order by hand build the schema Up builds.
func TestMigrations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pg := hoppermigrate.Migrations()
	ms := hoppersqlite.Migrations()
	if len(ms) != len(pg) {
		t.Fatalf("%d migrations, want %d", len(ms), len(pg))
	}
	d := openEmpty(t)
	for i, m := range ms {
		if m.Version != pg[i].Version || m.Name != pg[i].Name || m.Up == "" || m.Down == "" || m.Up == pg[i].Up {
			t.Fatalf("migration %d = version %d %q, up %d bytes, down %d bytes; want SQLite's scripts of version %d %q",
				i, m.Version, m.Name, len(m.Up), len(m.Down), pg[i].Version, pg[i].Name)
		}
		if _, err := d.DB().ExecContext(ctx, m.Up); err != nil {
			t.Fatalf("version %d: %v", m.Version, err)
		}
		if _, err := d.DB().ExecContext(ctx, "INSERT INTO hopper_schema (version, applied_at) VALUES (?, 0)", m.Version); err != nil {
			t.Fatal(err)
		}
	}
	if v, err := hoppermigrate.Version(ctx, d); err != nil || v != hoppermigrate.Latest() {
		t.Errorf("version after applying the scripts by hand = %d, %v", v, err)
	}
	if res, err := hoppermigrate.Up(ctx, d, quiet); err != nil || len(res.Applied) != 0 {
		t.Errorf("Up after applying the scripts by hand = %+v, %v", res, err)
	}
}

// TestUpConcurrent runs Up from several Drivers on one database at once:
// one applies the migrations, and the others find them applied.
func TestUpConcurrent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "hopper.db")
	var applied atomic.Int64
	var wg sync.WaitGroup
	for range 4 {
		d, err := hoppersqlite.Open(path, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		wg.Go(func() {
			res, err := hoppermigrate.Up(ctx, d, quiet)
			if err != nil {
				t.Error(err)
				return
			}
			applied.Add(int64(len(res.Applied)))
		})
	}
	wg.Wait()
	if n := applied.Load(); n != int64(hoppermigrate.Latest()) {
		t.Errorf("applied %d migrations in all, want %d", n, hoppermigrate.Latest())
	}
}

type upgradeJob struct {
	N int `json:"n"`
}

func (upgradeJob) Kind() string { return "upgrade" }

// TestUpgradeUnderTraffic migrates to the latest version from every
// earlier one, the upgrades a release can need, while jobs wait in every
// state and inserts, claims and finalizes keep running, and then has a
// client work every job. Those statements are written as a client of an
// earlier release would write them, with the columns of the first version,
// which every later one accepts. The migration holds the write lock while
// it runs, as every transaction that writes does, so they wait for it; none
// of them may fail, and no job may be lost.
func TestUpgradeUnderTraffic(t *testing.T) {
	t.Parallel()
	for from := 1; from < hoppermigrate.Latest(); from++ {
		t.Run(fmt.Sprintf("from_v%d", from), func(t *testing.T) {
			t.Parallel()
			upgradeUnderTraffic(t, from)
		})
	}
}

func upgradeUnderTraffic(t *testing.T, from int) {
	ctx := context.Background()
	d := openEmpty(t)
	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: quiet.Logger, Target: from}); err != nil {
		t.Fatal(err)
	}
	db := d.DB()
	now := time.Now().UnixMicro()
	var next atomic.Int64
	insert := func(state string, scheduledAt int64) (driver.JobID, error) {
		var id driver.JobID
		_, _ = rand.Read(id[:])
		args, _ := json.Marshal(upgradeJob{N: int(next.Add(1))})
		_, err := db.ExecContext(ctx, `INSERT INTO hopper_jobs (id, kind, queue, state, priority, max_attempts, scheduled_at, args, created_at)
			VALUES (?, 'upgrade', 'default', ?, 2, 5, ?, ?, ?)`, id[:], state, scheduledAt, string(args), time.Now().UnixMicro())
		return id, err
	}
	// Jobs in every state, as an earlier release leaves them: available,
	// scheduled, retryable, running on a client that has since died, and
	// finished in history.
	for _, state := range []string{"available", "scheduled", "retryable"} {
		at := now
		if state == "scheduled" {
			at = now + time.Second.Microseconds()
		}
		for range 5 {
			if _, err := insert(state, at); err != nil {
				t.Fatal(err)
			}
		}
	}
	var dead int64
	if err := db.QueryRowContext(ctx, `INSERT INTO hopper_clients (hostname, started_at, expires_at, info) VALUES ('gone', ?, ?, '{}') RETURNING id`,
		now-time.Hour.Microseconds(), now-time.Minute.Microseconds()).Scan(&dead); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		id, err := insert("available", now)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `UPDATE hopper_jobs SET state = 'running', attempt = 1, attempted_at = ?, attempted_by = ? WHERE id = ?`,
			now, dead, id[:]); err != nil {
			t.Fatal(err)
		}
	}
	finished := func(id driver.JobID) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback() //nolint:errcheck // committed below
		if _, err := tx.ExecContext(ctx, `INSERT INTO hopper_job_history (id, seq, kind, queue, state, priority, attempt, max_attempts,
			scheduled_at, args, metadata, errors, await, created_at, finalized_at)
			SELECT id, seq, kind, queue, 'completed', priority, attempt, max_attempts, scheduled_at, args, metadata, errors, await,
			  created_at, ? FROM hopper_jobs WHERE id = ?`, time.Now().UnixMicro(), id[:]); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM hopper_jobs WHERE id = ?`, id[:]); err != nil {
			return err
		}
		return tx.Commit()
	}
	for range 3 {
		id, err := insert("available", now)
		if err != nil {
			t.Fatal(err)
		}
		if err := finished(id); err != nil {
			t.Fatal(err)
		}
	}

	// Traffic while the migration runs: a producer inserting, and a client
	// of the earlier release claiming and finishing jobs with statements
	// every version accepts. A claimed job's number is recorded as worked.
	var (
		mu      sync.Mutex
		worked  = map[int]int{}
		longest time.Duration
		stop    = make(chan struct{})
		wg      sync.WaitGroup
	)
	timed := func(fn func() error) error {
		start := time.Now()
		err := fn()
		mu.Lock()
		longest = max(longest, time.Since(start))
		mu.Unlock()
		return err
	}
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := timed(func() error { _, err := insert("available", time.Now().UnixMicro()); return err }); err != nil {
				t.Error(err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	})
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			err := timed(func() error {
				var (
					id   []byte
					args string
				)
				err := db.QueryRowContext(ctx, `UPDATE hopper_jobs SET state = 'running', attempt = attempt + 1, attempted_at = ?, attempted_by = ?
					WHERE seq = (SELECT seq FROM hopper_jobs WHERE state = 'available' AND scheduled_at <= ? ORDER BY seq LIMIT 1)
					RETURNING id, args`, time.Now().UnixMicro(), dead, time.Now().UnixMicro()).Scan(&id, &args)
				if err != nil {
					return nil //nolint:nilerr // nothing to claim
				}
				var job upgradeJob
				if err := json.Unmarshal([]byte(args), &job); err != nil {
					return err
				}
				mu.Lock()
				worked[job.N]++
				mu.Unlock()
				return finished(driver.JobID(id))
			})
			if err != nil {
				t.Error(err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	})
	time.Sleep(20 * time.Millisecond)
	res, err := hoppermigrate.Up(ctx, d, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if res.Version != hoppermigrate.Latest() {
		t.Fatalf("Up = %+v", res)
	}
	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()
	if longest > 5*time.Second {
		t.Errorf("a statement waited %s for the migration", longest)
	}

	// The release's client works every job left, the running ones once
	// rescued from the dead client.
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(_ context.Context, job *hopper.Job[upgradeJob]) error {
		mu.Lock()
		worked[job.Args.N]++
		mu.Unlock()
		return nil
	})
	c, err := hopper.NewClient(d, &hopper.Config{
		Queues:       map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 10}},
		Workers:      workers,
		PollInterval: 20 * time.Millisecond,
		Logger:       slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Stop(ctx) //nolint:errcheck // stopped below
	total := int(next.Load())
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		var live int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM hopper_jobs`).Scan(&live); err != nil {
			t.Fatal(err)
		}
		if live == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d jobs still live", live)
		}
	}
	if err := c.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	// The finished jobs inserted before the migration were never worked.
	if len(worked) != total-3 {
		t.Errorf("%d jobs worked, want %d", len(worked), total-3)
	}
	for n, times := range worked {
		// A job running on the dead client was worked by it before (not
		// recorded) and by the rescue after.
		if times != 1 {
			t.Errorf("job %d worked %d times", n, times)
		}
	}
}
