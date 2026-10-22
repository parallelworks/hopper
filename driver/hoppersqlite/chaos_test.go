package hoppersqlite_test

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/hoppersqlite"
)

type chaosArgs struct {
	N int `json:"n"`
}

func (chaosArgs) Kind() string { return "chaos" }

// chaosWorker is the "worker" role of TestHelperProcess: a client whose
// worker writes the job's effect and completes the job in one
// transaction, until the test kills it.
func chaosWorker(ctx context.Context, t *testing.T, d *hoppersqlite.Driver) {
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[chaosArgs]) error {
		time.Sleep(time.Duration(rand.IntN(20)) * time.Millisecond)
		tx, err := d.DB().BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback() //nolint:errcheck // committed below
		if _, err := tx.ExecContext(ctx, "INSERT INTO effects (n, attempt) VALUES (?, ?)", job.Args.N, job.Attempt); err != nil {
			return err
		}
		if err := hopper.CompleteTx(ctx, tx, job); err != nil {
			return err
		}
		return tx.Commit()
	})
	c, err := hopper.NewClient(d, &hopper.Config{
		Queues:       map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 8}},
		Workers:      workers,
		PollInterval: 20 * time.Millisecond,
		LeaseTTL:     time.Second,
		Logger:       slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	fmt.Println("started")
	time.Sleep(time.Minute)
}

// hog is the "hog" role of TestHelperProcess: eight goroutines that write
// as fast as they can for three seconds.
func hog(ctx context.Context, t *testing.T, d *hoppersqlite.Driver) {
	x := d.Executor()
	deadline := time.Now().Add(3 * time.Second)
	fmt.Println("hogging")
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for time.Now().Before(deadline) {
				if _, err := x.JobInsertMany(ctx, []driver.JobInsertParams{{Kind: "hog", Queue: "hog", Priority: 2, MaxAttempts: 1}}, driver.JobInsertOpts{}); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
}

// TestChaos runs jobs in worker processes that write each job's effect and
// complete it in one transaction, and kills a process at random every few
// hundred milliseconds, often inside a transaction, starting another in
// its place. Every job ends completed with exactly one effect, and the
// database passes its integrity check.
func TestChaos(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "hopper.db")
	d := openAt(t, path)
	if _, err := d.DB().ExecContext(ctx, "CREATE TABLE effects (n INTEGER, attempt INTEGER)"); err != nil {
		t.Fatal(err)
	}
	const jobs, procs = 600, 4
	c, err := hopper.NewClient(d, &hopper.Config{Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	params := make([]hopper.InsertParams, jobs)
	for i := range params {
		params[i] = hopper.InsertParams{Args: chaosArgs{N: i}}
	}
	if _, err := c.InsertMany(ctx, params); err != nil {
		t.Fatal(err)
	}
	var running []*exec.Cmd
	defer func() {
		for _, cmd := range running {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	for range procs {
		cmd, _ := helper(t, path, "worker")
		running = append(running, cmd)
	}
	kills := 0
	live := func() int {
		var n int
		if err := d.DB().QueryRowContext(ctx, "SELECT count(*) FROM hopper_jobs").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	for start := time.Now(); time.Since(start) < 5*time.Second && live() > 0; {
		time.Sleep(time.Duration(100+rand.IntN(200)) * time.Millisecond)
		i := rand.IntN(len(running))
		_ = running[i].Process.Signal(syscall.SIGKILL)
		_ = running[i].Wait()
		kills++
		running = slices.Delete(running, i, i+1)
		cmd, _ := helper(t, path, "worker")
		running = append(running, cmd)
	}
	left := live()
	for deadline := time.Now().Add(time.Minute); left > 0 && time.Now().Before(deadline); left = live() {
		time.Sleep(100 * time.Millisecond)
	}
	var completed, effects, distinct int
	if err := d.DB().QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM hopper_job_history WHERE state = 'completed'),
		(SELECT count(*) FROM effects), (SELECT count(DISTINCT n) FROM effects)`).Scan(&completed, &effects, &distinct); err != nil {
		t.Fatal(err)
	}
	t.Logf("killed %d processes; %d jobs left, %d completed, %d effects", kills, left, completed, effects)
	if left != 0 || completed != jobs || effects != jobs || distinct != jobs {
		t.Errorf("%d jobs: %d left, %d completed, %d effects of %d jobs; want every job completed with one effect", jobs, left, completed, effects, distinct)
	}
	var check string
	if err := d.DB().QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&check); err != nil || check != "ok" {
		t.Errorf("integrity_check = %q, %v", check, err)
	}
}

// TestFairness writes in this process while another writes as fast as it
// can, from eight goroutines, for three seconds: writes here are held up,
// since SQLite's busy handler polls for the lock rather than queueing for
// it, but not for as long as the other process writes. It runs alone, not
// in parallel, for its waits to be the lock's.
func TestFairness(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "hopper.db")
	d := openAt(t, path)
	x := d.Executor()
	cmd, out := helper(t, path, "hog")
	for !strings.Contains(out.String(), "hogging") {
		time.Sleep(5 * time.Millisecond)
	}
	var waits []time.Duration
	for end := time.Now().Add(2 * time.Second); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		start := time.Now()
		if err := x.QueueEnsure(ctx, []string{"q" + strconv.Itoa(len(waits))}); err != nil {
			t.Fatal(err)
		}
		waits = append(waits, time.Since(start))
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("hogging process: %v\n%s", err, out)
	}
	slices.Sort(waits)
	p50, p90, worst := waits[len(waits)/2], waits[len(waits)*9/10], waits[len(waits)-1]
	t.Logf("%d writes: p50 %s, p90 %s, max %s", len(waits), p50, p90, worst)
	if worst > slowdown*2*time.Second {
		t.Errorf("a write waited %s while another process wrote", worst)
	}
}
