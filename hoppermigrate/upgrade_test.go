package hoppermigrate_test

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver/hopperpgx"
	"github.com/parallelworks/hopper/hoppermigrate"
	"github.com/parallelworks/hopper/internal/testdb"
)

type upgradeJob struct {
	N int `json:"n"`
}

func (upgradeJob) Kind() string { return "upgrade" }

// TestUpgradeUnderTraffic migrates from the previous schema version to the
// latest while a client is working jobs in every state, and checks that
// traffic continues throughout. Every schema change ships with this test
// covering it, because migrations that touch hopper_jobs must not block
// claims for longer than one statement.
func TestUpgradeUnderTraffic(t *testing.T) {
	t.Parallel()
	if hoppermigrate.Latest() < 2 {
		t.Skip("only one schema version")
	}
	ctx := context.Background()
	pool := testdb.EmptyPool(t, 0)
	d := hopperpgx.New(pool)
	opts := &hoppermigrate.Options{Logger: slog.New(slog.DiscardHandler)}
	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Target: hoppermigrate.Latest() - 1, Logger: opts.Logger}); err != nil {
		t.Fatal(err)
	}

	var completed atomic.Int64
	release := make(chan struct{})
	workers := hopper.NewWorkers()
	hopper.AddWorkFunc(workers, func(ctx context.Context, job *hopper.Job[upgradeJob]) error {
		switch job.Args.N % 10 {
		case 1: // a job that keeps failing: retryable, later discarded
			return errors.New("flaky")
		case 2: // a long-running job: running throughout the migration
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		completed.Add(1)
		return nil
	})
	client, err := hopper.NewClient(d, &hopper.Config{
		Queues:       map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 8}},
		Workers:      workers,
		Logger:       opts.Logger,
		PollInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer client.Stop(ctx) //nolint:errcheck // cleanup

	// Jobs in every state: available, scheduled, running, retryable, and
	// finalized ones in history.
	insert := func(from, to int) {
		t.Helper()
		params := make([]hopper.InsertParams, 0, to-from)
		for i := from; i < to; i++ {
			p := hopper.InsertParams{Args: upgradeJob{N: i}, Opts: &hopper.InsertOpts{MaxAttempts: 2}}
			if i%10 == 3 {
				p.Opts.ScheduledAt = time.Now().Add(time.Hour)
			}
			params = append(params, p)
		}
		if _, err := client.InsertMany(ctx, params); err != nil {
			t.Fatal(err)
		}
	}
	insert(0, 100)
	time.Sleep(300 * time.Millisecond)

	// Keep inserting while migrating.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 100; ; i += 10 {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			insert(i, i+10)
		}
	}()
	start := time.Now()
	res, err := hoppermigrate.Up(ctx, d, opts)
	took := time.Since(start)
	close(stop)
	<-done
	if err != nil {
		t.Fatalf("Up under traffic: %v", err)
	}
	if res.Version != hoppermigrate.Latest() || len(res.Applied) != 1 {
		t.Errorf("Up = %+v", res)
	}
	if took > 10*time.Second {
		t.Errorf("migration took %s under traffic", took)
	}

	// Traffic continues on the new schema, and the jobs that were running
	// finish normally.
	before := completed.Load()
	close(release)
	insert(100000, 100050)
	deadline := time.Now().Add(15 * time.Second)
	for completed.Load() < before+50 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if completed.Load() < before+50 {
		t.Errorf("jobs did not complete after the migration: %d -> %d", before, completed.Load())
	}
	v, err := hoppermigrate.Version(ctx, d)
	if err != nil || v != hoppermigrate.Latest() {
		t.Errorf("Version = %d, %v", v, err)
	}
}
