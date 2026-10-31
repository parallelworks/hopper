package hopper

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/internal/testdb"
)

type managedLogArgs struct{}

func (managedLogArgs) Kind() string { return "managed-logs" }

func TestManagedLogsPostgresLifecycle(t *testing.T) {
	pool := testdb.Pool(t)
	store := &managedTestStore{}
	workers := NewWorkers()
	AddWorkFunc(workers, func(ctx context.Context, _ *Job[managedLogArgs]) error {
		Logger(ctx).InfoContext(ctx, "output before completion")
		return nil
	})
	c, err := NewClient(testdb.Driver(pool), &Config{
		Logger: slog.New(slog.DiscardHandler), Workers: workers,
		Queues:  map[string]QueueConfig{"kept": {MaxWorkers: 1}, "ephemeral": {MaxWorkers: 1, DeleteCompleted: true}},
		JobLogs: JobLogsConfig{Store: store, Retention: &LogRetentionConfig{Interval: time.Hour, GracePeriod: time.Nanosecond}},
	})
	if err != nil {
		t.Fatal(err)
	}
	startManagedClient(t, c)
	// A leader term starts with a pass, and the next waits an hour. Each
	// cleanup below is then the explicit call's.
	deadline := time.Now().Add(15 * time.Second)
	for s := c.LogRetentionStats(); !c.IsLeader() || s.Passes+s.Failures == 0; s = c.LogRetentionStats() {
		if time.Now().After(deadline) {
			t.Fatalf("no retention pass at the start of the term: %+v", s)
		}
		time.Sleep(time.Millisecond)
	}
	for _, test := range []struct{ queue, action string }{{"kept", "delete"}, {"ephemeral", "delete-completed"}, {"kept", "history-expiry"}} {
		queue := test.queue
		inserted, err := c.Insert(t.Context(), managedLogArgs{}, &InsertOpts{Queue: queue, Await: true})
		if err != nil {
			t.Fatal(err)
		}
		_, err = Await[struct{}](t.Context(), c, inserted.Job.ID)
		if queue == "ephemeral" && !errors.Is(err, ErrNotFound) || queue == "kept" && err != nil {
			t.Fatalf("%s await = %v", queue, err)
		}
		read, err := c.JobLogRead(t.Context(), inserted.Job.ID, nil)
		if err != nil || read.Run.Fence != 1 || read.Log == "" {
			t.Fatalf("%s read = %+v, %v", queue, read, err)
		}
		res, err := c.MaintainJobLogs(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if queue == "ephemeral" && res.Deleted != 1 {
			t.Fatalf("DeleteCompleted cleanup = %+v", res)
		}
		if queue == "kept" {
			if _, err := c.JobGet(t.Context(), inserted.Job.ID); err != nil {
				t.Fatal(err)
			}
			switch test.action {
			case "delete":
				if _, err := c.JobDelete(t.Context(), inserted.Job.ID); err != nil {
					t.Fatal(err)
				}
			case "history-expiry":
				if _, err := pool.Exec(t.Context(), "UPDATE hopper_job_history SET finalized_at = now() - interval '2 days' WHERE id = $1", inserted.Job.ID.String()); err != nil {
					t.Fatal(err)
				}
				if _, err := c.exec.HistoryMaintain(t.Context(), driver.HistoryMaintainParams{CompletedRetention: 24 * time.Hour, FailedRetention: 7 * 24 * time.Hour, StreamRetention: 24 * time.Hour}); err != nil {
					t.Fatal(err)
				}
				if _, err := c.JobGet(t.Context(), inserted.Job.ID); !errors.Is(err, ErrNotFound) {
					t.Fatalf("expired history = %v", err)
				}
			}
			res, err = c.MaintainJobLogs(t.Context())
			if err != nil || res.Deleted != 1 {
				t.Fatalf("explicit delete cleanup = %+v, %v", res, err)
			}
		}
	}
}

func startManagedClient(t *testing.T, c *Client[pgx.Tx]) {
	t.Helper()
	c.tuning.leaderInterval = 10 * time.Millisecond
	c.tuning.claimCooldown = time.Millisecond
	c.tuning.finalizeInterval = time.Millisecond
	if err := c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 10*time.Second)
		defer cancel()
		if err := c.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
}

func TestManagedLogsLeaderReplacement(t *testing.T) {
	pool := testdb.Pool(t)
	store := &managedTestStore{}
	config := &Config{Logger: slog.New(slog.DiscardHandler), Workers: NewWorkers(), Queues: map[string]QueueConfig{"default": {MaxWorkers: 1}}, JobLogs: JobLogsConfig{
		Store: store, Retention: &LogRetentionConfig{Interval: 10 * time.Millisecond},
	}}
	first, err := NewClient(testdb.Driver(pool), config)
	if err != nil {
		t.Fatal(err)
	}
	id := JobID{15: 1}
	store.put(id, 1, 1, "orphan", time.Now().Add(-2*time.Hour))
	startManagedClient(t, first)
	waitManagedInventoryEmpty(t, store)
	second, err := NewClient(testdb.Driver(pool), config)
	if err != nil {
		t.Fatal(err)
	}
	startManagedClient(t, second)
	if err := first.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	store.put(id, 2, 1, "orphan after leadership change", time.Now().Add(-2*time.Hour))
	waitManagedInventoryEmpty(t, store)
	// The deletion is visible just before the pass publishes its statistics.
	if err := second.logRetention.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	stats := second.LogRetentionStats()
	second.logRetention.release()
	if stats.Deleted == 0 {
		t.Fatal("replacement leader did not run maintenance")
	}
}

func waitManagedInventoryEmpty(t *testing.T, store *managedTestStore) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		store.mu.Lock()
		n := len(store.runs)
		store.mu.Unlock()
		if n == 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("leader did not remove expired logs")
		case <-ticker.C:
		}
	}
}
