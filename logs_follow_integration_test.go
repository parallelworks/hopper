package hopper

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/hopper/internal/testdb"
)

func TestJobLogFollowBeforeWorkerFinishes(t *testing.T) {
	pool := testdb.Pool(t)
	store := &followTestStore{followTestReader: &followTestReader{managedTestStore: &managedTestStore{}, idle: make(chan struct{}, 1)}}
	release := make(chan struct{})
	defer close(release)
	workers := NewWorkers()
	AddWorkFunc(workers, func(ctx context.Context, _ *Job[managedLogArgs]) error {
		Logger(ctx).InfoContext(ctx, "worker is still running")
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	client, err := NewClient(testdb.Driver(pool), &Config{
		Workers: workers, Logger: slog.New(slog.DiscardHandler),
		Queues:  map[string]QueueConfig{"default": {MaxWorkers: 1}},
		JobLogs: JobLogsConfig{Store: store},
	})
	if err != nil {
		t.Fatal(err)
	}
	startManagedClient(t, client)
	inserted, err := client.Insert(t.Context(), managedLogArgs{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewClient(testdb.Driver(pool), &Config{JobLogs: JobLogsConfig{Store: store}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	found := false
	for update, err := range reader.JobLogFollow(ctx, inserted.Job.ID, nil) {
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(update.Log, "worker is still running") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("follower did not see live output")
	}
	row, err := client.JobGet(t.Context(), inserted.Job.ID)
	if err != nil || row.State != JobStateRunning {
		t.Fatalf("worker finished before its log was read: %+v, %v", row, err)
	}
}
