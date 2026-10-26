package hopper

import (
	"context"
	"strconv"
	"sync"
	"testing"
)

// TestWaitForJobsWhileRemovingQueues checks that Stop's wait for running
// jobs, which reads the client's producers, can run while Queues().Remove
// takes them out. Under -race it fails if the wait reads the producers
// without the lock Remove holds.
func TestWaitForJobsWhileRemovingQueues(t *testing.T) {
	const queues = 32
	c := &Client[struct{}]{producers: make(map[string]*producer, queues)}
	for i := range queues {
		done := make(chan struct{})
		close(done)
		c.producers["q"+strconv.Itoa(i)] = &producer{cancel: func() {}, done: done}
	}
	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Go(func() {
		for range queues {
			if !c.waitForJobs(ctx) {
				t.Error("waitForJobs gave up with nothing running")
			}
		}
	})
	for i := range queues {
		wg.Go(func() {
			if err := c.Queues().Remove(ctx, "q"+strconv.Itoa(i)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if len(c.producers) != 0 {
		t.Errorf("%d producers left", len(c.producers))
	}
}
