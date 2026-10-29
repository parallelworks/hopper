package hopper

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type observedLogValue struct{ calls *atomic.Int64 }

func (v observedLogValue) LogValue() slog.Value {
	v.calls.Add(1)
	return slog.StringValue("observed")
}

func TestCappedExternalLogKeepsServiceOutputWithoutFormattingTwice(t *testing.T) {
	var service bytes.Buffer
	a := cappedLogAttempt(t, slog.New(slog.NewTextHandler(&service, nil)))
	var calls atomic.Int64
	value := observedLogValue{calls: &calls}
	a.logger.InfoContext(t.Context(), "after cap", "value", value)
	if got := calls.Load(); got != 1 {
		t.Fatalf("record formatted %d times, want service output only", got)
	}
	child := a.logger.WithGroup("child").With("bound", value)
	if got := calls.Load(); got != 2 {
		t.Fatalf("bound value formatted %d times, want one more service value", got)
	}
	child.InfoContext(t.Context(), "child after cap")
	if text := service.String(); !strings.Contains(text, "after cap") || !strings.Contains(text, "child.bound=observed") {
		t.Fatalf("service output was lost: %s", text)
	}
	if !a.stored.full.Load() {
		t.Fatal("external byte cap was not propagated to the handler")
	}
	p := []byte("later bytes")
	if allocations := testing.AllocsPerRun(100, func() { _, _ = a.stored.Write(p) }); allocations != 0 {
		t.Fatalf("capped writes allocate %.1f times", allocations)
	}
	a.stored.mu.Lock()
	defer a.stored.mu.Unlock()
	if len(a.stored.queue) != 0 {
		t.Fatal("capped output was queued")
	}
}

type countedFollowStore struct {
	*managedTestStore
	reads atomic.Int64
	lists atomic.Int64
}

func (s *countedFollowStore) Read(ctx context.Context, p LogReadParams) (LogChunk, error) {
	s.reads.Add(1)
	return s.managedTestStore.Read(ctx, p)
}

func (s *countedFollowStore) List(ctx context.Context, p LogListParams) (LogRunPage, error) {
	s.lists.Add(1)
	return s.managedTestStore.List(ctx, p)
}

func TestJobLogFollowWaitsAtEndWithoutRedundantRead(t *testing.T) {
	store := &countedFollowStore{managedTestStore: &managedTestStore{}}
	c, e := managedClient(t, store, nil)
	id := JobID{15: 1}
	store.put(id, 1, 1, "first", e.now)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	updates := 0
	for _, err := range c.JobLogFollow(ctx, id, &LogFollowOpts{PollInterval: time.Hour}) {
		if err != nil {
			t.Fatal(err)
		}
		updates++
		// The absence of another read while the follower is idle is the assertion.
		timer := time.AfterFunc(10*time.Millisecond, cancel)
		defer timer.Stop()
	}
	if updates != 1 || store.reads.Load() != 1 || store.lists.Load() != 1 {
		t.Fatalf("updates=%d reads=%d lists=%d, want one of each", updates, store.reads.Load(), store.lists.Load())
	}
}

func TestJobLogFollowSeesAppendWhileYieldingCaughtUpChunk(t *testing.T) {
	store := &followTestStore{followTestReader: &followTestReader{managedTestStore: &managedTestStore{}, idle: make(chan struct{}, 1)}}
	c, e := managedClient(t, store, nil)
	id := JobID{15: 1}
	store.put(id, 1, 1, "first", e.now)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	updates := 0
	for update, err := range c.JobLogFollow(ctx, id, &LogFollowOpts{PollInterval: time.Hour}) {
		if err != nil {
			t.Fatal(err)
		}
		updates++
		if updates == 1 {
			store.put(id, 1, 1, "first appended", e.now)
			store.Notify(id)
			continue
		}
		if update.Log != " appended" || update.Reset {
			t.Fatalf("append = %+v", update)
		}
		break
	}
	if updates != 2 {
		t.Fatalf("received %d updates, want the append without polling", updates)
	}
}
