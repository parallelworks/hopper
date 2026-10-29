package hopper

import (
	"context"
	"errors"
	"io"
	"iter"
	"slices"
	"sync"
	"testing"
	"time"
)

type followTestReader struct {
	*managedTestStore
	idle chan struct{}
}

func (s *followTestReader) Read(ctx context.Context, p LogReadParams) (LogChunk, error) {
	chunk, err := s.managedTestStore.Read(ctx, p)
	if err == nil && chunk.NextOffset == chunk.Size {
		select {
		case s.idle <- struct{}{}:
		default:
		}
	}
	return chunk, err
}

type followTestStore struct {
	*followTestReader
	LogNotifications
	watchErr     error
	closed       bool
	watchContext context.Context
}

func (s *followTestStore) Writer(ctx context.Context, row *JobRow) (io.WriteCloser, error) {
	w, err := s.managedTestStore.Writer(ctx, row)
	if err != nil {
		return nil, err
	}
	s.Notify(row.ID)
	return &notifyingTestWriter{WriteCloser: w, notify: func() { s.Notify(row.ID) }}, nil
}

type notifyingTestWriter struct {
	io.WriteCloser
	notify func()
}

func (w *notifyingTestWriter) Write(p []byte) (int, error) {
	n, err := w.WriteCloser.Write(p)
	if n > 0 {
		w.notify()
	}
	return n, err
}

func (w *notifyingTestWriter) Close() error {
	err := w.WriteCloser.Close()
	w.notify()
	return err
}

func (s *followTestStore) Watch(ctx context.Context, id JobID) (<-chan struct{}, error) {
	s.watchContext = ctx
	if s.watchErr != nil {
		return nil, s.watchErr
	}
	if s.closed {
		ch := make(chan struct{})
		close(ch)
		return ch, nil
	}
	return s.LogNotifications.Watch(ctx, id)
}

type followTestEvent struct {
	update LogUpdate
	err    error
}

func startTestLogFollow(t *testing.T, c *Client[struct{}], id JobID, opts *LogFollowOpts) <-chan followTestEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	out := make(chan followTestEvent, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(out)
		for update, err := range c.JobLogFollow(ctx, id, opts) {
			select {
			case out <- followTestEvent{update, err}:
			case <-ctx.Done():
				return
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("follower did not stop")
		}
	})
	return out
}

func nextTestLogUpdate(t *testing.T, out <-chan followTestEvent) LogUpdate {
	t.Helper()
	select {
	case event, open := <-out:
		if !open {
			t.Fatal("follower ended before the expected update")
		}
		if event.err != nil {
			t.Fatal(event.err)
		}
		return event.update
	case <-time.After(time.Second):
		t.Fatal("follower missed a change")
	}
	return LogUpdate{}
}

func waitLogFollowIdle(t *testing.T, s *followTestReader) {
	t.Helper()
	select {
	case <-s.idle:
	case <-time.After(time.Second):
		t.Fatal("follower did not drain its current bytes")
	}
}

func TestJobLogFollowNotificationsAndRemoval(t *testing.T) {
	store := &followTestStore{followTestReader: &followTestReader{managedTestStore: &managedTestStore{}, idle: make(chan struct{}, 1)}}
	c, e := managedClient(t, store, nil)
	id := JobID{15: 1}
	store.put(id, 1, 1, "first", e.now)
	out := startTestLogFollow(t, c, id, &LogFollowOpts{PollInterval: time.Hour})
	first := nextTestLogUpdate(t, out)
	if first.Log != "first" || !first.Reset {
		t.Fatalf("first update = %+v", first)
	}
	waitLogFollowIdle(t, store.followTestReader)
	store.put(id, 1, 1, "first appended", e.now)
	store.Notify(id)
	appended := nextTestLogUpdate(t, out)
	if appended.Log != " appended" || appended.Reset {
		t.Fatalf("append = %+v", appended)
	}
	waitLogFollowIdle(t, store.followTestReader)
	store.put(id, 2, 1, "second", e.now)
	store.Notify(id)
	second := nextTestLogUpdate(t, out)
	if second.Log != "second" || !second.Reset || second.Run.Fence != 2 {
		t.Fatalf("new snooze = %+v", second)
	}
	waitLogFollowIdle(t, store.followTestReader)
	store.mu.Lock()
	clear(store.runs)
	store.mu.Unlock()
	store.Notify(id)
	missing := nextTestLogUpdate(t, out)
	if !missing.Missing || !missing.Reset || missing.Run.Fence != 2 {
		t.Fatalf("removed output = %+v", missing)
	}
	store.put(id, 3, 1, "after removal", e.now)
	store.Notify(id)
	restored := nextTestLogUpdate(t, out)
	if restored.Missing || !restored.Reset || restored.Run.Fence != 3 {
		t.Fatalf("new execution after removal = %+v", restored)
	}
}

func TestJobLogFollowPollFallbacks(t *testing.T) {
	for _, mode := range []string{"no watcher", "missed hint", "watch error", "closed subscription"} {
		t.Run(mode, func(t *testing.T) {
			reader := &followTestReader{managedTestStore: &managedTestStore{}, idle: make(chan struct{}, 1)}
			var store LogStore = reader
			watched := &followTestStore{followTestReader: reader}
			switch mode {
			case "missed hint":
				store = watched
			case "watch error":
				watched.watchErr = errors.New("notifications unavailable")
				store = watched
			case "closed subscription":
				watched.closed = true
				store = watched
			}
			c, e := managedClient(t, store, nil)
			id := JobID{15: 1}
			reader.put(id, 1, 1, "first", e.now)
			out := startTestLogFollow(t, c, id, &LogFollowOpts{PollInterval: time.Millisecond})
			_ = nextTestLogUpdate(t, out)
			waitLogFollowIdle(t, reader)
			reader.put(id, 1, 1, "first polled", e.now)
			if next := nextTestLogUpdate(t, out); next.Log != " polled" || next.Reset {
				t.Fatalf("poll fallback = %+v", next)
			}
			if (mode == "watch error" || mode == "closed subscription") && watched.watchContext.Err() == nil {
				t.Fatal("failed subscription kept its context alive")
			}
		})
	}
}

func TestJobLogFollowStartsBeforeFirstOutput(t *testing.T) {
	store := &followTestStore{followTestReader: &followTestReader{managedTestStore: &managedTestStore{}, idle: make(chan struct{}, 1)}}
	c, e := managedClient(t, store, nil)
	id := JobID{15: 1}
	out := startTestLogFollow(t, c, id, &LogFollowOpts{PollInterval: time.Hour})
	if first := nextTestLogUpdate(t, out); !first.Missing {
		t.Fatalf("expected no output: %+v", first)
	}
	store.put(id, 1, 1, "started", e.now)
	store.Notify(id)
	if first := nextTestLogUpdate(t, out); first.Missing || !first.Reset || first.Log != "started" {
		t.Fatalf("first output = %+v", first)
	}
}

func TestJobLogFollowBoundedReadsAndConsumerStop(t *testing.T) {
	store := &followTestStore{followTestReader: &followTestReader{managedTestStore: &managedTestStore{}, idle: make(chan struct{}, 1)}}
	c, e := managedClient(t, store, nil)
	id := JobID{15: 1}
	store.put(id, 1, 1, "abcdefg", e.now)
	next, stop := iter.Pull2(c.JobLogFollow(t.Context(), id, &LogFollowOpts{LogReadOpts: LogReadOpts{Limit: 3}, PollInterval: time.Hour}))
	defer stop()
	var texts []string
	for i := range 3 {
		update, err, open := next()
		if err != nil || !open || update.Reset != (i == 0) {
			t.Fatalf("chunk %d = %+v, %v, %v", i, update, err, open)
		}
		texts = append(texts, update.Log)
	}
	if !slices.Equal(texts, []string{"abc", "def", "g"}) {
		t.Fatalf("chunks = %q", texts)
	}
	stop()
	if !errors.Is(store.watchContext.Err(), context.Canceled) {
		t.Fatal("consumer stop left subscription alive")
	}
}

func TestJobLogFollowFixedExecution(t *testing.T) {
	store := &followTestStore{followTestReader: &followTestReader{managedTestStore: &managedTestStore{}, idle: make(chan struct{}, 1)}}
	c, e := managedClient(t, store, nil)
	id := JobID{15: 1}
	store.put(id, 1, 1, "old", e.now)
	store.put(id, 2, 1, "latest", e.now)
	out := startTestLogFollow(t, c, id, &LogFollowOpts{LogReadOpts: LogReadOpts{Fence: 1}, PollInterval: time.Hour})
	if first := nextTestLogUpdate(t, out); first.Run.Fence != 1 || first.Log != "old" {
		t.Fatalf("fixed execution = %+v", first)
	}
	waitLogFollowIdle(t, store.followTestReader)
	store.put(id, 3, 1, "newer", e.now)
	store.Notify(id)
	waitLogFollowIdle(t, store.followTestReader)
	store.mu.Lock()
	fence := store.lastRead.Fence
	store.mu.Unlock()
	if fence != 1 {
		t.Fatal("fixed follower switched executions")
	}
	store.put(id, 1, 1, "old appended", e.now)
	store.Notify(id)
	if next := nextTestLogUpdate(t, out); next.Log != " appended" || next.Run.Fence != 1 {
		t.Fatalf("fixed append = %+v", next)
	}
}

func TestJobLogFollowRejectsInvalidOptions(t *testing.T) {
	for _, opts := range []*LogFollowOpts{
		{PollInterval: -1}, {LogReadOpts: LogReadOpts{Limit: MaxLogReadBytes + 1}}, {LogReadOpts: LogReadOpts{Cursor: LogCursor{Offset: -2}}},
	} {
		c, _ := managedClient(t, &managedTestStore{}, nil)
		count := 0
		for _, err := range c.JobLogFollow(t.Context(), JobID{15: 1}, opts) {
			count++
			if err == nil {
				t.Fatal("accepted invalid options")
			}
		}
		if count != 1 {
			t.Fatalf("error count = %d", count)
		}
	}
}

func TestJobLogFollowResumesWithoutReset(t *testing.T) {
	store := &managedTestStore{}
	c, e := managedClient(t, store, nil)
	id := JobID{15: 1}
	store.put(id, 1, 1, "prefix suffix", e.now)
	opts := &LogFollowOpts{LogReadOpts: LogReadOpts{Cursor: LogCursor{Fence: 1, Offset: 6}}, PollInterval: time.Hour}
	seen := false
	for update, err := range c.JobLogFollow(t.Context(), id, opts) {
		if err != nil {
			t.Fatal(err)
		}
		if update.Reset || update.Log != " suffix" || update.Cursor.Offset != 13 {
			t.Fatalf("resume = %+v", update)
		}
		seen = true
		break
	}
	if !seen {
		t.Fatal("no resumed output")
	}
}

func TestLogNotificationsCoalesceAndScope(t *testing.T) {
	var n LogNotifications
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	id := JobID{15: 1}
	ch, err := n.Watch(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	other, err := n.Watch(ctx, JobID{15: 2})
	if err != nil {
		t.Fatal(err)
	}
	for range 10000 {
		n.Notify(id)
	}
	if len(ch) != 1 || len(other) != 0 {
		t.Fatalf("hint buffers = %d, %d", len(ch), len(other))
	}
	cancel()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		select {
		case _, open := <-ch:
			if !open {
				return
			}
		case <-deadline.C:
			t.Fatal("canceled subscription did not close")
		}
	}
}

func TestLogNotificationsConcurrentCancellation(t *testing.T) {
	var n LogNotifications
	id := JobID{15: 1}
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 10000 {
			n.Notify(id)
		}
	})
	for range 1000 {
		ctx, cancel := context.WithCancel(t.Context())
		if _, err := n.Watch(ctx, id); err != nil {
			t.Fatal(err)
		}
		cancel()
	}
	wg.Wait()
}
