package hopper_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
)

type allocationCreated struct {
	ID int `json:"id"`
}

func (allocationCreated) Topic() string { return "allocation.created" }

type inventoryUpdated struct {
	ID int `json:"id"`
}

func (inventoryUpdated) Topic() string { return "inventory.updated" }

type userSignedUp struct {
	Email string `json:"email"`
}

func (userSignedUp) Topic() string { return "user.signed_up" }

// deliveries records what each subscription received.
type deliveries struct {
	mu   sync.Mutex
	got  map[string][]string // subscription -> ordered "topic:id" entries
	done chan struct{}
}

func newDeliveries() *deliveries {
	return &deliveries{got: map[string][]string{}, done: make(chan struct{}, 100)}
}

func (d *deliveries) record(sub, entry string) {
	d.mu.Lock()
	d.got[sub] = append(d.got[sub], entry)
	d.mu.Unlock()
	d.done <- struct{}{}
}

func (d *deliveries) of(sub string) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.got[sub]...)
}

func TestPublishFansOutToMatchingSubscriptions(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	d := newDeliveries()
	workers := hopper.NewWorkers()
	hopper.Subscribe(workers, hopper.Subscription{Name: "billing", Pattern: "allocation.*"}, func(_ context.Context, msg *hopper.Message[allocationCreated]) error {
		d.record("billing", msg.Topic+":"+msg.ID)
		if msg.Payload.ID == 0 || msg.Headers["source"] != "test" || msg.Kind != "sub:billing" {
			t.Errorf("message = %+v", msg)
		}
		return nil
	})
	hopper.Subscribe(workers, hopper.Subscription{Name: "audit", Pattern: "#", Queue: "audit"}, func(_ context.Context, msg *hopper.Message[hopper.Raw]) error {
		d.record("audit", msg.Topic+":"+msg.ID)
		return nil
	})
	hopper.Subscribe(workers, hopper.Subscription{Name: "onboarding", Pattern: "user.signed_up"}, func(_ context.Context, msg *hopper.Message[userSignedUp]) error {
		d.record("onboarding", msg.Topic+":"+msg.ID)
		return nil
	})
	c := h.client(&hopper.Config{
		Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 4}, "audit": {MaxWorkers: 2}},
		Workers: workers,
	})
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	subs, err := c.Subscriptions(ctx)
	if err != nil || len(subs) != 3 || subs[0].Name != "audit" || subs[0].Queue != "audit" || subs[1].Kind != "sub:billing" {
		t.Fatalf("subscriptions = %v, %v", subs, err)
	}

	// allocation.created: billing and audit. user.signed_up: onboarding and audit.
	res, err := c.Publish(ctx, allocationCreated{ID: 42}, &hopper.PublishOpts{Headers: map[string]string{"source": "test"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Deliveries) != 2 || res.MessageID == "" {
		t.Fatalf("publish = %+v", res)
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.PublishTx(ctx, tx, userSignedUp{Email: "a@example.com"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// Only the catch-all matches this topic.
	only, err := c.Publish(ctx, inventoryUpdated{ID: 1}, nil)
	if err != nil || len(only.Deliveries) != 1 || only.Deliveries[0].Job.Kind != "sub:audit" {
		t.Fatalf("publish with only the catch-all = %+v, %v", only, err)
	}

	for range 5 {
		select {
		case <-d.done:
		case <-time.After(10 * time.Second):
			t.Fatalf("deliveries so far: billing %v audit %v onboarding %v", d.of("billing"), d.of("audit"), d.of("onboarding"))
		}
	}
	if got := d.of("billing"); len(got) != 1 || got[0] != "allocation.created:"+res.MessageID {
		t.Errorf("billing = %v", got)
	}
	if got := d.of("audit"); len(got) != 3 {
		t.Errorf("audit = %v", got)
	}
	if got := d.of("onboarding"); len(got) != 1 || !strings.HasPrefix(got[0], "user.signed_up:") {
		t.Errorf("onboarding = %v", got)
	}
}

func TestPublishDedupAndReplay(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var calls sync.Map
	workers := hopper.NewWorkers()
	hopper.Subscribe(workers, hopper.Subscription{Name: "flaky", Pattern: "allocation.created", MaxAttempts: 1}, func(_ context.Context, msg *hopper.Message[allocationCreated]) error {
		n, _ := calls.LoadOrStore(msg.ID, new(int))
		*n.(*int)++
		if *n.(*int) == 1 {
			return errors.New("first delivery fails")
		}
		return nil
	})
	c := h.started(workers, 2)

	first, err := c.Publish(ctx, allocationCreated{ID: 1}, &hopper.PublishOpts{DedupKey: "evt-1"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := c.Publish(ctx, allocationCreated{ID: 1}, &hopper.PublishOpts{DedupKey: "evt-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Deliveries) != 1 || !again.Deliveries[0].Duplicate || again.Deliveries[0].Job.ID != first.Deliveries[0].Job.ID {
		t.Errorf("repeated publish = %+v", again.Deliveries[0])
	}

	// The single attempt fails and the delivery is dead-lettered; replay
	// re-drives it and it succeeds.
	id := first.Deliveries[0].Job.ID
	waitForJob(t, c, id, hopper.JobStateDiscarded)
	n, err := c.ReplayDiscarded(ctx, "flaky")
	if err != nil || n != 1 {
		t.Fatalf("ReplayDiscarded = %d, %v", n, err)
	}
	job := waitForJob(t, c, id, hopper.JobStateCompleted)
	if job.Attempt != 2 {
		t.Errorf("replayed delivery = %+v", job)
	}
	// Once the earlier delivery is finalized, the key is free again.
	third, err := c.Publish(ctx, allocationCreated{ID: 1}, &hopper.PublishOpts{DedupKey: "evt-1"})
	if err != nil || third.Deliveries[0].Duplicate {
		t.Errorf("publish after finalize = %+v, %v", third, err)
	}
}

func TestOrderingKeysAreFIFOPerKey(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var (
		mu      sync.Mutex
		order   = map[string][]int{}
		running = map[string]int{}
		overlap bool
		total   int
	)
	workers := hopper.NewWorkers()
	hopper.Subscribe(workers, hopper.Subscription{Name: "ordered", Pattern: "allocation.created"}, func(_ context.Context, msg *hopper.Message[allocationCreated]) error {
		mu.Lock()
		if running[msg.OrderingKey] > 0 {
			overlap = true
		}
		running[msg.OrderingKey]++
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		running[msg.OrderingKey]--
		order[msg.OrderingKey] = append(order[msg.OrderingKey], msg.Payload.ID)
		total++
		mu.Unlock()
		return nil
	})
	// Several clients with many workers, all competing for the same keys.
	for range 3 {
		h.started(workers, 8)
	}
	inserter := h.client(&hopper.Config{})
	const keys, perKey = 4, 15
	for i := range perKey {
		for k := range keys {
			key := "alloc:" + string(rune('a'+k))
			if _, err := inserter.Publish(ctx, allocationCreated{ID: i}, &hopper.PublishOpts{OrderingKey: key}); err != nil {
				t.Fatal(err)
			}
		}
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return total == keys*perKey })
	mu.Lock()
	defer mu.Unlock()
	if overlap {
		t.Error("two deliveries of one key ran at once")
	}
	for key, got := range order {
		for i, v := range got {
			if v != i {
				t.Errorf("key %s delivered out of order: %v", key, got)
				break
			}
		}
	}
}

func TestOrderingKeyBlocksOnFailureAndUnblocksOnDeadLetter(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var (
		mu    sync.Mutex
		seen  []int
		fails int
	)
	workers := hopper.NewWorkers()
	hopper.Subscribe(workers, hopper.Subscription{Name: "ordered", Pattern: "allocation.created", MaxAttempts: 2}, func(_ context.Context, msg *hopper.Message[allocationCreated]) error {
		mu.Lock()
		defer mu.Unlock()
		if msg.Payload.ID == 1 {
			fails++
			return errors.New("poison")
		}
		seen = append(seen, msg.Payload.ID)
		return nil
	})
	c := h.started(workers, 4)
	for i := 1; i <= 3; i++ {
		if _, err := c.Publish(ctx, allocationCreated{ID: i}, &hopper.PublishOpts{OrderingKey: "k"}); err != nil {
			t.Fatal(err)
		}
	}
	// While message 1 retries (its first retry is seconds away), 2 and 3 wait.
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	if len(seen) != 0 || fails != 1 {
		t.Fatalf("seen %v, fails %d while the key is blocked", seen, fails)
	}
	mu.Unlock()
	// Bring the retry forward; it fails again, is dead-lettered, and the key
	// unblocks in order.
	if _, err := h.pool.Exec(ctx, "UPDATE hopper_jobs SET scheduled_at = now() WHERE state = 'retryable'"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) == 2 })
	mu.Lock()
	defer mu.Unlock()
	if seen[0] != 2 || seen[1] != 3 || fails != 2 {
		t.Errorf("seen %v, fails %d", seen, fails)
	}
	if h.count("SELECT count(*) FROM hopper_job_history WHERE state = 'discarded'") != 1 {
		t.Error("poison message not dead-lettered")
	}
}

func TestPublishAwaitAndSQLContract(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	hopper.Subscribe(workers, hopper.Subscription{Name: "reply", Pattern: "allocation.created"}, func(ctx context.Context, msg *hopper.Message[allocationCreated]) error {
		return hopper.SetOutput(ctx, msg.Payload.ID*2)
	})
	c := h.started(workers, 2)

	res, err := c.Publish(ctx, allocationCreated{ID: 21}, &hopper.PublishOpts{Await: true, Delay: 10 * time.Millisecond, TTL: time.Minute, Priority: hopper.PriorityHigh})
	if err != nil {
		t.Fatal(err)
	}
	if j := res.Deliveries[0].Job; j.State != hopper.JobStateScheduled || j.ExpiresAt.IsZero() || j.Priority != 1 || !j.Await {
		t.Errorf("delivery = %+v", j)
	}
	actx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := hopper.Await[int](actx, c, res.Deliveries[0].Job.ID)
	if err != nil || out != 42 {
		t.Errorf("Await = %d, %v", out, err)
	}

	// The SQL contract: publish and insert from plain SQL.
	var ids []string
	rows, err := h.pool.Query(ctx, `SELECT hopper_publish('allocation.created', '{"id": 5}', '{"ordering_key": "k", "headers": {"h": "v"}, "await": true}')::text`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) != 1 {
		t.Fatalf("hopper_publish returned %d deliveries", len(ids))
	}
	id, err := hopper.ParseJobID(ids[0])
	if err != nil {
		t.Fatal(err)
	}
	out, err = hopper.Await[int](actx, c, id)
	if err != nil || out != 10 {
		t.Errorf("Await of SQL-published delivery = %d, %v", out, err)
	}
	job, err := c.JobGet(ctx, id)
	if err != nil || job.OrderingKey != "k" || !strings.Contains(string(job.Metadata), `"h": "v"`) || job.MaxAttempts != 10 {
		t.Errorf("SQL-published delivery = %+v, %v", job, err)
	}

	var jobID string
	if err := h.pool.QueryRow(ctx, `SELECT hopper_insert('noop', '{"n": 7}', '{"queue": "q", "priority": 3, "unique_key": "u", "ttl_seconds": 60}')::text`).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	var dupID string
	if err := h.pool.QueryRow(ctx, `SELECT hopper_insert('noop', '{"n": 8}', '{"unique_key": "u"}')::text`).Scan(&dupID); err != nil {
		t.Fatal(err)
	}
	if dupID != jobID {
		t.Errorf("hopper_insert unique conflict returned %s, want %s", dupID, jobID)
	}
	inserted, err := c.JobGet(ctx, mustID(t, jobID))
	if err != nil || inserted.Queue != "q" || inserted.Priority != 3 || inserted.ExpiresAt.IsZero() || string(inserted.Args) != `{"n": 7}` {
		t.Errorf("hopper_insert job = %+v, %v", inserted, err)
	}
}

func mustID(t *testing.T, s string) hopper.JobID {
	t.Helper()
	id, err := hopper.ParseJobID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestTopicPatterns(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	cases := []struct {
		pattern, topic string
		match          bool
	}{
		{"#", "a", true},
		{"#", "a.b.c", true},
		{"a.*", "a.b", true},
		{"a.*", "a.b.c", false},
		{"a.*", "a", false},
		{"a.#", "a", true},
		{"a.#", "a.b.c", true},
		{"a.#", "ab", false},
		{"#.c", "c", true},
		{"#.c", "a.b.c", true},
		{"#.c", "a.b", false},
		{"a.#.c", "a.c", true},
		{"a.#.c", "a.x.y.c", true},
		{"a.#.c", "a.c.d", false},
		{"*.*", "a.b", true},
		{"*.*", "a", false},
		{"user.signed_up", "user.signed_up", true},
		{"user.signed_up", "user.signed", false},
		{"a.b", "a+b", false},
		{"a.b", "axb", false},
	}
	for _, tc := range cases {
		var got bool
		if err := h.pool.QueryRow(ctx, "SELECT $1 ~ hopper_topic_regex($2)", tc.topic, tc.pattern).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != tc.match {
			t.Errorf("pattern %q topic %q: match = %v, want %v", tc.pattern, tc.topic, got, tc.match)
		}
	}
}

func TestSubscribeValidation(t *testing.T) {
	t.Parallel()
	for name, sub := range map[string]hopper.Subscription{
		"empty name":    {Pattern: "a"},
		"empty pattern": {Name: "s"},
		"empty word":    {Name: "s", Pattern: "a..b"},
		"negative":      {Name: "s", Pattern: "a", MaxAttempts: -1},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: Subscribe did not panic", name)
				}
			}()
			hopper.Subscribe(hopper.NewWorkers(), sub, func(context.Context, *hopper.Message[hopper.Raw]) error { return nil })
		}()
	}
	workers := hopper.NewWorkers()
	hopper.Subscribe(workers, hopper.Subscription{Name: "s", Pattern: "a"}, func(context.Context, *hopper.Message[hopper.Raw]) error { return nil })
	defer func() {
		if recover() == nil {
			t.Error("duplicate subscription did not panic")
		}
	}()
	hopper.Subscribe(workers, hopper.Subscription{Name: "s", Pattern: "b"}, func(context.Context, *hopper.Message[hopper.Raw]) error { return nil })
}
