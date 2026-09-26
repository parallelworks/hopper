package drivertest

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

func testSubscriptions[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	exec := f.NewDriver(t).Executor()
	if err := exec.SubscriptionUpsert(ctx, nil); err != nil {
		t.Fatal(err)
	}
	subs := []driver.SubscriptionRow{
		{Name: "billing", Pattern: "allocation.*", Kind: "sub:billing", Queue: "billing", MaxAttempts: 3, Metadata: json.RawMessage(`{"team":"bill"}`)},
		{Name: "audit", Pattern: "#", Kind: "sub:audit", Queue: "default"},
	}
	if err := exec.SubscriptionUpsert(ctx, subs); err != nil {
		t.Fatal(err)
	}
	// Upsert by name updates the settings.
	subs[1].Pattern = "allocation.#"
	if err := exec.SubscriptionUpsert(ctx, subs[1:]); err != nil {
		t.Fatal(err)
	}
	got, err := exec.SubscriptionList(ctx)
	if err != nil || len(got) != 2 {
		t.Fatalf("list = %v, %v", got, err)
	}
	if got[0].Name != "audit" || got[0].Pattern != "allocation.#" || got[0].MaxAttempts != 0 || got[0].CreatedAt.IsZero() {
		t.Errorf("audit = %+v", got[0])
	}
	if got[1].Name != "billing" || got[1].Queue != "billing" || got[1].MaxAttempts != 3 || !strings.Contains(string(got[1].Metadata), "bill") {
		t.Errorf("billing = %+v", got[1])
	}
}

func testPublish[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	if err := exec.SubscriptionUpsert(ctx, []driver.SubscriptionRow{
		{Name: "billing", Pattern: "allocation.*", Kind: "sub:billing", Queue: "billing", MaxAttempts: 3, Metadata: json.RawMessage(`{"team":"bill"}`)},
		{Name: "audit", Pattern: "#", Kind: "sub:audit", Queue: "default"},
		{Name: "users", Pattern: "user.*", Kind: "sub:users", Queue: "default"},
	}); err != nil {
		t.Fatal(err)
	}
	res, err := exec.MessagePublish(ctx, driver.MessagePublishParams{
		Topic: "allocation.created", Payload: json.RawMessage(`{"id":42}`), Headers: map[string]string{"h": "v"},
		OrderingKey: "alloc:42", DedupKey: "evt-1", Priority: 1, TTL: time.Hour, Await: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Fatalf("deliveries = %d, want billing and audit", len(res))
	}
	var messageID string
	for _, r := range res {
		j := r.Job
		var meta struct {
			Topic     string            `json:"topic"`
			MessageID string            `json:"message_id"`
			Headers   map[string]string `json:"headers"`
			Team      string            `json:"team"`
		}
		if err := json.Unmarshal(j.Metadata, &meta); err != nil {
			t.Fatal(err)
		}
		if r.Duplicate || meta.Topic != "allocation.created" || meta.MessageID == "" || meta.Headers["h"] != "v" {
			t.Errorf("delivery = %+v meta %+v", j, meta)
		}
		if messageID == "" {
			messageID = meta.MessageID
		} else if meta.MessageID != messageID {
			t.Error("deliveries of one message have different IDs")
		}
		if j.OrderingKey != "alloc:42" || j.UniqueKey != "msg:evt-1" || j.Priority != 1 || j.ExpiresAt.IsZero() || !j.Await || string(j.Args) != `{"id": 42}` {
			t.Errorf("delivery = %+v", j)
		}
		switch j.Kind {
		case "sub:billing":
			if j.Queue != "billing" || j.MaxAttempts != 3 || meta.Team != "bill" {
				t.Errorf("billing delivery = %+v meta %+v", j, meta)
			}
		case "sub:audit":
			if j.Queue != "default" || j.MaxAttempts != 10 { // ordered: 10 by default
				t.Errorf("audit delivery = %+v", j)
			}
		default:
			t.Errorf("unexpected delivery kind %s", j.Kind)
		}
	}
	// Repeating the publish with the same dedup key is a no-op.
	again, err := exec.MessagePublish(ctx, driver.MessagePublishParams{Topic: "allocation.created", Payload: json.RawMessage(`{}`), DedupKey: "evt-1"})
	if err != nil || len(again) != 2 || !again[0].Duplicate || !again[1].Duplicate {
		t.Errorf("repeated publish = %v, %v", again, err)
	}
	// Unordered deliveries default to 25 attempts; a delay schedules them.
	users, err := exec.MessagePublish(ctx, driver.MessagePublishParams{Topic: "user.signed_up", Payload: json.RawMessage(`{}`), Delay: time.Hour})
	if err != nil || len(users) != 2 {
		t.Fatalf("user publish = %v, %v", users, err)
	}
	for _, r := range users {
		if r.Job.State != driver.JobStateScheduled || (r.Job.Kind == "sub:users" && r.Job.MaxAttempts != 25) {
			t.Errorf("delayed delivery = %+v", r.Job)
		}
	}
	// The catch-all matches everything; narrow it and a topic with no
	// subscription inserts nothing.
	catchAll, err := exec.MessagePublish(ctx, driver.MessagePublishParams{Topic: "nothing.here"})
	if err != nil || len(catchAll) != 1 {
		t.Errorf("publish to the catch-all = %v, %v", catchAll, err)
	}
	if err := exec.SubscriptionUpsert(ctx, []driver.SubscriptionRow{{Name: "audit", Pattern: "allocation.#", Kind: "sub:audit", Queue: "default"}}); err != nil {
		t.Fatal(err)
	}
	none, err := exec.MessagePublish(ctx, driver.MessagePublishParams{Topic: "nothing.here"})
	if err != nil || len(none) != 0 {
		t.Errorf("publish with no match = %v, %v", none, err)
	}
	if n := f.CountLive(ctx, t, d); n != 5 {
		t.Errorf("live = %d", n)
	}
}

func testOrderingClaim[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	p := params("k", 6)
	p[0].OrderingKey, p[1].OrderingKey, p[2].OrderingKey = "a", "a", "a"
	p[3].OrderingKey, p[4].OrderingKey = "b", "b"
	inserted := insert(ctx, t, exec, p)
	clientID := register(ctx, t, exec)

	// One per key plus the unkeyed job, oldest of each key first.
	jobs := claim(ctx, t, exec, clientID, 10)
	if len(jobs) != 3 {
		t.Fatalf("claimed %d, want a, b and the unkeyed job", len(jobs))
	}
	keyed := map[string]int{}
	for _, j := range jobs {
		if j.OrderingKey != "" {
			keyed[j.OrderingKey] = argsN(j)
		}
	}
	if keyed["a"] != 0 || keyed["b"] != 3 {
		t.Errorf("claimed per key = %v", keyed)
	}
	// While a key's job runs, nothing else of that key is claimable, on
	// this or another client.
	other := register(ctx, t, exec)
	if got := claim(ctx, t, exec, other, 10); len(got) != 0 {
		t.Errorf("claimed %v while keys were running", got)
	}
	// Finishing a key's job frees the next one; a retry with a delay keeps
	// the key blocked until it is due.
	finalize(ctx, t, exec,
		driver.JobFinalize{ID: inserted[0].Job.ID, AttemptedBy: clientID, State: driver.JobStateCompleted, Archive: true},
		driver.JobFinalize{ID: inserted[3].Job.ID, AttemptedBy: clientID, State: driver.JobStateRetryable, Delay: time.Hour, Error: &driver.AttemptError{Attempt: 1, Error: "x"}},
	)
	got := claim(ctx, t, exec, other, 10)
	if len(got) != 1 || got[0].ID != inserted[1].Job.ID {
		t.Errorf("after finishing a's first job: claimed %v", got)
	}
	f.MakeClaimable(ctx, t, d, inserted[3].Job.ID)
	got = claim(ctx, t, exec, other, 10)
	if len(got) != 1 || got[0].ID != inserted[3].Job.ID {
		t.Errorf("after the retry came due: claimed %v", got)
	}
}
