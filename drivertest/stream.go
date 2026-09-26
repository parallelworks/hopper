package drivertest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/hopper/driver"
)

func testStreams[TTx any](t *testing.T, f Fixture[TTx]) {
	ctx := context.Background()
	d := f.NewDriver(t)
	exec := d.Executor()
	appendEvent := func(topic, key string, i int) *driver.StreamEvent {
		t.Helper()
		ev, err := exec.StreamAppend(ctx, driver.StreamAppendParams{
			Topic: topic, Key: key, Payload: json.RawMessage(fmt.Sprintf(`{"i":%d}`, i)),
			Headers: map[string]string{"n": fmt.Sprint(i)},
		})
		if err != nil {
			t.Fatal(err)
		}
		return ev
	}
	read := func(pattern string, after driver.StreamPosition) []*driver.StreamEvent {
		t.Helper()
		events, err := exec.StreamRead(ctx, driver.StreamReadParams{Pattern: pattern, After: after, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		return events
	}
	pump := func(name string) driver.StreamPumpResult {
		t.Helper()
		res, err := exec.StreamPump(ctx, name, 100)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	deliveries := func(queue string) []*driver.JobRow {
		t.Helper()
		jobs, err := exec.JobList(ctx, driver.JobListParams{Queue: queue, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		return jobs
	}

	// Appends are ordered and readable by pattern.
	e1 := appendEvent("order.created", "o1", 1)
	e2 := appendEvent("order.paid", "o1", 2)
	e3 := appendEvent("user.created", "", 3)
	if e1.Position.IsZero() || !e1.Position.Less(e2.Position) || !e2.Position.Less(e3.Position) {
		t.Errorf("positions not ascending: %v %v %v", e1.Position, e2.Position, e3.Position)
	}
	if e1.Key != "o1" || e1.Headers["n"] != "1" || e1.MessageID == "" || e1.CreatedAt.IsZero() || string(e1.Payload) != `{"i": 1}` {
		t.Errorf("event = %+v", e1)
	}
	if got := read("", driver.StreamPosition{}); len(got) != 3 || got[0].Position != e1.Position || got[2].Position != e3.Position {
		t.Errorf("read all = %v", got)
	}
	if got := read("order.*", driver.StreamPosition{}); len(got) != 2 || got[1].Topic != "order.paid" {
		t.Errorf("read order.* = %v", got)
	}
	if got := read("user.created", e1.Position); len(got) != 1 || got[0].Position != e3.Position {
		t.Errorf("read after = %v", got)
	}
	if got := read("", e3.Position); len(got) != 0 {
		t.Errorf("read past the end = %v", got)
	}

	// A consumer created at the earliest position delivers everything so
	// far, once, as jobs carrying the event; one created at latest starts
	// with what comes next.
	err := exec.StreamConsumerUpsert(ctx, []driver.StreamConsumerRow{
		{Name: "orders", Pattern: "order.#", Kind: "stream:orders", Queue: "orders", Start: driver.StreamStartEarliest, Metadata: json.RawMessage(`{"c":1}`)},
		{Name: "audit", Pattern: "#", Kind: "stream:audit", Queue: "audit", Start: driver.StreamStartLatest, MaxAttempts: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	consumers, err := exec.StreamConsumerList(ctx)
	if err != nil || len(consumers) != 2 || consumers[0].Name != "audit" || consumers[1].Name != "orders" {
		t.Fatalf("consumers = %v, %v", consumers, err)
	}
	if c := consumers[1]; !c.Position.IsZero() || c.Queue != "orders" || string(c.Metadata) != `{"c": 1}` || c.CreatedAt.IsZero() || c.Snapshot == "" {
		t.Errorf("orders consumer = %+v", c)
	}
	if c := consumers[0]; c.MaxAttempts != 3 || c.Snapshot == consumers[1].Snapshot {
		t.Errorf("audit consumer = %+v", c)
	}
	res := pump("orders")
	if res.Delivered != 2 || res.Position != e2.Position || len(res.Queues) != 1 || res.Queues[0] != "orders" {
		t.Errorf("first pump = %+v", res)
	}
	jobs := deliveries("orders")
	if len(jobs) != 2 {
		t.Fatalf("deliveries = %v", jobs)
	}
	j := jobs[0]
	if argsN(j) != 1 {
		j = jobs[1]
	}
	if j.Kind != "stream:orders" || j.OrderingKey != "o1" || j.MaxAttempts != 10 || string(j.Args) != `{"i": 1}` {
		t.Errorf("delivery = %+v", j)
	}
	var meta struct {
		Topic     string            `json:"topic"`
		MessageID string            `json:"message_id"`
		Stream    string            `json:"stream"`
		Position  string            `json:"position"`
		Headers   map[string]string `json:"headers"`
		C         int               `json:"c"`
	}
	if err := json.Unmarshal(j.Metadata, &meta); err != nil || meta.Topic != "order.created" || meta.MessageID != e1.MessageID ||
		meta.Stream != "orders" || meta.Position != e1.Position.String() || meta.Headers["n"] != "1" || meta.C != 1 {
		t.Errorf("delivery metadata = %s (%v)", j.Metadata, err)
	}
	if res := pump("orders"); res.Delivered != 0 {
		t.Errorf("second pump = %+v", res)
	}
	if res := pump("audit"); res.Delivered != 0 {
		t.Errorf("latest consumer delivered old events: %+v", res)
	}
	if c, _ := exec.StreamConsumerList(ctx); c[1].DeliveredAt.IsZero() || !c[0].DeliveredAt.IsZero() {
		t.Errorf("delivered_at: orders %v audit %v", c[1].DeliveredAt, c[0].DeliveredAt)
	}

	// New events reach both, and a pump of a small batch continues where
	// it stopped.
	e4 := appendEvent("user.deleted", "", 4)
	e5 := appendEvent("order.shipped", "o1", 5)
	if res := pump("orders"); res.Delivered != 1 || res.Position != e5.Position {
		t.Errorf("pump after appends = %+v", res)
	}
	if res, err := exec.StreamPump(ctx, "audit", 1); err != nil || res.Delivered != 1 || res.Position != e4.Position {
		t.Errorf("pump of one = %+v, %v", res, err)
	}
	if res, err := exec.StreamPump(ctx, "audit", 1); err != nil || res.Delivered != 1 || res.Position != e5.Position {
		t.Errorf("pump of the next one = %+v, %v", res, err)
	}
	if res := pump("audit"); res.Delivered != 0 {
		t.Errorf("audit pump after the delta = %+v", res)
	}
	if jobs := deliveries("audit"); len(jobs) != 2 || jobs[0].MaxAttempts != 3 || jobs[0].OrderingKey != "" {
		t.Errorf("audit deliveries = %v", jobs)
	}

	// Seeking replays: to a position, to a time, to the start, to now.
	seek := func(params driver.StreamSeekParams) {
		t.Helper()
		if err := exec.StreamConsumerSeek(ctx, "orders", params); err != nil {
			t.Fatal(err)
		}
	}
	seek(driver.StreamSeekParams{Position: e2.Position})
	if res := pump("orders"); res.Delivered != 1 || res.Position != e5.Position {
		t.Errorf("pump after seek to position = %+v", res)
	}
	seek(driver.StreamSeekParams{Earliest: true})
	if res := pump("orders"); res.Delivered != 3 {
		t.Errorf("pump after seek to earliest = %+v", res)
	}
	seek(driver.StreamSeekParams{Time: e4.CreatedAt})
	if res := pump("orders"); res.Delivered != 1 || res.Position != e5.Position {
		t.Errorf("pump after seek to time = %+v", res)
	}
	seek(driver.StreamSeekParams{Earliest: true})
	seek(driver.StreamSeekParams{Latest: true})
	if res := pump("orders"); res.Delivered != 0 {
		t.Errorf("pump after seek to latest = %+v", res)
	}
	if err := exec.StreamConsumerSeek(ctx, "nobody", driver.StreamSeekParams{Earliest: true}); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("seek unknown = %v", err)
	}
	if _, err := exec.StreamPump(ctx, "nobody", 10); !errors.Is(err, driver.ErrNotFound) {
		t.Errorf("pump unknown = %v", err)
	}

	// A transaction that commits late is delivered when it commits, and
	// does not hold up the events of younger transactions meanwhile.
	tx, commit, rollback := f.Begin(ctx, t, d)
	defer rollback() //nolint:errcheck // already committed on the happy path
	old, err := d.UnwrapTx(tx).StreamAppend(ctx, driver.StreamAppendParams{Topic: "order.held", Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	young := appendEvent("order.after", "", 6)
	if !old.Position.Less(young.Position) {
		t.Fatalf("older transaction sorts after the younger one: %v %v", old.Position, young.Position)
	}
	if res := pump("orders"); res.Delivered != 1 || res.Position != young.Position {
		t.Errorf("pump with a transaction open = %+v", res)
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	if res := pump("orders"); res.Delivered != 1 || res.Position != old.Position {
		t.Errorf("pump after the late commit = %+v", res)
	}
	if got := read("", e5.Position); len(got) != 2 || got[0].Position != old.Position || got[1].Position != young.Position {
		t.Errorf("read after commit = %v", got)
	}
	if res := pump("orders"); res.Delivered != 0 {
		t.Errorf("late commit delivered twice: %+v", res)
	}

	// Retention keeps a partition ahead and drops nothing young.
	m, err := exec.HistoryMaintain(ctx, driver.HistoryMaintainParams{CompletedRetention: time.Hour, FailedRetention: time.Hour, StreamRetention: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if !containsPrefix(m.Created, "hopper_stream_events_") {
		t.Errorf("no stream partition created: %v", m.Created)
	}
	if got := read("", driver.StreamPosition{}); len(got) != 7 {
		t.Errorf("events after maintenance = %d, want 7", len(got))
	}
}

func containsPrefix(names []string, prefix string) bool {
	for _, n := range names {
		if strings.HasPrefix(n, prefix) {
			return true
		}
	}
	return false
}
