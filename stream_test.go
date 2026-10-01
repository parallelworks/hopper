package hopper_test

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver"
)

type orderEvent struct {
	ID int    `json:"id"`
	At string `json:"at"`
}

func (orderEvent) Topic() string { return "order.placed" }

type auditEvent struct {
	What string `json:"what"`
}

func (auditEvent) Topic() string { return "audit.note" }

func TestStreams(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	var (
		mu       sync.Mutex
		received []string // the orders consumer's deliveries, in order
		streams  []string
		audited  int
	)
	workers := hopper.NewWorkers()
	hopper.Consume(workers, hopper.Consumer{Name: "orders", Pattern: "order.*", Start: hopper.StreamStartEarliest},
		func(_ context.Context, msg *hopper.Message[orderEvent]) error {
			mu.Lock()
			defer mu.Unlock()
			received = append(received, msg.Payload.At)
			streams = append(streams, msg.Stream+"@"+msg.Position.String()+"/"+msg.OrderingKey)
			return nil
		})
	hopper.Consume(workers, hopper.Consumer{Name: "audit", Pattern: "#"},
		func(_ context.Context, msg *hopper.Message[hopper.Raw]) error {
			mu.Lock()
			defer mu.Unlock()
			audited++
			return nil
		})
	c := h.started(workers, 4)
	s := c.Streams()

	// Every event reaches both consumers, in order, through the pool and
	// through a transaction alike.
	before, err := s.Append(ctx, orderEvent{ID: 1, At: "before"}, &hopper.AppendOpts{Key: "o1"})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendTx(ctx, tx, orderEvent{ID: 2, At: "in-tx"}, &hopper.AppendOpts{Key: "o1"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(ctx, auditEvent{What: "x"}, nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(received) == 2 && audited == 3 })
	mu.Lock()
	if received[0] != "before" || received[1] != "in-tx" {
		t.Errorf("received = %v", received)
	}
	if streams[0] != "orders@"+before.Position.String()+"/o1" {
		t.Errorf("stream metadata = %v", streams)
	}
	received = nil
	mu.Unlock()

	// Read pages the log; consumers report their positions; a seek
	// replays.
	var topics []string
	for ev, err := range s.Read(ctx, "", hopper.StreamPosition{}, 2) {
		if err != nil {
			t.Fatal(err)
		}
		topics = append(topics, ev.Topic)
	}
	if len(topics) != 3 || topics[0] != "order.placed" || topics[2] != "audit.note" {
		t.Errorf("read = %v", topics)
	}
	consumers, err := s.Consumers(ctx)
	if err != nil || len(consumers) != 2 || consumers[1].Name != "orders" || consumers[1].DeliveredAt.IsZero() {
		t.Errorf("consumers = %v, %v", consumers, err)
	}
	if err := s.Seek(ctx, "orders", hopper.SeekOpts{Earliest: true}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(received) == 2 })
	mu.Lock()
	if received[0] != "before" || received[1] != "in-tx" {
		t.Errorf("replayed = %v", received)
	}
	mu.Unlock()
	if err := s.Seek(ctx, "nobody", hopper.SeekOpts{Latest: true}); err == nil {
		t.Error("seek of an unknown consumer succeeded")
	}
	if _, err := s.Append(ctx, nil, nil); err == nil {
		t.Error("nil event accepted")
	}
}

// Events are ordered by transaction ID as a number. The IDs are written
// here by hand to straddle a power of ten, where the order of their text
// forms is the opposite one.
func TestStreamOrderAcrossTransactionIDDigits(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	exec := h.d.Executor()
	// These transaction IDs are older than any snapshot's xmin, so every
	// snapshot but the earliest sees them as committed.
	if _, err := h.pool.Exec(ctx, `INSERT INTO hopper_stream_events (xid, topic) VALUES ('9', 't.a'), ('10', 't.b'), ('11', 't.c')`); err != nil {
		t.Fatal(err)
	}
	xids := func(events []*driver.StreamEvent) []uint64 {
		out := make([]uint64, len(events))
		for i, e := range events {
			out[i] = e.Position.Xid
		}
		return out
	}

	events, err := exec.StreamRead(ctx, driver.StreamReadParams{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if got := xids(events); !slices.Equal(got, []uint64{9, 10, 11}) {
		t.Errorf("read order = %v, want [9 10 11]", got)
	}

	// A pump that stops part way keeps the last event it delivered as its
	// cursor, and the next one continues after it.
	err = exec.StreamConsumerUpsert(ctx, []driver.StreamConsumerRow{
		{Name: "digits", Pattern: "#", Kind: "stream:digits", Queue: "digits", Start: driver.StreamStartEarliest},
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := exec.StreamPump(ctx, "digits", 2)
	if err != nil {
		t.Fatal(err)
	}
	if res.Delivered != 2 || res.Position.Xid != 10 {
		t.Errorf("pump of two = %+v, want 2 delivered up to transaction 10", res)
	}
	res, err = exec.StreamPump(ctx, "digits", 2)
	if err != nil {
		t.Fatal(err)
	}
	if res.Delivered != 1 || res.Position.Xid != 11 {
		t.Errorf("pump of the rest = %+v, want 1 delivered up to transaction 11", res)
	}

	// A seek to a time starts at the first event at or after it.
	if err := exec.StreamConsumerSeek(ctx, "digits", driver.StreamSeekParams{Time: events[0].CreatedAt}); err != nil {
		t.Fatal(err)
	}
	res, err = exec.StreamPump(ctx, "digits", 10)
	if err != nil {
		t.Fatal(err)
	}
	if res.Delivered != 3 {
		t.Errorf("pump after seek to time = %+v, want 3 delivered", res)
	}
}
