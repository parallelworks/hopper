package hopper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strings"
	"time"
	"unicode"

	"github.com/parallelworks/hopper/driver"
)

// StreamEvent is one event of the stream log.
type StreamEvent = driver.StreamEvent

// StreamPosition is a place in the stream log.
type StreamPosition = driver.StreamPosition

// ParseStreamPosition parses a position's String form ("xid:seq").
func ParseStreamPosition(s string) (StreamPosition, error) { return driver.ParseStreamPosition(s) }

// StreamStart says where a new consumer starts.
type StreamStart = driver.StreamStart

const (
	// StreamStartLatest delivers events appended from now on. It is the
	// default.
	StreamStartLatest = driver.StreamStartLatest
	// StreamStartEarliest delivers every retained event first.
	StreamStartEarliest = driver.StreamStartEarliest
)

// Consumer reads the stream log from a position of its own and delivers
// the events whose topic matches Pattern to a queue, as jobs of kind
// "stream:<Name>". Unlike a Subscription, which is fanned out to at publish
// time, a consumer can start anywhere in the retained log and be moved
// (Streams.Seek), so late subscribers and replays see past events.
type Consumer struct {
	// Name identifies the consumer; the delivery job kind is derived from
	// it.
	Name    string
	Pattern string
	// Queue is where deliveries go. Defaults to QueueDefault.
	Queue string
	// Start applies when the consumer is created. Defaults to
	// StreamStartLatest.
	Start StreamStart
	// MaxAttempts is the retry budget of deliveries. Zero means 25, or 10
	// for events with a key, whose retries block their key.
	MaxAttempts int
	// Metadata is merged into every delivery's metadata.
	Metadata json.RawMessage
}

// Kind returns the job kind of the consumer's deliveries.
func (c Consumer) Kind() string { return "stream:" + c.Name }

func (c *Consumer) validate() error {
	if c.Name == "" || strings.ContainsFunc(c.Name, unicode.IsSpace) {
		return fmt.Errorf("hopper: consumer name %q must be a non-empty word", c.Name)
	}
	if c.Pattern == "" {
		return fmt.Errorf("hopper: consumer %q has no pattern", c.Name)
	}
	for word := range strings.SplitSeq(c.Pattern, ".") {
		if word == "" {
			return fmt.Errorf("hopper: consumer %q: pattern %q has an empty word", c.Name, c.Pattern)
		}
	}
	if c.Queue == "" {
		c.Queue = QueueDefault
	}
	switch c.Start {
	case "":
		c.Start = StreamStartLatest
	case StreamStartLatest, StreamStartEarliest:
	default:
		return fmt.Errorf("hopper: consumer %q: unknown start %q", c.Name, c.Start)
	}
	if c.MaxAttempts < 0 {
		return fmt.Errorf("hopper: consumer %q: MaxAttempts must not be negative", c.Name)
	}
	if len(c.Metadata) > 0 && !json.Valid(c.Metadata) {
		return fmt.Errorf("hopper: consumer %q: Metadata is not valid JSON", c.Name)
	}
	return nil
}

// Consume registers a handler for a consumer's deliveries. The consumer is
// created (at its Start) or updated when a client using workers starts,
// and the leader delivers its events in order as jobs; fn receives them as
// Subscribe's handler does, with Message.Stream and Message.Position set.
// An event's key becomes the delivery's ordering key, so events with the
// same key are handled one at a time, in order.
//
// It panics on an invalid consumer or a name registered twice, like
// AddWorker.
func Consume[T any](workers *Workers, consumer Consumer, fn func(ctx context.Context, msg *Message[T]) error) {
	if fn == nil {
		panic("hopper: Consume called with a nil function")
	}
	if err := consumer.validate(); err != nil {
		panic(err)
	}
	workers.add(&workerInfo{
		kind: consumer.Kind(),
		newUnit: func(row *JobRow, codec Codec) (workUnit, error) {
			msg, err := decodeMessage[T](row, codec)
			if err != nil {
				return nil, err
			}
			return &messageUnit[T]{fn: fn, msg: msg}, nil
		},
	})
	workers.mu.Lock()
	workers.consumers = append(workers.consumers, consumer)
	workers.mu.Unlock()
}

// consumerRows returns the registered consumers as rows.
func (w *Workers) consumerRows() []driver.StreamConsumerRow {
	w.mu.RLock()
	defer w.mu.RUnlock()
	rows := make([]driver.StreamConsumerRow, len(w.consumers))
	for i, c := range w.consumers {
		rows[i] = driver.StreamConsumerRow{Name: c.Name, Pattern: c.Pattern, Kind: c.Kind(), Queue: c.Queue, Start: c.Start, MaxAttempts: c.MaxAttempts, Metadata: c.Metadata}
	}
	return rows
}

// AppendOpts customizes an append. A nil *AppendOpts means the defaults.
type AppendOpts struct {
	// Key orders the event's deliveries with others of the same key: a
	// consumer handles them one at a time, in order.
	Key string
	// Headers travel with the event and are available on Message.
	Headers map[string]string
}

// Streams is the stream log: an append-only, retained record of events
// that consumers read from positions of their own.
type Streams[TTx any] struct {
	c *Client[TTx]
}

// Streams returns the client's stream log.
func (c *Client[TTx]) Streams() *Streams[TTx] {
	return &Streams[TTx]{c: c}
}

// Append appends an event to the log. Consumers whose pattern matches its
// topic deliver it, in order, after everything appended before it.
func (s *Streams[TTx]) Append(ctx context.Context, event Topic, opts *AppendOpts) (*StreamEvent, error) {
	return s.append(ctx, s.c.exec, event, opts, false)
}

// AppendTx is Append in the caller's transaction: the event exists only if
// tx commits, and is delivered after events of every transaction that
// started before tx, however they commit.
func (s *Streams[TTx]) AppendTx(ctx context.Context, tx TTx, event Topic, opts *AppendOpts) (*StreamEvent, error) {
	return s.append(ctx, s.c.driver.UnwrapTx(tx), event, opts, true)
}

func (s *Streams[TTx]) append(ctx context.Context, exec driver.Executor, event Topic, opts *AppendOpts, inTx bool) (*StreamEvent, error) {
	if event == nil {
		return nil, errors.New("hopper: event must not be nil")
	}
	topic := event.Topic()
	if topic == "" || strings.ContainsFunc(topic, unicode.IsSpace) {
		return nil, fmt.Errorf("hopper: topic %q must be a non-empty word", topic)
	}
	if opts == nil {
		opts = &AppendOpts{}
	}
	payload, err := s.c.cfg.Codec.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("hopper: encode event for %q: %w", topic, err)
	}
	ev, err := exec.StreamAppend(ctx, driver.StreamAppendParams{Topic: topic, Key: opts.Key, Payload: payload, Headers: opts.Headers, Notify: inTx})
	if err != nil {
		return nil, err
	}
	if !inTx {
		s.c.pokeStreams()
		s.c.notifier.markStream(topic)
	}
	return ev, nil
}

// Read returns the events after a position whose topic matches pattern
// (empty for every topic), oldest first, in pages of limit. It sees only
// events that every earlier transaction has finished around, as consumers
// do, so paging by the last position never skips an event.
func (s *Streams[TTx]) Read(ctx context.Context, pattern string, after StreamPosition, limit int) iter.Seq2[*StreamEvent, error] {
	if limit <= 0 {
		limit = 100
	}
	return func(yield func(*StreamEvent, error) bool) {
		pos := after
		for {
			events, err := s.c.exec.StreamRead(ctx, driver.StreamReadParams{Pattern: pattern, After: pos, Limit: limit})
			if err != nil {
				yield(nil, err)
				return
			}
			for _, ev := range events {
				if !yield(ev, nil) {
					return
				}
				pos = ev.Position
			}
			if len(events) < limit {
				return
			}
		}
	}
}

// ConsumerRow is a consumer as recorded in the database, with its
// position.
type ConsumerRow = driver.StreamConsumerRow

// Consumers lists every consumer known to the cluster.
func (s *Streams[TTx]) Consumers(ctx context.Context) ([]*ConsumerRow, error) {
	return s.c.exec.StreamConsumerList(ctx)
}

// SeekOpts says where to move a consumer. Exactly one field applies, in
// this order of precedence.
type SeekOpts struct {
	// Position makes the event after it the next delivered.
	Position StreamPosition
	// Time replays from the first event appended at or after it.
	Time time.Time
	// Earliest replays every retained event.
	Earliest bool
	// Latest skips to the events appended from now on.
	Latest bool
}

// Seek moves a consumer's position; the events after it are delivered
// (again) from the next pump. Deliveries already inserted are unaffected.
func (s *Streams[TTx]) Seek(ctx context.Context, consumer string, opts SeekOpts) error {
	if err := s.c.exec.StreamConsumerSeek(ctx, consumer, driver.StreamSeekParams(opts)); err != nil {
		return err
	}
	s.c.pokeStreams()
	if err := s.c.exec.Notify(ctx, driver.ChannelStream, []string{""}); err != nil {
		s.c.logger.WarnContext(ctx, "hopper: notify stream seek", "error", err)
	}
	return nil
}

// pokeStreams asks the leader's stream loop to pump now.
func (c *Client[TTx]) pokeStreams() {
	select {
	case c.streamPoke <- struct{}{}:
	default:
	}
}

// streamLoop runs on the leader: it pumps every consumer when events are
// appended (a notification, or a local append) and every poll interval
// regardless, since a client without a listener cannot be told.
func (c *Client[TTx]) streamLoop(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()
	for {
		c.pumpStreams(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-c.streamPoke:
		}
	}
}

// pumpStreams delivers the pending events of every consumer.
func (c *Client[TTx]) pumpStreams(ctx context.Context) {
	consumers, err := c.exec.StreamConsumerList(ctx)
	if err != nil {
		if ctx.Err() == nil {
			c.logger.WarnContext(ctx, "hopper: list stream consumers", "error", err)
		}
		return
	}
	limit := max(c.tuning.streamBatch, 1)
	for _, consumer := range consumers {
		for {
			res, err := c.exec.StreamPump(ctx, consumer.Name, limit)
			if err != nil {
				if ctx.Err() == nil && !errors.Is(err, ErrNotFound) {
					c.logger.ErrorContext(ctx, "hopper: pump stream consumer", "consumer", consumer.Name, "error", err)
				}
				break
			}
			for _, q := range res.Queues {
				c.wakeQueue(q)
			}
			if res.Delivered < limit {
				break
			}
		}
	}
}
