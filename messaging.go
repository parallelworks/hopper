package hopper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/parallelworks/hopper/driver"
)

// Topic is implemented by message payloads: Topic names the topic the
// message is published to, in dot-separated words ("allocation.created").
type Topic interface {
	Topic() string
}

// Raw is a message payload left undecoded, for a subscription that matches
// several topics with different payload types; dispatch on Message.Topic.
type Raw = json.RawMessage

// Message is one delivery of a published message to a subscription.
type Message[T any] struct {
	*JobRow
	Payload T
	Topic   string
	// ID identifies the published message; every subscription's delivery of
	// it shares the ID.
	ID          string
	Headers     map[string]string
	OrderingKey string
}

// Subscription routes messages whose topic matches Pattern to a queue, as
// jobs of kind "sub:<Name>".
//
// Pattern uses AMQP topic syntax: dot-separated words, where "*" matches one
// word and "#" zero or more. "#" alone receives everything, and a literal
// topic exactly that topic. Fan-out happens at publish time, so a
// subscription created later does not receive earlier messages.
type Subscription struct {
	// Name identifies the subscription; the delivery job kind is derived
	// from it.
	Name    string
	Pattern string
	// Queue is where deliveries go. Defaults to QueueDefault.
	Queue string
	// MaxAttempts is the retry budget of deliveries. Zero means 25, or 10
	// for deliveries with an ordering key, whose retries block their key.
	MaxAttempts int
	// Metadata is merged into every delivery's metadata.
	Metadata json.RawMessage
}

// Kind returns the job kind of the subscription's deliveries.
func (s Subscription) Kind() string { return "sub:" + s.Name }

func (s *Subscription) validate() error {
	if s.Name == "" || strings.ContainsFunc(s.Name, unicode.IsSpace) {
		return fmt.Errorf("hopper: subscription name %q must be a non-empty word", s.Name)
	}
	if s.Pattern == "" {
		return fmt.Errorf("hopper: subscription %q has no pattern", s.Name)
	}
	for word := range strings.SplitSeq(s.Pattern, ".") {
		if word == "" {
			return fmt.Errorf("hopper: subscription %q: pattern %q has an empty word", s.Name, s.Pattern)
		}
	}
	if s.Queue == "" {
		s.Queue = QueueDefault
	}
	if s.MaxAttempts < 0 {
		return fmt.Errorf("hopper: subscription %q: MaxAttempts must not be negative", s.Name)
	}
	if len(s.Metadata) > 0 && !json.Valid(s.Metadata) {
		return fmt.Errorf("hopper: subscription %q: Metadata is not valid JSON", s.Name)
	}
	return nil
}

// Subscribe registers a consumer for a subscription. The subscription row
// is created or updated when a client using workers starts. fn is called
// with each delivery: returning nil acks it, an error nacks it with backoff
// (capped at five minutes for ordered deliveries), and Snooze and Cancel
// work as for jobs. Deliveries out of attempts are dead-lettered in history,
// where ReplayDiscarded can re-drive them.
//
// It panics on an invalid subscription or a name registered twice, like
// AddWorker.
func Subscribe[T any](workers *Workers, sub Subscription, fn func(ctx context.Context, msg *Message[T]) error) {
	if fn == nil {
		panic("hopper: Subscribe called with a nil function")
	}
	if err := sub.validate(); err != nil {
		panic(err)
	}
	workers.add(&workerInfo{
		kind: sub.Kind(),
		newUnit: func(row *JobRow, codec Codec) (workUnit, error) {
			msg, err := decodeMessage[T](row, codec)
			if err != nil {
				return nil, err
			}
			return &messageUnit[T]{fn: fn, msg: msg}, nil
		},
	})
	workers.mu.Lock()
	workers.subscriptions = append(workers.subscriptions, sub)
	workers.mu.Unlock()
}

// messageMetadata is the part of a delivery's metadata hopper writes.
type messageMetadata struct {
	Topic     string            `json:"topic"`
	MessageID string            `json:"message_id"`
	Headers   map[string]string `json:"headers"`
}

func decodeMessage[T any](row *JobRow, codec Codec) (*Message[T], error) {
	var meta messageMetadata
	if err := json.Unmarshal(row.Metadata, &meta); err != nil {
		return nil, fmt.Errorf("hopper: decode message metadata: %w", err)
	}
	msg := &Message[T]{JobRow: row, Topic: meta.Topic, ID: meta.MessageID, Headers: meta.Headers, OrderingKey: row.OrderingKey}
	if err := codec.Unmarshal(row.Args, &msg.Payload); err != nil {
		return nil, fmt.Errorf("hopper: decode message payload: %w", err)
	}
	return msg, nil
}

// orderedBackoffCap bounds retry delays of ordered deliveries, so a poison
// message is dead-lettered within about an hour instead of blocking its key
// for days.
const orderedBackoffCap = 5 * time.Minute

type messageUnit[T any] struct {
	fn  func(ctx context.Context, msg *Message[T]) error
	msg *Message[T]
}

func (u *messageUnit[T]) Work(ctx context.Context) error { return u.fn(ctx, u.msg) }
func (u *messageUnit[T]) Timeout() time.Duration         { return 0 }

func (u *messageUnit[T]) NextRetry() time.Time {
	if u.msg.OrderingKey == "" {
		return time.Time{}
	}
	return time.Now().Add(min(defaultBackoff(u.msg.Attempt), orderedBackoffCap))
}

// PublishOpts customizes a publish. A nil *PublishOpts means the defaults.
type PublishOpts struct {
	// OrderingKey makes delivery FIFO per key, per queue: at most one
	// delivery with the key runs at a time, oldest first, and a failing
	// delivery blocks its key while it retries. Keys are independent.
	OrderingKey string
	// DedupKey makes the publish idempotent: while a delivery with the key
	// is live, a repeated publish inserts nothing and reports Duplicate.
	DedupKey string
	// Headers travel with the message and are available on Message.
	Headers map[string]string
	// Delay defers delivery.
	Delay time.Duration
	// TTL discards deliveries not started within this long.
	TTL time.Duration
	// Priority defaults to PriorityNormal.
	Priority Priority
	// Await announces each delivery's finalize, for Await.
	Await bool
}

// PublishResult reports the deliveries a publish produced, one per matching
// subscription.
type PublishResult struct {
	// MessageID identifies the message across its deliveries. It is empty
	// when no subscription matched.
	MessageID  string
	Deliveries []*InsertResult
}

// Publish delivers a message to every subscription matching its topic, as
// one job each, in one statement. With no matching subscription nothing is
// inserted, as with a broker.
func (c *Client[TTx]) Publish(ctx context.Context, msg Topic, opts *PublishOpts) (*PublishResult, error) {
	return c.publish(ctx, c.exec, msg, opts, false)
}

// PublishTx is Publish in the caller's transaction: the deliveries exist
// only if tx commits.
func (c *Client[TTx]) PublishTx(ctx context.Context, tx TTx, msg Topic, opts *PublishOpts) (*PublishResult, error) {
	return c.publish(ctx, c.driver.UnwrapTx(tx), msg, opts, true)
}

func (c *Client[TTx]) publish(ctx context.Context, exec driver.Executor, msg Topic, opts *PublishOpts, inTx bool) (*PublishResult, error) {
	if msg == nil {
		return nil, errors.New("hopper: message must not be nil")
	}
	topic := msg.Topic()
	if topic == "" || strings.ContainsFunc(topic, unicode.IsSpace) {
		return nil, fmt.Errorf("hopper: topic %q must be a non-empty word", topic)
	}
	if opts == nil {
		opts = &PublishOpts{}
	}
	if opts.Priority == 0 {
		opts.Priority = PriorityNormal
	}
	if opts.Priority < PriorityHigh || opts.Priority > PriorityLowest {
		return nil, fmt.Errorf("hopper: priority %d is out of range 1..4", opts.Priority)
	}
	if opts.Delay < 0 || opts.TTL < 0 {
		return nil, errors.New("hopper: Delay and TTL must not be negative")
	}
	if len(opts.DedupKey) > maxUniqueKey-4 {
		return nil, fmt.Errorf("hopper: DedupKey exceeds %d bytes", maxUniqueKey-4)
	}
	payload, err := c.cfg.Codec.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("hopper: encode message for %q: %w", topic, err)
	}
	rows, err := exec.MessagePublish(ctx, driver.MessagePublishParams{
		Topic:       topic,
		Payload:     payload,
		Headers:     opts.Headers,
		OrderingKey: opts.OrderingKey,
		DedupKey:    opts.DedupKey,
		Delay:       opts.Delay,
		TTL:         opts.TTL,
		Priority:    int(opts.Priority),
		Await:       opts.Await,
		Notify:      inTx,
	})
	if err != nil {
		return nil, err
	}
	res := &PublishResult{Deliveries: make([]*InsertResult, len(rows))}
	for i, r := range rows {
		res.Deliveries[i] = &InsertResult{Job: r.Job, Duplicate: r.Duplicate}
		if res.MessageID == "" {
			var meta messageMetadata
			if json.Unmarshal(r.Job.Metadata, &meta) == nil {
				res.MessageID = meta.MessageID
			}
		}
		if !inTx && !r.Duplicate {
			c.wakeQueue(r.Job.Queue)
			c.notifier.mark(r.Job.Queue)
		}
	}
	return res, nil
}

// SubscriptionRow is a subscription as recorded in the database.
type SubscriptionRow = driver.SubscriptionRow

// Subscriptions lists every subscription known to the cluster.
func (c *Client[TTx]) Subscriptions(ctx context.Context) ([]*SubscriptionRow, error) {
	return c.exec.SubscriptionList(ctx)
}

// ReplayDiscarded re-drives every dead-lettered delivery of a subscription
// and returns how many.
func (c *Client[TTx]) ReplayDiscarded(ctx context.Context, subscription string) (int, error) {
	kind := Subscription{Name: subscription}.Kind()
	var ids []JobID
	for job, err := range c.Jobs(ctx, JobFilter{Kinds: []string{kind}, States: []JobState{JobStateDiscarded}}) {
		if err != nil {
			return 0, err
		}
		ids = append(ids, job.ID)
	}
	for i, id := range ids {
		if _, err := c.JobRetry(ctx, id); err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrUniqueConflict) {
			return i, err
		}
	}
	return len(ids), nil
}

// subscriptionRows returns the registered subscriptions as rows.
func (w *Workers) subscriptionRows() []driver.SubscriptionRow {
	w.mu.RLock()
	defer w.mu.RUnlock()
	rows := make([]driver.SubscriptionRow, len(w.subscriptions))
	for i, s := range w.subscriptions {
		rows[i] = driver.SubscriptionRow{Name: s.Name, Pattern: s.Pattern, Kind: s.Kind(), Queue: s.Queue, MaxAttempts: s.MaxAttempts, Metadata: s.Metadata}
	}
	return rows
}
