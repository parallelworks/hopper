package hoppersqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// SubscriptionUpsert implements driver.Executor.
func (e *executor) SubscriptionUpsert(ctx context.Context, subs []driver.SubscriptionRow) error {
	if len(subs) == 0 {
		return nil
	}
	type row struct {
		maxAttempts any
		metadata    string
	}
	rows := make([]row, len(subs))
	for i, s := range subs {
		if s.MaxAttempts > 0 {
			v, err := smallint(s.MaxAttempts, "max_attempts")
			if err != nil {
				return err
			}
			rows[i].maxAttempts = v
		}
		var err error
		if rows[i].metadata, err = jsonValue(s.Metadata, "{}", "subscription metadata"); err != nil {
			return err
		}
	}
	err := e.write(ctx, func(x *w) error {
		for i, s := range subs {
			if _, err := x.ExecContext(ctx, `
				INSERT INTO hopper_subscriptions (name, pattern, kind, queue, max_attempts, metadata, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT (name) DO UPDATE SET pattern = excluded.pattern, kind = excluded.kind, queue = excluded.queue,
				  max_attempts = excluded.max_attempts, metadata = excluded.metadata`,
				s.Name, s.Pattern, s.Kind, s.Queue, rows[i].maxAttempts, rows[i].metadata, x.us()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("hopper: upsert subscriptions: %w", err)
	}
	return nil
}

// SubscriptionList implements driver.Executor.
func (e *executor) SubscriptionList(ctx context.Context) ([]*driver.SubscriptionRow, error) {
	subs, err := queryAll(ctx, e.read(), func(r scanner) (*driver.SubscriptionRow, error) {
		var (
			s           driver.SubscriptionRow
			maxAttempts sql.NullInt64
			metadata    string
			created     int64
		)
		if err := r.Scan(&s.Name, &s.Pattern, &s.Kind, &s.Queue, &maxAttempts, &metadata, &created); err != nil {
			return nil, err
		}
		s.MaxAttempts, s.Metadata, s.CreatedAt = int(maxAttempts.Int64), json.RawMessage(metadata), time.UnixMicro(created).UTC()
		return &s, nil
	}, `SELECT name, pattern, kind, queue, max_attempts, metadata, created_at FROM hopper_subscriptions ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("hopper: list subscriptions: %w", err)
	}
	return subs, nil
}

// mergeObjects returns a JSON object with base's members and over's, over
// winning, as jsonb's || has it for two objects. A value that is not an
// object adds nothing.
func mergeObjects(base, over string) (string, error) {
	m := map[string]json.RawMessage{}
	_ = json.Unmarshal([]byte(base), &m)
	var o map[string]json.RawMessage
	_ = json.Unmarshal([]byte(over), &o)
	for k, v := range o {
		m[k] = v
	}
	b, err := json.Marshal(m)
	return string(b), err
}

// MessagePublish implements driver.Executor: it inserts one delivery per
// subscription whose pattern matches the topic. Each delivery's metadata
// records the topic, the message ID and the headers, merged with the
// subscription's own; a dedup key becomes the deliveries' unique key,
// scoped by kind, so each subscription deduplicates on its own.
func (e *executor) MessagePublish(ctx context.Context, p driver.MessagePublishParams) ([]driver.JobInsertResult, error) {
	if p.Priority == 0 {
		p.Priority = 2
	}
	priority, err := smallint(p.Priority, "priority")
	if err != nil {
		return nil, err
	}
	headers := json.RawMessage("{}")
	if p.Headers != nil {
		if headers, err = json.Marshal(p.Headers); err != nil {
			return nil, fmt.Errorf("hopper: publish: encode headers: %w", err)
		}
	}
	payload, err := jsonValue(p.Payload, "null", "payload")
	if err != nil {
		return nil, err
	}
	var results []driver.JobInsertResult
	err = e.write(ctx, func(x *w) error {
		results = results[:0]
		type sub struct {
			kind, queue string
			maxAttempts sql.NullInt64
			metadata    string
		}
		subs, err := queryAll(ctx, x, func(r scanner) (sub, error) {
			var s sub
			err := r.Scan(&s.kind, &s.queue, &s.maxAttempts, &s.metadata)
			return s, err
		}, `SELECT kind, queue, max_attempts, metadata FROM hopper_subscriptions
			WHERE hopper_topic_matches(pattern, ?) ORDER BY name`, p.Topic)
		if err != nil {
			return err
		}
		messageID := newID(x.now).String()
		base, err := json.Marshal(map[string]any{"topic": p.Topic, "message_id": messageID, "headers": headers})
		if err != nil {
			return err
		}
		var queues []string
		for _, s := range subs {
			metadata, err := mergeObjects(string(base), s.metadata)
			if err != nil {
				return err
			}
			attempts := 25
			switch {
			case s.maxAttempts.Valid:
				attempts = int(s.maxAttempts.Int64)
			case p.OrderingKey != "":
				attempts = 10
			}
			j := jobInsert{
				kind: s.kind, queue: s.queue, priority: priority, maxAttempts: attempts, scheduledAt: x.now.Add(max(p.Delay, 0)),
				args: payload, metadata: metadata, orderingKey: p.OrderingKey, ttl: p.TTL, await: p.Await,
			}
			if p.DedupKey != "" {
				j.uniqueKey = "msg:" + p.DedupKey
			}
			r, err := x.insertJobs(ctx, []jobInsert{j}, driver.ConflictSkip)
			if err != nil {
				return err
			}
			results = append(results, r[0])
			if !slices.Contains(queues, s.queue) {
				queues = append(queues, s.queue)
			}
		}
		if p.Notify {
			return x.notify(ctx, driver.ChannelInsert, queues...)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("hopper: publish: %w", err)
	}
	return results, nil
}

// Streams. SQLite runs one writer at a time, so events commit in the order
// they are appended: an event's position is its seq, and a consumer has
// read every event up to the seq it has seen, with nothing committed later
// to sort before it. Positions carry no transaction ID (Xid is 0).

func position(seq int64) driver.StreamPosition { return driver.StreamPosition{Seq: seq} }

const streamEventColumns = `seq, topic, key, payload, headers, message_id, created_at`

func scanEvent(r scanner) (*driver.StreamEvent, error) {
	var (
		ev               driver.StreamEvent
		seq, created     int64
		key              sql.NullString
		payload, headers string
		messageID        []byte
	)
	if err := r.Scan(&seq, &ev.Topic, &key, &payload, &headers, &messageID, &created); err != nil {
		return nil, err
	}
	var id driver.JobID
	if err := id.Scan(messageID); err != nil {
		return nil, err
	}
	ev.Position, ev.Key, ev.Payload, ev.MessageID, ev.CreatedAt = position(seq), key.String, json.RawMessage(payload), id.String(), time.UnixMicro(created).UTC()
	if headers != "{}" {
		if err := json.Unmarshal([]byte(headers), &ev.Headers); err != nil {
			return nil, fmt.Errorf("headers: %w", err)
		}
	}
	return &ev, nil
}

// StreamAppend implements driver.Executor.
func (e *executor) StreamAppend(ctx context.Context, params driver.StreamAppendParams) (*driver.StreamEvent, error) {
	headers := []byte("{}")
	if params.Headers != nil {
		var err error
		if headers, err = json.Marshal(params.Headers); err != nil {
			return nil, fmt.Errorf("hopper: append: encode headers: %w", err)
		}
	}
	payload, err := jsonValue(params.Payload, "null", "payload")
	if err != nil {
		return nil, err
	}
	var ev *driver.StreamEvent
	err = e.write(ctx, func(x *w) error {
		var err error
		if ev, err = scanEvent(x.QueryRowContext(ctx, `INSERT INTO hopper_stream_events (topic, key, payload, headers, message_id, created_at)
			VALUES (?, ?, ?, ?, ?, ?) RETURNING `+streamEventColumns,
			params.Topic, nullable(params.Key), payload, string(headers), idParam(newID(x.now)), x.us())); err != nil {
			return err
		}
		if params.Notify {
			return x.notify(ctx, driver.ChannelStream, params.Topic)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("hopper: append: %w", err)
	}
	return ev, nil
}

// StreamRead implements driver.Executor.
func (e *executor) StreamRead(ctx context.Context, params driver.StreamReadParams) ([]*driver.StreamEvent, error) {
	events, err := queryAll(ctx, e.read(), scanEvent, `SELECT `+streamEventColumns+` FROM hopper_stream_events
		WHERE seq > ? AND (? = '' OR hopper_topic_matches(?, topic)) ORDER BY seq LIMIT ?`,
		params.After.Seq, params.Pattern, params.Pattern, max(params.Limit, 1))
	if err != nil {
		return nil, fmt.Errorf("hopper: read stream: %w", err)
	}
	return events, nil
}

// StreamConsumerUpsert implements driver.Executor. A consumer starting at
// latest has seen every event appended so far.
func (e *executor) StreamConsumerUpsert(ctx context.Context, consumers []driver.StreamConsumerRow) error {
	if len(consumers) == 0 {
		return nil
	}
	err := e.write(ctx, func(x *w) error {
		var latest int64
		if err := x.QueryRowContext(ctx, `SELECT coalesce(max(seq), 0) FROM hopper_stream_events`).Scan(&latest); err != nil {
			return err
		}
		for _, c := range consumers {
			var maxAttempts any
			if c.MaxAttempts > 0 {
				v, err := smallint(c.MaxAttempts, "max_attempts")
				if err != nil {
					return err
				}
				maxAttempts = v
			}
			metadata, err := jsonValue(c.Metadata, "{}", "consumer metadata")
			if err != nil {
				return err
			}
			seen := latest
			if c.Start == driver.StreamStartEarliest {
				seen = 0
			}
			if _, err := x.ExecContext(ctx, `
				INSERT INTO hopper_stream_consumers (name, pattern, kind, queue, max_attempts, metadata, seen, created_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT (name) DO UPDATE SET pattern = excluded.pattern, kind = excluded.kind, queue = excluded.queue,
				  max_attempts = excluded.max_attempts, metadata = excluded.metadata`,
				c.Name, c.Pattern, c.Kind, c.Queue, maxAttempts, metadata, seen, x.us()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("hopper: upsert stream consumers: %w", err)
	}
	return nil
}

// StreamConsumerList implements driver.Executor. A consumer's Snapshot is
// the seq it has read up to, and its Position the last event it delivered.
func (e *executor) StreamConsumerList(ctx context.Context) ([]*driver.StreamConsumerRow, error) {
	consumers, err := queryAll(ctx, e.read(), func(r scanner) (*driver.StreamConsumerRow, error) {
		var (
			c                  driver.StreamConsumerRow
			maxAttempts        sql.NullInt64
			metadata           string
			seen, pos, created int64
			delivered          micros
		)
		if err := r.Scan(&c.Name, &c.Pattern, &c.Kind, &c.Queue, &maxAttempts, &metadata, &seen, &pos, &delivered, &created); err != nil {
			return nil, err
		}
		c.MaxAttempts, c.Metadata = int(maxAttempts.Int64), json.RawMessage(metadata)
		c.Snapshot, c.Position = strconv.FormatInt(seen, 10), position(pos)
		c.DeliveredAt, c.CreatedAt = delivered.time(), time.UnixMicro(created).UTC()
		return &c, nil
	}, `SELECT name, pattern, kind, queue, max_attempts, metadata, seen, position, delivered_at, created_at
		FROM hopper_stream_consumers ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("hopper: list stream consumers: %w", err)
	}
	return consumers, nil
}

// StreamConsumerSeek implements driver.Executor.
func (e *executor) StreamConsumerSeek(ctx context.Context, name string, params driver.StreamSeekParams) error {
	err := e.write(ctx, func(x *w) error {
		var (
			seen   int64
			latest bool
		)
		switch {
		case !params.Position.IsZero():
			seen = params.Position.Seq
		case !params.Time.IsZero():
			// Just before the first event appended at or after the time.
			err := x.QueryRowContext(ctx, `SELECT seq - 1 FROM hopper_stream_events WHERE created_at >= ? ORDER BY seq LIMIT 1`,
				params.Time.UnixMicro()).Scan(&seen)
			if errors.Is(err, sql.ErrNoRows) {
				latest = true
				break
			}
			if err != nil {
				return err
			}
		case params.Earliest:
		case params.Latest:
			latest = true
		default:
			return errors.New("nothing to seek to")
		}
		if latest {
			if err := x.QueryRowContext(ctx, `SELECT coalesce(max(seq), 0) FROM hopper_stream_events`).Scan(&seen); err != nil {
				return err
			}
		}
		res, err := x.ExecContext(ctx, `UPDATE hopper_stream_consumers SET seen = ?, position = ? WHERE name = ?`, seen, seen, name)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil || n == 0 {
			return cmpErr(err, driver.ErrNotFound)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, driver.ErrNotFound) {
			return err
		}
		return fmt.Errorf("hopper: seek stream consumer %s: %w", name, err)
	}
	return nil
}

// cmpErr returns err, or def if err is nil.
func cmpErr(err, def error) error {
	if err != nil {
		return err
	}
	return def
}

// StreamPump implements driver.Executor: it turns up to limit events past
// a consumer's seen seq that match its pattern into delivery jobs, and
// advances it past them, or past every event if fewer matched. Deliveries
// carry the topic, message ID, headers, consumer name and position in
// their metadata, over the consumer's own, and the event's key as their
// ordering key.
func (e *executor) StreamPump(ctx context.Context, name string, limit int) (driver.StreamPumpResult, error) {
	var res driver.StreamPumpResult
	limit = max(limit, 1)
	err := e.write(ctx, func(x *w) error {
		res = driver.StreamPumpResult{}
		var (
			pattern, kind, queue, metadata string
			maxAttempts                    sql.NullInt64
			seen, pos                      int64
		)
		err := x.QueryRowContext(ctx, `SELECT pattern, kind, queue, max_attempts, metadata, seen, position FROM hopper_stream_consumers WHERE name = ?`,
			name).Scan(&pattern, &kind, &queue, &maxAttempts, &metadata, &seen, &pos)
		if errors.Is(err, sql.ErrNoRows) {
			return driver.ErrNotFound
		}
		if err != nil {
			return err
		}
		res.Position = position(pos)
		events, err := queryAll(ctx, x, scanEvent, `SELECT `+streamEventColumns+` FROM hopper_stream_events
			WHERE seq > ? AND hopper_topic_matches(?, topic) ORDER BY seq LIMIT ?`, seen, pattern, limit)
		if err != nil {
			return err
		}
		for _, ev := range events {
			headers, err := json.Marshal(ev.Headers)
			if err != nil {
				return err
			}
			if ev.Headers == nil {
				headers = []byte("{}")
			}
			over, err := json.Marshal(map[string]any{
				"topic": ev.Topic, "message_id": ev.MessageID, "headers": json.RawMessage(headers),
				"stream": name, "position": ev.Position.String(),
			})
			if err != nil {
				return err
			}
			meta, err := mergeObjects(metadata, string(over))
			if err != nil {
				return err
			}
			attempts := 25
			switch {
			case maxAttempts.Valid:
				attempts = int(maxAttempts.Int64)
			case ev.Key != "":
				attempts = 10
			}
			if _, err := x.insertJob(ctx, jobInsert{
				kind: kind, queue: queue, priority: 2, maxAttempts: attempts, args: string(ev.Payload), metadata: meta, orderingKey: ev.Key,
			}); err != nil {
				return err
			}
			res.Position = ev.Position
			seen = ev.Position.Seq
		}
		res.Delivered = len(events)
		if res.Delivered < limit {
			// Every event matching the pattern has been delivered: the
			// consumer has seen every event appended so far.
			if err := x.QueryRowContext(ctx, `SELECT max(?, coalesce(max(seq), 0)) FROM hopper_stream_events`, seen).Scan(&seen); err != nil {
				return err
			}
		}
		var delivered any
		if res.Delivered > 0 {
			delivered = x.us()
			res.Queues = []string{queue}
		}
		if _, err := x.ExecContext(ctx, `UPDATE hopper_stream_consumers SET seen = ?, position = ?, delivered_at = coalesce(?, delivered_at) WHERE name = ?`,
			seen, res.Position.Seq, delivered, name); err != nil {
			return err
		}
		if res.Delivered > 0 {
			return x.notify(ctx, driver.ChannelInsert, queue)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, driver.ErrNotFound) {
			return res, err
		}
		return res, fmt.Errorf("hopper: pump stream consumer %s: %w", name, err)
	}
	return res, nil
}
