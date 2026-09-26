package pgsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/parallelworks/hopper/driver"
)

// SubscriptionUpsert implements driver.Executor.
func (e *Executor) SubscriptionUpsert(ctx context.Context, subs []driver.SubscriptionRow) error {
	if len(subs) == 0 {
		return nil
	}
	n := len(subs)
	var (
		names, patterns, kinds, queues = make([]string, n), make([]string, n), make([]string, n), make([]string, n)
		maxAttempts                    = make([]*int64, n)
		metadata                       = make([][]byte, n)
	)
	for i, s := range subs {
		names[i], patterns[i], kinds[i], queues[i] = s.Name, s.Pattern, s.Kind, s.Queue
		if s.MaxAttempts > 0 {
			v, err := smallint(s.MaxAttempts, "max_attempts")
			if err != nil {
				return err
			}
			n := int64(v)
			maxAttempts[i] = &n
		}
		metadata[i] = []byte(jsonOrEmptyObject(s.Metadata))
	}
	_, err := e.Conn.Exec(ctx, `
		INSERT INTO hopper_subscriptions (name, pattern, kind, queue, max_attempts, metadata)
		SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::smallint[], $6::jsonb[])
		ON CONFLICT (name) DO UPDATE SET pattern = EXCLUDED.pattern, kind = EXCLUDED.kind, queue = EXCLUDED.queue,
		  max_attempts = EXCLUDED.max_attempts, metadata = EXCLUDED.metadata`,
		textArray(names), textArray(patterns), textArray(kinds), textArray(queues), maxAttempts, jsonArray(metadata))
	if err != nil {
		return fmt.Errorf("hopper: upsert subscriptions: %w", err)
	}
	return nil
}

// SubscriptionList implements driver.Executor.
func (e *Executor) SubscriptionList(ctx context.Context) ([]*driver.SubscriptionRow, error) {
	subs, err := collect(func(r Row) (*driver.SubscriptionRow, error) {
		var (
			s           driver.SubscriptionRow
			maxAttempts sql.NullInt64
			metadata    jsonText
		)
		if err := r.Scan(&s.Name, &s.Pattern, &s.Kind, &s.Queue, &maxAttempts, &metadata, &s.CreatedAt); err != nil {
			return nil, err
		}
		s.MaxAttempts = int(maxAttempts.Int64)
		s.Metadata = metadata.raw()
		return &s, nil
	})(e.Conn.Query(ctx, `SELECT name, pattern, kind, queue, max_attempts, metadata, created_at FROM hopper_subscriptions ORDER BY name`))
	if err != nil {
		return nil, fmt.Errorf("hopper: list subscriptions: %w", err)
	}
	return subs, nil
}

// messagePublishSQL fans a message out to every matching subscription in one
// statement. Each delivery's metadata records the topic, the message ID
// (generated once, by the database) and the headers, merged with the
// subscription's own metadata. A dedup key becomes the deliveries' unique key,
// scoped by kind, so each subscription deduplicates independently.
var messagePublishSQL = fmt.Sprintf(`
WITH msg AS (SELECT hopper_uuidv7()::text AS id),
ins AS (
  INSERT INTO hopper_jobs (kind, queue, state, priority, max_attempts, scheduled_at, args, metadata, unique_key,
                           ordering_key, expires_at, await)
  SELECT s.kind, s.queue,
    CASE WHEN $5::float8 > 0 THEN 'scheduled' ELSE 'available' END::hopper_job_state,
    $6::smallint,
    coalesce(s.max_attempts, CASE WHEN $4::text IS NOT NULL THEN 10 ELSE 25 END),
    now() + make_interval(secs => $5::float8),
    $2::jsonb,
    jsonb_build_object('topic', $1::text, 'message_id', msg.id, 'headers', $3::jsonb) || s.metadata,
    CASE WHEN $7::text IS NOT NULL THEN 'msg:' || $7::text END,
    $4::text,
    CASE WHEN $8::float8 > 0 THEN now() + make_interval(secs => $8::float8) END,
    $9::boolean
  FROM hopper_subscriptions s, msg
  WHERE $1::text ~ hopper_topic_regex(s.pattern)
  ON CONFLICT (kind, unique_key) WHERE unique_key IS NOT NULL DO UPDATE SET kind = EXCLUDED.kind
  RETURNING %s, (xmax <> 0) AS duplicate
)
SELECT * FROM ins`, JobColumns(""))

// publishNotifySQL notifies the queues of the subscriptions matching a
// topic, which are the delivery queues.
const publishNotifySQL = `SELECT pg_notify($1, q) FROM (SELECT DISTINCT queue AS q FROM hopper_subscriptions WHERE $2::text ~ hopper_topic_regex(pattern)) d`

// MessagePublish implements driver.Executor.
func (e *Executor) MessagePublish(ctx context.Context, p driver.MessagePublishParams) ([]driver.JobInsertResult, error) {
	if p.Priority == 0 {
		p.Priority = 2
	}
	priority, err := smallint(p.Priority, "priority")
	if err != nil {
		return nil, err
	}
	headers := "{}"
	if p.Headers != nil {
		b, err := json.Marshal(p.Headers)
		if err != nil {
			return nil, fmt.Errorf("hopper: publish: encode headers: %w", err)
		}
		headers = string(b)
	}
	payload := "null"
	if len(p.Payload) > 0 {
		payload = string(p.Payload)
	}
	args := []any{
		p.Topic, payload, headers, nullable(p.OrderingKey), max(p.Delay, 0).Seconds(), priority,
		nullable(p.DedupKey), max(p.TTL, 0).Seconds(), p.Await,
	}
	var results []driver.JobInsertResult
	if p.Notify {
		results, err = scanInsertResults(e.Conn.QueryExec(ctx, messagePublishSQL, args, publishNotifySQL, []any{driver.ChannelInsert, p.Topic}))
	} else {
		results, err = scanInsertResults(e.Conn.Query(ctx, messagePublishSQL, args...))
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: publish: %w", err)
	}
	return results, nil
}
