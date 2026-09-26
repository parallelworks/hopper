package hopperpgx

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/parallelworks/hopper/driver"
)

func (e *executor) SubscriptionUpsert(ctx context.Context, subs []driver.SubscriptionRow) error {
	if len(subs) == 0 {
		return nil
	}
	n := len(subs)
	var (
		names       = make([]string, n)
		patterns    = make([]string, n)
		kinds       = make([]string, n)
		queues      = make([]string, n)
		maxAttempts = make([]pgtype.Int2, n)
		metadata    = make([][]byte, n)
	)
	for i, s := range subs {
		names[i], patterns[i], kinds[i], queues[i] = s.Name, s.Pattern, s.Kind, s.Queue
		if s.MaxAttempts > 0 {
			v, err := smallint(s.MaxAttempts, "max_attempts")
			if err != nil {
				return err
			}
			maxAttempts[i] = pgtype.Int2{Int16: v, Valid: true}
		}
		metadata[i] = jsonOrEmptyObject(s.Metadata)
	}
	_, err := e.db.Exec(ctx, `
		INSERT INTO hopper_subscriptions (name, pattern, kind, queue, max_attempts, metadata)
		SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::smallint[], $6::jsonb[])
		ON CONFLICT (name) DO UPDATE SET pattern = EXCLUDED.pattern, kind = EXCLUDED.kind, queue = EXCLUDED.queue,
		  max_attempts = EXCLUDED.max_attempts, metadata = EXCLUDED.metadata`,
		names, patterns, kinds, queues, maxAttempts, metadata)
	if err != nil {
		return fmt.Errorf("hopperpgx: upsert subscriptions: %w", err)
	}
	return nil
}

func (e *executor) SubscriptionList(ctx context.Context) ([]*driver.SubscriptionRow, error) {
	rows, err := e.db.Query(ctx, `SELECT name, pattern, kind, queue, max_attempts, metadata, created_at FROM hopper_subscriptions ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: list subscriptions: %w", err)
	}
	subs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*driver.SubscriptionRow, error) {
		var (
			s           driver.SubscriptionRow
			maxAttempts pgtype.Int2
		)
		if err := row.Scan(&s.Name, &s.Pattern, &s.Kind, &s.Queue, &maxAttempts, &s.Metadata, &s.CreatedAt); err != nil {
			return nil, err
		}
		s.MaxAttempts = int(maxAttempts.Int16)
		return &s, nil
	})
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: list subscriptions: %w", err)
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
SELECT * FROM ins`, jobColumns(""))

func (e *executor) MessagePublish(ctx context.Context, p driver.MessagePublishParams) ([]driver.JobInsertResult, error) {
	if p.Priority == 0 {
		p.Priority = 2
	}
	priority, err := smallint(p.Priority, "priority")
	if err != nil {
		return nil, err
	}
	headers, err := json.Marshal(p.Headers)
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: publish: encode headers: %w", err)
	}
	if p.Headers == nil {
		headers = []byte("{}")
	}
	payload := p.Payload
	if len(payload) == 0 {
		payload = []byte("null")
	}
	nullable := func(s string) pgtype.Text { return pgtype.Text{String: s, Valid: s != ""} }
	args := []any{
		p.Topic, payload, headers, nullable(p.OrderingKey), max(p.Delay, 0).Seconds(), priority,
		nullable(p.DedupKey), max(p.TTL, 0).Seconds(), p.Await,
	}
	collect := func(rows pgx.Rows, err error) ([]driver.JobInsertResult, error) {
		if err != nil {
			return nil, err
		}
		return pgx.CollectRows(rows, func(row pgx.CollectableRow) (driver.JobInsertResult, error) {
			return scanInsertResult(row)
		})
	}
	var results []driver.JobInsertResult
	if !p.Notify {
		results, err = collect(e.db.Query(ctx, messagePublishSQL, args...))
		if err != nil {
			return nil, fmt.Errorf("hopperpgx: publish: %w", err)
		}
		return results, nil
	}
	// Deliveries and their notifications in one pipelined batch. The queues
	// are only known once the statement has run, so the notify statement
	// derives them from the subscriptions matching the topic.
	b := &pgx.Batch{}
	b.Queue(messagePublishSQL, args...)
	b.Queue(`SELECT pg_notify($1, q) FROM (SELECT DISTINCT queue AS q FROM hopper_subscriptions WHERE $2::text ~ hopper_topic_regex(pattern)) d`,
		driver.ChannelInsert, p.Topic)
	br := e.db.SendBatch(ctx, b)
	results, err = collect(br.Query())
	if err == nil {
		_, err = br.Exec()
	}
	if cerr := br.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: publish: %w", err)
	}
	return results, nil
}
