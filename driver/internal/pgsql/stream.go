package pgsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/parallelworks/hopper/driver"
)

// Events are identified by (xid, seq): the transaction that wrote them,
// then the event within it. A consumer reads the log by snapshot deltas.
// Its row holds the snapshot it has read to (seen): every transaction
// visible in it has been delivered. A pump takes a fresh snapshot and
// delivers the events of the transactions visible in the new one but not
// in seen, in (xid, seq) order; when a pump stops part way through a
// delta, the row keeps the new snapshot (reading) and the last event
// delivered, and the next pump continues the same delta. A transaction
// that commits late is therefore delivered when it commits, whatever its
// xid, and a long-running transaction holds nothing back. The cursor is
// reset when a delta is exhausted, since the next delta's events may sort
// before it; a seek sets it to skip part of a transaction.

const streamEventColumns = `xid::text, seq, topic, key, payload, headers, message_id::text, created_at`

func scanEvent(r Row) (*driver.StreamEvent, error) {
	var (
		e       driver.StreamEvent
		xid     string
		key     sql.NullString
		payload jsonText
		headers jsonText
	)
	if err := r.Scan(&xid, &e.Position.Seq, &e.Topic, &key, &payload, &headers, &e.MessageID, &e.CreatedAt); err != nil {
		return nil, err
	}
	var err error
	if e.Position.Xid, err = strconv.ParseUint(xid, 10, 64); err != nil {
		return nil, fmt.Errorf("xid %q: %w", xid, err)
	}
	e.Key, e.Payload = key.String, payload.raw()
	if headers != "" && headers != "{}" {
		if err := json.Unmarshal([]byte(headers), &e.Headers); err != nil {
			return nil, fmt.Errorf("headers: %w", err)
		}
	}
	return &e, nil
}

var streamEvents = collect(scanEvent)

func xidParam(p driver.StreamPosition) string { return strconv.FormatUint(p.Xid, 10) }

// StreamAppend implements driver.Executor.
func (e *Executor) StreamAppend(ctx context.Context, params driver.StreamAppendParams) (*driver.StreamEvent, error) {
	headers := "{}"
	if params.Headers != nil {
		b, err := json.Marshal(params.Headers)
		if err != nil {
			return nil, fmt.Errorf("hopper: append: encode headers: %w", err)
		}
		headers = string(b)
	}
	payload := "null"
	if len(params.Payload) > 0 {
		payload = string(params.Payload)
	}
	query := `INSERT INTO hopper_stream_events (topic, key, payload, headers) VALUES ($1, $2, $3::jsonb, $4::jsonb) RETURNING ` + streamEventColumns
	args := []any{params.Topic, nullable(params.Key), payload, headers}
	var (
		events []*driver.StreamEvent
		err    error
	)
	if params.Notify {
		events, err = streamEvents(e.Conn.QueryExec(ctx, query, args, NotifySQL, []any{driver.ChannelStream, textArray([]string{params.Topic})}))
	} else {
		events, err = streamEvents(e.Conn.Query(ctx, query, args...))
	}
	if err != nil {
		return nil, fmt.Errorf("hopper: append: %w", err)
	}
	return events[0], nil
}

// streamReadSQL pages through committed events by position. The row
// comparison walks the (xid, seq) index in order.
const streamReadSQL = `
SELECT ` + streamEventColumns + ` FROM hopper_stream_events
WHERE (xid, seq) > ($1::xid8, $2::bigint) AND ($3::text = '' OR topic ~ hopper_topic_regex($3::text))
ORDER BY xid, seq LIMIT $4`

// StreamRead implements driver.Executor.
func (e *Executor) StreamRead(ctx context.Context, params driver.StreamReadParams) ([]*driver.StreamEvent, error) {
	events, err := streamEvents(e.Conn.Query(ctx, streamReadSQL, xidParam(params.After), params.After.Seq, params.Pattern, max(params.Limit, 1)))
	if err != nil {
		return nil, fmt.Errorf("hopper: read stream: %w", err)
	}
	return events, nil
}

// earliestSnapshot sees no transaction at all: 3 is the first transaction
// ID Postgres assigns.
const earliestSnapshot = "3:3:"

// StreamConsumerUpsert implements driver.Executor.
func (e *Executor) StreamConsumerUpsert(ctx context.Context, consumers []driver.StreamConsumerRow) error {
	if len(consumers) == 0 {
		return nil
	}
	n := len(consumers)
	var (
		names, patterns, kinds, queues, starts = make([]string, n), make([]string, n), make([]string, n), make([]string, n), make([]string, n)
		maxAttempts                            = make([]*int64, n)
		metadata                               = make([][]byte, n)
	)
	for i, c := range consumers {
		names[i], patterns[i], kinds[i], queues[i] = c.Name, c.Pattern, c.Kind, c.Queue
		starts[i] = string(c.Start)
		if c.MaxAttempts > 0 {
			v, err := smallint(c.MaxAttempts, "max_attempts")
			if err != nil {
				return err
			}
			n := int64(v)
			maxAttempts[i] = &n
		}
		metadata[i] = []byte(jsonOrEmptyObject(c.Metadata))
	}
	// A consumer starting at latest has seen everything committed so far;
	// transactions still running are delivered when they commit.
	_, err := e.Conn.Exec(ctx, `
		INSERT INTO hopper_stream_consumers (name, pattern, kind, queue, max_attempts, metadata, seen)
		SELECT p.name, p.pattern, p.kind, p.queue, p.max_attempts, p.metadata,
		  CASE WHEN p.start = 'earliest' THEN $8::pg_snapshot ELSE pg_current_snapshot() END
		FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::smallint[], $6::jsonb[], $7::text[])
		  AS p(name, pattern, kind, queue, max_attempts, metadata, start)
		ON CONFLICT (name) DO UPDATE SET pattern = EXCLUDED.pattern, kind = EXCLUDED.kind, queue = EXCLUDED.queue,
		  max_attempts = EXCLUDED.max_attempts, metadata = EXCLUDED.metadata`,
		textArray(names), textArray(patterns), textArray(kinds), textArray(queues), maxAttempts, jsonArray(metadata), textArray(starts), earliestSnapshot)
	if err != nil {
		return fmt.Errorf("hopper: upsert stream consumers: %w", err)
	}
	return nil
}

const streamConsumerColumns = `name, pattern, kind, queue, max_attempts, metadata, seen::text, xid::text, seq, delivered_at, created_at`

func scanConsumer(r Row) (*driver.StreamConsumerRow, error) {
	var (
		c           driver.StreamConsumerRow
		maxAttempts sql.NullInt64
		metadata    jsonText
		xid         string
		delivered   sql.NullTime
	)
	if err := r.Scan(&c.Name, &c.Pattern, &c.Kind, &c.Queue, &maxAttempts, &metadata, &c.Snapshot, &xid, &c.Position.Seq, &delivered, &c.CreatedAt); err != nil {
		return nil, err
	}
	var err error
	if c.Position.Xid, err = strconv.ParseUint(xid, 10, 64); err != nil {
		return nil, fmt.Errorf("xid %q: %w", xid, err)
	}
	c.MaxAttempts, c.Metadata, c.DeliveredAt = int(maxAttempts.Int64), metadata.raw(), delivered.Time
	return &c, nil
}

// StreamConsumerList implements driver.Executor.
func (e *Executor) StreamConsumerList(ctx context.Context) ([]*driver.StreamConsumerRow, error) {
	consumers, err := collect(scanConsumer)(e.Conn.Query(ctx, `SELECT `+streamConsumerColumns+` FROM hopper_stream_consumers ORDER BY name`))
	if err != nil {
		return nil, fmt.Errorf("hopper: list stream consumers: %w", err)
	}
	return consumers, nil
}

// StreamConsumerSeek implements driver.Executor. Seeking to a position or
// a time resumes the log from a transaction ID onwards: the snapshot
// "x:x:" sees every transaction below x, and the cursor skips the events
// of x itself up to the position.
func (e *Executor) StreamConsumerSeek(ctx context.Context, name string, params driver.StreamSeekParams) error {
	err := e.withTx(ctx, func(tx Tx) error {
		var (
			pos driver.StreamPosition
			n   int64
			err error
		)
		switch {
		case !params.Position.IsZero():
			pos = params.Position
		case !params.Time.IsZero():
			// Just before the first event appended at or after the time.
			// Events of one transaction share created_at, so seq - 1 is
			// before all of them.
			var xid string
			err := tx.QueryRow(ctx, `SELECT xid::text, seq FROM hopper_stream_events WHERE created_at >= $1 ORDER BY xid, seq LIMIT 1`, params.Time).Scan(&xid, &pos.Seq)
			if errors.Is(err, ErrNoRows) {
				params.Latest = true
				break
			}
			if err != nil {
				return err
			}
			if pos.Xid, err = strconv.ParseUint(xid, 10, 64); err != nil {
				return err
			}
			pos.Seq--
		case params.Earliest:
		case params.Latest:
		default:
			return errors.New("nothing to seek to")
		}
		switch {
		case params.Latest:
			n, err = tx.Exec(ctx, `UPDATE hopper_stream_consumers SET seen = pg_current_snapshot(), reading = NULL, xid = '0', seq = 0 WHERE name = $1`, name)
		case pos.IsZero():
			n, err = tx.Exec(ctx, `UPDATE hopper_stream_consumers SET seen = $2::pg_snapshot, reading = NULL, xid = '0', seq = 0 WHERE name = $1`, name, earliestSnapshot)
		default:
			snapshot := fmt.Sprintf("%d:%d:", pos.Xid, pos.Xid)
			n, err = tx.Exec(ctx, `UPDATE hopper_stream_consumers SET seen = $2::pg_snapshot, reading = NULL, xid = $3::xid8, seq = $4 WHERE name = $1`, name, snapshot, xidParam(pos), pos.Seq)
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return driver.ErrNotFound
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

// streamPumpSQL delivers the events of the transactions visible in one
// snapshot but not in another, after a cursor, as jobs. The xid range
// bounds the index scan: a transaction not visible in the old snapshot has
// xid >= its xmin, and one visible in the new snapshot has xid < its xmax.
// Deliveries carry the topic, message ID, headers, consumer name and
// position in their metadata, and the event's key as their ordering key.
// It returns how many, the last position, and the queues delivered to.
const streamPumpSQL = `
WITH ev AS (
  SELECT xid, seq, topic, key, payload, headers, message_id FROM hopper_stream_events
  WHERE xid >= pg_snapshot_xmin($1::pg_snapshot) AND xid < pg_snapshot_xmax($2::pg_snapshot)
    AND (xid, seq) > ($3::xid8, $4::bigint)
    AND pg_visible_in_snapshot(xid, $2::pg_snapshot) AND NOT pg_visible_in_snapshot(xid, $1::pg_snapshot)
    AND topic ~ hopper_topic_regex($5::text)
  ORDER BY xid, seq LIMIT $6
),
ins AS (
  INSERT INTO hopper_jobs (kind, queue, priority, max_attempts, args, metadata, ordering_key)
  SELECT $7::text, $8::text, 2, coalesce($9::smallint, CASE WHEN ev.key IS NOT NULL THEN 10 ELSE 25 END), ev.payload,
    $10::jsonb || jsonb_build_object('topic', ev.topic, 'message_id', ev.message_id::text, 'headers', ev.headers,
                                     'stream', $11::text, 'position', ev.xid::text || ':' || ev.seq::text),
    ev.key
  FROM ev
  RETURNING queue
),
last AS (SELECT xid::text AS xid, seq FROM ev ORDER BY xid DESC, seq DESC LIMIT 1)
SELECT (SELECT count(*) FROM ins), (SELECT xid FROM last), (SELECT seq FROM last), (SELECT string_agg(DISTINCT queue, $12::text) FROM ins)`

// StreamPump implements driver.Executor.
func (e *Executor) StreamPump(ctx context.Context, name string, limit int) (driver.StreamPumpResult, error) {
	var res driver.StreamPumpResult
	limit = max(limit, 1)
	err := e.withTx(ctx, func(tx Tx) error {
		// Lock first, then read: a statement that waited for the lock keeps
		// its snapshot, and would not see what the previous pump delivered.
		var (
			c             driver.StreamConsumerRow
			maxAttempts   sql.NullInt64
			metadata      jsonText
			seen, reading sql.NullString
			xid           string
		)
		err := tx.QueryRow(ctx, `SELECT pattern, kind, queue, max_attempts, metadata, seen::text, reading::text, xid::text, seq FROM hopper_stream_consumers WHERE name = $1 FOR UPDATE`, name).
			Scan(&c.Pattern, &c.Kind, &c.Queue, &maxAttempts, &metadata, &seen, &reading, &xid, &c.Position.Seq)
		if errors.Is(err, ErrNoRows) {
			return driver.ErrNotFound
		}
		if err != nil {
			return err
		}
		if c.Position.Xid, err = strconv.ParseUint(xid, 10, 64); err != nil {
			return err
		}
		res.Position = c.Position
		// Continue a delta left part way, or start a new one from a fresh
		// snapshot: everything committed by now.
		if !reading.Valid {
			if err := tx.QueryRow(ctx, `SELECT pg_current_snapshot()::text`).Scan(&reading.String); err != nil {
				return err
			}
		}
		var attempts *int64
		if maxAttempts.Valid {
			attempts = &maxAttempts.Int64
		}
		var (
			delivered int64
			lastXid   sql.NullString
			lastSeq   sql.NullInt64
			queues    joined
		)
		if err := tx.QueryRow(ctx, streamPumpSQL, seen.String, reading.String, xidParam(c.Position), c.Position.Seq, c.Pattern, limit,
			c.Kind, c.Queue, attempts, jsonOrEmptyObject(metadata.raw()), name, joinSep).Scan(&delivered, &lastXid, &lastSeq, &queues); err != nil {
			return err
		}
		res.Delivered = int(delivered)
		res.Queues = queues
		if lastXid.Valid {
			if res.Position.Xid, err = strconv.ParseUint(lastXid.String, 10, 64); err != nil {
				return err
			}
			res.Position.Seq = lastSeq.Int64
		}
		if res.Delivered < limit {
			// The delta is exhausted: the new snapshot is what has been seen.
			_, err = tx.Exec(ctx, `UPDATE hopper_stream_consumers SET seen = $2::pg_snapshot, reading = NULL, xid = '0', seq = 0,
				delivered_at = CASE WHEN $3 > 0 THEN now() ELSE delivered_at END WHERE name = $1`,
				name, reading.String, res.Delivered)
		} else {
			_, err = tx.Exec(ctx, `UPDATE hopper_stream_consumers SET reading = $2::pg_snapshot, xid = $3::xid8, seq = $4, delivered_at = now() WHERE name = $1`,
				name, reading.String, xidParam(res.Position), res.Position.Seq)
		}
		if err != nil {
			return err
		}
		if len(queues) > 0 {
			if _, err := tx.Exec(ctx, NotifySQL, driver.ChannelInsert, textArray(queues)); err != nil {
				return err
			}
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
