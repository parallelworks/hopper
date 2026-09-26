package hopperpgx

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/internal/pgsql"
)

// JobInsertCopy loads a batch with COPY. IDs and the insert time come from
// the database first, so that results are complete and IDs are generated
// the same way as on every other path.
func (e *executor) JobInsertCopy(ctx context.Context, params []driver.JobInsertParams, opts driver.JobInsertOpts) ([]driver.JobInsertResult, error) {
	if len(params) == 0 {
		return nil, nil
	}
	n := len(params)

	// COPY encodes in binary, so the connection's type map must know the
	// enum. Hold one connection for the ID query and the COPY.
	c, copier, release, err := e.copyConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: copy jobs: %w", err)
	}
	defer release()
	if err := registerJobState(ctx, c); err != nil {
		return nil, fmt.Errorf("hopperpgx: copy jobs: %w", err)
	}

	rows, err := c.Query(ctx, "SELECT hopper_uuidv7(), now() FROM generate_series(1, $1)", n)
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: copy jobs: generate ids: %w", err)
	}
	var now time.Time
	ids, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (driver.JobID, error) {
		var id driver.JobID
		err := row.Scan(&id, &now)
		return id, err
	})
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: copy jobs: generate ids: %w", err)
	}

	results := make([]driver.JobInsertResult, n)
	values := make([][]any, n)
	for i, p := range params {
		if p.Priority < -32768 || p.Priority > 32767 || p.MaxAttempts < -32768 || p.MaxAttempts > 32767 {
			return nil, fmt.Errorf("hopperpgx: copy jobs: priority or max_attempts out of range")
		}
		scheduledAt := p.ScheduledAt
		if scheduledAt.IsZero() {
			scheduledAt = now
		}
		state := driver.JobStateAvailable
		if scheduledAt.After(now) {
			state = driver.JobStateScheduled
		}
		j := &driver.JobRow{
			ID:           ids[i],
			Kind:         p.Kind,
			Queue:        p.Queue,
			State:        state,
			Priority:     p.Priority,
			MaxAttempts:  p.MaxAttempts,
			ScheduledAt:  scheduledAt,
			Args:         []byte(orEmptyObject(p.Args)),
			Metadata:     []byte(orEmptyObject(p.Metadata)),
			Errors:       []driver.AttemptError{},
			Await:        p.Await,
			OrderingKey:  p.OrderingKey,
			PartitionKey: p.PartitionKey,
			BatchID:      p.BatchID,
			CreatedAt:    now,
		}
		var expiresAt pgtype.Timestamptz
		if p.TTL > 0 {
			j.ExpiresAt = now.Add(p.TTL)
			expiresAt = pgtype.Timestamptz{Time: j.ExpiresAt, Valid: true}
		}
		results[i] = driver.JobInsertResult{Job: j}
		values[i] = []any{
			[16]byte(j.ID), j.Kind, j.Queue, string(j.State), int16(j.Priority), int16(j.MaxAttempts), //nolint:gosec // range checked above
			j.ScheduledAt, []byte(j.Args), []byte(j.Metadata), expiresAt, j.Await,
			pgtype.Text{String: p.OrderingKey, Valid: p.OrderingKey != ""},
			pgtype.Text{String: p.PartitionKey, Valid: p.PartitionKey != ""},
			pgtype.UUID{Bytes: p.BatchID, Valid: !p.BatchID.IsZero()},
			j.CreatedAt,
		}
	}
	columns := []string{"id", "kind", "queue", "state", "priority", "max_attempts", "scheduled_at", "args", "metadata", "expires_at", "await", "ordering_key", "partition_key", "batch_id", "created_at"}
	copied, err := copier.CopyFrom(ctx, pgx.Identifier{"hopper_jobs"}, columns, pgx.CopyFromRows(values))
	if err != nil {
		return nil, fmt.Errorf("hopperpgx: copy jobs: %w", err)
	}
	if copied != int64(n) {
		return nil, fmt.Errorf("hopperpgx: copy jobs: copied %d rows for %d inputs", copied, n)
	}
	if opts.Notify {
		queues := map[string]struct{}{}
		var list []string
		for _, p := range params {
			if _, ok := queues[p.Queue]; !ok {
				queues[p.Queue] = struct{}{}
				list = append(list, p.Queue)
			}
		}
		if err := (&pgsql.Executor{Conn: conn{db: copier}}).Notify(ctx, driver.ChannelInsert, list); err != nil {
			return nil, fmt.Errorf("hopperpgx: copy jobs: %w", err)
		}
	}
	return results, nil
}

func orEmptyObject(b []byte) string {
	if len(b) == 0 {
		return "{}"
	}
	return string(b)
}

type copyFromer interface {
	dbtx
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
}

// copyConn returns a single connection to run a COPY on, and the object to
// run it through (the transaction, when inside one).
func (e *executor) copyConn(ctx context.Context) (*pgx.Conn, copyFromer, func(), error) {
	if e.tx != nil {
		return e.tx.Conn(), e.tx, func() {}, nil
	}
	c, err := e.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	return c.Conn(), poolConn{c}, c.Release, nil
}

// poolConn gives an acquired pool connection the dbtx shape.
type poolConn struct {
	*pgxpool.Conn
}

func (p poolConn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return p.Conn.Exec(ctx, sql, args...)
}

// registerJobState teaches a connection the hopper_job_state enum, once per
// connection, so that COPY can encode it in binary.
func registerJobState(ctx context.Context, c *pgx.Conn) error {
	if _, ok := c.TypeMap().TypeForName("hopper_job_state"); ok {
		return nil
	}
	t, err := c.LoadType(ctx, "hopper_job_state")
	if err != nil {
		return fmt.Errorf("load type hopper_job_state: %w", err)
	}
	c.TypeMap().RegisterType(t)
	return nil
}
