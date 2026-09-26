// Package pgsql implements hopper's driver operations for PostgreSQL over a
// small transport interface, so that the pgx driver (hopperpgx) and the
// database/sql driver (hoppersql) share one implementation of the SQL and
// the logic around it.
//
// Everything crosses the transport boundary as portable values: parameters
// are strings, numbers, booleans, times, plain slices of them and pointers
// to them for NULL (JSON goes as text, and every statement casts its
// parameters, so a transport that cannot encode slices renders them with
// ArrayLiteral), and scan targets are the standard library's types plus a
// few sql.Scanner implementations here. A transport therefore needs no
// type registration.
package pgsql

import (
	"context"
	"errors"
)

// ErrNoRows is returned by Row.Scan when the query matched nothing.
// Transports translate their own sentinel to it.
var ErrNoRows = errors.New("pgsql: no rows")

// Conn runs statements: a pool, a dedicated connection or a transaction.
type Conn interface {
	// Exec runs a statement and returns the rows affected.
	Exec(ctx context.Context, sql string, args ...any) (int64, error)
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
	// QueryExec runs a query followed by a statement, pipelined in one
	// round trip where the transport can, and inside one transaction. It
	// returns the query's rows.
	QueryExec(ctx context.Context, query string, queryArgs []any, exec string, execArgs []any) (Rows, error)
	// Begin starts a transaction, or a savepoint inside one.
	Begin(ctx context.Context) (Tx, error)
}

// Tx is a transaction (or savepoint) on a Conn.
type Tx interface {
	Conn
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Rows is a result set. Close must be called; Err reports iteration errors.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

// Row is a single-row result. Scan returns ErrNoRows when there is none.
type Row interface {
	Scan(dest ...any) error
}

// Executor implements driver.Executor over a Conn. InTx says the Conn is
// the caller's transaction, which rules out operations that must not run
// inside one.
type Executor struct {
	Conn Conn
	InTx bool
}

// withTx runs fn in a transaction (a savepoint when already in one).
func (e *Executor) withTx(ctx context.Context, fn func(tx Tx) error) (err error) {
	tx, err := e.Conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback(context.WithoutCancel(ctx)))
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// collect returns a consumer that reads every row of a query result through
// scan and closes the rows. It is shaped so that a Query call's two results
// can be passed straight in: collect(scan)(conn.Query(...)).
func collect[T any](scan func(Row) (T, error)) func(Rows, error) ([]T, error) {
	return func(rows Rows, err error) ([]T, error) {
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []T
		for rows.Next() {
			v, err := scan(rows)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, rows.Err()
	}
}
