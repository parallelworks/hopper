package pgsql

import (
	"context"
	"strings"
	"sync"
)

// Schema binds SQL templates to one namespace. Cached statements are shared
// by all executors and transactions of a driver.
type Schema struct {
	Name    string
	queries sync.Map
}

// NewSchema resolves the default namespace.
func NewSchema(name string) *Schema {
	if name == "" {
		name = "hopper"
	}
	if strings.ContainsRune(name, 0) {
		panic("hopper: schema contains NUL")
	}
	return &Schema{Name: name}
}

// Table returns a safely quoted, schema-qualified object name.
func (s *Schema) Table(name string) string { return quoteIdent(s.Name) + "." + quoteIdent(name) }

// SQL expands explicit namespace placeholders in internal SQL templates.
func (s *Schema) SQL(query string) string {
	if !strings.Contains(query, "{{schema}}") {
		return query
	}
	if v, ok := s.queries.Load(query); ok {
		return v.(string)
	}
	resolved := strings.ReplaceAll(query, "{{schema}}", quoteIdent(s.Name))
	s.queries.Store(query, resolved)
	return resolved
}

// Wrap qualifies templates without changing the underlying connection.
func (s *Schema) Wrap(c Conn) Conn { return schemaConn{Conn: c, schema: s} }

type schemaConn struct {
	Conn
	schema *Schema
}

func (c schemaConn) Exec(ctx context.Context, q string, args ...any) (int64, error) {
	return c.Conn.Exec(ctx, c.schema.SQL(q), args...)
}

func (c schemaConn) Query(ctx context.Context, q string, args ...any) (Rows, error) {
	return c.Conn.Query(ctx, c.schema.SQL(q), args...)
}

func (c schemaConn) QueryRow(ctx context.Context, q string, args ...any) Row {
	return c.Conn.QueryRow(ctx, c.schema.SQL(q), args...)
}

func (c schemaConn) QueryExec(ctx context.Context, q string, qa []any, x string, xa []any) (Rows, error) {
	return c.Conn.QueryExec(ctx, c.schema.SQL(q), qa, c.schema.SQL(x), xa)
}

func (c schemaConn) Begin(ctx context.Context) (Tx, error) {
	tx, err := c.Conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return schemaTx{schemaConn: schemaConn{Conn: tx, schema: c.schema}, tx: tx}, nil
}

type schemaTx struct {
	schemaConn
	tx Tx
}

func (t schemaTx) Commit(ctx context.Context) error   { return t.tx.Commit(ctx) }
func (t schemaTx) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }
