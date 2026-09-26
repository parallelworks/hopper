package hoppersql

import (
	"database/sql"
	"fmt"
	"time"
)

// bufferedRows holds a result set read to completion, so that another
// statement can run on the same transaction before the rows are consumed.
type bufferedRows struct {
	rows [][]any
	pos  int
	err  error
}

func bufferRows(rows *sql.Rows) (*bufferedRows, error) {
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	b := &bufferedRows{}
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		b.rows = append(b.rows, values)
	}
	return b, rows.Err()
}

func (b *bufferedRows) Next() bool {
	if b.pos >= len(b.rows) {
		return false
	}
	b.pos++
	return true
}

func (b *bufferedRows) Err() error { return b.err }
func (b *bufferedRows) Close()     {}

// Scan assigns the current row's driver values to dest, covering the
// target types the shared code uses.
func (b *bufferedRows) Scan(dest ...any) error {
	row := b.rows[b.pos-1]
	if len(dest) != len(row) {
		return fmt.Errorf("hoppersql: scan %d values into %d targets", len(row), len(dest))
	}
	for i, d := range dest {
		if err := assign(d, row[i]); err != nil {
			return fmt.Errorf("hoppersql: column %d: %w", i, err)
		}
	}
	return nil
}

func assign(dest, src any) error {
	if s, ok := dest.(sql.Scanner); ok {
		return s.Scan(src)
	}
	switch d := dest.(type) {
	case *string:
		switch v := src.(type) {
		case string:
			*d = v
		case []byte:
			*d = string(v)
		default:
			return fmt.Errorf("cannot assign %T to *string", src)
		}
	case *int:
		v, ok := src.(int64)
		if !ok {
			return fmt.Errorf("cannot assign %T to *int", src)
		}
		*d = int(v)
	case *int64:
		v, ok := src.(int64)
		if !ok {
			return fmt.Errorf("cannot assign %T to *int64", src)
		}
		*d = v
	case *float64:
		v, ok := src.(float64)
		if !ok {
			return fmt.Errorf("cannot assign %T to *float64", src)
		}
		*d = v
	case *bool:
		v, ok := src.(bool)
		if !ok {
			return fmt.Errorf("cannot assign %T to *bool", src)
		}
		*d = v
	case *time.Time:
		v, ok := src.(time.Time)
		if !ok {
			return fmt.Errorf("cannot assign %T to *time.Time", src)
		}
		*d = v
	default:
		return fmt.Errorf("unsupported scan target %T", dest)
	}
	return nil
}
