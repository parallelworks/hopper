package pgsql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/parallelworks/hopper/driver"
)

// Array parameters cross the transport as plain slices, which pgx encodes
// natively; a database/sql transport turns them into literals with
// ArrayLiteral. Every statement casts its array parameters, so either
// form works. Nil elements are NULL; a nil slice is an empty array, never
// NULL, so that filters can compare against '{}'.

func textArray(values []string) []string { return orEmpty(values) }

// nullableTextArray sends empty strings as NULL.
func nullableTextArray(values []string) []*string {
	elems := make([]*string, len(values))
	for i := range values {
		if values[i] != "" {
			elems[i] = &values[i]
		}
	}
	return elems
}

func intArray(values []int) []int64 {
	out := make([]int64, len(values))
	for i, v := range values {
		out[i] = int64(v)
	}
	return out
}

func int64Array(values []int64) []int64 { return orEmpty(values) }

func floatArray(values []float64) []float64 { return orEmpty(values) }

func boolArray(values []bool) []bool { return orEmpty(values) }

func orEmpty[T any](values []T) []T {
	if values == nil {
		return []T{}
	}
	return values
}

// jsonArray sends each element as a JSON document, or NULL when empty.
func jsonArray(values [][]byte) [][]byte {
	elems := make([][]byte, len(values))
	for i, v := range values {
		if len(v) > 0 {
			elems[i] = v
		}
	}
	return elems
}

// timeArray sends zero times as NULL.
func timeArray(values []time.Time) []*time.Time {
	elems := make([]*time.Time, len(values))
	for i := range values {
		if !values[i].IsZero() {
			elems[i] = &values[i]
		}
	}
	return elems
}

// uuidArray sends zero IDs as NULL.
func uuidArray(values []driver.JobID) []*[16]byte {
	elems := make([]*[16]byte, len(values))
	for i := range values {
		if !values[i].IsZero() {
			elems[i] = (*[16]byte)(&values[i])
		}
	}
	return elems
}

// ArrayLiteral renders a slice parameter as a Postgres array literal, for a
// transport that cannot encode slices itself. ok is false for any other
// value.
func ArrayLiteral(v any) (literal string, ok bool) {
	var b strings.Builder
	b.WriteByte('{')
	write := func(i int, s string, null bool) {
		if i > 0 {
			b.WriteByte(',')
		}
		if null {
			b.WriteString("NULL")
			return
		}
		b.WriteByte('"')
		for _, r := range s {
			if r == '"' || r == '\\' {
				b.WriteByte('\\')
			}
			b.WriteRune(r)
		}
		b.WriteByte('"')
	}
	switch v := v.(type) {
	case []string:
		for i, e := range v {
			write(i, e, false)
		}
	case []*string:
		for i, e := range v {
			write(i, deref(e), e == nil)
		}
	case []int64:
		for i, e := range v {
			write(i, strconv.FormatInt(e, 10), false)
		}
	case []*int64:
		for i, e := range v {
			write(i, strconv.FormatInt(deref(e), 10), e == nil)
		}
	case []float64:
		for i, e := range v {
			write(i, strconv.FormatFloat(e, 'g', -1, 64), false)
		}
	case []bool:
		for i, e := range v {
			write(i, strconv.FormatBool(e), false)
		}
	case [][]byte:
		for i, e := range v {
			write(i, string(e), e == nil)
		}
	case []*time.Time:
		for i, e := range v {
			var s string
			if e != nil {
				s = e.UTC().Format(time.RFC3339Nano)
			}
			write(i, s, e == nil)
		}
	case []*[16]byte:
		for i, e := range v {
			var s string
			if e != nil {
				s = driver.JobID(*e).String()
			}
			write(i, s, e == nil)
		}
	default:
		return "", false
	}
	b.WriteByte('}')
	return b.String(), true
}

func deref[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}

// Scalar parameters.

// nullable returns a NULL parameter for the empty string.
func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullableInt(v int) *int64 {
	if v <= 0 {
		return nil
	}
	n := int64(v)
	return &n
}

func nullableFloat(v float64) *float64 {
	if v <= 0 {
		return nil
	}
	return &v
}

func jsonOrEmptyObject(b []byte) string {
	if len(b) == 0 {
		return "{}"
	}
	return string(b)
}

func uuidParam(id driver.JobID) string { return id.String() }

// smallint checks a value bound for a smallint column.
func smallint(v int, column string) (int, error) {
	if v < -32768 || v > 32767 {
		return 0, fmt.Errorf("hopper: %s %d is out of range", column, v)
	}
	return v, nil
}

// Scan targets.

// jsonText scans a json or jsonb column as text.
type jsonText string

func (j *jsonText) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*j = ""
	case string:
		*j = jsonText(v)
	case []byte:
		*j = jsonText(v)
	default:
		return fmt.Errorf("pgsql: cannot scan %T as json", src)
	}
	return nil
}

func (j jsonText) raw() json.RawMessage {
	if j == "" {
		return nil
	}
	return json.RawMessage(j)
}

// attemptErrors scans a jsonb array of error records.
type attemptErrors []driver.AttemptError

func (a *attemptErrors) Scan(src any) error {
	var text jsonText
	if err := text.Scan(src); err != nil {
		return err
	}
	*a = attemptErrors{}
	if text == "" {
		return nil
	}
	return json.Unmarshal([]byte(text), (*[]driver.AttemptError)(a))
}

// nullUUID scans a nullable uuid column.
type nullUUID struct {
	ID    driver.JobID
	Valid bool
}

func (n *nullUUID) Scan(src any) error {
	if src == nil {
		*n = nullUUID{}
		return nil
	}
	n.Valid = true
	return n.ID.Scan(src)
}

// strings scans a text column of separator-joined values (string_agg),
// which is how the shared SQL returns lists, since array output differs
// between transports.
type joined []string

const joinSep = "\x1f"

func (j *joined) Scan(src any) error {
	var s sql.NullString
	if err := s.Scan(src); err != nil {
		return err
	}
	if !s.Valid || s.String == "" {
		*j = nil
		return nil
	}
	*j = strings.Split(s.String, joinSep)
	return nil
}
