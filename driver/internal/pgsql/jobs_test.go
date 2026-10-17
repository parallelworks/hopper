package pgsql

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/parallelworks/hopper/driver"
)

// listConn records the statements JobList runs: a query fails, and a
// single-row query returns true.
type listConn struct {
	Conn
	statements int
}

var errListConn = errors.New("listConn")

func (c *listConn) Query(context.Context, string, ...any) (Rows, error) {
	c.statements++
	return nil, errListConn
}

func (c *listConn) Exec(context.Context, string, ...any) (int64, error) {
	c.statements++
	return 0, nil
}

func (c *listConn) QueryRow(context.Context, string, ...any) Row {
	c.statements++
	return trueRow{}
}

type trueRow struct{}

func (trueRow) Scan(dest ...any) error {
	for _, d := range dest {
		if b, ok := d.(*bool); ok {
			*b = true
		}
	}
	return nil
}

// TestJobListMetadataInOneStatement checks that a listing with Metadata runs
// one statement, so that a paged listing does not check the same filter
// again on every page.
func TestJobListMetadataInOneStatement(t *testing.T) {
	c := &listConn{}
	e := &Executor{Conn: c, Schema: NewSchema("")}
	_, err := e.JobList(context.Background(), driver.JobListParams{Metadata: json.RawMessage(`{"a": 1}`), Limit: 10})
	if !errors.Is(err, errListConn) || c.statements != 1 {
		t.Errorf("JobList with Metadata ran %d statements, %v; want 1", c.statements, err)
	}
}

// TestJobListLeavesSchemaCache checks that listings with different filters
// add nothing to the schema's statement cache, whose texts are never
// evicted: a listing's text varies with its filters.
func TestJobListLeavesSchemaCache(t *testing.T) {
	s := NewSchema("")
	e := &Executor{Conn: s.Wrap(&listConn{}), Schema: s}
	for n := 1; n <= 5; n++ {
		tags := make([]string, n)
		for i := range tags {
			tags[i] = string(rune('a' + i))
		}
		_, _ = e.JobList(context.Background(), driver.JobListParams{TagsAny: tags, Limit: n})
		_, _ = e.JobList(context.Background(), driver.JobListParams{TagsAll: tags, Limit: n})
	}
	cached := 0
	s.queries.Range(func(any, any) bool { cached++; return true })
	if cached != 0 {
		t.Errorf("JobList left %d statements in the schema's cache, want 0", cached)
	}
}

// Begin counts the savepoint as a statement and runs the transaction's
// statements on the same listConn.
func (c *listConn) Begin(context.Context) (Tx, error) {
	c.statements++
	return listTx{c}, nil
}

type listTx struct{ *listConn }

func (t listTx) Commit(context.Context) error {
	t.statements++
	return nil
}

func (t listTx) Rollback(context.Context) error {
	t.statements++
	return nil
}

// TestJobListTxSavepointOnlyForRefusableFilters checks that a listing in the
// caller's transaction takes a savepoint, two more round trips, only when
// the server could refuse its filters: text beyond ASCII or with a NUL, or
// Metadata.
func TestJobListTxSavepointOnlyForRefusableFilters(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params driver.JobListParams
		want   int
	}{
		{"ascii", driver.JobListParams{Kinds: []string{"email"}, Queue: "default", TagsAny: []string{"a", "b"}}, 1},
		{"no filters", driver.JobListParams{}, 1},
		{"non-ascii tag", driver.JobListParams{TagsAll: []string{"café"}}, 3},
		{"nul kind", driver.JobListParams{Kinds: []string{"a\x00"}}, 3},
		{"metadata", driver.JobListParams{Metadata: json.RawMessage(`{"a": 1}`)}, 3},
	} {
		c := &listConn{}
		e := &Executor{Conn: c, Schema: NewSchema(""), InTx: true}
		tc.params.Limit = 10
		if _, err := e.JobList(context.Background(), tc.params); !errors.Is(err, errListConn) || c.statements != tc.want {
			t.Errorf("%s: JobList in a transaction ran %d statements, %v; want %d", tc.name, c.statements, err, tc.want)
		}
	}
}
