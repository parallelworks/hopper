package pgsql

import "testing"

func TestSchemaSQL(t *testing.T) {
	if got := NewSchema("").Name; got != "hopper" {
		t.Fatalf("default = %q", got)
	}
	s := NewSchema(`queue"jobs`)
	const query = `SELECT {{schema}}.hopper_uuidv7() FROM {{schema}}.hopper_jobs`
	const want = `SELECT "queue""jobs".hopper_uuidv7() FROM "queue""jobs".hopper_jobs`
	for range 2 {
		if got := s.SQL(query); got != want {
			t.Fatalf("SQL = %q", got)
		}
	}
}
