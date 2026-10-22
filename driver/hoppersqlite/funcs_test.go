package hoppersqlite

import (
	"encoding/binary"
	"strings"
	"testing"
	"time"
)

// TestTopicPatterns checks AMQP patterns against topics, with the results
// Postgres's hopper_topic_regex gives, odd patterns included.
func TestTopicPatterns(t *testing.T) {
	for _, c := range []struct {
		pattern, topic string
		want           bool
	}{
		{"#", "", true},
		{"#", "a.b.c", true},
		{"a.*", "a.b", true},
		{"a.*", "a", false},
		{"a.*", "a.b.c", false},
		{"a.*", "a.", false},
		{"a.#", "a", true},
		{"a.#", "a.b.c", true},
		{"a.#", "ab", false},
		{"#.b", "b", true},
		{"#.b", "a.x.b", true},
		{"#.b", "ab", false},
		{"a.#.b", "a.b", true},
		{"a.#.b", "a.x.y.b", true},
		{"a.#.b", "a.xb", false},
		{"*.created", "order.created", true},
		{"*.created", "order.paid", false},
		{"a#b", "axyzb", true}, // '#' between letters is '.*', as on Postgres
		{"a+b", "a+b", true},
		{"a+b", "aab", false},
		{"(x)", "(x)", true},
		{"a.b", "a.b", true},
		{"a.b", "axb", false},
		{"#", "line\nbreak", true},
	} {
		re, err := topicRegexp(c.pattern)
		if err != nil {
			t.Fatalf("%q: %v", c.pattern, err)
		}
		if got := re.MatchString(c.topic); got != c.want {
			t.Errorf("%q matches %q = %v, want %v", c.pattern, c.topic, got, c.want)
		}
	}
}

// TestJSONContains checks containment as jsonb's @> has it.
func TestJSONContains(t *testing.T) {
	for _, c := range []struct {
		have, want string
		contains   bool
	}{
		{`{"a": 1, "b": 2}`, `{"a": 1}`, true},
		{`{"a": 1}`, `{"a": 1, "b": 2}`, false},
		{`{"a": 1.0}`, `{"a": 1}`, true},
		{`{"a": 100}`, `{"a": 1e2}`, true},
		{`{"a": 0.10}`, `{"a": 1e-1}`, true},
		{`{"a": -0}`, `{"a": 0}`, true},
		{`{"a": "1"}`, `{"a": 1}`, false},
		{`{"a": {"b": [1, 2, 3]}}`, `{"a": {"b": [3, 1]}}`, true},
		{`{"a": [1, 2]}`, `{"a": [1, 1, 1]}`, true},
		{`{"a": [1, 2]}`, `{"a": [4]}`, false},
		{`{"a": [[1, 3], 2]}`, `{"a": [[3]]}`, true},
		{`{"a": [[1, 3], 2]}`, `{"a": [3]}`, false},
		{`{"a": null}`, `{"a": null}`, true},
		{`{"a": false}`, `{"a": null}`, false},
		{`{}`, `{}`, true},
		{`["x", "y"]`, `"x"`, true},
		{`{"a": ["x"]}`, `{"a": "x"}`, false},
		{`[1]`, `{}`, false},
	} {
		have, err := decodeJSON(c.have)
		if err != nil {
			t.Fatal(err)
		}
		want, err := decodeJSON(c.want)
		if err != nil {
			t.Fatal(err)
		}
		if got := jsonContains(have, want, true); got != c.contains {
			t.Errorf("%s @> %s = %v, want %v", c.have, c.want, got, c.contains)
		}
	}
}

// TestJSONFilterChecks checks that a metadata filter is refused where
// Postgres's jsonb input refuses it, and only there.
func TestJSONFilterChecks(t *testing.T) {
	for filter, refused := range map[string]bool{
		`{"a": 1}`:                        false,
		`{"a": "é😀"}`:                     false,
		`{"a": 1e131071}`:                 false,
		`{"a": 1e131072}`:                 true,
		`{"a": 1e1000000}`:                true,
		`{"a": 1e-16383}`:                 false,
		`{"a": 1e-16384}`:                 true,
		`{"a": 12.5e-16382}`:              false,
		`{"a": 12.5e-16383}`:              true,
		`{"a": "\u0000"}`:                 true,
		`{"a": "x\\u0000"}`:               false,
		`{"a": "\ud800x"}`:                true,
		`{"a": "\udc00\ud800"}`:           true,
		"{\"a\": \"\xff\"}":               true,
		`{"a": 1`:                         true,
		`{"a": 99999999999999999999e-20}`: false,
	} {
		err := checkJSONFilter(filter)
		if (err != nil) != refused {
			t.Errorf("checkJSONFilter(%s) = %v, want refused %v", filter, err, refused)
		}
		if err != nil && strings.Contains(err.Error(), "Metadata") {
			t.Errorf("checkJSONFilter(%s) names the field: %v", filter, err)
		}
	}
}

// TestNewID checks that IDs are UUIDv7s of the time they were minted, in
// order across milliseconds.
func TestNewID(t *testing.T) {
	now := time.Date(2026, 10, 22, 10, 0, 0, 500_000, time.UTC)
	id := newID(now)
	if v := id[6] >> 4; v != 7 {
		t.Errorf("version %d", v)
	}
	if v := id[8] >> 6; v != 2 {
		t.Errorf("variant %b", v)
	}
	if ms := int64(binary.BigEndian.Uint64(id[0:8]) >> 16); ms != now.UnixMilli() {
		t.Errorf("time %d, want %d", ms, now.UnixMilli())
	}
	if fraction := binary.BigEndian.Uint16(id[6:8]) & 0x0fff; fraction != 2048 {
		t.Errorf("fraction %d, want 2048 (half a millisecond)", fraction)
	}
	if later := newID(now.Add(time.Millisecond)); string(later[:]) <= string(id[:]) {
		t.Errorf("%s does not sort after %s", later, id)
	}
	if again := newID(now); again == id {
		t.Error("two IDs minted at one time are equal")
	}
}
