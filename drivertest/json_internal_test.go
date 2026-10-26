package drivertest

import "testing"

// TestJSONEqual checks that JSON texts compare by value: key order,
// spacing and how a number is written do not count, its exact value does,
// and a text with anything after its value matches nothing.
func TestJSONEqual(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		got, want string
		equal     bool
	}{
		{`{"a":1,"b":[true,null]}`, `{ "b": [true, null], "a": 1 }`, true},
		{`{"n":1.50}`, `{"n":1.5}`, true},
		{`{"n":1e2}`, `{"n":100}`, true},
		{`{"n":9007199254740993}`, `{"n":9007199254740992}`, false},
		{`{"a":"x"}`, `{"a":"y"}`, false},
		{`{"a":1} {"b":2}`, `{"a":1}`, false},
		{`{"a":1}`, `{"a":1}x`, false},
		{`{"a":`, `{"a":1}`, false},
	} {
		if got := jsonEqual([]byte(c.got), c.want); got != c.equal {
			t.Errorf("jsonEqual(%s, %s) = %v, want %v", c.got, c.want, got, c.equal)
		}
	}
}
