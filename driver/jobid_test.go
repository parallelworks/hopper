package driver

import (
	"encoding/json"
	"testing"
)

func TestJobIDRoundTrip(t *testing.T) {
	t.Parallel()
	const s = "0192d8a4-3f2e-7c1a-9b5d-0123456789ab"
	id, err := ParseJobID(s)
	if err != nil {
		t.Fatal(err)
	}
	if got := id.String(); got != s {
		t.Fatalf("String() = %q, want %q", got, s)
	}
	if id.IsZero() {
		t.Fatal("parsed ID reported zero")
	}

	data, err := json.Marshal(struct{ ID JobID }{id})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"ID":"` + s + `"}`; string(data) != want {
		t.Fatalf("json = %s, want %s", data, want)
	}
	var back struct{ ID JobID }
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if back.ID != id {
		t.Fatalf("unmarshalled %v, want %v", back.ID, id)
	}

	var scanned JobID
	if err := scanned.Scan(s); err != nil {
		t.Fatal(err)
	}
	if scanned != id {
		t.Fatalf("Scan(string) = %v, want %v", scanned, id)
	}
	if err := scanned.Scan(id[:]); err != nil {
		t.Fatal(err)
	}
	if scanned != id {
		t.Fatalf("Scan([]byte) = %v, want %v", scanned, id)
	}
	if err := scanned.Scan(nil); err != nil || !scanned.IsZero() {
		t.Fatalf("Scan(nil) = %v, %v; want zero, nil", scanned, err)
	}
	v, err := id.Value()
	if err != nil || v != s {
		t.Fatalf("Value() = %v, %v; want %q", v, err, s)
	}
}

func TestParseJobIDRejectsGarbage(t *testing.T) {
	t.Parallel()
	for _, s := range []string{
		"",
		"0192d8a4-3f2e-7c1a-9b5d-0123456789a",   // short
		"0192d8a4-3f2e-7c1a-9b5d-0123456789abc", // long
		"0192d8a43f2e7c1a9b5d0123456789ab0000",  // no dashes
		"0192d8a4-3f2e-7c1a-9b5d-0123456789zz",  // not hex
	} {
		if _, err := ParseJobID(s); err == nil {
			t.Errorf("ParseJobID(%q) succeeded", s)
		}
	}
}

func TestJobStateTerminal(t *testing.T) {
	t.Parallel()
	for _, s := range []JobState{JobStateCompleted, JobStateCancelled, JobStateDiscarded} {
		if !s.Terminal() {
			t.Errorf("%s should be terminal", s)
		}
	}
	for _, s := range []JobState{JobStatePending, JobStateAvailable, JobStateScheduled, JobStateRunning, JobStateRetryable} {
		if s.Terminal() {
			t.Errorf("%s should not be terminal", s)
		}
	}
}
