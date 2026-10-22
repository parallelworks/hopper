// Package jobcodec encodes and decodes the JSON values a job row keeps
// beside its args and metadata: its errors, progress, logs and tags. Every
// engine stores them the same way, and reads them the same way: a value
// that a direct SQL write left in another form reads as what can be made of
// it, rather than failing the read, so that one such row fails neither the
// claim that takes it (whose update has committed by the time it is read)
// nor a listing or the rescuer.
package jobcodec

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/internal/jobtags"
)

// StorableText replaces what a database may refuse in text and JSON
// strings, NUL and invalid UTF-8, so that one worker's text cannot fail the
// statement that finalizes a whole batch.
func StorableText(s string) string {
	return strings.ReplaceAll(strings.ToValidUTF8(s, "�"), "\x00", "�")
}

// MarshalAttemptError encodes an error record without its timestamp, which
// the engine fills in from database time, its text made storable.
func MarshalAttemptError(e *driver.AttemptError) ([]byte, error) {
	return json.Marshal(struct {
		Attempt int    `json:"attempt"`
		Fence   int64  `json:"fence,omitempty"`
		Error   string `json:"error"`
		Trace   string `json:"trace,omitempty"`
	}{e.Attempt, e.Fence, StorableText(e.Error), StorableText(e.Trace)})
}

// MarshalAttemptLog encodes the log a finalize appends for result f, which
// a snooze marks Snoozed, its text made storable, or returns nil when there
// is none to keep (JobFinalize.KeepsLog).
func MarshalAttemptLog(f driver.JobFinalize) ([]byte, error) {
	if f.Log == nil || !f.KeepsLog() {
		return nil, nil
	}
	return json.Marshal(driver.AttemptLog{Attempt: f.Log.Attempt, Snoozed: f.Snooze, Log: StorableText(f.Log.Log)})
}

// DecodeErrors reads a job's errors, a JSON array of error records. An
// empty value reads as none.
func DecodeErrors(stored string) ([]driver.AttemptError, error) {
	errs := []driver.AttemptError{}
	if stored == "" {
		return errs, nil
	}
	if err := json.Unmarshal([]byte(stored), &errs); err != nil {
		return nil, err
	}
	return errs, nil
}

// DecodeProgress reads a progress value, keeping what it can of one that is
// not in the form an engine writes: a value that is not an object reads as
// none.
func DecodeProgress(stored string) *driver.Progress {
	var v struct {
		Step   any             `json:"step"`
		Cursor json.RawMessage `json:"cursor"`
		At     any             `json:"at"`
	}
	if !strings.HasPrefix(strings.TrimSpace(stored), "{") || json.Unmarshal([]byte(stored), &v) != nil {
		return nil
	}
	p := &driver.Progress{Cursor: v.Cursor}
	if s, ok := v.Step.(string); ok {
		p.Step = s
	}
	if s, ok := v.At.(string); ok {
		p.RecordedAt, _ = time.Parse(time.RFC3339Nano, s)
	}
	if string(p.Cursor) == "null" {
		p.Cursor = nil
	}
	return p
}

// DecodeLogs reads a job's logs, skipping entries that are not in the form
// a finalize writes.
func DecodeLogs(stored string) []driver.AttemptLog {
	// Anything but an array (or null) fails to decode into entries.
	var entries []json.RawMessage
	if json.Unmarshal([]byte(stored), &entries) != nil {
		return nil
	}
	var logs []driver.AttemptLog
	for _, raw := range entries {
		var l driver.AttemptLog
		if json.Unmarshal(raw, &l) == nil && l.Log != "" {
			logs = append(logs, l)
		}
	}
	return logs
}

// DecodeTags reads a tags value, a JSON array of strings: a value that is
// not an array reads as no tags, and elements that are not strings are
// skipped, as are empty strings and repeats.
func DecodeTags(stored string) []string {
	if stored == "" {
		return nil
	}
	var elems []json.RawMessage
	_ = json.Unmarshal([]byte(stored), &elems) // anything but an array reads as no tags
	var tags []string
	for _, e := range elems {
		var s string
		if len(e) > 0 && e[0] == '"' && json.Unmarshal(e, &s) == nil && s != "" {
			tags = append(tags, s)
		}
	}
	return jobtags.Distinct(tags)
}
