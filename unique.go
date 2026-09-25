package hopper

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// UniqueConflict says what happens when a unique job already exists.
type UniqueConflict int

// Unique conflict actions.
const (
	// UniqueSkip keeps the existing job. Insert reports Duplicate: true with
	// the existing job.
	UniqueSkip UniqueConflict = iota
	// UniqueReplace updates the existing job's args, metadata, priority,
	// max attempts and schedule, unless it is running. This gives
	// debouncing: inserting again pushes the job back. Insert still reports
	// Duplicate: true.
	UniqueReplace
)

// UniqueOpts makes a job unique among live jobs (those not yet finalized) of
// its kind. The zero value is unique by kind alone: at most one live job of
// the kind at a time.
type UniqueOpts struct {
	// ByArgs includes the args in the key: the fields tagged
	// `hopper:"unique"`, or the whole args if no field is tagged.
	ByArgs bool
	// ByQueue includes the queue in the key.
	ByQueue bool
	// ByPeriod includes the current time, truncated to this period, so that
	// at most one job exists per period. For example 24 * time.Hour allows
	// one job per UTC day.
	ByPeriod time.Duration
	// OnConflict is what to do when the job exists.
	OnConflict UniqueConflict
}

// maxUniqueKey bounds the stored key. Keys are readable text, not hashes.
const maxUniqueKey = 1024

// uniqueKeySeparator joins the key's parts. A newline cannot appear in
// compact JSON output, and queue names may not contain control characters,
// so parts never run together.
const uniqueKeySeparator = "\n"

// key builds the canonical key text for the given insert.
func (o *UniqueOpts) key(args JobArgs, encodedArgs []byte, queue string, now time.Time) (string, error) {
	var parts []string
	if o.ByArgs {
		a, err := uniqueArgs(args, encodedArgs)
		if err != nil {
			return "", err
		}
		parts = append(parts, "a="+string(a))
	}
	if o.ByQueue {
		parts = append(parts, "q="+queue)
	}
	if o.ByPeriod > 0 {
		parts = append(parts, "p="+now.UTC().Truncate(o.ByPeriod).Format(time.RFC3339))
	}
	key := "*"
	if len(parts) > 0 {
		key = strings.Join(parts, uniqueKeySeparator)
	}
	if len(key) > maxUniqueKey {
		return "", fmt.Errorf("hopper: unique key of %d bytes exceeds the %d byte limit", len(key), maxUniqueKey)
	}
	return key, nil
}

// uniqueArgs returns the args' contribution to the key: the tagged fields as
// a JSON object with sorted keys, or the whole encoded args re-encoded
// canonically.
func uniqueArgs(args JobArgs, encoded []byte) ([]byte, error) {
	if fields := taggedFields(reflect.ValueOf(args), nil); fields != nil {
		return json.Marshal(fields)
	}
	var v any
	if err := json.Unmarshal(encoded, &v); err != nil {
		return nil, fmt.Errorf("hopper: unique key needs JSON-encoded args: %w", err)
	}
	return json.Marshal(v)
}

// taggedFields collects the values of fields tagged `hopper:"unique"`,
// keyed by their JSON name, descending into embedded structs. It returns
// nil when no field is tagged.
func taggedFields(v reflect.Value, out map[string]any) map[string]any {
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return out
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return out
	}
	t := v.Type()
	for i := range t.NumField() {
		f := t.Field(i)
		if f.Anonymous {
			// Embedded structs may be unexported types; their exported
			// fields are still reachable.
			out = taggedFields(v.Field(i), out)
			continue
		}
		if !f.IsExported() {
			continue
		}
		tag, ok := f.Tag.Lookup("hopper")
		if !ok || !hasTagOption(tag, "unique") {
			continue
		}
		if out == nil {
			out = map[string]any{}
		}
		out[jsonName(f)] = v.Field(i).Interface()
	}
	return out
}

func hasTagOption(tag, opt string) bool {
	for part := range strings.SplitSeq(tag, ",") {
		if strings.TrimSpace(part) == opt {
			return true
		}
	}
	return false
}

// jsonName is the field's name in its JSON encoding, or its Go name when
// the json tag gives none.
func jsonName(f reflect.StructField) string {
	if tag, ok := f.Tag.Lookup("json"); ok {
		name, _, _ := strings.Cut(tag, ",")
		if name != "" && name != "-" {
			return name
		}
	}
	return f.Name
}
