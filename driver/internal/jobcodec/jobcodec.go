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
	"fmt"
	"math"
	"strconv"
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
		Rescue  string `json:"rescue,omitempty"`
	}{e.Attempt, e.Fence, StorableText(e.Error), StorableText(e.Trace), string(e.Rescue)})
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

// StepsJSON checks a workflow's step names against its n jobs and returns
// them as the JSON object, keyed by job ID, that a workflow row stores, or
// nil if no step is named.
func StepsJSON(steps []string, ids []driver.JobID) ([]byte, error) {
	if len(steps) == 0 {
		return nil, nil
	}
	if len(steps) != len(ids) {
		return nil, fmt.Errorf("%d step names for %d jobs", len(steps), len(ids))
	}
	named := make(map[string]string, len(steps))
	seen := make(map[string]bool, len(steps))
	for i, s := range steps {
		if s == "" {
			continue
		}
		if seen[s] {
			return nil, fmt.Errorf("step name %q is used twice", s)
		}
		seen[s] = true
		named[ids[i].String()] = s
	}
	if len(named) == 0 {
		return nil, nil
	}
	return json.Marshal(named)
}

// DecodeSteps reads the step names a workflow row stores, skipping what is
// not in the form StepsJSON writes.
func DecodeSteps(stored string) map[driver.JobID]string {
	var named map[string]string
	if json.Unmarshal([]byte(stored), &named) != nil || len(named) == 0 {
		return nil
	}
	out := make(map[driver.JobID]string, len(named))
	for k, v := range named {
		if id, err := driver.ParseJobID(k); err == nil {
			out[id] = v
		}
	}
	return out
}

// dependency is a driver.Dependency as a claim reads it, in JSON. A null
// step or state, of an unnamed step or a dependency without a result,
// leaves the field empty.
type dependency struct {
	ID     driver.JobID    `json:"id"`
	Step   string          `json:"step"`
	State  driver.JobState `json:"state"`
	Output json.RawMessage `json:"output"`
}

// DecodeDependencies reads a claim's JSON array of dependencies, or none
// if it is not one.
func DecodeDependencies(stored string) []driver.Dependency {
	var deps []dependency
	if stored == "" || json.Unmarshal([]byte(stored), &deps) != nil || len(deps) == 0 {
		return nil
	}
	out := make([]driver.Dependency, len(deps))
	for i, d := range deps {
		out[i] = driver.Dependency(d)
		// A JSON null reads as no output, as on SQLite.
		if string(d.Output) == "null" {
			out[i].Output = nil
		}
	}
	return out
}

// CheckDependencies checks a workflow's edges against its n jobs, and
// returns which jobs have dependencies, to insert pending. An edge given
// twice must be in the same group both times: the engines keep one row per
// pair, the first, while EdgesJSON records every edge given.
func CheckDependencies(deps []driver.JobDependency, n int) ([]bool, error) {
	pending := make([]bool, n)
	anyOf := false
	for _, d := range deps {
		if d.Job < 0 || d.Job >= n || d.DependsOn < 0 || d.DependsOn >= n || d.Job == d.DependsOn {
			return nil, fmt.Errorf("dependency %d -> %d is out of range", d.Job, d.DependsOn)
		}
		if d.AnyOf < 0 || d.AnyOf > math.MaxInt16 {
			return nil, fmt.Errorf("dependency %d -> %d has any-of group %d, out of range", d.Job, d.DependsOn, d.AnyOf)
		}
		pending[d.Job] = true
		anyOf = anyOf || d.AnyOf != 0
	}
	if anyOf {
		groups := make(map[[2]int]int, len(deps))
		policies := map[[2]int]string{}
		for _, d := range deps {
			pair := [2]int{d.Job, d.DependsOn}
			if g, seen := groups[pair]; seen && g != d.AnyOf {
				return nil, fmt.Errorf("dependency %d -> %d is given in two any-of groups, or in one and as an ordinary edge", d.Job, d.DependsOn)
			}
			groups[pair] = d.AnyOf
			if d.AnyOf == 0 {
				continue
			}
			group, policy := [2]int{d.Job, d.AnyOf}, EdgePolicy(d)
			if p, seen := policies[group]; seen && p != policy {
				return nil, fmt.Errorf("any-of group %d of job %d has edges with different OnFailure", d.AnyOf, d.Job)
			}
			policies[group] = policy
		}
	}
	return pending, nil
}

// EdgePolicy is what an edge stores as its on_failure: the dependent's
// policy, cancel by default, and for an edge of an any-of group, that
// policy prefixed with "any_". A group's edges never store "cancel", which
// cancels the dependent as soon as the one job fails, so an engine of a
// release before any-of groups, which knows only "cancel" and "ignore",
// leaves them alone, as edges of a dependent that runs once they have all
// finished.
func EdgePolicy(d driver.JobDependency) string {
	policy := d.OnFailure
	if policy == "" {
		policy = driver.DependencyCancel
	}
	if d.AnyOf != 0 {
		return "any_" + string(policy)
	}
	return string(policy)
}

// EdgesJSON returns a workflow's edges as the JSON array a workflow row
// stores for WorkflowGet: a pair of job IDs per edge, the dependent's
// first, and for an edge of an any-of group its group number after them,
// as a string. A reader of a release before any-of groups decodes the pair
// and drops the number.
func EdgesJSON(deps []driver.JobDependency, ids []driver.JobID) ([]byte, error) {
	edges := make([][]string, len(deps))
	for i, d := range deps {
		edges[i] = []string{ids[d.Job].String(), ids[d.DependsOn].String()}
		if d.AnyOf != 0 {
			edges[i] = append(edges[i], strconv.Itoa(d.AnyOf))
		}
	}
	return json.Marshal(edges)
}

// DecodeEdges reads the edges EdgesJSON writes.
func DecodeEdges(stored string) ([]driver.JobEdge, error) {
	var edges [][]string
	if err := json.Unmarshal([]byte(stored), &edges); err != nil {
		return nil, err
	}
	out := make([]driver.JobEdge, len(edges))
	for i, e := range edges {
		if len(e) < 2 {
			return nil, fmt.Errorf("edge %d has %d elements", i, len(e))
		}
		var err error
		if out[i].Job, err = driver.ParseJobID(e[0]); err != nil {
			return nil, err
		}
		if out[i].DependsOn, err = driver.ParseJobID(e[1]); err != nil {
			return nil, err
		}
		if len(e) > 2 {
			if out[i].AnyOf, err = strconv.Atoi(e[2]); err != nil {
				return nil, fmt.Errorf("edge %d: any-of group: %w", i, err)
			}
		}
	}
	return out, nil
}
