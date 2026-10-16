package hopper

import (
	"encoding/json"
	"fmt"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/internal/jobtags"
)

// MaxTagBytes (255) is the longest tag InsertOpts.Tags accepts, and MaxTags
// (64) the most distinct tags a job may have.
const (
	MaxTagBytes = jobtags.MaxBytes
	MaxTags     = jobtags.Max
)

// ErrInvalidFilter is the error Jobs yields for a JobFilter that it cannot
// list by, such as an out-of-range priority, Metadata that is not a JSON
// object, an ordering key without a queue, a tag no job could have, more
// distinct TagsAny or TagsAll than MaxTags, Metadata that Postgres's jsonb
// refuses, such as a number beyond its numeric's range or a \u0000 escape,
// or a queue, kind, state or tag in characters the database's encoding lacks.
var ErrInvalidFilter = driver.ErrInvalidFilter

// checkJobFilter rejects a filter the listing could only fail on, or one
// that could never match, with an error that says why. An ordering key
// without a queue is the driver's to refuse, as its listing needs the queue.
func checkJobFilter(f JobFilter) error {
	for _, p := range f.Priorities {
		if !p.valid() {
			return fmt.Errorf("%w: priority %d is out of range 1..4", ErrInvalidFilter, p)
		}
	}
	for _, t := range f.TagsAny {
		if err := jobtags.CheckOne(t); err != nil {
			return fmt.Errorf("%w: %w; no job has such a tag", ErrInvalidFilter, err)
		}
	}
	// Each of TagsAny's tags is an arm of the listing's statement, so their
	// number is bounded as a job's are.
	if _, ok := jobtags.DistinctAtMost(f.TagsAny, MaxTags); !ok {
		return fmt.Errorf("%w: more distinct TagsAny than MaxTags (%d)", ErrInvalidFilter, MaxTags)
	}
	// TagsAll is checked as a job's tags are: no job has a tag an insert
	// refuses, nor more than MaxTags of them.
	if _, err := jobtags.Check(f.TagsAll); err != nil {
		return fmt.Errorf("%w: %w; no job has such tags", ErrInvalidFilter, err)
	}
	if len(f.Metadata) > 0 {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(f.Metadata, &obj); err != nil || obj == nil {
			return fmt.Errorf("%w: Metadata must be a JSON object", ErrInvalidFilter)
		}
	}
	return nil
}
