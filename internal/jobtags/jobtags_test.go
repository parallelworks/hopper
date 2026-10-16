package jobtags_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/parallelworks/hopper/internal/jobtags"
)

// TestCheckStopsPastMax checks that a long list of distinct tags is refused
// once past Max, without first deduplicating every tag against every other.
func TestCheckStopsPastMax(t *testing.T) {
	tags := make([]string, 10000)
	for i := range tags {
		tags[i] = strconv.Itoa(i)
	}
	if _, err := jobtags.Check(tags); err == nil {
		t.Fatal("Check took more than Max tags")
	}
	if out, ok := jobtags.DistinctAtMost(append(tags[:jobtags.Max:jobtags.Max], tags[:3]...), jobtags.Max); !ok || len(out) != jobtags.Max {
		t.Errorf("DistinctAtMost of Max distinct tags and repeats = %d, %t", len(out), ok)
	}
	// Deduplicating all of them would grow the result to 10,000 tags.
	if n := testing.AllocsPerRun(10, func() { _, _ = jobtags.Check(tags) }); n > 12 {
		t.Errorf("Check of 10000 distinct tags made %v allocations, as if it deduplicated all of them", n)
	}
}

// TestDistinctOfLongRow checks that Distinct, which a scan runs on every
// stored row, costs a row of many distinct tags (only a direct SQL write
// stores one) its length rather than its square.
func TestDistinctOfLongRow(t *testing.T) {
	tags := make([]string, 100000)
	for i := range tags {
		tags[i] = strconv.Itoa(i)
	}
	tags = append(tags, tags[:10]...)
	start := time.Now()
	out := jobtags.Distinct(tags)
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("Distinct of 100,000 tags took %s, as if it compared each with every other", took)
	}
	if len(out) != 100000 || out[0] != "0" || out[99999] != "99999" {
		t.Errorf("Distinct of 100,000 tags and repeats = %d tags", len(out))
	}
}
