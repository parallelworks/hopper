package hopper_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
)

type taggedDefaults struct{}

func (taggedDefaults) Kind() string { return "tagged_defaults" }
func (taggedDefaults) InsertOpts() hopper.InsertOpts {
	return hopper.InsertOpts{Tags: []string{"default"}}
}

func TestInsertTags(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	c := h.client(&hopper.Config{})

	res, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{Tags: []string{"b", "a", "b", "tenant:acme"}})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"b", "a", "tenant:acme"}; !slices.Equal(res.Job.Tags, want) {
		t.Errorf("tags = %q, want %q (repeats dropped, order kept)", res.Job.Tags, want)
	}
	res, err = c.Insert(ctx, taggedDefaults{}, nil)
	if err != nil || !slices.Equal(res.Job.Tags, []string{"default"}) {
		t.Errorf("default tags = %+v, %v", res, err)
	}
	res, err = c.Insert(ctx, taggedDefaults{}, &hopper.InsertOpts{Tags: []string{"mine"}})
	if err != nil || !slices.Equal(res.Job.Tags, []string{"mine"}) {
		t.Errorf("tags over defaults = %+v, %v", res, err)
	}
	res, err = c.Insert(ctx, noop{}, &hopper.InsertOpts{Tags: []string{}})
	if err != nil || res.Job.Tags != nil {
		t.Errorf("empty tags = %+v, %v", res, err)
	}

	for name, tags := range map[string][]string{
		"empty":        {"ok", ""},
		"too long":     {strings.Repeat("x", hopper.MaxTagBytes+1)},
		"control":      {"line\nbreak"},
		"nul":          {"a\x00b"},
		"invalid utf8": {"\xff"},
	} {
		if _, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{Tags: tags}); err == nil {
			t.Errorf("%s tag inserted", name)
		}
	}
	many := make([]string, hopper.MaxTags+1)
	for i := range many {
		many[i] = strconv.Itoa(i)
	}
	if _, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{Tags: many}); err == nil {
		t.Error("more than MaxTags tags inserted")
	}
	// Repeats do not count toward the limit.
	many[hopper.MaxTags] = "0"
	if res, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{Tags: many}); err != nil || len(res.Job.Tags) != hopper.MaxTags {
		t.Errorf("MaxTags tags with a repeat = %+v, %v", res, err)
	}
	// 127 two-byte runes and one more byte: MaxTagBytes exactly.
	if _, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{Tags: []string{strings.Repeat("é", hopper.MaxTagBytes/2) + "x"}}); err != nil {
		t.Errorf("a tag of MaxTagBytes bytes: %v", err)
	}
}

func TestJobsFilters(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	c := h.client(&hopper.Config{})
	insert := func(n int, opts hopper.InsertOpts) hopper.JobID {
		t.Helper()
		res, err := c.Insert(ctx, noop{N: n}, &opts)
		if err != nil {
			t.Fatal(err)
		}
		return res.Job.ID
	}
	a := insert(1, hopper.InsertOpts{Tags: []string{"x", "y"}, Priority: hopper.PriorityHigh, Metadata: json.RawMessage(`{"tenant": "acme"}`)})
	b := insert(2, hopper.InsertOpts{Tags: []string{"y"}, Priority: hopper.PriorityLow, Metadata: json.RawMessage(`{"tenant": "other"}`)})
	cID := insert(3, hopper.InsertOpts{})
	list := func(f hopper.JobFilter) []hopper.JobID {
		t.Helper()
		var ids []hopper.JobID
		for job, err := range c.Jobs(ctx, f) {
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, job.ID)
		}
		return ids
	}
	for name, c := range map[string]struct {
		filter hopper.JobFilter
		want   []hopper.JobID
	}{
		"all":        {hopper.JobFilter{TagsAll: []string{"x", "y"}}, []hopper.JobID{a}},
		"any":        {hopper.JobFilter{TagsAny: []string{"x", "y"}}, []hopper.JobID{a, b}},
		"metadata":   {hopper.JobFilter{Metadata: json.RawMessage(`{"tenant": "other"}`)}, []hopper.JobID{b}},
		"priorities": {hopper.JobFilter{Priorities: []hopper.Priority{hopper.PriorityLow, hopper.PriorityNormal}}, []hopper.JobID{b, cID}},
		"ids":        {hopper.JobFilter{IDs: []hopper.JobID{cID, a}}, []hopper.JobID{a, cID}},
		// Escapes that jsonb takes, a surrogate pair among them.
		"escaped metadata": {hopper.JobFilter{Metadata: json.RawMessage(`{"tenant": "\u0061cme"}`)}, []hopper.JobID{a}},
		"surrogate pair":   {hopper.JobFilter{Metadata: json.RawMessage(`{"tenant": "\ud83d\ude00"}`)}, nil},
	} {
		// Jobs inserted in one millisecond can list in either order.
		got, want := list(c.filter), slices.Clone(c.want)
		byID := func(x, y hopper.JobID) int { return strings.Compare(x.String(), y.String()) }
		slices.SortFunc(got, byID)
		slices.SortFunc(want, byID)
		if !slices.Equal(got, want) {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	tooMany := make([]string, hopper.MaxTags+1)
	for i := range tooMany {
		tooMany[i] = strconv.Itoa(i)
	}
	for name, f := range map[string]hopper.JobFilter{
		"priority out of range": {Priorities: []hopper.Priority{5}},
		"metadata array":        {Metadata: json.RawMessage(`[1]`)},
		"metadata null":         {Metadata: json.RawMessage(`null`)},
		"metadata invalid":      {Metadata: json.RawMessage(`{`)},
		"empty tag":             {TagsAny: []string{""}},
		"tag with NUL":          {TagsAll: []string{"a\x00"}},
		"more than MaxTags":     {TagsAll: tooMany},
		"more than MaxTags any": {TagsAny: tooMany},
		"ordering key alone":    {OrderingKey: "k"},
		"metadata not UTF-8":    {Metadata: json.RawMessage("{\"a\": \"\xff\"}")},
		// Valid JSON that jsonb refuses.
		"metadata with NUL":            {Metadata: json.RawMessage(`{"a": "\u0000"}`)},
		"metadata with high surrogate": {Metadata: json.RawMessage(`{"a": "\ud800x"}`)},
		"metadata with low surrogate":  {Metadata: json.RawMessage(`{"\udc00": 1}`)},
		"metadata number too large":    {Metadata: json.RawMessage(`{"a": 1e1000000}`)},
		"metadata number too precise":  {Metadata: json.RawMessage(`{"a": 1e-1000000}`)},
	} {
		failed := false
		for _, err := range c.Jobs(ctx, f) {
			failed = errors.Is(err, hopper.ErrInvalidFilter)
			break
		}
		if !failed {
			t.Errorf("%s: listed without ErrInvalidFilter", name)
		}
	}
}

type tagReader struct {
	hopper.WorkerDefaults[noop]
	tags chan []string
}

func (w *tagReader) Work(_ context.Context, job *hopper.Job[noop]) error {
	w.tags <- job.Tags
	return nil
}

func TestWorkerSeesTags(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	ctx := context.Background()
	workers := hopper.NewWorkers()
	w := &tagReader{tags: make(chan []string, 1)}
	hopper.AddWorker(workers, w)
	c := h.started(workers, 1)
	if _, err := c.Insert(ctx, noop{}, &hopper.InsertOpts{Tags: []string{"nightly"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case tags := <-w.tags:
		if !slices.Equal(tags, []string{"nightly"}) {
			t.Errorf("worker saw tags %q", tags)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("job did not run")
	}
}
