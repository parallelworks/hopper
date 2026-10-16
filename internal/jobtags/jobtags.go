// Package jobtags checks a job's tags. The hopper package checks an
// insert's and a listing's tags with it, and hoppertest those of a job it
// runs, so that a test accepts only the tags an insert would.
package jobtags

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// MaxBytes is the longest tag, and Max the most distinct tags a job may
// have. hopper exports them as MaxTagBytes and MaxTags.
const (
	MaxBytes = 255
	Max      = 64
)

// Check validates a job's tags and drops repeats, keeping the first of
// each. It returns nil for none.
func Check(tags []string) ([]string, error) {
	for _, t := range tags {
		if err := CheckOne(t); err != nil {
			return nil, err
		}
	}
	out, ok := DistinctAtMost(tags, Max)
	if !ok {
		return nil, fmt.Errorf("hopper: a job has more than MaxTags (%d) tags", Max)
	}
	return out, nil
}

// Distinct returns tags without repeats, keeping the first of each, or nil
// for none: the tags a job with them has, and those a listing walks.
func Distinct(tags []string) []string {
	out, _ := DistinctAtMost(tags, len(tags))
	return out
}

// DistinctAtMost is Distinct that gives up, reporting false, once it has
// found more than n distinct tags, so that a long list costs a check of n
// tags each rather than one of every tag against every other. Past Max
// distinct tags, which only a row a direct SQL write stored can hold (a
// scan reads it with Distinct), repeats are found with a set, so that such
// a row costs its length rather than its square.
func DistinctAtMost(tags []string, n int) ([]string, bool) {
	var (
		out  []string
		seen map[string]struct{}
	)
	for _, t := range tags {
		if seen != nil {
			if _, ok := seen[t]; ok {
				continue
			}
		} else if slices.Contains(out, t) {
			continue
		}
		if len(out) == n {
			return nil, false
		}
		out = append(out, t)
		switch {
		case seen != nil:
			seen[t] = struct{}{}
		case len(out) > Max:
			seen = make(map[string]struct{}, len(out))
			for _, o := range out {
				seen[o] = struct{}{}
			}
		}
	}
	return out, true
}

// CheckOne validates one tag: 1 to MaxBytes bytes of valid UTF-8 without
// control characters.
func CheckOne(t string) error {
	switch {
	case t == "":
		return errors.New("hopper: a tag is empty")
	case len(t) > MaxBytes:
		return fmt.Errorf("hopper: a tag is %d bytes, longer than MaxTagBytes (%d)", len(t), MaxBytes)
	case !utf8.ValidString(t) || strings.ContainsFunc(t, unicode.IsControl):
		return fmt.Errorf("hopper: tag %q is not valid UTF-8 or contains control characters", t)
	}
	return nil
}
