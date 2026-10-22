package hoppersqlite

import (
	"bytes"
	sqldriver "database/sql/driver"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode/utf16"
	"unicode/utf8"

	"modernc.org/sqlite"
)

// sqliteDriver is modernc's driver with hopper's SQL functions, which
// only this package's connections have: registered on the package-level
// driver, they would reach every connection the process opens.
var sqliteDriver = func() *sqlite.Driver {
	d := &sqlite.Driver{}
	d.MustRegisterDeterministicScalarFunction("hopper_topic_matches", 2, topicMatchesSQL)
	d.MustRegisterDeterministicScalarFunction("hopper_json_contains", 2, jsonContainsSQL)
	return d
}()

// textArg reads a TEXT (or BLOB) argument of a SQL function, and reports
// false for NULL.
func textArg(v sqldriver.Value) (string, bool) {
	switch v := v.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	}
	return "", false
}

// topicMatchesSQL is hopper_topic_matches(pattern, topic): whether an AMQP
// topic pattern matches a topic, NULL if either is.
func topicMatchesSQL(_ *sqlite.FunctionContext, args []sqldriver.Value) (sqldriver.Value, error) {
	pattern, ok := textArg(args[0])
	topic, ok2 := textArg(args[1])
	if !ok || !ok2 {
		return nil, nil
	}
	re, err := topicRegexp(pattern)
	if err != nil {
		return nil, err
	}
	return re.MatchString(topic), nil
}

// topicPatterns caches compiled patterns, which subscriptions, consumers
// and stream reads repeat. It is emptied once it holds maxTopicPatterns, so
// that reads with ever new patterns cannot grow it without bound.
var topicPatterns = struct {
	mu sync.Mutex
	m  map[string]*regexp.Regexp
}{m: map[string]*regexp.Regexp{}}

const maxTopicPatterns = 1024

// topicSpecial are the characters Postgres's hopper_topic_regex escapes
// before it turns the wildcards into regular expressions.
var topicSpecial = regexp.MustCompile(`([.+?^${}()|\[\]\\])`)

// topicRegexp builds the regular expression an AMQP pattern means, as
// Postgres's hopper_topic_regex does (schema v2): words are separated by
// dots, '*' matches one word and '#' zero or more. The steps are the same,
// in the same order, so that a pattern, however odd, matches the same
// topics on either engine; (?s) gives '.' Postgres's meaning, which takes a
// newline too.
func topicRegexp(pattern string) (*regexp.Regexp, error) {
	topicPatterns.mu.Lock()
	defer topicPatterns.mu.Unlock()
	if re, ok := topicPatterns.m[pattern]; ok {
		return re, nil
	}
	expr := topicSpecial.ReplaceAllString(pattern, `\$1`)
	for _, r := range [][2]string{
		{`*`, `[^.]+`},         // one word (before '#', whose rules introduce '.*')
		{`\.#\.`, `(\..*)?\.`}, // a.#.b: zero or more words between
		{`\.#`, `(\..*)?`},     // a.#: the word and anything under it
		{`#\.`, `(.*\.)?`},     // #.b: anything above the word
		{`#`, `.*`},            // #: everything
	} {
		expr = strings.ReplaceAll(expr, r[0], r[1])
	}
	re, err := regexp.Compile(`(?s)^` + expr + `$`)
	if err != nil {
		return nil, fmt.Errorf("hoppersqlite: topic pattern %q: %w", pattern, err)
	}
	if len(topicPatterns.m) >= maxTopicPatterns {
		clear(topicPatterns.m)
	}
	topicPatterns.m[pattern] = re
	return re, nil
}

// jsonContainsSQL is hopper_json_contains(doc, filter): whether the JSON
// document doc contains filter as Postgres's jsonb @> has it, NULL if
// either is. A document that is not JSON contains nothing.
func jsonContainsSQL(_ *sqlite.FunctionContext, args []sqldriver.Value) (sqldriver.Value, error) {
	doc, ok := textArg(args[0])
	filter, ok2 := textArg(args[1])
	if !ok || !ok2 {
		return nil, nil
	}
	want, err := decodeJSON(filter)
	if err != nil {
		return nil, fmt.Errorf("hoppersqlite: metadata filter: %w", err)
	}
	if have, err := decodeJSON(doc); err == nil {
		return jsonContains(have, want, true), nil
	}
	return false, nil
}

// decodeJSON decodes a JSON value, keeping numbers as their text.
func decodeJSON(s string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, fmt.Errorf("text after the JSON value")
	}
	return v, nil
}

// jsonContains reports whether have contains want as jsonb's @> has it: an
// object contains another whose every key it has with a value that
// contains the other's; an array contains another each of whose elements
// some element of it contains, whatever their order and repeats; scalars
// contain the equal scalar, numbers by value. At the top level, an array
// also contains a scalar it has as an element.
func jsonContains(have, want any, top bool) bool {
	switch w := want.(type) {
	case map[string]any:
		h, ok := have.(map[string]any)
		if !ok {
			return false
		}
		for k, wv := range w {
			hv, ok := h[k]
			if !ok || !jsonContains(hv, wv, false) {
				return false
			}
		}
		return true
	case []any:
		h, ok := have.([]any)
		if !ok {
			return false
		}
		for _, wv := range w {
			found := false
			for _, hv := range h {
				if jsonContains(hv, wv, false) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}
	if h, ok := have.([]any); ok && top {
		for _, hv := range h {
			if jsonScalarEqual(hv, want) {
				return true
			}
		}
		return false
	}
	return jsonScalarEqual(have, want)
}

func jsonScalarEqual(a, b any) bool {
	switch a := a.(type) {
	case json.Number:
		b, ok := b.(json.Number)
		if !ok {
			return false
		}
		ca, err := canonicalNumber(string(a))
		cb, err2 := canonicalNumber(string(b))
		return err == nil && err2 == nil && ca == cb
	case string:
		b, ok := b.(string)
		return ok && a == b
	case bool:
		b, ok := b.(bool)
		return ok && a == b
	case nil:
		return b == nil
	}
	return false
}

// decimal is a JSON number as digits and a power of ten: the number is
// digits × 10^exp, with no leading or trailing zero in digits, which is
// empty for zero.
type decimal struct {
	negative bool
	digits   string
	exp      int64
}

func (d decimal) String() string {
	if d.digits == "" {
		return "0"
	}
	sign := ""
	if d.negative {
		sign = "-"
	}
	return sign + d.digits + "e" + strconv.FormatInt(d.exp, 10)
}

// parseDecimal parses a JSON number. Its exponent may be as large as an
// int64 holds, beyond which it is refused.
func parseDecimal(s string) (decimal, error) {
	var d decimal
	if strings.HasPrefix(s, "-") {
		d.negative, s = true, s[1:]
	}
	mantissa, exponent, hasExp := strings.Cut(strings.ToLower(s), "e")
	if hasExp {
		e, err := strconv.ParseInt(strings.TrimPrefix(exponent, "+"), 10, 64)
		if err != nil {
			return decimal{}, fmt.Errorf("number %s: exponent out of range", s)
		}
		d.exp = e
	}
	whole, frac, _ := strings.Cut(mantissa, ".")
	digits := whole + frac
	if d.exp-int64(len(frac)) > d.exp {
		return decimal{}, fmt.Errorf("number %s: exponent out of range", s)
	}
	d.exp -= int64(len(frac))
	digits = strings.TrimLeft(digits, "0")
	trimmed := strings.TrimRight(digits, "0")
	d.exp += int64(len(digits) - len(trimmed))
	d.digits = trimmed
	if d.digits == "" {
		d.negative, d.exp = false, 0
	}
	return d, nil
}

// canonicalNumber is a number's text that equal numbers share.
func canonicalNumber(s string) (string, error) {
	d, err := parseDecimal(s)
	if err != nil {
		return "", err
	}
	return d.String(), nil
}

// Postgres's numeric, which jsonb keeps numbers in, holds up to 131072
// digits before the decimal point and 16383 after it.
const (
	maxNumericWhole    = 131072
	maxNumericFraction = 16383
)

// checkJSONFilter refuses a metadata filter Postgres's jsonb input would,
// so that a listing's filter means the same on either engine: text that is
// not UTF-8, a \u0000 escape, an unpaired surrogate escape, or a number
// beyond numeric's range.
func checkJSONFilter(s string) error {
	if !utf8.ValidString(s) {
		return fmt.Errorf("the metadata filter is not UTF-8")
	}
	if err := checkEscapes(s); err != nil {
		return err
	}
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	for {
		tok, err := dec.Token()
		if err != nil {
			if err.Error() == "EOF" {
				return nil
			}
			return fmt.Errorf("the metadata filter is not JSON: %w", err)
		}
		n, ok := tok.(json.Number)
		if !ok {
			continue
		}
		d, err := parseDecimal(string(n))
		if err != nil {
			return fmt.Errorf("the metadata filter's %w", err)
		}
		if whole := int64(len(d.digits)) + d.exp; d.digits != "" && whole > maxNumericWhole {
			return fmt.Errorf("the metadata filter's number %.20s... is beyond the range of Postgres's numeric", n)
		}
		if d.exp < -maxNumericFraction {
			return fmt.Errorf("the metadata filter's number %.20s... has more decimals than Postgres's numeric holds", n)
		}
	}
}

// checkEscapes refuses a \u0000 escape and an unpaired surrogate escape in
// a JSON text's strings, which decoding would turn into a NUL and U+FFFD.
func checkEscapes(s string) error {
	b := []byte(s)
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' {
			continue
		}
		if i+1 < len(b) && b[i+1] != 'u' {
			i++
			continue
		}
		if i+6 > len(b) {
			return nil // the decoder reports the truncated escape
		}
		r, err := strconv.ParseUint(string(b[i+2:i+6]), 16, 16)
		switch {
		case err != nil:
			// The decoder reports the malformed escape.
		case r == 0:
			return fmt.Errorf("the metadata filter has a \\u0000 escape")
		case utf16.IsSurrogate(rune(r)):
			if r >= 0xdc00 || i+12 > len(b) || !bytes.HasPrefix(b[i+6:], []byte(`\u`)) {
				return fmt.Errorf("the metadata filter has an unpaired surrogate escape")
			}
			low, err := strconv.ParseUint(string(b[i+8:i+12]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return fmt.Errorf("the metadata filter has an unpaired surrogate escape")
			}
			i += 11
		default:
			i += 5
		}
	}
	return nil
}
