package hopper

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule computes the slots of a periodic job.
type Schedule interface {
	// Next returns the first slot strictly after t, or the zero time if
	// there is none within a reasonable horizon.
	Next(t time.Time) time.Time
}

// IntervalSchedule fires every Interval, at slots aligned to the interval
// (every 30 seconds fires at :00 and :30), so slots are the same whichever
// leader computes them.
type IntervalSchedule struct {
	Interval time.Duration
}

// Next implements Schedule.
func (s IntervalSchedule) Next(t time.Time) time.Time {
	return t.Truncate(s.Interval).Add(s.Interval)
}

// CronSchedule is a parsed cron expression. Use ParseCron.
type CronSchedule struct {
	spec                                  string
	second, minute, hour, dom, month, dow uint64
	domStar, dowStar                      bool
	loc                                   *time.Location
}

// ParseCron parses a cron expression.
//
// Standard five fields (minute, hour, day of month, month, day of week) or
// six with a leading seconds field. Each field takes "*", a value, a range
// ("1-5"), a step ("*/15", "1-30/5"), a list ("1,15") and, for months and
// days of the week, three-letter names ("JAN", "MON"). Day of week is 0-6
// with 7 also meaning Sunday. When both day fields are restricted, a day
// matches if either does, as in Vixie cron.
//
// The descriptors @yearly (@annually), @monthly, @weekly, @daily (@midnight)
// and @hourly are accepted, as is "@every <duration>" for an
// IntervalSchedule-like cadence.
//
// Times are evaluated in UTC unless the expression starts with "CRON_TZ=" or
// "TZ=" and an IANA zone ("CRON_TZ=America/Chicago 0 9 * * MON-FRI"), or
// the schedule is given a location with In. Around a daylight-saving change,
// a time in the skipped hour does not occur that day, and a time in the
// repeated hour occurs twice, as in Vixie cron.
func ParseCron(spec string) (*CronSchedule, error) {
	s := &CronSchedule{spec: spec, loc: time.UTC}
	expr := strings.TrimSpace(spec)

	for _, prefix := range []string{"CRON_TZ=", "TZ="} {
		if rest, ok := strings.CutPrefix(expr, prefix); ok {
			zone, after, found := strings.Cut(rest, " ")
			if !found {
				return nil, fmt.Errorf("hopper: cron %q: time zone without an expression", spec)
			}
			loc, err := time.LoadLocation(zone)
			if err != nil {
				return nil, fmt.Errorf("hopper: cron %q: %w", spec, err)
			}
			s.loc = loc
			expr = strings.TrimSpace(after)
			break
		}
	}

	if strings.HasPrefix(expr, "@") {
		switch expr {
		case "@yearly", "@annually":
			expr = "0 0 1 1 *"
		case "@monthly":
			expr = "0 0 1 * *"
		case "@weekly":
			expr = "0 0 * * 0"
		case "@daily", "@midnight":
			expr = "0 0 * * *"
		case "@hourly":
			expr = "0 * * * *"
		default:
			if durText, ok := strings.CutPrefix(expr, "@every "); ok {
				d, err := time.ParseDuration(strings.TrimSpace(durText))
				if err != nil || d <= 0 {
					return nil, fmt.Errorf("hopper: cron %q: invalid @every duration", spec)
				}
				return nil, fmt.Errorf("hopper: cron %q: use hopper.Every(%s, ...) for interval schedules", spec, d)
			}
			return nil, fmt.Errorf("hopper: cron %q: unknown descriptor", spec)
		}
	}

	fields := strings.Fields(expr)
	if len(fields) == 5 {
		fields = append([]string{"0"}, fields...)
	}
	if len(fields) != 6 {
		return nil, fmt.Errorf("hopper: cron %q: expected 5 or 6 fields, got %d", spec, len(fields))
	}
	var err error
	if s.second, _, err = parseCronField(fields[0], 0, 59, nil); err != nil {
		return nil, fmt.Errorf("hopper: cron %q: seconds: %w", spec, err)
	}
	if s.minute, _, err = parseCronField(fields[1], 0, 59, nil); err != nil {
		return nil, fmt.Errorf("hopper: cron %q: minutes: %w", spec, err)
	}
	if s.hour, _, err = parseCronField(fields[2], 0, 23, nil); err != nil {
		return nil, fmt.Errorf("hopper: cron %q: hours: %w", spec, err)
	}
	if s.dom, s.domStar, err = parseCronField(fields[3], 1, 31, nil); err != nil {
		return nil, fmt.Errorf("hopper: cron %q: day of month: %w", spec, err)
	}
	if s.month, _, err = parseCronField(fields[4], 1, 12, monthNames); err != nil {
		return nil, fmt.Errorf("hopper: cron %q: month: %w", spec, err)
	}
	if s.dow, s.dowStar, err = parseCronField(fields[5], 0, 7, dayNames); err != nil {
		return nil, fmt.Errorf("hopper: cron %q: day of week: %w", spec, err)
	}
	// 7 is Sunday too.
	if s.dow&(1<<7) != 0 {
		s.dow |= 1
		s.dow &^= 1 << 7
	}
	return s, nil
}

// In returns a copy of the schedule evaluated in loc.
func (s *CronSchedule) In(loc *time.Location) *CronSchedule {
	c := *s
	c.loc = loc
	return &c
}

// String returns the expression the schedule was parsed from.
func (s *CronSchedule) String() string { return s.spec }

var monthNames = map[string]int{
	"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
	"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

var dayNames = map[string]int{
	"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
}

// parseCronField returns the set of matching values as a bitmask, and
// whether the field is unrestricted ("*" or "?" without a step).
func parseCronField(expr string, lo, hi int, names map[string]int) (bits uint64, star bool, err error) {
	if expr == "" {
		return 0, false, errors.New("empty field")
	}
	for part := range strings.SplitSeq(expr, ",") {
		rangeExpr, stepExpr, hasStep := strings.Cut(part, "/")
		var start, end int
		switch rangeExpr {
		case "*", "?":
			start, end = lo, hi
			if !hasStep {
				star = true
			}
		default:
			a, b, isRange := strings.Cut(rangeExpr, "-")
			if start, err = parseCronValue(a, names); err != nil {
				return 0, false, err
			}
			switch {
			case isRange:
				if end, err = parseCronValue(b, names); err != nil {
					return 0, false, err
				}
			case hasStep:
				end = hi
			default:
				end = start
			}
		}
		step := 1
		if hasStep {
			if step, err = strconv.Atoi(stepExpr); err != nil || step <= 0 {
				return 0, false, fmt.Errorf("invalid step %q", stepExpr)
			}
		}
		if start < lo || end > hi || start > end {
			return 0, false, fmt.Errorf("%q is outside %d-%d", part, lo, hi)
		}
		for i := start; i <= end; i += step {
			bits |= 1 << uint(i)
		}
	}
	return bits, star, nil
}

func parseCronValue(s string, names map[string]int) (int, error) {
	if v, ok := names[strings.ToLower(s)]; ok {
		return v, nil
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid value %q", s)
	}
	return v, nil
}

// Next implements Schedule. It advances field by field, from months down to
// seconds, resetting the lower fields whenever a higher one moves.
func (s *CronSchedule) Next(t time.Time) time.Time {
	loc := s.loc
	t = t.In(loc)
	// The next whole second is the first candidate.
	t = t.Add(time.Second - time.Duration(t.Nanosecond()))
	yearLimit := t.Year() + 5

wrap:
	// reset is set once per pass when a field moves, so that the lower
	// fields start from zero rather than from where t happened to be.
	reset := false
	if t.Year() > yearLimit {
		return time.Time{}
	}
	for s.month&(1<<uint(t.Month())) == 0 {
		if !reset {
			reset = true
			t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, loc)
		}
		t = t.AddDate(0, 1, 0)
		if t.Month() == time.January {
			goto wrap
		}
	}
	for !s.dayMatches(t) {
		if !reset {
			reset = true
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
		}
		t = t.AddDate(0, 0, 1)
		// A DST change can leave the wall clock off midnight.
		if t.Hour() != 0 {
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
		}
		if t.Day() == 1 {
			goto wrap
		}
	}
	// Below the day, truncation subtracts durations rather than rebuilding
	// the wall clock with time.Date: in the hour repeated by a fall-back
	// DST change, time.Date would pick the earlier offset and jump back.
	for s.hour&(1<<uint(t.Hour())) == 0 {
		if !reset {
			reset = true
			t = t.Add(-(time.Duration(t.Minute())*time.Minute + time.Duration(t.Second())*time.Second + time.Duration(t.Nanosecond())))
		}
		t = t.Add(time.Hour)
		if t.Hour() == 0 {
			goto wrap
		}
	}
	for s.minute&(1<<uint(t.Minute())) == 0 {
		if !reset {
			reset = true
			t = t.Add(-(time.Duration(t.Second())*time.Second + time.Duration(t.Nanosecond())))
		}
		t = t.Add(time.Minute)
		if t.Minute() == 0 {
			goto wrap
		}
	}
	for s.second&(1<<uint(t.Second())) == 0 {
		if !reset {
			reset = true
			t = t.Add(-time.Duration(t.Nanosecond()))
		}
		t = t.Add(time.Second)
		if t.Second() == 0 {
			goto wrap
		}
	}
	return t
}

func (s *CronSchedule) dayMatches(t time.Time) bool {
	dom := s.dom&(1<<uint(t.Day())) != 0
	dow := s.dow&(1<<uint(t.Weekday())) != 0
	if s.domStar || s.dowStar {
		return dom && dow
	}
	return dom || dow
}
