package hopper_test

import (
	"testing"
	"time"

	"github.com/parallelworks/hopper"
)

func TestParseCronNext(t *testing.T) {
	t.Parallel()
	chicago, err := time.LoadLocation("America/Chicago")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, time.September, 25, 18, 45, 30, 500, time.UTC) // a Friday
	cases := []struct {
		spec string
		from time.Time
		want []time.Time
	}{
		{"* * * * *", base, []time.Time{
			time.Date(2026, 9, 25, 18, 46, 0, 0, time.UTC),
			time.Date(2026, 9, 25, 18, 47, 0, 0, time.UTC),
		}},
		{"*/15 * * * *", base, []time.Time{
			time.Date(2026, 9, 25, 19, 0, 0, 0, time.UTC),
			time.Date(2026, 9, 25, 19, 15, 0, 0, time.UTC),
		}},
		{"30 2 * * *", base, []time.Time{
			time.Date(2026, 9, 26, 2, 30, 0, 0, time.UTC),
			time.Date(2026, 9, 27, 2, 30, 0, 0, time.UTC),
		}},
		{"0 9 * * MON-FRI", base, []time.Time{
			time.Date(2026, 9, 28, 9, 0, 0, 0, time.UTC), // skips the weekend
			time.Date(2026, 9, 29, 9, 0, 0, 0, time.UTC),
		}},
		{"0 0 1 * *", base, []time.Time{
			time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
		}},
		{"0 0 29 FEB *", base, []time.Time{
			time.Date(2028, 2, 29, 0, 0, 0, 0, time.UTC), // leap year
		}},
		{"0 12 13 * FRI", base, []time.Time{ // both day fields set: either matches
			time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC),
			time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC),
			time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC),
		}},
		{"5,35 8-10 * * *", base, []time.Time{
			time.Date(2026, 9, 26, 8, 5, 0, 0, time.UTC),
			time.Date(2026, 9, 26, 8, 35, 0, 0, time.UTC),
			time.Date(2026, 9, 26, 9, 5, 0, 0, time.UTC),
		}},
		{"*/20 * * * * *", base, []time.Time{ // with seconds
			time.Date(2026, 9, 25, 18, 45, 40, 0, time.UTC),
			time.Date(2026, 9, 25, 18, 46, 0, 0, time.UTC),
		}},
		{"@hourly", base, []time.Time{time.Date(2026, 9, 25, 19, 0, 0, 0, time.UTC)}},
		{"@daily", base, []time.Time{time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)}},
		{"@weekly", base, []time.Time{time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)}},
		{"@monthly", base, []time.Time{time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}},
		{"@yearly", base, []time.Time{time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)}},
		{"0 0 * * 7", base, []time.Time{time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)}}, // 7 is Sunday
		{"CRON_TZ=America/Chicago 0 9 * * *", base, []time.Time{
			time.Date(2026, 9, 26, 9, 0, 0, 0, chicago),
		}},
		// Across the fall-back DST change in Chicago (2026-11-01 02:00 CDT -> 01:00 CST):
		// 01:30 occurs twice, an hour apart, then once a day again.
		{"TZ=America/Chicago 30 1 * * *", time.Date(2026, 10, 31, 12, 0, 0, 0, chicago), []time.Time{
			time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC), // 01:30 CDT
			time.Date(2026, 11, 1, 7, 30, 0, 0, time.UTC), // 01:30 CST
			time.Date(2026, 11, 2, 7, 30, 0, 0, time.UTC), // 01:30 CST
		}},
		// An hourly job keeps an hourly cadence through the change.
		{"TZ=America/Chicago 0 * * * *", time.Date(2026, 11, 1, 0, 30, 0, 0, chicago), []time.Time{
			time.Date(2026, 11, 1, 6, 0, 0, 0, time.UTC),
			time.Date(2026, 11, 1, 7, 0, 0, 0, time.UTC),
			time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC),
		}},
		// Across spring-forward (2026-03-08 02:00 CST -> 03:00 CDT): 02:30 does not exist.
		{"TZ=America/Chicago 30 2 * * *", time.Date(2026, 3, 7, 12, 0, 0, 0, chicago), []time.Time{
			time.Date(2026, 3, 9, 2, 30, 0, 0, chicago),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.spec, func(t *testing.T) {
			s, err := hopper.ParseCron(tc.spec)
			if err != nil {
				t.Fatal(err)
			}
			from := tc.from
			for i, want := range tc.want {
				got := s.Next(from)
				if !got.Equal(want) {
					t.Fatalf("Next #%d from %s = %s, want %s", i, from, got, want)
				}
				from = got
			}
		})
	}
}

func TestParseCronRejects(t *testing.T) {
	t.Parallel()
	for _, spec := range []string{
		"", "* * * *", "* * * * * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "* * * 13 *", "* * * * 8",
		"*/0 * * * *", "5-3 * * * *", "a * * * *", "@fortnightly", "@every 5x", "TZ=Mars/Olympus * * * * *",
		"@every 5m", // intervals go through Every
	} {
		if _, err := hopper.ParseCron(spec); err == nil {
			t.Errorf("ParseCron(%q) succeeded", spec)
		}
	}
}

func TestIntervalScheduleAligns(t *testing.T) {
	t.Parallel()
	s := hopper.IntervalSchedule{Interval: 30 * time.Second}
	from := time.Date(2026, 9, 25, 18, 45, 17, 0, time.UTC)
	if got := s.Next(from); !got.Equal(time.Date(2026, 9, 25, 18, 45, 30, 0, time.UTC)) {
		t.Errorf("Next = %s", got)
	}
	// The same slot whoever computes it.
	if a, b := s.Next(from), s.Next(from.Add(5*time.Second)); !a.Equal(b) {
		t.Errorf("slots differ: %s vs %s", a, b)
	}
}

func TestPeriodicJobValidation(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	workers := hopper.NewWorkers()
	for name, cfg := range map[string]*hopper.Config{
		"bad cron":       {Workers: workers, Periodic: []hopper.PeriodicJob{hopper.Cron("bad", noop{}, nil)}},
		"bad interval":   {Workers: workers, Periodic: []hopper.PeriodicJob{hopper.Every(0, noop{}, nil)}},
		"nil args":       {Workers: workers, Periodic: []hopper.PeriodicJob{hopper.Every(time.Second, nil, nil)}},
		"duplicate name": {Workers: workers, Periodic: []hopper.PeriodicJob{hopper.Every(time.Second, noop{}, nil), hopper.Every(time.Minute, noop{}, nil)}},
		"nil middleware": {Workers: workers, Middleware: []hopper.Middleware{nil}},
	} {
		if _, err := hopper.NewClient(h.d, cfg); err == nil {
			t.Errorf("%s: NewClient succeeded", name)
		}
	}
	ok := &hopper.Config{Workers: workers, Periodic: []hopper.PeriodicJob{
		hopper.Every(time.Second, noop{}, nil),
		hopper.Every(time.Minute, noop{}, &hopper.PeriodicOpts{Name: "slow"}),
	}}
	if _, err := hopper.NewClient(h.d, ok); err != nil {
		t.Errorf("distinct names rejected: %v", err)
	}
}
