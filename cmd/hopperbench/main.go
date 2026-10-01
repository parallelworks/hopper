// Command hopperbench measures hopper's hot paths against a real database and
// prints machine-readable results, one JSON object per scenario.
//
//	hopperbench -database-url postgres://... [-scenarios throughput,insert,copy,latency] [-jobs 50000] [-clients 4]
//	hopperbench -compare base.jsonl,head.jsonl [-threshold 0.10]
//
// It works in a schema of its own ("hopperbench"), which it creates and drops,
// so it can point at any database. Numbers depend on the hardware, the
// Postgres configuration and the network between them; compare runs made on
// the same setup, such as a PR branch against canary. The -compare mode does
// that: it reads two files of results, averages the faster half of the runs
// of each scenario, and exits non-zero if the second is slower than the first
// by more than the threshold on any scenario. Noise on a shared machine only
// ever slows a run down, so the faster half is the better estimate; averaging
// it, rather than taking the single best run, keeps one lucky run from
// deciding the outcome.
//
// Scenarios:
//
//	throughput  no-op jobs preloaded, then drained by -clients clients
//	mixed       as throughput, with all four priorities interleaved
//	scheduled   as throughput, with jobs scheduled up to 500ms in the future
//	retry       as throughput, with every job failing its first attempt
//	insert      InsertMany in batches of 100 (the unnest statement)
//	copy        InsertMany in batches of 5000 (the COPY path)
//	latency     commit-to-Work on an idle queue, local wake-up
//	loaded      commit-to-Work while -clients clients work a stream of inserts at -rate jobs/s for -seconds;
//	            with -report N it also prints one "loaded-window" line every N seconds (for soaks)
//
// -history preloads that many rows of history first, to measure the live
// table's independence from accumulated history.
package main

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver/hopperpgx"
	"github.com/parallelworks/hopper/hoppermigrate"
)

type benchArgs struct {
	N int `json:"n"`
}

func (benchArgs) Kind() string { return "bench" }

type result struct {
	Scenario   string  `json:"scenario"`
	Jobs       int     `json:"jobs"`
	Clients    int     `json:"clients,omitempty"`
	Workers    int     `json:"workers_per_client,omitempty"`
	History    int     `json:"history_rows,omitempty"`
	Seconds    float64 `json:"seconds"`
	JobsPerSec float64 `json:"jobs_per_sec,omitempty"`
	P50Millis  float64 `json:"p50_ms,omitempty"`
	P99Millis  float64 `json:"p99_ms,omitempty"`
	MaxMillis  float64 `json:"max_ms,omitempty"`
}

func main() {
	var (
		url       = flag.String("database-url", os.Getenv("HOPPER_TEST_DATABASE_URL"), "Postgres URL (default $HOPPER_TEST_DATABASE_URL)")
		scenarios = flag.String("scenarios", "throughput,insert,copy,latency", "comma-separated scenarios to run")
		jobs      = flag.Int("jobs", 50000, "jobs per throughput scenario")
		clients   = flag.Int("clients", 4, "clients working the throughput scenarios")
		workers   = flag.Int("workers", 100, "MaxWorkers per client")
		samples   = flag.Int("samples", 200, "latency samples")
		history   = flag.Int("history", 0, "rows of history to preload")
		rate      = flag.Int("rate", 40000, "insert rate of the loaded scenario, jobs/s")
		seconds   = flag.Int("seconds", 10, "duration of the loaded scenario")
		report    = flag.Int("report", 0, "with loaded: print a latency and rate line every N seconds (0 = off)")
		schema    = flag.String("schema", "hopperbench", "schema to create, use and drop")
		compare   = flag.String("compare", "", "compare two result files, base,head, instead of running")
		threshold = flag.Float64("threshold", 0.10, "with -compare: the fraction by which head may be slower than base")
	)
	flag.Parse()
	if *compare != "" {
		if err := runCompare(os.Stdout, *compare, *threshold); err != nil {
			fmt.Fprintln(os.Stderr, "hopperbench:", err)
			os.Exit(1)
		}
		return
	}
	if *url == "" {
		fmt.Fprintln(os.Stderr, "hopperbench: -database-url or HOPPER_TEST_DATABASE_URL is required")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err := run(ctx, *url, *schema, strings.Split(*scenarios, ","), options{
		jobs: *jobs, clients: *clients, workers: *workers, samples: *samples, history: *history, rate: *rate, seconds: *seconds, report: *report,
	})
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "hopperbench:", err)
		os.Exit(1)
	}
}

type options struct {
	jobs, clients, workers, samples, history, rate, seconds, report int
}

func run(ctx context.Context, url, schema string, scenarios []string, opts options) error {
	pool, cleanup, err := openSchema(ctx, url, schema)
	if err != nil {
		return err
	}
	defer cleanup()
	d := hopperpgx.NewWithConfig(pool, &hopperpgx.Config{Schema: schema})
	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: slog.New(slog.DiscardHandler)}); err != nil {
		return err
	}
	b := &bench{pool: pool, d: d, opts: opts, logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))}

	// The first clients to start elect a leader that creates the history
	// partitions, which briefly blocks finalizes; warm up so no measured
	// scenario pays for it.
	if _, err := b.drain(ctx, "warmup", b.params(2000, nil), noopWorker); err != nil {
		return fmt.Errorf("warmup: %w", err)
	}

	for _, s := range scenarios {
		var (
			r   result
			err error
		)
		switch strings.TrimSpace(s) {
		case "throughput":
			r, err = b.drain(ctx, "throughput", b.params(opts.jobs, nil), noopWorker)
		case "mixed":
			r, err = b.drain(ctx, "mixed", b.params(opts.jobs, func(i int, o *hopper.InsertOpts) {
				o.Priority = hopper.Priority(i%4 + 1)
			}), noopWorker)
		case "scheduled":
			start := time.Now()
			r, err = b.drain(ctx, "scheduled", b.params(opts.jobs, func(_ int, o *hopper.InsertOpts) {
				o.ScheduledAt = start.Add(time.Duration(rand.IntN(500)) * time.Millisecond)
			}), noopWorker)
		case "retry":
			r, err = b.drain(ctx, "retry", b.params(opts.jobs, nil), failOnceWorker)
		case "insert":
			r, err = b.insert(ctx, opts.jobs, 100)
		case "copy":
			r, err = b.insert(ctx, opts.jobs, 5000)
		case "latency":
			r, err = b.latency(ctx, opts.samples)
		case "loaded":
			r, err = b.loaded(ctx, opts.rate, time.Duration(opts.seconds)*time.Second)
		default:
			return fmt.Errorf("unknown scenario %q", s)
		}
		if err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
		r.History = opts.history
		if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
			return err
		}
	}
	return nil
}

func openSchema(ctx context.Context, url, schema string) (*pgxpool.Pool, func(), error) {
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		return nil, nil, err
	}
	ident := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+ident+" CASCADE"); err != nil {
		return nil, nil, err
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+ident); err != nil {
		return nil, nil, err
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, nil, err
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 64
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		pool.Close()
		ctx := context.Background()
		_, _ = admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+ident+" CASCADE")
		_ = admin.Close(ctx)
	}
	return pool, cleanup, nil
}

type bench struct {
	pool   *pgxpool.Pool
	d      *hopperpgx.Driver
	opts   options
	logger *slog.Logger
}

// reset empties the tables and preloads -history rows of history, spread
// over the last day into hourly partitions as a running system would have
// them, so finalizes during the run land in the current hour's small
// partition rather than in a large DEFAULT one.
func (b *bench) reset(ctx context.Context) error {
	if _, err := b.pool.Exec(ctx, "TRUNCATE hopper_jobs, hopper_job_history, hopper_clients, hopper_leader, hopper_periodic"); err != nil {
		return err
	}
	if b.opts.history > 0 {
		if _, err := b.pool.Exec(ctx, `
			DO $$
			DECLARE h int; start timestamptz;
			BEGIN
			  FOR h IN 0..24 LOOP
			    start := (date_trunc('hour', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') - make_interval(hours => h);
			    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF hopper_job_history_completed FOR VALUES FROM (%L) TO (%L)',
			      'hopper_job_history_completed_' || to_char(start AT TIME ZONE 'UTC', 'YYYYMMDDHH24'), start, start + interval '1 hour');
			  END LOOP;
			END $$`); err != nil {
			return fmt.Errorf("preload partitions: %w", err)
		}
		_, err := b.pool.Exec(ctx, `
			INSERT INTO hopper_job_history (id, seq, kind, queue, state, priority, attempt, max_attempts, scheduled_at,
			  attempted_at, attempted_by, args, metadata, errors, await, created_at, finalized_at)
			SELECT hopper_uuidv7(), i, 'preloaded', 'default', 'completed', 2, 1, 25, now(), now(), -1, '{}', '{}', '[]', false, now(),
			  now() - (random() * interval '23 hours')
			FROM generate_series(1, $1) AS i`, b.opts.history)
		if err != nil {
			return fmt.Errorf("preload history: %w", err)
		}
	}
	return nil
}

func (b *bench) params(n int, customize func(i int, o *hopper.InsertOpts)) []hopper.InsertParams {
	params := make([]hopper.InsertParams, n)
	for i := range params {
		params[i] = hopper.InsertParams{Args: benchArgs{N: i}}
		if customize != nil {
			opts := &hopper.InsertOpts{}
			customize(i, opts)
			params[i].Opts = opts
		}
	}
	return params
}

func noopWorker(context.Context, *hopper.Job[benchArgs]) error { return nil }

var errFirstAttempt = errors.New("first attempt")

// failOnceWorker fails every first attempt, so each job is finalized twice.
func failOnceWorker(_ context.Context, job *hopper.Job[benchArgs]) error {
	if job.Attempt == 1 {
		return errFirstAttempt
	}
	return nil
}

// immediateRetry is a worker whose failed attempts retry at once.
type immediateRetry struct {
	hopper.WorkerDefaults[benchArgs]
	fn func(context.Context, *hopper.Job[benchArgs]) error
}

func (w immediateRetry) Work(ctx context.Context, job *hopper.Job[benchArgs]) error {
	return w.fn(ctx, job)
}

func (immediateRetry) NextRetry(*hopper.Job[benchArgs]) time.Time { return time.Now() }

// drain preloads jobs, then measures how long -clients clients take to
// empty the queue.
func (b *bench) drain(ctx context.Context, name string, params []hopper.InsertParams, work func(context.Context, *hopper.Job[benchArgs]) error) (result, error) {
	if err := b.reset(ctx); err != nil {
		return result{}, err
	}
	inserter, err := hopper.NewClient(b.d, &hopper.Config{Logger: b.logger})
	if err != nil {
		return result{}, err
	}
	defer inserter.Stop(ctx) //nolint:errcheck // flushes pending notifications
	for i := 0; i < len(params); i += 10000 {
		if _, err := inserter.InsertMany(ctx, params[i:min(i+10000, len(params))]); err != nil {
			return result{}, err
		}
	}

	ws := hopper.NewWorkers()
	hopper.AddWorker(ws, immediateRetry{fn: work})
	cs := make([]*hopper.Client[pgx.Tx], b.opts.clients)
	for i := range cs {
		c, err := hopper.NewClient(b.d, &hopper.Config{
			Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: b.opts.workers}},
			Workers: ws,
			Logger:  b.logger,
		})
		if err != nil {
			return result{}, err
		}
		cs[i] = c
	}
	start := time.Now()
	for _, c := range cs {
		if err := c.Start(ctx); err != nil {
			return result{}, err
		}
	}
	for {
		var live int
		if err := b.pool.QueryRow(ctx, "SELECT count(*) FROM hopper_jobs").Scan(&live); err != nil {
			return result{}, err
		}
		if live == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	elapsed := time.Since(start)
	for _, c := range cs {
		if err := c.Stop(ctx); err != nil {
			return result{}, err
		}
	}
	var done int
	if err := b.pool.QueryRow(ctx, "SELECT count(*) FROM hopper_job_history WHERE state = 'completed' AND kind = 'bench'").Scan(&done); err != nil {
		return result{}, err
	}
	if done != len(params) {
		return result{}, fmt.Errorf("%d of %d jobs completed", done, len(params))
	}
	return result{
		Scenario: name, Jobs: len(params), Clients: b.opts.clients, Workers: b.opts.workers,
		Seconds: elapsed.Seconds(), JobsPerSec: float64(len(params)) / elapsed.Seconds(),
	}, nil
}

// insert measures InsertMany in batches of the given size: below the COPY
// threshold that is the unnest statement, above it the COPY path.
func (b *bench) insert(ctx context.Context, jobs, batch int) (result, error) {
	if err := b.reset(ctx); err != nil {
		return result{}, err
	}
	c, err := hopper.NewClient(b.d, &hopper.Config{Logger: b.logger})
	if err != nil {
		return result{}, err
	}
	defer c.Stop(ctx) //nolint:errcheck // flushes pending notifications
	params := b.params(jobs, nil)
	// Eight concurrent inserters, as an application with several replicas would have.
	const inserters = 8
	var wg sync.WaitGroup
	errs := make([]error, inserters)
	start := time.Now()
	for w := range inserters {
		wg.Go(func() {
			for i := w * batch; i < len(params); i += inserters * batch {
				if _, err := c.InsertMany(ctx, params[i:min(i+batch, len(params))]); err != nil {
					errs[w] = err
					return
				}
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	for _, err := range errs {
		if err != nil {
			return result{}, err
		}
	}
	name := "insert"
	if batch >= 256 {
		name = "copy"
	}
	return result{Scenario: name, Jobs: jobs, Seconds: elapsed.Seconds(), JobsPerSec: float64(jobs) / elapsed.Seconds()}, nil
}

// latency measures commit-to-Work on an idle queue, with the insert coming
// from the working client itself (local wake-up).
func (b *bench) latency(ctx context.Context, samples int) (result, error) {
	if err := b.reset(ctx); err != nil {
		return result{}, err
	}
	started := make(chan time.Time, 1)
	ws := hopper.NewWorkers()
	hopper.AddWorkFunc(ws, func(context.Context, *hopper.Job[benchArgs]) error {
		started <- time.Now()
		return nil
	})
	c, err := hopper.NewClient(b.d, &hopper.Config{
		Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: 10}},
		Workers: ws,
		Logger:  b.logger,
	})
	if err != nil {
		return result{}, err
	}
	if err := c.Start(ctx); err != nil {
		return result{}, err
	}
	defer c.Stop(ctx) //nolint:errcheck // best effort

	lat := make([]time.Duration, 0, samples)
	begin := time.Now()
	for i := range samples {
		t0 := time.Now()
		if _, err := c.Insert(ctx, benchArgs{N: i}, nil); err != nil {
			return result{}, err
		}
		select {
		case t1 := <-started:
			lat = append(lat, t1.Sub(t0))
		case <-time.After(10 * time.Second):
			return result{}, fmt.Errorf("job %d not picked up", i)
		}
		time.Sleep(5 * time.Millisecond) // let the queue go idle again
	}
	slices.Sort(lat)
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	return result{
		Scenario: "latency", Jobs: samples, Seconds: time.Since(begin).Seconds(),
		P50Millis: ms(lat[len(lat)/2]), P99Millis: ms(lat[len(lat)*99/100]), MaxMillis: ms(lat[len(lat)-1]),
	}, nil
}

// loaded measures commit-to-Work while the clients work a steady stream of
// inserts at the given rate, with every thousandth job sampled. Inserts go
// in batches of 100 from a separate inserter; a sample's clock starts before
// its batch is sent, so the figure includes the batch insert itself.
func (b *bench) loaded(ctx context.Context, rate int, duration time.Duration) (result, error) {
	if err := b.reset(ctx); err != nil {
		return result{}, err
	}
	const batch, every = 100, 1000
	var (
		mu      sync.Mutex
		sent    = map[int]time.Time{}
		lat     []time.Duration
		worked  int
		samples int
	)
	ws := hopper.NewWorkers()
	hopper.AddWorkFunc(ws, func(_ context.Context, job *hopper.Job[benchArgs]) error {
		if job.Args.N%every != 0 {
			return nil
		}
		t1 := time.Now()
		mu.Lock()
		defer mu.Unlock()
		if t0, ok := sent[job.Args.N]; ok {
			lat = append(lat, t1.Sub(t0))
			delete(sent, job.Args.N)
		}
		return nil
	})
	cs := make([]*hopper.Client[pgx.Tx], b.opts.clients)
	for i := range cs {
		c, err := hopper.NewClient(b.d, &hopper.Config{
			Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: b.opts.workers}},
			Workers: ws,
			Logger:  b.logger,
		})
		if err != nil {
			return result{}, err
		}
		if err := c.Start(ctx); err != nil {
			return result{}, err
		}
		defer c.Stop(ctx) //nolint:errcheck // best effort
		cs[i] = c
	}
	inserter, err := hopper.NewClient(b.d, &hopper.Config{Logger: b.logger})
	if err != nil {
		return result{}, err
	}
	defer inserter.Stop(ctx) //nolint:errcheck // flushes pending notifications

	// Pace batches with a ticker; several inserters share the schedule so
	// one slow batch does not stall the stream.
	interval := time.Duration(float64(time.Second) * batch / float64(rate))
	ticks := make(chan int, 64)
	ticks2 := make(chan struct{}) // closed when inserting is done, ends reporting
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for w := range errs {
		wg.Go(func() {
			for n := range ticks {
				params := b.params(batch, nil)
				for i := range params {
					params[i].Args = benchArgs{N: n + i}
				}
				if n%every == 0 {
					mu.Lock()
					sent[n] = time.Now()
					mu.Unlock()
				}
				if _, err := inserter.InsertMany(ctx, params); err != nil {
					errs[w] = err
					return
				}
				mu.Lock()
				worked += batch
				mu.Unlock()
			}
		})
	}
	start := time.Now()
	// Windowed reporting for long runs: the samples and jobs since the
	// last report, so drift over hours shows up as it happens.
	reportDone := make(chan struct{})
	if b.opts.report > 0 {
		go func() {
			defer close(reportDone)
			t := time.NewTicker(time.Duration(b.opts.report) * time.Second)
			defer t.Stop()
			seen, workedAt, last := 0, 0, time.Now()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticks2:
					return
				case now := <-t.C:
					mu.Lock()
					window := slices.Clone(lat[seen:])
					seen = len(lat)
					n := worked - workedAt
					workedAt = worked
					mu.Unlock()
					r := result{Scenario: "loaded-window", Jobs: n, Seconds: now.Sub(last).Seconds(), JobsPerSec: float64(n) / now.Sub(last).Seconds()}
					last = now
					if len(window) > 0 {
						slices.Sort(window)
						ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
						r.P50Millis, r.P99Millis, r.MaxMillis = ms(window[len(window)/2]), ms(window[len(window)*99/100]), ms(window[len(window)-1])
					}
					_ = json.NewEncoder(os.Stdout).Encode(r)
				}
			}
		}()
	} else {
		close(reportDone)
	}
	ticker := time.NewTicker(interval)
	for n := 0; time.Since(start) < duration; n += batch {
		<-ticker.C
		ticks <- n
	}
	ticker.Stop()
	close(ticks)
	wg.Wait()
	close(ticks2)
	<-reportDone
	for _, err := range errs {
		if err != nil {
			return result{}, err
		}
	}
	// Let the backlog drain before reading the samples.
	for {
		var live int
		if err := b.pool.QueryRow(ctx, "SELECT count(*) FROM hopper_jobs").Scan(&live); err != nil {
			return result{}, err
		}
		if live == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	elapsed := time.Since(start)
	mu.Lock()
	defer mu.Unlock()
	samples = len(lat)
	if samples == 0 {
		return result{}, errors.New("no latency samples")
	}
	slices.Sort(lat)
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	return result{
		Scenario: "loaded", Jobs: worked, Clients: b.opts.clients, Workers: b.opts.workers,
		Seconds: elapsed.Seconds(), JobsPerSec: float64(worked) / elapsed.Seconds(),
		P50Millis: ms(lat[samples/2]), P99Millis: ms(lat[samples*99/100]), MaxMillis: ms(lat[samples-1]),
	}, nil
}

// runCompare reads two files of results and fails if head is slower than
// base by more than threshold on any scenario, using the faster half of the
// runs of each.
func runCompare(out *os.File, files string, threshold float64) error {
	names := strings.Split(files, ",")
	if len(names) != 2 {
		return errors.New("-compare takes two files: base,head")
	}
	base, err := readResults(names[0])
	if err != nil {
		return err
	}
	head, err := readResults(names[1])
	if err != nil {
		return err
	}
	var failed []string
	for _, scenario := range slices.Sorted(maps.Keys(base)) {
		b, h := base[scenario], head[scenario]
		if h == nil {
			fmt.Fprintf(out, "%-12s base %s; head: missing\n", scenario, b)
			continue
		}
		change := h.change(b)
		verdict := "ok"
		if change < -threshold {
			verdict = "REGRESSION"
			failed = append(failed, scenario)
		}
		fmt.Fprintf(out, "%-12s base %s -> head %s (%+.1f%%) %s\n", scenario, b, h, change*100, verdict)
	}
	if len(failed) > 0 {
		return fmt.Errorf("slower than base by more than %.0f%%: %s", threshold*100, strings.Join(failed, ", "))
	}
	return nil
}

// change is the relative improvement of r over base: positive is faster.
func (r *result) change(base *result) float64 {
	if r.JobsPerSec > 0 {
		return r.JobsPerSec/base.JobsPerSec - 1
	}
	// Latency: lower is better.
	return base.P99Millis/r.P99Millis - 1
}

func (r *result) String() string {
	if r.JobsPerSec > 0 {
		return fmt.Sprintf("%.0f jobs/s", r.JobsPerSec)
	}
	return fmt.Sprintf("p50 %.2fms p99 %.2fms", r.P50Millis, r.P99Millis)
}

// readResults reduces each scenario's runs to one: the average of the
// faster half (by throughput, or by p99 latency), which noise on a shared
// machine, being one-sided, biases the least.
func readResults(path string) (map[string]*result, error) {
	f, err := os.Open(path) //nolint:gosec // a result file named on the command line
	if err != nil {
		return nil, err
	}
	defer f.Close()
	runs := map[string][]*result{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var r result
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		runs[r.Scenario] = append(runs[r.Scenario], &r)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(runs) == 0 {
		return nil, fmt.Errorf("%s: no results", path)
	}
	out := map[string]*result{}
	for scenario, rs := range runs {
		// Fastest first.
		slices.SortFunc(rs, func(a, b *result) int { return cmp.Compare(b.change(a), 0) })
		best := rs[:(len(rs)+1)/2]
		avg := *best[0]
		avg.JobsPerSec, avg.Seconds, avg.P50Millis, avg.P99Millis, avg.MaxMillis = 0, 0, 0, 0, 0
		for _, r := range best {
			avg.JobsPerSec += r.JobsPerSec / float64(len(best))
			avg.Seconds += r.Seconds / float64(len(best))
			avg.P50Millis += r.P50Millis / float64(len(best))
			avg.P99Millis += r.P99Millis / float64(len(best))
			avg.MaxMillis += r.MaxMillis / float64(len(best))
		}
		out[scenario] = &avg
	}
	return out, nil
}
