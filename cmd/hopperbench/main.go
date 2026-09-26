// Command hopperbench measures hopper's hot paths against a real database and
// prints machine-readable results, one JSON object per scenario.
//
//	hopperbench -database-url postgres://... [-scenarios throughput,insert,copy,latency] [-jobs 50000] [-clients 4]
//
// It works in a schema of its own ("hopperbench"), which it creates and drops,
// so it can point at any database. Numbers depend on the hardware, the
// Postgres configuration and the network between them; compare runs made on
// the same setup, such as a PR branch against canary.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
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
		clients   = flag.Int("clients", 4, "clients working the throughput scenario")
		workers   = flag.Int("workers", 100, "MaxWorkers per client")
		samples   = flag.Int("samples", 200, "latency samples")
		schema    = flag.String("schema", "hopperbench", "schema to create, use and drop")
	)
	flag.Parse()
	if *url == "" {
		fmt.Fprintln(os.Stderr, "hopperbench: -database-url or HOPPER_TEST_DATABASE_URL is required")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err := run(ctx, *url, *schema, strings.Split(*scenarios, ","), *jobs, *clients, *workers, *samples)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "hopperbench:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, url, schema string, scenarios []string, jobs, clients, workers, samples int) error {
	pool, cleanup, err := openSchema(ctx, url, schema)
	if err != nil {
		return err
	}
	defer cleanup()
	d := hopperpgx.New(pool)
	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: slog.New(slog.DiscardHandler)}); err != nil {
		return err
	}
	b := &bench{pool: pool, d: d, logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))}

	for _, s := range scenarios {
		var (
			r   result
			err error
		)
		switch strings.TrimSpace(s) {
		case "throughput":
			r, err = b.throughput(ctx, jobs, clients, workers)
		case "insert":
			r, err = b.insert(ctx, jobs, 100)
		case "copy":
			r, err = b.insert(ctx, jobs, 5000)
		case "latency":
			r, err = b.latency(ctx, samples)
		default:
			return fmt.Errorf("unknown scenario %q", s)
		}
		if err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
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
	logger *slog.Logger
}

func (b *bench) truncate(ctx context.Context) error {
	_, err := b.pool.Exec(ctx, "TRUNCATE hopper_jobs, hopper_job_history, hopper_clients")
	return err
}

func (b *bench) params(n int) []hopper.InsertParams {
	params := make([]hopper.InsertParams, n)
	for i := range params {
		params[i] = hopper.InsertParams{Args: benchArgs{N: i}}
	}
	return params
}

// throughput preloads jobs, then measures how long `clients` clients with
// no-op workers take to drain the queue.
func (b *bench) throughput(ctx context.Context, jobs, clients, workers int) (result, error) {
	if err := b.truncate(ctx); err != nil {
		return result{}, err
	}
	inserter, err := hopper.NewClient(b.d, &hopper.Config{Logger: b.logger})
	if err != nil {
		return result{}, err
	}
	defer inserter.Stop(ctx) //nolint:errcheck // flushes pending notifications
	params := b.params(jobs)
	for i := 0; i < len(params); i += 10000 {
		if _, err := inserter.InsertMany(ctx, params[i:min(i+10000, len(params))]); err != nil {
			return result{}, err
		}
	}

	ws := hopper.NewWorkers()
	hopper.AddWorkFunc(ws, func(context.Context, *hopper.Job[benchArgs]) error { return nil })
	cs := make([]*hopper.Client[pgx.Tx], clients)
	for i := range cs {
		c, err := hopper.NewClient(b.d, &hopper.Config{
			Queues:  map[string]hopper.QueueConfig{hopper.QueueDefault: {MaxWorkers: workers}},
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
	if err := b.pool.QueryRow(ctx, "SELECT count(*) FROM hopper_job_history WHERE state = 'completed'").Scan(&done); err != nil {
		return result{}, err
	}
	if done != jobs {
		return result{}, fmt.Errorf("%d of %d jobs completed", done, jobs)
	}
	return result{
		Scenario: "throughput", Jobs: jobs, Clients: clients, Workers: workers,
		Seconds: elapsed.Seconds(), JobsPerSec: float64(jobs) / elapsed.Seconds(),
	}, nil
}

// insert measures InsertMany in batches of the given size: below the COPY
// threshold that is the unnest statement, above it the COPY path.
func (b *bench) insert(ctx context.Context, jobs, batch int) (result, error) {
	if err := b.truncate(ctx); err != nil {
		return result{}, err
	}
	c, err := hopper.NewClient(b.d, &hopper.Config{Logger: b.logger})
	if err != nil {
		return result{}, err
	}
	defer c.Stop(ctx) //nolint:errcheck // flushes pending notifications
	params := b.params(jobs)
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
// from the working client itself (local wake-up). Inserts from other
// processes rely on notifications, which land with M2; until then they are
// bounded by the poll interval.
func (b *bench) latency(ctx context.Context, samples int) (result, error) {
	if err := b.truncate(ctx); err != nil {
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
