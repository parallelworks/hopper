// Command hopperbench-sqlite measures hopper's hot paths on SQLite, as
// hopperbench does on Postgres, and prints its results in hopperbench's
// format, one JSON object per scenario, which hopperbench -compare reads.
//
//	hopperbench-sqlite [-dir /tmp] [-scenarios throughput,workflow,insert,latency] [-jobs 50000] [-clients 4]
//
// Each scenario runs on a database file of its own in a temporary directory
// under -dir, removed afterwards. -dir must be on a local disk.
//
// Scenarios:
//
//	throughput  no-op jobs preloaded, then drained by -clients clients
//	retry       as throughput, with every job failing its first attempt
//	workflow    as throughput, in workflows of ten steps (one, then eight after it, then one after those),
//	            each step recording a small output; -jobs is rounded down to a multiple of ten
//	insert      InsertMany in batches of 100
//	latency     commit-to-Work on an idle queue, local wake-up
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver/hoppersqlite"
	"github.com/parallelworks/hopper/hoppermigrate"
)

type benchArgs struct {
	N int `json:"n"`
}

func (benchArgs) Kind() string { return "bench" }

// result is hopperbench's.
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

type options struct {
	jobs, clients, workers, samples int
	synchronous                     string
}

func main() { os.Exit(benchMain()) }

// benchMain runs the command and returns its exit status, after its
// deferred calls, which os.Exit would skip.
func benchMain() int {
	var (
		dir         = flag.String("dir", os.TempDir(), "directory for the scenarios' databases, on a local disk")
		scenarios   = flag.String("scenarios", "throughput,workflow,insert,latency", "comma-separated scenarios to run")
		jobs        = flag.Int("jobs", 50000, "jobs per throughput scenario")
		clients     = flag.Int("clients", 4, "clients working the throughput scenarios")
		workers     = flag.Int("workers", 100, "MaxWorkers per client")
		samples     = flag.Int("samples", 200, "latency samples")
		synchronous = flag.String("synchronous", "FULL", "SQLite's synchronous setting: FULL or NORMAL")
		cpuProfile  = flag.String("cpuprofile", "", "write a CPU profile of the run to this file")
	)
	flag.Parse()
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "hopperbench-sqlite:", err)
			return 1
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintln(os.Stderr, "hopperbench-sqlite:", err)
			return 1
		}
		defer pprof.StopCPUProfile()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err := run(ctx, *dir, strings.Split(*scenarios, ","), options{jobs: *jobs, clients: *clients, workers: *workers, samples: *samples, synchronous: *synchronous})
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "hopperbench-sqlite:", err)
		return 1
	}
	return 0
}

func run(ctx context.Context, dir string, scenarios []string, opts options) error {
	for _, s := range scenarios {
		b, cleanup, err := newBench(ctx, dir, opts)
		if err != nil {
			return err
		}
		var r result
		switch s = strings.TrimSpace(s); s {
		case "throughput":
			r, err = b.drain(ctx, "throughput", b.params(opts.jobs), noopWorker)
		case "retry":
			r, err = b.drain(ctx, "retry", b.params(opts.jobs), failOnceWorker)
		case "workflow":
			r, err = b.workflows(ctx, opts.jobs)
		case "insert":
			r, err = b.insert(ctx, opts.jobs, 100)
		case "latency":
			r, err = b.latency(ctx, opts.samples)
		default:
			err = fmt.Errorf("unknown scenario %q", s)
		}
		cleanup()
		if err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
			return err
		}
	}
	return nil
}

type bench struct {
	d      *hoppersqlite.Driver
	opts   options
	logger *slog.Logger
}

// newBench opens a migrated database in a new directory under dir.
func newBench(ctx context.Context, dir string, opts options) (*bench, func(), error) {
	tmp, err := os.MkdirTemp(dir, "hopperbench-sqlite")
	if err != nil {
		return nil, nil, err
	}
	d, err := hoppersqlite.Open(filepath.Join(tmp, "hopper.db"), &hoppersqlite.Config{Synchronous: opts.synchronous})
	if err != nil {
		_ = os.RemoveAll(tmp)
		return nil, nil, err
	}
	cleanup := func() {
		_ = d.Close()
		_ = os.RemoveAll(tmp)
	}
	if _, err := hoppermigrate.Up(ctx, d, &hoppermigrate.Options{Logger: slog.New(slog.DiscardHandler)}); err != nil {
		cleanup()
		return nil, nil, err
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	return &bench{d: d, opts: opts, logger: logger}, cleanup, nil
}

func (b *bench) params(n int) []hopper.InsertParams {
	params := make([]hopper.InsertParams, n)
	for i := range params {
		params[i] = hopper.InsertParams{Args: benchArgs{N: i}}
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

const workflowSteps = 10

func (b *bench) drain(ctx context.Context, name string, params []hopper.InsertParams, work func(context.Context, *hopper.Job[benchArgs]) error) (result, error) {
	return b.drainLoaded(ctx, name, len(params), func(inserter *hopper.Client[*sql.Tx]) error {
		for i := 0; i < len(params); i += 10000 {
			if _, err := inserter.InsertMany(ctx, params[i:min(i+10000, len(params))]); err != nil {
				return err
			}
		}
		return nil
	}, work)
}

func (b *bench) workflows(ctx context.Context, jobs int) (result, error) {
	n := jobs / workflowSteps
	if n < 1 {
		return result{}, fmt.Errorf("-jobs must be at least %d", workflowSteps)
	}
	load := func(inserter *hopper.Client[*sql.Tx]) error {
		return fanOut(ctx, n, 100, func(ctx context.Context, lo, hi int) error {
			tx, err := b.d.DB().BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback() //nolint:errcheck // committed below
			for i := lo; i < hi; i++ {
				if _, err := inserter.InsertWorkflowTx(ctx, tx, benchWorkflow(i)); err != nil {
					return err
				}
			}
			return tx.Commit()
		})
	}
	r, err := b.drainLoaded(ctx, "workflow", n*workflowSteps, load, outputWorker)
	if err != nil {
		return result{}, err
	}
	var outputs, open int
	if err := b.d.DB().QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM hopper_job_history WHERE batch_id IS NOT NULL AND kind = 'bench' AND output IS NOT NULL),
		(SELECT count(*) FROM hopper_batches WHERE completed_at IS NULL)`).Scan(&outputs, &open); err != nil {
		return result{}, err
	}
	if outputs != r.Jobs {
		return result{}, fmt.Errorf("%d of %d jobs stored an output", outputs, r.Jobs)
	}
	if open != 0 {
		return result{}, fmt.Errorf("%d of %d workflows not completed", open, n)
	}
	return r, nil
}

func benchWorkflow(i int) *hopper.Workflow {
	wf := hopper.NewWorkflow("bench-"+strconv.Itoa(i), nil)
	n := i * workflowSteps
	root := wf.Add(benchArgs{N: n}, nil)
	middle := make([]hopper.Step, workflowSteps-2)
	for j := range middle {
		middle[j] = wf.Add(benchArgs{N: n + 1 + j}, hopper.After(root))
	}
	wf.Add(benchArgs{N: n + workflowSteps - 1}, hopper.After(middle...))
	return wf
}

type benchOutput struct {
	N        int    `json:"n"`
	Artifact string `json:"artifact"`
}

func outputWorker(ctx context.Context, job *hopper.Job[benchArgs]) error {
	return hopper.SetOutput(ctx, benchOutput{N: job.Args.N, Artifact: "bench/" + strconv.Itoa(job.Args.N)})
}

// fanOut runs fn over [0, n) in chunks from eight inserters, as an
// application with several goroutines inserting would; the first error
// cancels the rest.
func fanOut(ctx context.Context, n, chunk int, fn func(ctx context.Context, lo, hi int) error) error {
	const inserters = 8
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wg sync.WaitGroup
	for w := range inserters {
		wg.Go(func() {
			for lo := w * chunk; lo < n && ctx.Err() == nil; lo += inserters * chunk {
				if err := fn(ctx, lo, min(lo+chunk, n)); err != nil {
					cancel(err)
					return
				}
			}
		})
	}
	wg.Wait()
	return context.Cause(ctx)
}

func (b *bench) drainLoaded(ctx context.Context, name string, jobs int, load func(*hopper.Client[*sql.Tx]) error, work func(context.Context, *hopper.Job[benchArgs]) error) (result, error) {
	inserter, err := hopper.NewClient(b.d, &hopper.Config{Logger: b.logger})
	if err != nil {
		return result{}, err
	}
	defer inserter.Stop(ctx) //nolint:errcheck // flushes pending notifications
	if err := load(inserter); err != nil {
		return result{}, err
	}
	ws := hopper.NewWorkers()
	hopper.AddWorker(ws, immediateRetry{fn: work})
	cs := make([]*hopper.Client[*sql.Tx], b.opts.clients)
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
	defer func() {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		for _, c := range cs {
			_ = c.Stop(sctx)
		}
	}()
	start := time.Now()
	for _, c := range cs {
		if err := c.Start(ctx); err != nil {
			return result{}, err
		}
	}
	if err := b.waitDrained(ctx); err != nil {
		return result{}, err
	}
	elapsed := time.Since(start)
	for _, c := range cs {
		if err := c.Stop(ctx); err != nil {
			return result{}, err
		}
	}
	var done int
	if err := b.d.DB().QueryRowContext(ctx, "SELECT count(*) FROM hopper_job_history WHERE state = 'completed' AND kind = 'bench'").Scan(&done); err != nil {
		return result{}, err
	}
	if done != jobs {
		return result{}, fmt.Errorf("%d of %d jobs completed", done, jobs)
	}
	return result{
		Scenario: name, Jobs: jobs, Clients: b.opts.clients, Workers: b.opts.workers,
		Seconds: elapsed.Seconds(), JobsPerSec: float64(jobs) / elapsed.Seconds(),
	}, nil
}

func (b *bench) waitDrained(ctx context.Context) error {
	last, changed := -1, time.Now()
	for {
		var live int
		if err := b.d.DB().QueryRowContext(ctx, "SELECT count(*) FROM hopper_jobs").Scan(&live); err != nil {
			return err
		}
		if live == 0 {
			return nil
		}
		if live != last {
			last, changed = live, time.Now()
		} else if time.Since(changed) > 2*time.Minute {
			return fmt.Errorf("no progress for two minutes with %d jobs left", live)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// insert measures InsertMany in batches of the given size.
func (b *bench) insert(ctx context.Context, jobs, batch int) (result, error) {
	c, err := hopper.NewClient(b.d, &hopper.Config{Logger: b.logger})
	if err != nil {
		return result{}, err
	}
	defer c.Stop(ctx) //nolint:errcheck // flushes pending notifications
	params := b.params(jobs)
	start := time.Now()
	err = fanOut(ctx, len(params), batch, func(ctx context.Context, lo, hi int) error {
		_, err := c.InsertMany(ctx, params[lo:hi])
		return err
	})
	elapsed := time.Since(start)
	if err != nil {
		return result{}, err
	}
	return result{Scenario: "insert", Jobs: jobs, Seconds: elapsed.Seconds(), JobsPerSec: float64(jobs) / elapsed.Seconds()}, nil
}

// latency measures commit-to-Work on an idle queue, with the insert coming
// from the working client itself (local wake-up).
func (b *bench) latency(ctx context.Context, samples int) (result, error) {
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
			return result{}, errors.New("job not picked up within 10s")
		}
		time.Sleep(5 * time.Millisecond)
	}
	slices.Sort(lat)
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	return result{
		Scenario: "latency", Jobs: samples, Seconds: time.Since(begin).Seconds(),
		P50Millis: ms(lat[len(lat)/2]), P99Millis: ms(lat[len(lat)*99/100]), MaxMillis: ms(lat[len(lat)-1]),
	}, nil
}
