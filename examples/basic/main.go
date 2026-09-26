// Command basic is a small, complete hopper application: it installs the
// schema, registers a worker, starts a client, inserts a few jobs (one of
// them inside a transaction) and waits for a result.
//
//	HOPPER_DATABASE_URL=postgres://... go run ./examples/basic
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver/hopperpgx"
	"github.com/parallelworks/hopper/hoppermigrate"
)

// SendEmail is a job's args. Kind names the job type; the fields are the
// payload, stored as JSON.
type SendEmail struct {
	UserID int    `json:"user_id"`
	Tmpl   string `json:"tmpl"`
}

func (SendEmail) Kind() string { return "send_email" }

// InsertOpts gives the kind its defaults; callers can override per insert.
func (SendEmail) InsertOpts() hopper.InsertOpts {
	return hopper.InsertOpts{Queue: "email", MaxAttempts: 5}
}

// SendEmailWorker works SendEmail jobs.
type SendEmailWorker struct {
	hopper.WorkerDefaults[SendEmail]
}

func (w *SendEmailWorker) Work(ctx context.Context, job *hopper.Job[SendEmail]) error {
	slog.Info("sending email", "user_id", job.Args.UserID, "tmpl", job.Args.Tmpl, "attempt", job.Attempt)
	// Returning an error retries with backoff; hopper.Cancel discards;
	// hopper.Snooze reschedules without using an attempt.
	return hopper.SetOutput(ctx, fmt.Sprintf("sent %s to user %d", job.Args.Tmpl, job.Args.UserID))
}

// Timeout bounds one attempt. Zero means the client's default.
func (w *SendEmailWorker) Timeout(*hopper.Job[SendEmail]) time.Duration { return 30 * time.Second }

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	url := os.Getenv("HOPPER_DATABASE_URL")
	if url == "" {
		return errors.New("set HOPPER_DATABASE_URL")
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return err
	}
	defer pool.Close()
	driver := hopperpgx.New(pool)

	// Install or upgrade the schema. Safe to run from every replica.
	if _, err := hoppermigrate.Up(ctx, driver, nil); err != nil {
		return err
	}

	workers := hopper.NewWorkers()
	hopper.AddWorker(workers, &SendEmailWorker{})

	client, err := hopper.NewClient(driver, &hopper.Config{
		Queues:  map[string]hopper.QueueConfig{"email": {MaxWorkers: 10}},
		Workers: workers,
		Periodic: []hopper.PeriodicJob{
			hopper.Cron("0 9 * * MON-FRI", SendEmail{UserID: 0, Tmpl: "digest"}, &hopper.PeriodicOpts{Name: "daily-digest"}),
		},
	})
	if err != nil {
		return err
	}
	if err := client.Start(ctx); err != nil {
		return err
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = client.Stop(stopCtx)
	}()

	// A plain insert.
	if _, err := client.Insert(ctx, SendEmail{UserID: 1, Tmpl: "welcome"}, nil); err != nil {
		return err
	}

	// A transactional insert: the job exists only if tx commits, so it can
	// go in the same transaction as the business data that caused it.
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	if _, err := client.InsertTx(ctx, tx, SendEmail{UserID: 2, Tmpl: "receipt"}, &hopper.InsertOpts{Priority: hopper.PriorityHigh}); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// A unique job: a second insert within the hour is a no-op.
	res, err := client.Insert(ctx, SendEmail{UserID: 3, Tmpl: "reminder"}, &hopper.InsertOpts{
		Unique: &hopper.UniqueOpts{ByArgs: true, ByPeriod: time.Hour},
	})
	if err != nil {
		return err
	}
	slog.Info("inserted", "job_id", res.Job.ID, "duplicate", res.Duplicate)

	// Request/reply: wait for a job's output.
	res, err = client.Insert(ctx, SendEmail{UserID: 4, Tmpl: "report"}, &hopper.InsertOpts{Await: true})
	if err != nil {
		return err
	}
	out, err := hopper.Await[string](ctx, client, res.Job.ID)
	if err != nil {
		return err
	}
	slog.Info("job finished", "output", out)

	stats, err := client.Stats(ctx)
	if err != nil {
		return err
	}
	slog.Info("stats", "queues", stats.Queues, "leader", stats.Leader)
	return nil
}
