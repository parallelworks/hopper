// Command hopper administers a hopper installation from the shell.
//
//	hopper migrate up|down|version [-target N]
//	hopper jobs list [-queue Q] [-kind K] [-state S] [-limit N]
//	hopper jobs get|retry|cancel <id>
//	hopper queues list|pause|resume [name]
//	hopper queues limit <name> [-global N] [-rate R] [-burst B] [-partition P] [-aging D]
//	hopper clients list
//	hopper stats
//
// The database comes from -database-url or HOPPER_DATABASE_URL. Every command
// takes -json for machine-readable output.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver/hopperpgx"
	"github.com/parallelworks/hopper/hoppermigrate"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err := run(ctx, os.Args[1:], os.Stdout)
	stop()
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(2)
		}
		fmt.Fprintln(os.Stderr, "hopper:", err)
		os.Exit(1)
	}
}

const usage = `usage: hopper [-database-url URL] [-json] <command> [args]

commands:
  migrate up|down|version [-target N]
  jobs list [-queue Q] [-kind K] [-state S] [-limit N]
  jobs get|retry|cancel <id>
  queues list
  queues pause|resume <name>
  queues limit <name> [-global N] [-rate R] [-burst B] [-partition P] [-aging D]   (omitted limits are removed)
  clients list
  workflows get <id>
  stats
  bench                        (see the hopperbench command)

The database comes from -database-url or HOPPER_DATABASE_URL.`

type cli struct {
	client *hopper.Client[pgx.Tx]
	driver *hopperpgx.Driver
	out    io.Writer
	json   bool
}

func run(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("hopper", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.Usage = func() { fmt.Fprintln(out, usage) }
	url := fs.String("database-url", os.Getenv("HOPPER_DATABASE_URL"), "Postgres URL (default $HOPPER_DATABASE_URL)")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) == 0 {
		fs.Usage()
		return flag.ErrHelp
	}
	if rest[0] == "bench" {
		fmt.Fprintln(out, "Benchmarks are in the hopperbench command: go run github.com/parallelworks/hopper/cmd/hopperbench -help")
		return nil
	}
	if *url == "" {
		return errors.New("-database-url or HOPPER_DATABASE_URL is required")
	}
	pool, err := pgxpool.New(ctx, *url)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()
	d := hopperpgx.New(pool)
	client, err := hopper.NewClient(d, nil)
	if err != nil {
		return err
	}
	c := &cli{client: client, driver: d, out: out, json: *asJSON}

	switch rest[0] {
	case "migrate":
		return c.migrate(ctx, rest[1:])
	case "jobs":
		return c.jobs(ctx, rest[1:])
	case "queues":
		return c.queues(ctx, rest[1:])
	case "clients":
		return c.clients(ctx, rest[1:])
	case "workflows":
		return c.workflows(ctx, rest[1:])
	case "stats":
		return c.stats(ctx)
	default:
		fs.Usage()
		return fmt.Errorf("unknown command %q", rest[0])
	}
}

func (c *cli) migrate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("hopper migrate", flag.ContinueOnError)
	fs.SetOutput(c.out)
	target := fs.Int("target", 0, "version to migrate to (default: latest for up, 0 for down)")
	if len(args) == 0 {
		return errors.New("migrate: up, down or version")
	}
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	opts := &hoppermigrate.Options{Target: *target}
	switch args[0] {
	case "up":
		res, err := hoppermigrate.Up(ctx, c.driver, opts)
		if err != nil {
			return err
		}
		return c.print(res, func(w io.Writer) {
			fmt.Fprintf(w, "applied %v; schema version %d\n", res.Applied, res.Version)
		})
	case "down":
		res, err := hoppermigrate.Down(ctx, c.driver, opts)
		if err != nil {
			return err
		}
		return c.print(res, func(w io.Writer) {
			fmt.Fprintf(w, "reverted %v; schema version %d\n", res.Applied, res.Version)
		})
	case "version":
		v, err := hoppermigrate.Version(ctx, c.driver)
		if err != nil {
			return err
		}
		return c.print(map[string]int{"version": v, "latest": hoppermigrate.Latest()}, func(w io.Writer) {
			fmt.Fprintf(w, "schema version %d (latest %d)\n", v, hoppermigrate.Latest())
		})
	default:
		return fmt.Errorf("migrate: unknown subcommand %q", args[0])
	}
}

func (c *cli) jobs(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("jobs: list, get, retry or cancel")
	}
	switch args[0] {
	case "list":
		fs := flag.NewFlagSet("hopper jobs list", flag.ContinueOnError)
		fs.SetOutput(c.out)
		queue := fs.String("queue", "", "queue")
		kind := fs.String("kind", "", "kind")
		state := fs.String("state", "", "state (available, running, completed, ...)")
		limit := fs.Int("limit", 50, "maximum jobs to print")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		filter := hopper.JobFilter{Queue: *queue}
		if *kind != "" {
			filter.Kinds = []string{*kind}
		}
		if *state != "" {
			filter.States = []hopper.JobState{hopper.JobState(*state)}
		}
		var jobs []*hopper.JobRow
		for job, err := range c.client.Jobs(ctx, filter) {
			if err != nil {
				return err
			}
			jobs = append(jobs, job)
			if len(jobs) >= *limit {
				break
			}
		}
		return c.print(jobs, func(w io.Writer) {
			tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "ID\tKIND\tQUEUE\tSTATE\tATTEMPT\tSCHEDULED\tCREATED")
			for _, j := range jobs {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d/%d\t%s\t%s\n", j.ID, j.Kind, j.Queue, j.State, j.Attempt, j.MaxAttempts,
					j.ScheduledAt.Local().Format(time.RFC3339), j.CreatedAt.Local().Format(time.RFC3339))
			}
			tw.Flush()
		})
	case "get", "retry", "cancel":
		if len(args) != 2 {
			return fmt.Errorf("jobs %s: one job ID is required", args[0])
		}
		id, err := hopper.ParseJobID(args[1])
		if err != nil {
			return err
		}
		var job *hopper.JobRow
		switch args[0] {
		case "get":
			job, err = c.client.JobGet(ctx, id)
		case "retry":
			job, err = c.client.JobRetry(ctx, id)
		case "cancel":
			job, err = c.client.JobCancel(ctx, id)
		}
		if err != nil {
			return err
		}
		return c.print(job, func(w io.Writer) { printJob(w, job) })
	default:
		return fmt.Errorf("jobs: unknown subcommand %q", args[0])
	}
}

func printJob(w io.Writer, j *hopper.JobRow) {
	fmt.Fprintf(w, "id:            %s\nkind:          %s\nqueue:         %s\nstate:         %s\npriority:      %d\nattempt:       %d/%d\n",
		j.ID, j.Kind, j.Queue, j.State, j.Priority, j.Attempt, j.MaxAttempts)
	fmt.Fprintf(w, "scheduled_at:  %s\ncreated_at:    %s\n", j.ScheduledAt.Local().Format(time.RFC3339), j.CreatedAt.Local().Format(time.RFC3339))
	if !j.AttemptedAt.IsZero() {
		fmt.Fprintf(w, "attempted_at:  %s (client %d)\n", j.AttemptedAt.Local().Format(time.RFC3339), j.AttemptedBy)
	}
	if !j.FinalizedAt.IsZero() {
		fmt.Fprintf(w, "finalized_at:  %s\n", j.FinalizedAt.Local().Format(time.RFC3339))
	}
	if !j.ExpiresAt.IsZero() {
		fmt.Fprintf(w, "expires_at:    %s\n", j.ExpiresAt.Local().Format(time.RFC3339))
	}
	if j.UniqueKey != "" {
		fmt.Fprintf(w, "unique_key:    %q\n", j.UniqueKey)
	}
	fmt.Fprintf(w, "args:          %s\nmetadata:      %s\n", j.Args, j.Metadata)
	if len(j.Output) > 0 {
		fmt.Fprintf(w, "output:        %s\n", j.Output)
	}
	for _, e := range j.Errors {
		fmt.Fprintf(w, "error #%d:      %s (%s)\n", e.Attempt, e.Error, e.At.Local().Format(time.RFC3339))
	}
}

func (c *cli) queues(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("queues: list, pause, resume or limit")
	}
	switch args[0] {
	case "list":
		queues, err := c.client.Queues().List(ctx)
		if err != nil {
			return err
		}
		return c.print(queues, func(w io.Writer) {
			tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "NAME\tPAUSED\tGLOBAL\tRATE/S\tBURST\tPARTITION\tAGING\tUPDATED")
			for _, q := range queues {
				paused := ""
				if !q.PausedAt.IsZero() {
					paused = "since " + q.PausedAt.Local().Format(time.RFC3339)
				}
				l := q.Limits
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", q.Name, paused, orDash(l.GlobalLimit), orDashF(l.RatePerSec),
					orDash(l.RateBurst), orDash(l.PartitionLimit), orDashD(l.Aging), q.UpdatedAt.Local().Format(time.RFC3339))
			}
			tw.Flush()
		})
	case "limit":
		if len(args) < 2 {
			return errors.New("queues limit: a queue name is required")
		}
		fs := flag.NewFlagSet("hopper queues limit", flag.ContinueOnError)
		fs.SetOutput(c.out)
		var l hopper.QueueLimits
		fs.IntVar(&l.GlobalLimit, "global", 0, "max running jobs across all clients")
		fs.Float64Var(&l.RatePerSec, "rate", 0, "max claims per second across all clients")
		fs.IntVar(&l.RateBurst, "burst", 0, "token bucket size for -rate")
		fs.IntVar(&l.PartitionLimit, "partition", 0, "max running jobs per partition key")
		fs.DurationVar(&l.Aging, "aging", 0, "promote waiting jobs one priority level per this period")
		if err := fs.Parse(args[2:]); err != nil {
			return err
		}
		if err := c.client.Queues().SetLimits(ctx, args[1], l); err != nil {
			return err
		}
		l.Name = args[1]
		return c.print(l, func(w io.Writer) { fmt.Fprintf(w, "set limits on queue %s\n", args[1]) })
	case "pause", "resume":
		if len(args) != 2 {
			return fmt.Errorf("queues %s: one queue name is required", args[0])
		}
		var err error
		if args[0] == "pause" {
			err = c.client.Queues().Pause(ctx, args[1])
		} else {
			err = c.client.Queues().Resume(ctx, args[1])
		}
		if err != nil {
			return err
		}
		return c.print(map[string]string{"queue": args[1], "action": args[0]}, func(w io.Writer) {
			fmt.Fprintf(w, "%sd queue %s\n", args[0], args[1])
		})
	default:
		return fmt.Errorf("queues: unknown subcommand %q", args[0])
	}
}

func (c *cli) clients(ctx context.Context, args []string) error {
	if len(args) != 1 || args[0] != "list" {
		return errors.New("clients: list")
	}
	clients, err := c.client.Clients(ctx)
	if err != nil {
		return err
	}
	return c.print(clients, func(w io.Writer) {
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tHOST\tSTARTED\tLEASE\tINFO")
		for _, cl := range clients {
			lease := "expired"
			if until := time.Until(cl.ExpiresAt); until > 0 {
				lease = "live (" + until.Round(time.Second).String() + ")"
			}
			fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\n", cl.ID, cl.Hostname, cl.StartedAt.Local().Format(time.RFC3339), lease, cl.Info)
		}
		tw.Flush()
	})
}

func (c *cli) workflows(ctx context.Context, args []string) error {
	if len(args) != 2 || args[0] != "get" {
		return errors.New("workflows: get <id>")
	}
	id, err := hopper.ParseJobID(args[1])
	if err != nil {
		return err
	}
	wf, err := c.client.WorkflowGet(ctx, id)
	if err != nil {
		return err
	}
	return c.print(wf, func(w io.Writer) {
		b := wf.Batch
		fmt.Fprintf(w, "id:        %s\nname:      %s\nprogress:  %d of %d finished, %d failed\n", b.ID, b.Name, b.Total-b.Pending, b.Total, b.Failed)
		if !b.CompletedAt.IsZero() {
			fmt.Fprintf(w, "completed: %s\n", b.CompletedAt.Local().Format(time.RFC3339))
		}
		deps := map[hopper.JobID][]string{}
		for _, e := range wf.Edges {
			deps[e.Job] = append(deps[e.Job], e.DependsOn.String())
		}
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tKIND\tQUEUE\tSTATE\tATTEMPT\tAFTER")
		for _, j := range wf.Jobs {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d/%d\t%s\n", j.ID, j.Kind, j.Queue, j.State, j.Attempt, j.MaxAttempts, strings.Join(deps[j.ID], ","))
		}
		tw.Flush()
	})
}

func (c *cli) stats(ctx context.Context) error {
	stats, err := c.client.Stats(ctx)
	if err != nil {
		return err
	}
	return c.print(stats, func(w io.Writer) {
		fmt.Fprintf(w, "leader: client %d; live clients: %d\n", stats.Leader, stats.LiveClients)
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "QUEUE\tAVAILABLE\tSCHEDULED\tRETRYABLE\tRUNNING\tOLDEST\tCOMPLETED/MIN\tPAUSED")
		for _, name := range slices.Sorted(maps.Keys(stats.Queues)) {
			q := stats.Queues[name]
			fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%s\t%d\t%v\n", name, q.Available, q.Scheduled, q.Retryable, q.Running,
				q.OldestAvailable.Round(time.Second), q.CompletedLastMinute, q.Paused)
		}
		tw.Flush()
	})
}

func orDash(n int) string {
	if n == 0 {
		return "-"
	}
	return fmt.Sprint(n)
}

func orDashF(f float64) string {
	if f == 0 {
		return "-"
	}
	return fmt.Sprint(f)
}

func orDashD(d time.Duration) string {
	if d == 0 {
		return "-"
	}
	return d.String()
}

// print writes v as JSON, or through text when -json is not set.
func (c *cli) print(v any, text func(w io.Writer)) error {
	if c.json {
		enc := json.NewEncoder(c.out)
		enc.SetIndent("", "  ")
		return enc.Encode(v)
	}
	text(c.out)
	return nil
}
