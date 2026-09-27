// Command hopperui serves the hopper web UI on its own, for operators who
// do not embed it in an application.
//
//	hopperui -database-url postgres://... [-listen :8080] [-prefix /hopper] [-allow-actions]
//
// Without -allow-actions the UI is read-only. With it, every action is
// allowed for every request, so put the server behind your own
// authentication.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/driver/hopperpgx"
	"github.com/parallelworks/hopper/hopperui"
)

func main() {
	var (
		url     = flag.String("database-url", os.Getenv("HOPPER_DATABASE_URL"), "Postgres URL (or HOPPER_DATABASE_URL)")
		listen  = flag.String("listen", ":8080", "address to serve on")
		prefix  = flag.String("prefix", "", "path prefix to serve under, such as /hopper")
		title   = flag.String("title", "hopper", "title shown in the header")
		actions = flag.Bool("allow-actions", false, "allow retry, cancel, pause, resume and seek for every request")
	)
	flag.Parse()
	if err := run(*url, *listen, *prefix, *title, *actions); err != nil {
		fmt.Fprintln(os.Stderr, "hopperui:", err)
		os.Exit(1)
	}
}

func run(url, listen, prefix, title string, actions bool) error {
	if url == "" {
		return errors.New("-database-url or HOPPER_DATABASE_URL is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return err
	}
	defer pool.Close()
	client, err := hopper.NewClient(hopperpgx.New(pool), &hopper.Config{Logger: slog.Default()})
	if err != nil {
		return err
	}
	cfg := &hopperui.Config{Prefix: prefix, Title: title}
	if actions {
		cfg.Authorize = func(*http.Request, hopperui.Action) error { return nil }
	}
	mux := http.NewServeMux()
	if prefix == "" {
		mux.Handle("/", hopperui.New(client, cfg))
	} else {
		mux.Handle(prefix+"/", http.StripPrefix(prefix, hopperui.New(client, cfg)))
		mux.Handle("GET /{$}", http.RedirectHandler(prefix+"/", http.StatusFound))
	}
	srv := &http.Server{Addr: listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	slog.Info("hopperui: serving", "listen", listen, "prefix", prefix, "actions", actions)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
