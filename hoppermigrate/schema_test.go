package hoppermigrate_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/parallelworks/hopper/driver"
	"github.com/parallelworks/hopper/driver/hopperpgx"
	"github.com/parallelworks/hopper/driver/hoppersql"
	"github.com/parallelworks/hopper/hoppermigrate"
	"github.com/parallelworks/hopper/internal/testdb"
)

func TestConfiguredSchemasSharePool(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := testdb.EmptyPool(t, 0)
	appSchema := pool.Config().ConnConfig.RuntimeParams["search_path"]
	db := stdlib.OpenDB(*pool.Config().ConnConfig)
	defer db.Close()
	opts := &hoppermigrate.Options{Logger: slog.New(slog.DiscardHandler)}
	var schemas []string
	for i, transport := range []string{"pgx", "sql"} {
		// Both quote characters must work in identifiers and catalog lookups.
		schema := fmt.Sprintf("%s_queue_%d'\"", appSchema, i)
		schemas = append(schemas, schema)
		t.Cleanup(func() {
			_, err := pool.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
			if err != nil {
				t.Error(err)
			}
		})
		t.Run(transport, func(t *testing.T) {
			if transport == "pgx" {
				d := hopperpgx.NewWithConfig(pool, &hopperpgx.Config{Schema: schema})
				if _, err := hoppermigrate.Up(ctx, d, opts); err != nil {
					t.Fatal(err)
				}
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				inserted, err := d.UnwrapTx(tx).JobInsertCopy(ctx, []driver.JobInsertParams{{Kind: "rollback", Queue: "default", Priority: 2, MaxAttempts: 3}}, driver.JobInsertOpts{})
				if err != nil {
					t.Fatal(err)
				}
				if err := tx.Rollback(ctx); err != nil {
					t.Fatal(err)
				}
				if _, err := d.Executor().JobGet(ctx, inserted[0].Job.ID); !errors.Is(err, driver.ErrNotFound) {
					t.Fatalf("rolled back COPY: %v", err)
				}
				verifySchema(t, ctx, d.Executor())
			} else {
				d := hoppersql.NewWithConfig(db, &hoppersql.Config{Schema: schema})
				if _, err := hoppermigrate.Up(ctx, d, opts); err != nil {
					t.Fatal(err)
				}
				tx, err := db.BeginTx(ctx, &sql.TxOptions{})
				if err != nil {
					t.Fatal(err)
				}
				inserted, err := d.UnwrapTx(tx).JobInsertMany(ctx, []driver.JobInsertParams{{Kind: "rollback", Queue: "default", Priority: 2, MaxAttempts: 3}}, driver.JobInsertOpts{})
				if err != nil {
					t.Fatal(err)
				}
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
				if _, err := d.Executor().JobGet(ctx, inserted[0].Job.ID); !errors.Is(err, driver.ErrNotFound) {
					t.Fatalf("rolled back insert: %v", err)
				}
				verifySchema(t, ctx, d.Executor())
			}
			var id driver.JobID
			// The pool still uses the application namespace, including for SQL producers.
			if err := pool.QueryRow(ctx, "SELECT "+pgx.Identifier{schema, "hopper_insert"}.Sanitize()+"('external', '{}')").Scan(&id); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, "SELECT "+pgx.Identifier{schema, "hopper_publish"}.Sanitize()+"('topic', '{}')"); err != nil {
				t.Fatal(err)
			}
			var current string
			if err := pool.QueryRow(ctx, "SELECT current_schema()").Scan(&current); err != nil {
				t.Fatal(err)
			}
			if current != appSchema {
				t.Fatalf("application path changed to %q", current)
			}
			if err := db.QueryRowContext(ctx, "SELECT current_schema()").Scan(&current); err != nil {
				t.Fatal(err)
			}
			if current != appSchema {
				t.Fatalf("sql application path changed to %q", current)
			}
		})
	}
	// Two drivers using the same physical connections see separate queues.
	for i, schema := range schemas {
		d := hopperpgx.NewWithConfig(pool, &hopperpgx.Config{Schema: schema})
		if err := d.Executor().QueueEnsure(ctx, []string{fmt.Sprintf("only_%d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	for i, schema := range schemas {
		d := hopperpgx.NewWithConfig(pool, &hopperpgx.Config{Schema: schema})
		queues, err := d.Executor().QueueList(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range queues {
			if q.Name == fmt.Sprintf("only_%d", 1-i) {
				t.Fatal("queue leaked across schemas")
			}
		}
	}
}

// Concurrent claims and fenced finalization exercise the qualified state
// transitions while the underlying pool uses a different namespace.
func verifySchema(t *testing.T, ctx context.Context, e driver.Executor) {
	t.Helper()
	var clients [2]int64
	for i := range clients {
		id, err := e.ClientRegister(ctx, driver.ClientRegisterParams{Hostname: "schema-test", TTL: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = id
	}
	// Each claim asks for a whole claim bucket. A claim locks its bucket's
	// worth of rows and releases the ones beyond its count only when its
	// statement ends, so two concurrent claims of a smaller count could
	// leave jobs unclaimed.
	const perClient = 16
	params := make([]driver.JobInsertParams, len(clients)*perClient)
	for i := range params {
		params[i] = driver.JobInsertParams{Kind: "schema", Queue: "default", Priority: 2, MaxAttempts: 3}
	}
	if _, err := e.JobInsertMany(ctx, params, driver.JobInsertOpts{}); err != nil {
		t.Fatal(err)
	}
	var claims [2]driver.JobClaimResult
	var errs [2]error
	var wg sync.WaitGroup
	for i := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claims[i], errs[i] = e.JobClaim(ctx, driver.JobClaimParams{Queue: "default", ClientID: clients[i], Limit: perClient})
		}()
	}
	wg.Wait()
	seen := map[driver.JobID]bool{}
	for i, claim := range claims {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		for _, j := range claim.Jobs {
			if seen[j.ID] {
				t.Fatal("job claimed twice")
			}
			seen[j.ID] = true
			p := driver.JobFinalizeParams{Jobs: []driver.JobFinalize{{ID: j.ID, AttemptedBy: clients[1-i], State: driver.JobStateCompleted, Archive: true}}}
			ids, err := e.JobFinalizeMany(ctx, p)
			if err != nil {
				t.Fatal(err)
			}
			if len(ids) != 0 {
				t.Fatal("foreign owner finalized job")
			}
			p.Jobs[0].AttemptedBy = clients[i]
			ids, err = e.JobFinalizeMany(ctx, p)
			if err != nil {
				t.Fatal(err)
			}
			if len(ids) != 1 {
				t.Fatal("owner could not finalize")
			}
		}
	}
	if len(seen) != len(params) {
		t.Fatalf("claimed %d of %d jobs", len(seen), len(params))
	}
	if _, err := e.HistoryMaintain(ctx, driver.HistoryMaintainParams{CompletedRetention: time.Hour, FailedRetention: time.Hour, StreamRetention: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.JobsMaintain(ctx); err != nil {
		t.Fatal(err)
	}
}
