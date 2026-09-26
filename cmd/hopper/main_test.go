package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/internal/testdb"
)

type ping struct{}

func (ping) Kind() string { return "ping" }

func TestCLI(t *testing.T) {
	ctx := context.Background()
	pool := testdb.EmptyPool(t, 0)
	// The pool's search_path is the test schema; give the CLI a URL that
	// lands in the same schema.
	var schema string
	if err := pool.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	url := testdb.URL(t)
	sep := "?"
	if strings.Contains(url, "?") {
		sep = "&"
	}
	url += sep + "search_path=" + schema
	t.Setenv("HOPPER_DATABASE_URL", url)

	hopperCmd := func(args ...string) string {
		t.Helper()
		var out bytes.Buffer
		if err := run(ctx, args, &out); err != nil {
			t.Fatalf("hopper %s: %v\n%s", strings.Join(args, " "), err, out.String())
		}
		return out.String()
	}

	if got := hopperCmd("migrate", "version"); !strings.Contains(got, "schema version 0") {
		t.Errorf("version before migrate = %q", got)
	}
	if got := hopperCmd("migrate", "up"); !strings.Contains(got, "schema version 1") {
		t.Errorf("migrate up = %q", got)
	}
	var version map[string]int
	if err := json.Unmarshal([]byte(hopperCmd("-json", "migrate", "version")), &version); err != nil || version["version"] != 1 {
		t.Errorf("json version = %v, %v", version, err)
	}

	// Seed a job through the library, then drive it with the CLI.
	client, err := hopper.NewClient(newDriver(t, url), nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.Insert(ctx, ping{}, &hopper.InsertOpts{Queue: "q1"})
	if err != nil {
		t.Fatal(err)
	}
	id := res.Job.ID.String()

	if got := hopperCmd("jobs", "list"); !strings.Contains(got, id) || !strings.Contains(got, "ping") {
		t.Errorf("jobs list = %q", got)
	}
	if got := hopperCmd("jobs", "list", "-queue", "other"); strings.Contains(got, id) {
		t.Errorf("filtered jobs list = %q", got)
	}
	if got := hopperCmd("jobs", "get", id); !strings.Contains(got, "state:         available") {
		t.Errorf("jobs get = %q", got)
	}
	if got := hopperCmd("jobs", "cancel", id); !strings.Contains(got, "state:         cancelled") {
		t.Errorf("jobs cancel = %q", got)
	}
	if got := hopperCmd("jobs", "retry", id); !strings.Contains(got, "state:         available") {
		t.Errorf("jobs retry = %q", got)
	}
	var jobs []*hopper.JobRow
	if err := json.Unmarshal([]byte(hopperCmd("-json", "jobs", "list", "-state", "available")), &jobs); err != nil || len(jobs) != 1 {
		t.Errorf("json jobs = %v, %v", jobs, err)
	}

	hopperCmd("queues", "pause", "q1")
	if got := hopperCmd("queues", "list"); !strings.Contains(got, "q1") || !strings.Contains(got, "since") {
		t.Errorf("queues list = %q", got)
	}
	hopperCmd("queues", "resume", "q1")
	if got := hopperCmd("queues", "list"); strings.Contains(got, "since") {
		t.Errorf("queues list after resume = %q", got)
	}
	if got := hopperCmd("clients", "list"); !strings.Contains(got, "ID") {
		t.Errorf("clients list = %q", got)
	}
	if got := hopperCmd("stats"); !strings.Contains(got, "q1") {
		t.Errorf("stats = %q", got)
	}
	if got := hopperCmd("migrate", "down"); !strings.Contains(got, "schema version 0") {
		t.Errorf("migrate down = %q", got)
	}

	var out bytes.Buffer
	if err := run(ctx, []string{"nonsense"}, &out); err == nil {
		t.Error("unknown command succeeded")
	}
	if err := run(ctx, []string{"jobs", "get", "not-an-id"}, &out); err == nil {
		t.Error("bad job ID succeeded")
	}
}
