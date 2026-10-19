package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parallelworks/hopper"
	"github.com/parallelworks/hopper/hoppermigrate"
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
		if err := run(ctx, append([]string{"-schema", schema}, args...), &out); err != nil {
			t.Fatalf("hopper %s: %v\n%s", strings.Join(args, " "), err, out.String())
		}
		return out.String()
	}

	if got := hopperCmd("migrate", "version"); !strings.Contains(got, "schema version 0") {
		t.Errorf("version before migrate = %q", got)
	}
	latest := fmt.Sprintf("schema version %d", hoppermigrate.Latest())
	if got := hopperCmd("migrate", "up"); !strings.Contains(got, latest) {
		t.Errorf("migrate up = %q", got)
	}
	var version map[string]int
	if err := json.Unmarshal([]byte(hopperCmd("-json", "migrate", "version")), &version); err != nil || version["version"] != hoppermigrate.Latest() {
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
	keyed, err := client.Insert(ctx, ping{}, &hopper.InsertOpts{Queue: "q1", OrderingKey: "lane"})
	if err != nil {
		t.Fatal(err)
	}
	if got := hopperCmd("jobs", "list", "-queue", "q1", "-ordering-key", "lane"); !strings.Contains(got, keyed.Job.ID.String()) || strings.Contains(got, id) {
		t.Errorf("jobs list by ordering key = %q", got)
	}
	newer, older := keyed.Job.ID.String(), id
	if newer < older {
		newer, older = older, newer
	}
	if got := hopperCmd("jobs", "list", "-newest", "-limit", "1"); !strings.Contains(got, newer) || strings.Contains(got, older) {
		t.Errorf("newest jobs list = %q", got)
	}
	tagged, err := client.Insert(ctx, ping{}, &hopper.InsertOpts{
		Queue: "q2", Priority: hopper.PriorityLow, Tags: []string{"nightly", "tenant:acme"},
		Metadata: json.RawMessage(`{"tenant": "acme", "region": "eu"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	taggedID := tagged.Job.ID.String()
	for _, args := range [][]string{
		{"-tag", "nightly", "-tag", "tenant:acme"},
		{"-any-tag", "other", "-any-tag", "nightly"},
		{"-metadata", `{"tenant": "acme"}`},
		{"-priority", "3"},
		{"-id", taggedID, "-id", keyed.Job.ID.String()},
	} {
		got := hopperCmd(append([]string{"jobs", "list"}, args...)...)
		if !strings.Contains(got, taggedID) || !strings.Contains(got, "nightly,tenant:acme") || strings.Contains(got, id) {
			t.Errorf("jobs list %s = %q", strings.Join(args, " "), got)
		}
	}
	if got := hopperCmd("jobs", "list", "-tag", "nightly", "-tag", "other"); strings.Contains(got, taggedID) {
		t.Errorf("jobs list with a tag it lacks = %q", got)
	}
	remote, err := client.Insert(ctx, ping{}, &hopper.InsertOpts{Queue: "remote"})
	if err != nil {
		t.Fatal(err)
	}
	leased, err := client.Leases().Claim(ctx, remote.Job.ID, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := hopperCmd("jobs", "get", remote.Job.ID.String()); !strings.Contains(got, "(job lease, fence 1, expires "+leased.ExpiresAt.Local().Format(time.RFC3339)+")") {
		t.Errorf("jobs get of a leased job = %q", got)
	}
	if got := hopperCmd("jobs", "get", taggedID); !strings.Contains(got, `tags:          ["nightly" "tenant:acme"]`) {
		t.Errorf("jobs get of a tagged job = %q", got)
	}
	var badOut bytes.Buffer
	if err := run(ctx, []string{"-schema", schema, "jobs", "list", "-metadata", "[1]"}, &badOut); err == nil || !strings.Contains(err.Error(), "JSON object") {
		t.Errorf("jobs list with metadata that is not an object: %v", err)
	}
	if _, err := client.JobCancel(ctx, keyed.Job.ID); err != nil {
		t.Fatal(err)
	}
	if got := hopperCmd("jobs", "get", id); !strings.Contains(got, "state:         available") {
		t.Errorf("jobs get = %q", got)
	}
	if _, err := pool.Exec(ctx, `UPDATE hopper_jobs SET progress = '{"step": "fetch", "cursor": 3, "at": "2026-01-02T03:04:05Z"}' WHERE id = $1::uuid`, id); err != nil {
		t.Fatal(err)
	}
	if got := hopperCmd("jobs", "get", id); !strings.Contains(got, `progress:      step "fetch", cursor 3 (`) {
		t.Errorf("jobs get with progress = %q", got)
	}
	// A log written by hand may not end in a newline.
	if _, err := pool.Exec(ctx, `UPDATE hopper_jobs SET logs = '[{"attempt": 1, "log": "level=INFO msg=one\nlevel=WARN msg=two\n"}, {"attempt": 2, "log": "no newline"}]' WHERE id = $1::uuid`, id); err != nil {
		t.Fatal(err)
	}
	if got := hopperCmd("jobs", "get", id); !strings.Contains(got, "log #1:\n  level=INFO msg=one\n  level=WARN msg=two\nlog #2:\n  no newline\n") {
		t.Errorf("jobs get with logs = %q", got)
	}
	// A job whose logs cannot be read, here without a logs column, is still
	// shown, with why its logs are missing, and the command fails.
	if _, err := pool.Exec(ctx, "ALTER TABLE hopper_jobs RENAME COLUMN logs TO hidden_logs"); err != nil {
		t.Fatal(err)
	}
	var getOut bytes.Buffer
	err = run(ctx, []string{"-schema", schema, "jobs", "get", id}, &getOut)
	if got := getOut.String(); err == nil || !strings.Contains(got, "state:         available") || !strings.Contains(got, "logs:          unavailable: ") {
		t.Errorf("jobs get without a logs column = %v, %q", err, got)
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE hopper_jobs RENAME COLUMN hidden_logs TO logs"); err != nil {
		t.Fatal(err)
	}
	if got := hopperCmd("jobs", "retry", id); !strings.Contains(got, "progress:") {
		t.Errorf("jobs retry dropped the progress: %q", got)
	}
	if got := hopperCmd("jobs", "retry", "-from-start", id); strings.Contains(got, "progress:") || !strings.Contains(got, "state:         available") {
		t.Errorf("jobs retry -from-start = %q", got)
	}
	if got := hopperCmd("jobs", "cancel", id); !strings.Contains(got, "state:         canceled") {
		t.Errorf("jobs cancel = %q", got)
	}
	if got := hopperCmd("jobs", "retry", id); !strings.Contains(got, "state:         available") {
		t.Errorf("jobs retry = %q", got)
	}
	var jobs []*hopper.JobRow
	if err := json.Unmarshal([]byte(hopperCmd("-json", "jobs", "list", "-queue", "q1", "-state", "available")), &jobs); err != nil || len(jobs) != 1 {
		t.Errorf("json jobs = %v, %v", jobs, err)
	}
	if got := hopperCmd("jobs", "delete", keyed.Job.ID.String()); !strings.Contains(got, "state:         canceled") {
		t.Errorf("jobs delete = %q", got)
	}
	if got := hopperCmd("jobs", "list"); strings.Contains(got, keyed.Job.ID.String()) {
		t.Errorf("jobs list after delete = %q", got)
	}

	hopperCmd("queues", "pause", "q1")
	if got := hopperCmd("queues", "list"); !strings.Contains(got, "q1") || !strings.Contains(got, "since") {
		t.Errorf("queues list = %q", got)
	}
	hopperCmd("queues", "resume", "q1")
	if got := hopperCmd("queues", "list"); strings.Contains(got, "since") {
		t.Errorf("queues list after resume = %q", got)
	}
	hopperCmd("queues", "limit", "q1", "-global", "5", "-rate", "2.5", "-aging", "1m")
	if got := hopperCmd("queues", "list"); !strings.Contains(got, "5") || !strings.Contains(got, "2.5") || !strings.Contains(got, "1m0s") {
		t.Errorf("queues list after limit = %q", got)
	}
	hopperCmd("queues", "limit", "q1")
	if got := hopperCmd("queues", "list"); strings.Contains(got, "2.5") {
		t.Errorf("queues list after clearing limits = %q", got)
	}
	if got := hopperCmd("clients", "list"); !strings.Contains(got, "ID") {
		t.Errorf("clients list = %q", got)
	}
	if got := hopperCmd("stats"); !strings.Contains(got, "q1") {
		t.Errorf("stats = %q", got)
	}
	if got := hopperCmd("subscriptions", "list"); !strings.Contains(got, "NAME") {
		t.Errorf("subscriptions list = %q", got)
	}
	if got := hopperCmd("streams", "consumers"); !strings.Contains(got, "NAME") {
		t.Errorf("streams consumers = %q", got)
	}
	if err := run(ctx, []string{"streams", "seek", "nobody", "-earliest"}, new(bytes.Buffer)); err == nil {
		t.Error("seek of an unknown consumer succeeded")
	}
	wf := hopper.NewWorkflow("ingest", nil)
	first := wf.Add(ping{}, nil)
	wf.Add(ping{}, hopper.After(first))
	wres, err := client.InsertWorkflow(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	if got := hopperCmd("workflows", "get", wres.ID.String()); !strings.Contains(got, "name:      ingest") ||
		!strings.Contains(got, "0 of 2 finished") || !strings.Contains(got, "pending") || !strings.Contains(got, wres.Jobs[0].Job.ID.String()) {
		t.Errorf("workflows get = %q", got)
	}
	// A downgrade stops at its target, and an upgrade applies the rest.
	previous := fmt.Sprintf("schema version %d", hoppermigrate.Latest()-1)
	if got := hopperCmd("migrate", "down", "-target", strconv.Itoa(hoppermigrate.Latest()-1)); !strings.Contains(got, previous) {
		t.Errorf("migrate down -target = %q, want %q", got, previous)
	}
	if got := hopperCmd("migrate", "up"); !strings.Contains(got, latest) {
		t.Errorf("migrate up after a partial downgrade = %q", got)
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

func TestTagsColumn(t *testing.T) {
	for _, c := range []struct {
		tags []string
		want string
	}{
		{nil, ""},
		{[]string{"nightly", "tenant:acme", "with space"}, "nightly,tenant:acme,with space"},
		{[]string{"a,b", "c"}, `"a,b",c`},
		{[]string{`say "hi"`}, `"say \"hi\""`},
		{[]string{"evil\u202ename"}, `"evil\u202ename"`},
	} {
		if got := tagsColumn(c.tags); got != c.want {
			t.Errorf("tagsColumn(%q) = %s, want %s", c.tags, got, c.want)
		}
	}
}
